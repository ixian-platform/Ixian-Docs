import requests
import paho.mqtt.client as mqtt
import json
import time
import threading

QUIXI_API = "http://127.0.0.1:8001"

# ============================================================
# RECEIVE FLOW
# ============================================================

def generate_deposit_address(user_tag_hex):
    """
    Generate a unique extended address for a user deposit.

    Args:
        user_tag_hex: Hex-encoded tag (max 16 bytes) identifying the user/order.
                      Example: "75736572313233" (hex of "user123")
    Returns:
        The extended address string, or None on error.
    """
    response = requests.get(f"{QUIXI_API}/extendAddress", params={
        "flag": "1",        # E2E — best for online services
        "tag": user_tag_hex
    })
    data = response.json()
    if data.get("error"):
        print(f"Error generating address: {data['error']}")
        return None
    address = data["result"]
    print(f"Generated deposit address: {address}")
    return address


def poll_incoming_payments(last_from_id=None):
    """
    Poll /activity2 for recent incoming transactions.

    Args:
        last_from_id: ID of the last seen activity (for pagination).
    Returns:
        List of activity objects.
    """
    params = {
        "type": "100",         # 100 = TransactionReceived
        "count": "20",
        "descending": "false"
    }
    if last_from_id:
        params["fromId"] = last_from_id

    response = requests.get(f"{QUIXI_API}/activity2", params=params)
    data = response.json()
    activities = data.get("result", [])

    for activity in activities:
        tx_id = activity.get("id")
        value = activity.get("value")
        status = activity.get("status")
        to_list = activity.get("toAddressList", {})
        print(f"  TX {tx_id}: {value} IXI (status={status})")
        # Match 'to_list' addresses/tags against your user database
    return activities


def wait_for_confirmation(tx_id, timeout=600, interval=10):
    """
    Poll /getActivity until the transaction reaches a final status.

    Args:
        tx_id: transaction ID.
        timeout: Maximum seconds to wait.
        interval: Seconds between polls.
    Returns:
        True if confirmed (Final), False otherwise.
    """
    elapsed = 0
    while elapsed < timeout:
        response = requests.get(f"{QUIXI_API}/getActivity", params={
            "id": tx_id
        })
        data = response.json()
        activity = data.get("result")
        if activity:
            status = activity.get("status")
            if status == 2:  # Final
                print(f"Transaction confirmed at block {activity.get('appliedBlockHeight')}")
                return True
            elif status in (3, 4, 5):  # Expired, Reverted, Rejected
                print(f"Transaction failed with status {status}")
                return False
        time.sleep(interval)
        elapsed += interval
    print("Timeout waiting for confirmation")
    return False


# ============================================================
# SEND FLOW
# ============================================================

def send_payment(to_extended_address, amount):
    """
    Send IXI to a user's extended address.

    Args:
        to_extended_address: The recipient's extended address string.
        amount: Amount of IXI to send (as string or number).
    Returns:
        Transaction result dict, or None on error.
    """
    # Step 1: Resolve the extended address
    resolve_resp = requests.get(f"{QUIXI_API}/resolveExtendedAddress", params={
        "extendedAddress": to_extended_address,
        "amount": str(amount)
    })
    resolve_data = resolve_resp.json()
    if resolve_data.get("error"):
        print(f"Error resolving address: {resolve_data['error']}")
        return None

    resolved_address = resolve_data["result"]
    print(f"Address resolved: {resolved_address}")

    # Step 2: Send the transaction
    # Format: resolvedAddress_amount
    to_param = f"{resolved_address}_{amount}"
    tx_resp = requests.get(f"{QUIXI_API}/addTransaction", params={
        "to": to_param,
        "autofee": "true"
    })
    tx_data = tx_resp.json()
    if tx_data.get("error"):
        print(f"Error sending transaction: {tx_data['error']}")
        return None

    result = tx_data["result"]
    print(f"Transaction sent! ID: {result.get('id')}")
    return result


# ============================================================
# MQTT LISTENER (Real-Time Payment Events)
# ============================================================

def start_mqtt_listener(broker="localhost", port=1883):
    """
    Start an MQTT listener for real-time payment events.
    Subscribes to Transaction and TransactionStatusUpdate topics.
    """
    def on_connect(client, userdata, flags, rc):
        print(f"Connected to MQTT broker (rc={rc})")
        client.subscribe("Transaction/#")
        client.subscribe("TransactionStatusUpdate/#")
        print("Subscribed to Transaction, TransactionStatusUpdate")

    def on_message(client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
        except Exception:
            payload = msg.payload.decode()

        if msg.topic == "Transaction":
            print(f"\n[MQ] New transaction detected:")
            print(f"     {json.dumps(payload, indent=2, default=str)}")
            # Extract toList and match against your user database

        elif msg.topic == "TransactionStatusUpdate":
            print(f"\n[MQ] Transaction status update:")
            for tx_id, status in payload.items():
                print(f"     TX {tx_id} -> {status}")
                if status == "verified":
                    print("     -> Credit user account")
                elif status in ("rejected", "expired"):
                    print("     -> Do NOT credit")
                elif status == "reverted":
                    print("     -> Reverse any credit")

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker, port, 60)

    # Run in a background thread
    thread = threading.Thread(target=client.loop_forever, daemon=True)
    thread.start()
    return client


# ============================================================
# USAGE
# ============================================================

if __name__ == "__main__":
    # Start real-time listener
    mqtt_client = start_mqtt_listener()
    print("MQTT listener started in background\n")

    # Generate a deposit address for user "user123"
    user_tag = "75736572313233"  # hex("user123")
    deposit_address = generate_deposit_address(user_tag)

    # Poll for payments (alternative to MQTT)
    print("\nPolling for incoming payments...")
    poll_incoming_payments()

    # Send a payment (example - uncomment to use)
    # send_payment("4nHMRz...recipient_address..._2EfGh...", 100)

    # Keep main thread alive for MQTT
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
