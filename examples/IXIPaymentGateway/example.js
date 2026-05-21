const axios = require('axios');
const mqtt = require('mqtt');

const QUIXI_API = 'http://127.0.0.1:8001';

// ============================================================
// RECEIVE FLOW
// ============================================================

async function generateDepositAddress(userTagHex) {
    /**
     * Generate a unique extended address for a user deposit.
     * @param {string} userTagHex - Hex-encoded tag (max 16 bytes)
     */
    try {
        const { data } = await axios.get(`${QUIXI_API}/extendAddress`, {
            params: { flag: '1', tag: userTagHex }
        });
        if (data.error) throw new Error(data.error.message);
        console.log(`Generated deposit address: ${data.result}`);
        return data.result;
    } catch (error) {
        console.error('Error generating address:', error.message);
        return null;
    }
}

async function pollIncomingPayments(lastFromId = null) {
    /**
     * Poll /activity2 for recent incoming transactions.
     */
    const params = { type: '100', count: '20', descending: 'false' };
    if (lastFromId) params.fromId = lastFromId;

    try {
        const { data } = await axios.get(`${QUIXI_API}/activity2`, { params });
        const activities = data.result || [];

        for (const activity of activities) {
            console.log(`  TX ${activity.id}: ${activity.value} IXI (status=${activity.status})`);
            // Match toAddressList against your user database
        }
        return activities;
    } catch (error) {
        console.error('Error polling activities:', error.message);
        return [];
    }
}

async function waitForConfirmation(txId, timeoutMs = 600000, intervalMs = 10000) {
    /**
     * Poll /getActivity until the transaction reaches a final status.
     */
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        try {
            const { data } = await axios.get(`${QUIXI_API}/getActivity`, {
                params: { id: txId }
            });
            const activity = data.result;
            if (activity) {
                if (activity.status === 2) { // Final
                    console.log(`Transaction confirmed at block ${activity.appliedBlockHeight}`);
                    return true;
                } else if ([3, 4, 5].includes(activity.status)) {
                    console.log(`Transaction failed with status ${activity.status}`);
                    return false;
                }
            }
        } catch (error) {
            console.error('Error checking status:', error.message);
        }
        await new Promise(resolve => setTimeout(resolve, intervalMs));
    }
    console.log('Timeout waiting for confirmation');
    return false;
}

// ============================================================
// SEND FLOW
// ============================================================

async function sendPayment(toExtendedAddress, amount) {
    /**
     * Send IXI to a user's extended address.
     */
    try {
        // Step 1: Resolve the extended address
        const resolveResp = await axios.get(`${QUIXI_API}/resolveExtendedAddress`, {
            params: { extendedAddress: toExtendedAddress, amount: String(amount) }
        });
        if (resolveResp.data.error) throw new Error(resolveResp.data.error.message);

        const resolvedAddress = resolveResp.data.result;
        console.log(`Address resolved: ${resolvedAddress}`);

        // Step 2: Send the transaction (format: resolvedAddress_amount)
        const toParam = `${resolvedAddress}_${amount}`;
        const txResp = await axios.get(`${QUIXI_API}/addTransaction`, {
            params: { to: toParam, autofee: 'true' }
        });
        if (txResp.data.error) throw new Error(txResp.data.error.message);

        console.log(`Transaction sent! ID: ${txResp.data.result.id}`);
        return txResp.data.result;
    } catch (error) {
        console.error('Error sending payment:', error.message);
        return null;
    }
}

// ============================================================
// MQTT LISTENER (Real-Time Payment Events)
// ============================================================

function startMqttListener(brokerUrl = 'mqtt://localhost:1883') {
    const client = mqtt.connect(brokerUrl);

    client.on('connect', () => {
        console.log('Connected to MQTT broker');
        client.subscribe(['Transaction', 'TransactionStatusUpdate']);
        console.log('Subscribed to Transaction, TransactionStatusUpdate');
    });

    client.on('message', (topic, payload) => {
        let message;
        try {
            message = JSON.parse(payload.toString());
        } catch {
            message = payload.toString();
        }

        if (topic === 'Transaction') {
            console.log('\n[MQ] New transaction detected:');
            console.log('    ', JSON.stringify(message, null, 2));
            // Extract toList and match against your user database

        } else if (topic === 'TransactionStatusUpdate') {
            console.log('\n[MQ] Transaction status update:');
            for (const [txId, status] of Object.entries(message)) {
                console.log(`     TX ${txId} -> ${status}`);
                if (status === 'verified') {
                    console.log('     -> Credit user account');
                } else if (['rejected', 'expired'].includes(status)) {
                    console.log('     -> Do NOT credit');
                } else if (status === 'reverted') {
                    console.log('     -> Reverse any credit');
                }
            }
        }
    });

    return client;
}

// ============================================================
// USAGE
// ============================================================

(async () => {
    // Start real-time listener
    const mqttClient = startMqttListener();
    console.log('MQTT listener started\n');

    // Generate a deposit address for user "user123"
    const userTag = '75736572313233'; // hex("user123")
    const depositAddress = await generateDepositAddress(userTag);

    // Poll for payments (alternative to MQTT)
    console.log('\nPolling for incoming payments...');
    await pollIncomingPayments();

    // Send a payment (example - uncomment to use)
    // await sendPayment('4nHMRz...recipient_address..._2EfGh...', 100);
})();
