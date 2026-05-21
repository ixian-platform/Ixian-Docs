using IXICore;
using IXICore.Meta;
using System;
using System.Threading;

namespace IxianClient
{
    class Program
    {
        static Node? node = null;

        static void Main(string[] args)
        {
            Console.WriteLine("Ixian Client Example");
            Console.WriteLine("====================\n");

            Logging.start(AppDomain.CurrentDomain.BaseDirectory, (int)(LogSeverity.warn | LogSeverity.error));
            Logging.consoleOutput = true;
            
            Console.CancelKeyPress += (sender, e) => {
                e.Cancel = true;
                IxianHandler.requestShutdown();
            };

            try
            {
                node = new Node();

                if (IxianHandler.forceShutdown)
                {
                    return;
                }

                node.Start();
                                
                // Display initial state
                DisplayStatus();
                
                // Interactive menu
                RunMenu();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Fatal error: {e}");
            }
            finally
            {
                node?.Stop();
                Logging.stop();
            }
        }

        static void DisplayStatus()
        {
            var myAddress = IxianHandler.getWalletStorage().getPrimaryAddress();
            var balance = IxianHandler.getWalletBalance(myAddress);
            var blockHeight = IxianHandler.getHighestKnownNetworkBlockHeight();
            
            Console.WriteLine($"\nWallet Address: {myAddress}");
            Console.WriteLine($"Balance: {balance} IXI");
            Console.WriteLine($"Block Height: {blockHeight}\n");
        }

        static void RunMenu()
        {
            while (!IxianHandler.forceShutdown)
            {
                Console.WriteLine("\n===========================================");
                Console.WriteLine("Commands:");
                Console.WriteLine("  1 - Show status");
                Console.WriteLine("  2 - Send payment");
                Console.WriteLine("  3 - Check balance");
                Console.WriteLine("  4 - Check presence");
                Console.WriteLine("  5 - Check transaction status");
                Console.WriteLine("  6 - Request balance update");
                Console.WriteLine("  7 - Exit");
                Console.WriteLine("===========================================");
                Console.Write("Choice: ");

                var choice = Console.ReadLine();

                switch (choice)
                {
                    case "1":
                        DisplayStatus();
                        break;

                    case "2":
                        Console.Write("Recipient address: ");
                        var recipient = Console.ReadLine();
                        Console.Write("Amount (IXI): ");
                        var amount = Console.ReadLine();

                        if (!string.IsNullOrEmpty(recipient) && !string.IsNullOrEmpty(amount))
                        {
                            bool success = node?.SendPayment(recipient, amount) ?? false;
                            if (success)
                            {
                                Console.WriteLine("\n✓ Payment sent successfully!");
                                Console.WriteLine("  The transaction is now pending. Check status with option 5.");
                            }
                            else
                            {
                                Console.WriteLine("\n✗ Payment failed. Check the error message above.");
                            }
                        }
                        break;

                    case "3":
                        Console.Write("Your Address (or blank for your primary): ");
                        var addr = Console.ReadLine();

                        if (string.IsNullOrEmpty(addr))
                        {
                            var myBalance = node?.GetMyBalance() ?? new IxiNumber(0);
                            Console.WriteLine($"\nYour balance (cached): {myBalance} IXI");
                        }
                        else
                        {
                            var balance = node?.GetBalance(addr) ?? new IxiNumber(0);
                            Console.WriteLine($"\nYour balance (cached): {balance} IXI");
                        }
                        Console.WriteLine("Note: This is the cached value. Use option 6 to request fresh data from network.");
                        break;

                    case "4":
                        Console.Write("Address to check: ");
                        var presenceAddr = Console.ReadLine();

                        if (!string.IsNullOrEmpty(presenceAddr))
                        {
                            try
                            {
                                // First check cached presence
                                var isOnline = node?.IsAddressOnline(presenceAddr) ?? false;

                                if (!isOnline)
                                {
                                    Console.WriteLine("\nAddress not found in local cache. Requesting sectors from network...");
                                    node?.RequestSector(presenceAddr);
                                    Console.WriteLine("Waiting 2 seconds for network response...");
                                    Thread.Sleep(2000);

                                    Console.WriteLine("\nRequesting Presence from network...");
                                    node?.RequestPresence(presenceAddr);
                                    Console.WriteLine("Waiting 2 seconds for network response...");
                                    Thread.Sleep(2000);
                                }

                                // Display presence information
                                Console.WriteLine($"\nPresence information for {presenceAddr}:");
                                node?.DisplayPresenceInfo(presenceAddr);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Error: {ex.Message}");
                            }
                        }
                        break;

                    case "5":
                        Console.Write("Transaction ID: ");
                        var txid = Console.ReadLine();

                        if (!string.IsNullOrEmpty(txid))
                        {
                            try
                            {
                                CoreProtocolMessage.broadcastGetTransaction(Transaction.txIdLegacyToV8(txid), 0);
                                Console.WriteLine("Waiting 2 seconds for network response...");
                                Thread.Sleep(2000);
                                var status = node?.GetTransactionStatus(txid) ?? "Unknown";
                                Console.WriteLine($"\nTransaction status: {status}");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Error: {ex.Message}");
                            }
                        }
                        break;

                    case "6":
                        Console.Write("Address (or blank for your primary address): ");
                        var balAddr = Console.ReadLine();

                        try
                        {
                            Address addrToUpdate;
                            if (string.IsNullOrEmpty(balAddr))
                            {
                                addrToUpdate = IxianHandler.getWalletStorage().getPrimaryAddress();
                            }
                            else
                            {
                                addrToUpdate = new Address(balAddr);
                            }

                            Console.WriteLine($"\nRequesting balance update for {addrToUpdate}...");

                            node?.RequestBalanceUpdate(addrToUpdate);
                            Console.WriteLine("Request sent. Balance will update automatically in a few seconds.");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error: {ex.Message}");
                        }
                        break;

                    case "7":
                        Console.WriteLine("\nShutting down...");
                        IxianHandler.requestShutdown();
                        break;

                    default:
                        Console.WriteLine("Invalid choice. Please select 1-7.");
                        break;
                }
            }
        }
    }
}
