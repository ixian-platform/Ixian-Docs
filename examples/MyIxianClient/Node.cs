using IXICore;
using IXICore.Activity;
using IXICore.Inventory;
using IXICore.Meta;
using IXICore.Network;
using IXICore.Network.Messages;
using IXICore.RegNames;
using IXICore.Storage;
using IXICore.Streaming;
using IXICore.Utils;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using static IXICore.Transaction;

namespace IxianClient
{
    public class Node : IxianNode
    {
        private bool running = false;
        private Thread? mainLoopThread = null;

        private TransactionInclusion? tiv = null;

        private CoreStreamProcessor? streamProcessor = null;

        private long lastSectorUpdate = 0;

        private NetworkClientManagerStatic? networkClientManagerStatic = null;

        public static IActivityStorage? activityStorage = null;

        private IStorage? blockStorage = null;

        public Node()
        {
            Init();
        }

        private void Init()
        {
            // Initialize IxianHandler with your app version
            IxianHandler.init("MyClient v1.0.0", this, NetworkType.main);

            // Initialize wallet
            if (!InitWallet())
            {
                Console.WriteLine("Failed to initialize wallet. Shutting down.");
                IxianHandler.requestShutdown();
                return;
            }

            // Disable console output
            Logging.consoleOutput = false;

            // Initialize block header storage
            blockStorage = new RocksDBStorage("headers", 0, CoreConfig.maxBlockHeadersPerDatabase, 3, RocksDBOptimizations.Mobiles, 0);

            // Initialize activity/transaction storage
            activityStorage = new ActivityStorage("activity", 0, 0, RocksDBOptimizations.Mobiles, 0);

            // Initialize peer storage
            PeerStorage.init("");

            // Setup network client manager (queries blockchain via S2)
            networkClientManagerStatic = new NetworkClientManagerStatic(10);
            NetworkClientManager.init(networkClientManagerStatic);
            StreamClientManager.init(6, true);

            // Prepare the stream processor
            streamProcessor = new CoreStreamProcessor(new ICPendingMessageProcessor("", false), StreamCapabilities.Incoming);

            // Init TIV
            tiv = new TransactionInclusion(blockStorage, new ICTransactionInclusionCallbacks(), TIVBlockVerificationMode.Minimal);

            // Initialize presence list with keepalive
            PresenceList.init("", 0, 'C', CoreConfig.clientKeepAliveInterval);

            IxianHandler.localStorage = new LocalStorage("", new ICLocalStorageCallbacks());

            InventoryCache.init(new InventoryCacheClient(tiv));

            RelaySectors.init(CoreConfig.relaySectorLevels, null);

            Console.WriteLine($"Node initialized. Wallet: {IxianHandler.getWalletStorage().getPrimaryAddress()}");
        }

        private bool InitWallet()
        {
            string walletPath = "wallet.ixi";
            WalletStorage walletStorage = new WalletStorage(walletPath);

            Logging.flush();

            if (!walletStorage.walletExists())
            {
                ConsoleHelpers.displayBackupText();

                // Request a password
                string password = "";
                while (password.Length < 10)
                {
                    Logging.flush();
                    password = ConsoleHelpers.requestNewPassword("Enter a password for your new wallet: ");
                    if (IxianHandler.forceShutdown)
                    {
                        return false;
                    }
                }
                walletStorage.generateWallet(password);
            }
            else
            {
                ConsoleHelpers.displayBackupText();

                bool success = false;
                while (!success)
                {
                    string password = "";
                    if (password.Length < 10)
                    {
                        Logging.flush();
                        Console.Write("Enter wallet password: ");
                        password = ConsoleHelpers.getPasswordInput();
                    }
                    if (IxianHandler.forceShutdown)
                    {
                        return false;
                    }
                    if (walletStorage.readWallet(password))
                    {
                        success = true;
                    }
                }
            }


            if (walletStorage.getPrimaryPublicKey() == null)
            {
                return false;
            }

            // Wait for any pending log messages to be written
            Logging.flush();

            Console.WriteLine();
            Console.WriteLine("Your IXIAN addresses are: ");
            Console.ForegroundColor = ConsoleColor.Green;
            foreach (var entry in walletStorage.getMyAddressesBase58())
            {
                Console.WriteLine(entry);
            }
            Console.ResetColor();
            Console.WriteLine();

            Logging.info("Public Node Address: {0}", walletStorage.getPrimaryAddress().ToString());


            if (walletStorage.viewingWallet)
            {
                Logging.error("Viewing-only wallet {0} cannot be used as the primary wallet.", walletStorage.getPrimaryAddress().ToString());
                return false;
            }

            IxianHandler.addWallet(walletStorage);

            // Prepare the balances list
            List<Address> address_list = IxianHandler.getWalletStorage().getMyAddresses();
            foreach (Address addr in address_list)
            {
                IxianHandler.balances.Add(addr, new Balance(addr, 0));
            }

            return true;
        }

        public void Start()
        {
            if (running) return;

            running = true;

            if (!blockStorage!.prepareStorage(false))
            {
                Logging.error("Error while preparing block storage! Aborting.");
                return;
            }

            activityStorage!.prepareStorage(false);

            var pending_txs = activityStorage.getActivitiesByStatus(ActivityStatus.Pending, true);
            pending_txs.AddRange(activityStorage.getActivitiesByStatus(ActivityStatus.Reverted, true));
            // Load pending transactions
            foreach (var pending_tx in pending_txs)
            {
                if (pending_tx.type == ActivityType.TransactionReceived)
                {
                    PendingTransactions.addIncomingTransaction(pending_tx.transaction);
                }
                else if (pending_tx.type == ActivityType.TransactionSent
                        || pending_tx.type == ActivityType.IxiName)
                {
                    PendingTransactions.addOutgoingTransaction(pending_tx.transaction, pending_tx.transaction.toList.TakeLast(2).Select(x => x.Key).ToList());
                }
            }

            ulong block_height = 0;
            byte[]? block_checksum = null;
            if (IxianHandler.networkType == NetworkType.main)
            {
                // Use baked block as starting point for mainnet, to avoid having to sync from genesis
                block_height = CoreConfig.bakedBlockHeight;
                block_checksum = CoreConfig.bakedBlockChecksum;
            }

            // Start TIV
            tiv?.start(block_height, block_checksum, true);

            // Start presence keepalive (announces our presence to network)
            PresenceList.startKeepAlive();

            // Start the network queue
            NetworkQueue.start();

            streamProcessor.start();

            // Connect to your sector of S2 nodes
            NetworkClientManager.start(1);

            // Connect to S2 streaming nodes (for presence and messaging)
            StreamClientManager.start();

            // Start main loop for periodic tasks
            mainLoopThread = new Thread(MainLoop);
            mainLoopThread.Start();

            Console.WriteLine("Node started and connecting to network...");
        }

        public void Stop()
        {
            running = false;

            // First stop localStorage, to flush any pending chat messages to storage
            // The Node is currently in shutting down state, so no incoming messages will be processed by the message processors
            IxianHandler.localStorage?.stop();

            // Stop the stream processor, it includes pending messages
            streamProcessor.stop();

            // Stop everything else storage related
            activityStorage?.stopStorage();

            PeerStorage.savePeersFile(true);

            // Stop the block storage
            blockStorage?.stopStorage();

            tiv?.stop();
            
            PresenceList.stopKeepAlive();
            NetworkQueue.stop();
            NetworkClientManager.stop();
            StreamClientManager.stop();

            mainLoopThread?.Join();

            Console.WriteLine("Node stopped.");
        }

        private void MainLoop()
        {
            while (running)
            {
                try
                {
                    // Fetch sector nodes
                    RequestSectorUpdate();

                    // Request balance update periodically
                    RequestBalanceUpdate();

                    // Check for balance changes
                    CheckBalanceChanges();

                    // Cleanup old presence entries
                    PresenceList.performCleanup();

                    // Save peer data
                    PeerStorage.savePeersFile();
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Error in main loop: {e.Message}");
                }

                Thread.Sleep(5000);
            }
        }

        private void RequestSectorUpdate()
        {
            if (lastSectorUpdate + 300 < Clock.getTimestamp())
            {
                lastSectorUpdate = Clock.getTimestamp();
                CoreProtocolMessage.fetchSectorNodes(IxianHandler.primaryWalletAddress, CoreConfig.maxRelaySectorNodesToRequest);
            }
        }

        private void RequestBalanceUpdate()
        {
            foreach (var balance in IxianHandler.balances.Values)
            {
                // Request initial wallet balance
                if (balance.blockHeight == 0 || balance.lastUpdate + 300 < Clock.getTimestamp())
                {
                    CoreProtocolMessage.broadcastProtocolMessage(['M', 'H', 'R'], ProtocolMessageCode.getBalance2, balance.address.addressNoChecksum.GetIxiBytes(), null);
                }
            }
        }

        // ===== IxianNode Abstract Method Implementations =====

        public override Block? getBlockHeader(ulong blockNum)
        {
            return blockStorage!.getBlock(blockNum);
        }

        public override byte[]? getBlockHash(ulong blockNum)
        {
            var tsd = blockStorage!.getBlockTotalSignerDifficulty(blockNum);
            return tsd.blockHash;
        }

        public override Block? getLastBlock()
        {
            return tiv.getLastBlockHeader();
        }

        public override ulong getLastBlockHeight()
        {
            Block? block = tiv.getLastBlockHeader();
            if (block == null)
            {
                return 0;
            }
            return block.blockNum;
        }

        public override int getLastBlockVersion()
        {
            Block? block = tiv.getLastBlockHeader();
            if (block == null
                || block.version < Block.maxVersion)
            {
                // TODO Omega force to v10 after upgrade
                return Block.maxVersion - 1;
            }
            return block.version;
        }

        public override bool addIncomingTransaction(Transaction tx)
        {
            if (tx.timeStamp == 0)
            {
                tx.timeStamp = Clock.getTimestamp();
            }
            if (IxianHandler.addTransactionToActivityStorage(activityStorage, tx))
            {
                return PendingTransactions.addIncomingTransaction(tx);
            }
            return false;
        }

        public override bool addTransaction(Transaction tx, List<Address> relayNodeAddresses, List<ExtendedAddress>? extendedAddresses, byte[]? requestId, bool force_broadcast)
        {
            if (tx.timeStamp == 0)
            {
                tx.timeStamp = Clock.getTimestamp();
            }
            if (IxianHandler.addTransactionToActivityStorage(activityStorage, tx))
            {
                if (PendingTransactions.addOutgoingTransaction(tx, relayNodeAddresses))
                {
                    foreach (var address in relayNodeAddresses)
                    {
                        NetworkClientManager.sendToClient(address, ProtocolMessageCode.transactionData2, tx.getBytes(true, true));
                    }
                    if (extendedAddresses != null)
                    {
                        CoreStreamProcessor.transactionSend(tx, extendedAddresses, requestId);
                    }
                    return true;
                }
            }
            return false;
        }

        public override bool isAcceptingConnections()
        {
            return false; // Clients don't accept incoming connections
        }

        public override void parseProtocolMessage(ProtocolMessageCode code, byte[] data, RemoteEndpoint endpoint)
        {

            if (endpoint == null)
            {
                Logging.error("Endpoint was null. parseProtocolMessage");
                return;
            }
            try
            {
                switch (code)
                {
                    case ProtocolMessageCode.hello:
                        {
                            using (MemoryStream m = new MemoryStream(data))
                            {
                                using (BinaryReader reader = new BinaryReader(m))
                                {
                                    CoreProtocolMessage.processHelloMessageV6(endpoint, reader);

                                    Friend? friend = FriendList.getFriend(endpoint.presence.wallet);
                                    if (friend != null)
                                    {
                                        friend.updatedStreamingNodes = Clock.getNetworkTimestamp();
                                        friend.relayNode = new Peer(endpoint.getFullAddress(true), endpoint.presence.wallet, Clock.getTimestamp(), Clock.getTimestamp(), Clock.getTimestamp(), 0);
                                        friend.online = true;
                                    }
                                }
                            }
                        }
                        break;


                    case ProtocolMessageCode.helloData:
                        using (MemoryStream m = new MemoryStream(data))
                        {
                            using (BinaryReader reader = new BinaryReader(m))
                            {
                                if (!CoreProtocolMessage.processHelloMessageV6(endpoint, reader))
                                {
                                    return;
                                }

                                char node_type = endpoint.presenceAddress.type;

                                ulong last_block_num = reader.ReadIxiVarUInt();
                                int bcLen = (int)reader.ReadIxiVarUInt();
                                byte[] block_checksum = reader.ReadBytes(bcLen);

                                endpoint.blockHeight = last_block_num;

                                int block_version = (int)reader.ReadIxiVarUInt();

                                if (node_type != 'C' && node_type != 'R')
                                {
                                    ulong highest_block_height = IxianHandler.getHighestKnownNetworkBlockHeight();
                                    if (last_block_num + 10 < highest_block_height)
                                    {
                                        CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.tooFarBehind, string.Format("Your node is too far behind, your block height is {0}, highest network block height is {1}.", last_block_num, highest_block_height), highest_block_height.ToString(), true);
                                        return;
                                    }
                                }

                                // Process the hello data
                                endpoint.helloReceived = true;
                                NetworkClientManager.recalculateLocalTimeDifference();

                                if (node_type == 'R')
                                {
                                    if (!StreamClientManager.isConnectedTo(StreamClientManager.primaryS2Address)
                                        && StreamClientManager.isConnectedTo(endpoint))
                                    {
                                        // Update local presence information
                                        StreamClientManager.primaryS2Address = endpoint.getFullAddress(true);
                                        IxianHandler.publicPort = endpoint.incomingPort;
                                        IxianHandler.publicIP = endpoint.address;
                                        StreamClientManager.setPinnedNodes(new() { StreamClientManager.primaryS2Address });
                                        PresenceList.forceSendKeepAlive = true;
                                        Logging.info("Forcing KA from networkprotocol");
                                    }
                                    else
                                    {
                                        // Announce local presence
                                        var myPresence = PresenceList.curNodePresence;
                                        if (myPresence != null)
                                        {
                                            foreach (var pa in myPresence.addresses)
                                            {
                                                var iika = new InventoryItemKeepAlive2(pa.lastSeenTime, myPresence.wallet, pa.device);
                                                endpoint.addInventoryItem(iika);
                                            }
                                        }
                                    }

                                    // Fetch friends presences if outgoing stream capabilities are enabled
                                    if ((CoreStreamProcessor.streamCapabilities & StreamCapabilities.Outgoing) != 0)
                                    {
                                        CoreStreamProcessor.fetchAllFriendsPresencesInSector(endpoint.presence.wallet);
                                    }
                                }

                                if (node_type == 'M'
                                    || node_type == 'H'
                                    || node_type == 'R')
                                {
                                    CoreProtocolMessage.subscribeToEvents(endpoint);
                                }

                                Friend? friend = FriendList.getFriend(endpoint.presence.wallet);
                                if (friend != null)
                                {
                                    friend.updatedStreamingNodes = Clock.getNetworkTimestamp();
                                    friend.relayNode = new Peer(endpoint.getFullAddress(true), endpoint.presence.wallet, Clock.getTimestamp(), Clock.getTimestamp(), Clock.getTimestamp(), 0);
                                    friend.online = true;
                                    if (node_type == 'C')
                                    {
                                        if (friend.bot)
                                        {
                                            CoreStreamProcessor.sendGetBotInfo(friend);
                                        }
                                    }
                                }
                            }
                        }
                        break;

                    case ProtocolMessageCode.s2data:
                        {
                            streamProcessor.receiveData(data, endpoint);
                        }
                        break;

                    case ProtocolMessageCode.getPresence2:
                        {
                            using (MemoryStream m = new MemoryStream(data))
                            {
                                using (BinaryReader reader = new BinaryReader(m))
                                {
                                    int walletLen = (int)reader.ReadIxiVarUInt();
                                    Address wallet = new Address(reader.ReadBytes(walletLen));

                                    Presence? p = PresenceList.getPresenceByAddress(wallet);
                                    if (p != null)
                                    {
                                        lock (p)
                                        {
                                            byte[][] presence_chunks = p.getByteChunks();
                                            foreach (byte[] presence_chunk in presence_chunks)
                                            {
                                                endpoint.sendData(ProtocolMessageCode.updatePresence, presence_chunk);
                                            }
                                        }
                                    }
                                    else
                                    {
                                        Logging.warn("Node has requested presence information about {0} that is not in our PL.", wallet.ToString());
                                    }
                                }
                            }
                        }
                        break;

                    case ProtocolMessageCode.balance2:
                        {
                            if (endpoint.presenceAddress.type != 'M'
                                && endpoint.presenceAddress.type != 'H'
                                && endpoint.presenceAddress.type != 'R')
                            {
                                Logging.warn("Received balance2 from non-master node {0}. Ignoring.", endpoint.getFullAddress());
                                return;
                            }
                            using (MemoryStream m = new MemoryStream(data))
                            {
                                using (BinaryReader reader = new BinaryReader(m))
                                {
                                    int address_length = (int)reader.ReadIxiVarUInt();
                                    Address address = new Address(reader.ReadBytes(address_length));

                                    int balance_bytes_len = (int)reader.ReadIxiVarUInt();
                                    byte[] balance_bytes = reader.ReadBytes(balance_bytes_len);

                                    // Retrieve the latest balance
                                    IxiNumber ixi_balance = new IxiNumber(balance_bytes);

                                    // Retrieve the blockheight for the balance
                                    ulong block_height = reader.ReadIxiVarUInt();
                                    byte[] block_checksum = reader.ReadBytes((int)reader.ReadIxiVarUInt());

                                    foreach (Balance balance in IxianHandler.balances.Values)
                                    {
                                        if (address.addressNoChecksum.SequenceEqual(balance.address.addressNoChecksum))
                                        {
                                            if (block_height > balance.blockHeight && (balance.balance != ixi_balance || balance.blockHeight == 0))
                                            {
                                                balance.address = address;
                                                balance.balance = ixi_balance;
                                                balance.blockHeight = block_height;
                                                balance.blockChecksum = block_checksum;
                                                balance.verified = false;
                                            }

                                            balance.lastUpdate = Clock.getTimestamp();
                                        }
                                    }
                                }
                            }
                        }
                        break;

                    case ProtocolMessageCode.updatePresence:
                        HandleUpdatePresence(data, endpoint);
                        break;

                    case ProtocolMessageCode.keepAlivePresence:
                        HandleKeepAlivePresence(data, endpoint);
                        break;

                    case ProtocolMessageCode.blockHeaders4:
                        HandleBlockHeaders4(data, endpoint);
                        break;

                    case ProtocolMessageCode.pitData2:
                        {
                            if (endpoint.presenceAddress.type != 'M'
                                && endpoint.presenceAddress.type != 'H'
                                && endpoint.presenceAddress.type != 'R')
                            {
                                Logging.warn("Received pit data from non-master node {0}. Ignoring.", endpoint.getFullAddress());
                                return;
                            }
                            tiv?.receivedPIT2(data, endpoint);
                        }
                        break;

                    case ProtocolMessageCode.transactionData2:
                        HandleTransactionData(data, endpoint);
                        break;

                    case ProtocolMessageCode.bye:
                        CoreProtocolMessage.processBye(data, endpoint);
                        break;

                    case ProtocolMessageCode.sectorNodes:
                        HandleSectorNodes(data, endpoint);
                        break;

                    case ProtocolMessageCode.keepAlivesChunk:
                        HandleKeepAlivesChunk(data, endpoint);
                        break;

                    case ProtocolMessageCode.getKeepAlives:
                        CoreProtocolMessage.processGetKeepAlives(data, endpoint);
                        break;

                    case ProtocolMessageCode.inventory2:
                        break;

                    case ProtocolMessageCode.rejected:
                        HandleRejected(data, endpoint);
                        break;

                    case ProtocolMessageCode.transactionsChunk3:
                        HandleTransactionsChunk3(data, endpoint);
                        break;

                    default:
                        Logging.warn("Unknown protocol message: {0}, from {1} ({2})", code, endpoint.getFullAddress(), endpoint.serverWalletAddress);
                        break;
                }
            }
            catch (Exception e)
            {
                Logging.error("Error parsing network message. Details: {0}", e.ToString());
            }
        }

        public override void shutdown()
        {
            Stop();
        }

        public override IxiNumber getMinSignerPowDifficulty(ulong blockNum, int curBlockVersion, long curBlockTimestamp)
        {
            return tiv.getMinSignerPowDifficulty(blockNum, curBlockVersion, curBlockTimestamp);
        }

        public override RegisteredNameRecord getRegName(byte[] name, bool useAbsoluteId)
        {
            throw new NotImplementedException();
        }

        private (Transaction? transaction, List<Address>? relayNodeAddresses, List<ExtendedAddress>? extendedAddresses) PrepareTransactionFrom(Address fromAddress, ExtendedAddress toAddress, IxiNumber amount, bool check_balance = true)
        {
            IxiNumber fee = ConsensusConfig.forceTransactionPrice;
            Dictionary<Address, ToEntry> toList = new(new AddressComparer());
            Address pubKey = new(IxianHandler.getWalletStorage().getPrimaryPublicKey());

            if (!IxianHandler.getWalletStorage().isMyAddress(fromAddress))
            {
                Console.WriteLine("From address is not my address.");
                return (null, null, null);
            }

            Dictionary<byte[], IxiNumber> fromList = new(new ByteArrayComparer())
            {
                { IxianHandler.getWalletStorage().getAddress(fromAddress).nonce, amount }
            };

            List<ExtendedAddress> extendedAddresses = new List<ExtendedAddress>();

            toList.AddOrReplace(toAddress.PaymentAddress, new ToEntry(Transaction.getExpectedVersion(IxianHandler.getLastBlockVersion()), amount, toAddress.Tag));

            if (toAddress.Flag != AddressPaymentFlag.Primary)
            {
                extendedAddresses.Add(toAddress);
            }

            List<Address> tmpRelayNodeAddresses = NetworkClientManager.getRandomConnectedClientAddresses(2);
            List<Address> relayNodeAddresses = new List<Address>();
            IxiNumber relayFee = 0;
            foreach (Address relayNodeAddress in tmpRelayNodeAddresses)
            {
                if (toList.ContainsKey(relayNodeAddress))
                {
                    continue;
                }
                var tmpFee = fee > ConsensusConfig.transactionDustLimit ? fee : ConsensusConfig.transactionDustLimit;
                ToEntry toEntry = new ToEntry(getExpectedVersion(IxianHandler.getLastBlockVersion()),
                                              tmpFee,
                                              null,
                                              null);
                relayNodeAddresses.Add(relayNodeAddress);
                toList.Add(relayNodeAddress, toEntry);
                relayFee += tmpFee;
            }

            // Prepare transaction to calculate fee
            Transaction transaction = new((int)Transaction.Type.Normal, fee, toList, fromList, pubKey, IxianHandler.getHighestKnownNetworkBlockHeight());

            relayFee = 0;
            foreach (Address relayNodeAddress in relayNodeAddresses)
            {
                var tmpFee = transaction.fee > ConsensusConfig.transactionDustLimit ? transaction.fee : ConsensusConfig.transactionDustLimit;
                toList[relayNodeAddress].amount = tmpFee;
                relayFee += tmpFee;
            }

            byte[] first_address = fromList.Keys.First();
            fromList[first_address] = fromList[first_address] + relayFee + transaction.fee;
            if (check_balance)
            {
                IxiNumber wal_bal = IxianHandler.getWalletBalance(new Address(transaction.pubKey.addressNoChecksum, first_address));
                if (fromList[first_address] > wal_bal)
                {
                    IxiNumber maxAmount = wal_bal - transaction.fee;

                    if (maxAmount < 0)
                        maxAmount = 0;

                    Console.WriteLine($"Insufficient funds to cover amount and transaction fee.\nMaximum amount you can send is {maxAmount} IXI.\n");
                    return (null, null, null);
                }
            }
            // Prepare transaction with updated "from" amount to cover fee
            transaction = new((int)Transaction.Type.Normal, fee, toList, fromList, pubKey, IxianHandler.getHighestKnownNetworkBlockHeight());
            return (transaction, relayNodeAddresses, extendedAddresses);
        }

        public Transaction? SendTransactionFrom(Address fromAddress, ExtendedAddress toAddress, IxiNumber amount, byte[]? requestId)
        {
            var prepTx = PrepareTransactionFrom(fromAddress, toAddress, amount);
            var transaction = prepTx.transaction;
            var relayNodeAddresses = prepTx.relayNodeAddresses;
            
            if (transaction == null || relayNodeAddresses == null)
            {
                return null;
            }

            // Send the transaction
            if (IxianHandler.addTransaction(transaction, relayNodeAddresses, prepTx.extendedAddresses, requestId, true))
            {
                Console.WriteLine($"Sending transaction, txid: {transaction.getTxIdString()}");
                return transaction;
            }
            else
            {
                Console.WriteLine($"Could not send transaction, txid: {transaction.getTxIdString()}");
            }
            return null;
        }

        public bool SendPayment(string toAddress, string amount)
        {
            try
            {
                var recipient = new ExtendedAddress(toAddress);
                var txAmount = new IxiNumber(amount);
                var myAddress = IxianHandler.getWalletStorage().getPrimaryAddress();

                var tx = SendTransactionFrom(myAddress, recipient, txAmount, null);
                return tx != null;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error sending payment: {e.Message}");
                return false;
            }
        }

        private IxiNumber lastKnownBalance = new IxiNumber(0);

        private void CheckBalanceChanges()
        {
            var myAddress = IxianHandler.getWalletStorage().getPrimaryAddress();
            var currentBalance = IxianHandler.getWalletBalance(myAddress);

            if (currentBalance != lastKnownBalance)
            {
                Console.WriteLine($"\n*** Balance changed: {lastKnownBalance} -> {currentBalance} IXI ***\n");
                lastKnownBalance = currentBalance;
            }
        }

        // Query your own balance
        public IxiNumber GetMyBalance()
        {
            var myAddress = IxianHandler.getWalletStorage().getPrimaryAddress();
            return IxianHandler.getWalletBalance(myAddress);
        }

        // Query any address balance (returns cached value, use RequestBalanceUpdate to refresh)
        public IxiNumber GetBalance(string address)
        {
            var addr = new Address(address);
            return IxianHandler.getWalletBalance(addr);
        }

        // Request fresh balance from network for specific address
        public void RequestBalanceUpdate(Address address)
        {
            byte[] getBalanceBytes;
            using (var ms = new MemoryStream())
            {
                using (var writer = new BinaryWriter(ms))
                {
                    writer.WriteIxiVarInt(address.addressNoChecksum.Length);
                    writer.Write(address.addressNoChecksum);
                }
                getBalanceBytes = ms.ToArray();
            }

            CoreProtocolMessage.broadcastProtocolMessage(
                new char[] { 'M', 'H', 'R' },
                ProtocolMessageCode.getBalance2,
                getBalanceBytes,
                null
            );
        }

        // Check if an address is online (from local cache)
        public bool IsAddressOnline(string address)
        {
            var addr = new Address(address);
            var presence = PresenceList.getPresenceByAddress(addr);
            return presence != null;
        }

        // Get presence details for an address (from local cache)
        public Presence? GetPresence(string address)
        {
            var addr = new Address(address);
            return PresenceList.getPresenceByAddress(addr);
        }

        // Request sector nodes from the network that handle a specific address's sector
        // The sector is determined by the first 10 bytes of the address's sector prefix
        public void RequestSector(string address)
        {
            var addr = new Address(address);
            Console.WriteLine($"Requesting sectors for {address}...");
            
            // Fetch relay nodes (S2 nodes) that are responsible for this address's sector
            CoreProtocolMessage.fetchSectorNodes(addr, CoreConfig.maxRelaySectorNodesToRequest);
            
            // The network will respond with sectorNodes message containing relay node presences
            // This is handled by HandleSectorNodes() which updates RelaySectors and PeerStorage
        }

        // Request presence information for a specific address from the network
        // Uses the sector-based routing system to query the appropriate relay nodes
        public void RequestPresence(string address)
        {
            var addr = new Address(address);
            Console.WriteLine($"Requesting presence for {address}...");

            // Create a temporary friend object to use the sector-based presence fetching mechanism
            var friend = new Friend(FriendType.Temporary, FriendState.Approved, addr, null, "", null, null, 0, true);
            
            // Get relay nodes responsible for this address's sector from local cache
            List<Peer> peers = new();
            var relays = RelaySectors.Instance.getSectorNodes(addr.sectorPrefix, CoreConfig.maxRelaySectorNodesToRequest);
            foreach (var relay in relays)
            {
                var p = PresenceList.getPresenceByAddress(relay);
                if (p == null)
                {
                    continue;
                }
                var pa = p.addresses.First();
                peers.Add(new(pa.address, relay, pa.lastSeenTime, 0, 0, 0));
            }
            
            // Assign sector nodes to the friend and request presence via streaming protocol
            friend.sectorNodes = peers;
            friend.updatedSectorNodes = Clock.getTimestamp();
            CoreStreamProcessor.fetchFriendsPresence(friend);

            // The network will respond with updatePresence messages
        }

        // Display presence information for an address
        public void DisplayPresenceInfo(string address)
        {
            var addr = new Address(address);
            var presence = PresenceList.getPresenceByAddress(addr);

            if (presence == null)
            {
                Console.WriteLine($"  Status: Offline or not found in local cache");
                Console.WriteLine($"  Tip: The presence might not be cached yet. Wait a few seconds after RequestPresence().");
                return;
            }

            Console.WriteLine($"  Status: Online");
            Console.WriteLine($"  Wallet: {presence.wallet?.ToString() ?? "N/A"}");
            Console.WriteLine($"  Endpoints: {presence.addresses.Count}");

            foreach (var endpoint in presence.addresses)
            {
                char nodeType = endpoint.type;
                string typeDesc = nodeType switch
                {
                    'C' => "Client",
                    'M' => "Master (DLT)",
                    'H' => "Host (DLT)",
                    'R' => "Relay (S2)",
                    'W' => "Worker",
                    _ => "Unknown"
                };
                Console.WriteLine($"    - {endpoint.address} (type: {nodeType} - {typeDesc})");
            }
        }

        // Get transaction status
        public string GetTransactionStatus(byte[] txid)
        {
            // Check if transaction is confirmed
            ActivityObject? activity = activityStorage.getActivityById(txid);
            if (activity == null)
            {
                return "Unknown (not found)";
            }

            switch (activity.status)
            {
                case ActivityStatus.Final:
                    return $"Confirmed in block {activity.appliedBlockHeight}";
                case ActivityStatus.Pending:
                    return "Pending (waiting for confirmation)";
                case ActivityStatus.Reverted:
                    return "Reverted (transaction was included in a block that got reverted)";
                case ActivityStatus.Rejected:
                    return "Rejected (transaction was rejected by the network)";
                case ActivityStatus.Expired:
                    return "Expired (transaction was not included in a block within the expected time frame)";
                case ActivityStatus.Unknown:
                    return "Unknown (transaction status cannot be verified)";
            }

            return "Unknown status";
        }

        // Get transaction status by string txid
        public string GetTransactionStatus(string txidString)
        {
            try
            {
                byte[] txid = Transaction.txIdLegacyToV8(txidString);
                return GetTransactionStatus(txid);
            }
            catch
            {
                return "Invalid transaction ID";
            }
        }

        private void HandleTransactionsChunk3(byte[] data, RemoteEndpoint endpoint)
        {
            if (endpoint.presenceAddress.type != 'M'
                        && endpoint.presenceAddress.type != 'H'
                        && endpoint.presenceAddress.type != 'R')
            {
                Console.WriteLine($"Received transactions chunk from non-master node {endpoint.getFullAddress()}. Ignoring.");
                return;
            }
            using (MemoryStream m = new MemoryStream(data))
            {
                using (BinaryReader reader = new BinaryReader(m))
                {
                    long msg_id = reader.ReadIxiVarInt();

                    int tx_count = (int)reader.ReadIxiVarUInt();

                    int max_tx_per_chunk = CoreConfig.maximumTransactionsPerChunk;
                    if (tx_count > max_tx_per_chunk)
                    {
                        tx_count = max_tx_per_chunk;
                    }

                    var sw = new System.Diagnostics.Stopwatch();
                    sw.Start();
                    int processedTxCount = 0;
                    int totalTxCount = 0;
                    for (int i = 0; i < tx_count; i++)
                    {
                        if (m.Position == m.Length)
                        {
                            break;
                        }

                        int tx_len = (int)reader.ReadIxiVarUInt();
                        byte[] tx_bytes = reader.ReadBytes(tx_len);

                        Transaction tx = new Transaction(tx_bytes, false, true);

                        totalTxCount++;

                        if (IxianHandler.addIncomingTransaction(tx))
                        {
                            processedTxCount++;
                        }
                    }
                    sw.Stop();
                    TimeSpan elapsed = sw.Elapsed;
                    Logging.info("Processed {0}/{1} txs for #{2} in {3}ms", processedTxCount, totalTxCount, msg_id, elapsed.TotalMilliseconds);
                }
            }
        }

        private void HandleBlockHeaders4(byte[] data, RemoteEndpoint endpoint)
        {
            if (endpoint.presenceAddress.type != 'M'
                && endpoint.presenceAddress.type != 'H'
                && endpoint.presenceAddress.type != 'R')
            {
                Logging.warn("Received block headers from non-master node {0}. Ignoring.", endpoint.getFullAddress());
                return;
            }
            using (MemoryStream m = new MemoryStream(data))
            {
                using (BinaryReader reader = new BinaryReader(m))
                {
                    ulong from = reader.ReadIxiVarUInt();
                    if (from > IxianHandler.getLastBlockHeight() + 1)
                    {
                        Logging.warn("Received block headers starting from {0}, but our last block height is {1}. Ignoring.", from, IxianHandler.getLastBlockHeight());
                        return;
                    }
                    ulong totalCount = reader.ReadIxiVarUInt();

                    int filterLen = (int)reader.ReadIxiVarUInt();
                    byte[] filterBytes = reader.ReadBytes(filterLen);

                    byte[] headersBytes = new byte[reader.BaseStream.Length - reader.BaseStream.Position];
                    Array.Copy(data, reader.BaseStream.Position, headersBytes, 0, headersBytes.Length);

                    tiv.receivedBlockHeaders3(headersBytes, endpoint);
                }
            }
        }

        private void HandleTransactionData(byte[] data, RemoteEndpoint endpoint)
        {
            if (endpoint.presenceAddress.type != 'M'
                && endpoint.presenceAddress.type != 'H'
                && endpoint.presenceAddress.type != 'R')
            {
                Logging.warn("Received transaction data from non-master node {0}. Ignoring.", endpoint.getFullAddress());
                return;
            }

            Transaction tx = new Transaction(data, true, true);

            // Check if my transaction
            bool myTransaction = IxianHandler.isMyAddress(tx.pubKey);
            if (!myTransaction)
            {
                foreach (var toEntry in tx.toList.Keys)
                {
                    if (IxianHandler.isMyAddress(toEntry))
                    {
                        myTransaction = true;
                        break;
                    }
                }
            }

            Logging.trace("Received new transaction {0}", tx.getTxIdString());

            if (myTransaction)
            {
                // If transaction already processed
                ActivityObject? activity = activityStorage.getActivityById(tx.id, null);
                if (activity != null)
                {
                    if (activity.status != ActivityStatus.Final)
                    {
                        if (endpoint.presenceAddress.type == 'M'
                            || endpoint.presenceAddress.type == 'H'
                            || endpoint.presenceAddress.type == 'R')
                        {
                            PendingTransactions.increaseReceivedCount(tx.id, endpoint.presence.wallet);
                        }
                    }
                }
                else
                {
                    IxianHandler.addIncomingTransaction(tx);
                }
            }
        }

        private void HandleKeepAlivesChunk(byte[] data, RemoteEndpoint endpoint)
        {
            using (MemoryStream m = new MemoryStream(data))
            {
                using (BinaryReader reader = new BinaryReader(m))
                {
                    int ka_count = (int)reader.ReadIxiVarUInt();

                    int max_ka_per_chunk = CoreConfig.maximumKeepAlivesPerChunk;
                    if (ka_count > max_ka_per_chunk)
                    {
                        ka_count = max_ka_per_chunk;
                    }

                    for (int i = 0; i < ka_count; i++)
                    {
                        if (m.Position == m.Length)
                        {
                            break;
                        }

                        int ka_len = (int)reader.ReadIxiVarUInt();
                        byte[] ka_bytes = reader.ReadBytes(ka_len);

                        HandleKeepAlivePresence(ka_bytes, endpoint);
                    }
                }
            }
        }

        private void HandleUpdatePresence(byte[] data, RemoteEndpoint endpoint)
        {
            // Parse the data and update entries in the presence list
            Presence? p = PresenceList.updateFromBytes(data, IxianHandler.getMinSignerPowDifficulty(IxianHandler.getLastBlockHeight(), IxianHandler.getLastBlockVersion(), 0));
            if (p == null)
            {
                return;
            }

            Logging.trace("Received presence update for " + p.wallet);
            Friend? f = FriendList.getFriend(p.wallet);
            if (f != null)
            {
                if (f.publicKey == null)
                {
                    f.setPublicKey(p.pubkey);
                }
                var pa = p.addresses[0];
                if (f.lastSeenTime < pa.lastSeenTime)
                {
                    // TODO use actual wallet address once Presence hostname contains such address
                    f.relayNode = new Peer(pa.address, null, pa.lastSeenTime, 0, 0, 0);
                    f.updatedStreamingNodes = pa.lastSeenTime;
                    f.lastSeenTime = pa.lastSeenTime;
                }
            }
        }

        private void HandleKeepAlivePresence(byte[] data, RemoteEndpoint endpoint)
        {
            Address address;
            long last_seen = 0;
            byte[] device_id;
            char node_type;
            bool updated = PresenceList.receiveKeepAlive(data, out address, out last_seen, out device_id, out node_type, endpoint);

            InventoryCache.Instance.setProcessedFlag(InventoryItemTypes.keepAlive2, InventoryItemKeepAlive2.getHash(last_seen, address, device_id));

            if (!updated)
            {
                return;
            }

            Logging.trace("Received keepalive update for " + address);
            Presence? p = PresenceList.getPresenceByAddress(address);
            if (p == null)
                return;

            Friend? f = FriendList.getFriend(p.wallet);
            if (f != null)
            {
                var pa = p.addresses[0];
                if (f.lastSeenTime < pa.lastSeenTime)
                {
                    // TODO use actual wallet address once Presence hostname contains such address
                    f.relayNode = new Peer(pa.address, null, pa.lastSeenTime, 0, 0, 0);
                    f.updatedStreamingNodes = pa.lastSeenTime;
                    f.lastSeenTime = pa.lastSeenTime;
                }
            }
        }

        private void HandleSectorNodes(byte[] data, RemoteEndpoint endpoint)
        {
            if (endpoint.presenceAddress.type != 'M'
                && endpoint.presenceAddress.type != 'H'
                && endpoint.presenceAddress.type != 'R')
            {
                Logging.warn("Received sector nodes from non-master node {0}. Ignoring.", endpoint.getFullAddress());
                return;
            }

            int offset = 0;

            var prefixAndOffset = data.ReadIxiBytes(offset);
            offset += prefixAndOffset.bytesRead;
            byte[] prefix = prefixAndOffset.bytes;

            var nodeCountAndOffset = data.GetIxiVarUInt(offset);
            offset += nodeCountAndOffset.bytesRead;
            int nodeCount = (int)nodeCountAndOffset.num;

            for (int i = 0; i < nodeCount; i++)
            {
                var kaBytesAndOffset = data.ReadIxiBytes(offset);
                offset += kaBytesAndOffset.bytesRead;

                Presence? p = PresenceList.updateFromBytes(kaBytesAndOffset.bytes, IxianHandler.getMinSignerPowDifficulty(IxianHandler.getLastBlockHeight(), IxianHandler.getLastBlockVersion(), 0));
                if (p != null)
                {
                    RelaySectors.Instance.addRelayNode(p.wallet);
                }
            }

            List<Peer> peers = new();
            var relays = RelaySectors.Instance.getSectorNodes(prefix, CoreConfig.maxRelaySectorNodesToRequest);
            foreach (var relay in relays)
            {
                var p = PresenceList.getPresenceByAddress(relay);
                if (p == null)
                {
                    continue;
                }
                var pa = p.addresses.First();
                peers.Add(new(pa.address, relay, pa.lastSeenTime, 0, 0, 0));

                PeerStorage.addPeerToPeerList(pa.address, p.wallet, pa.lastSeenTime, 0, 0, 0);
            }

            if (IxianHandler.primaryWalletAddress.sectorPrefix.SequenceEqual(prefix))
            {
                networkClientManagerStatic.setClientsToConnectTo(peers);
            }

            var friends = FriendList.getFriendsBySectorPrefix(prefix);
            foreach (var friend in friends)
            {
                friend.updatedSectorNodes = Clock.getTimestamp();
                friend.sectorNodes = peers;
            }

            friends = IXISocketConnections.GetPendingSectorRequestsBySectorPrefix(prefix);
            foreach (var friend in friends)
            {
                friend.updatedSectorNodes = Clock.getTimestamp();
                friend.sectorNodes = peers;
                IXISocketConnections.RemovePendingSectorRequest(friend);
            }
        }

        private void HandleRejected(byte[] data, RemoteEndpoint endpoint)
        {
            try
            {
                Rejected rej = new Rejected(data);
                switch (rej.code)
                {
                    case RejectedCode.TransactionInvalid:
                    case RejectedCode.TransactionInsufficientFee:
                    case RejectedCode.TransactionDust:
                        if (endpoint.presenceAddress.type != 'M'
                            && endpoint.presenceAddress.type != 'H'
                            && endpoint.presenceAddress.type != 'R')
                        {
                            Logging.error("Received 'rejected' message {0} {1} from non-master {2}", rej.code, Transaction.getTxIdString(rej.data), endpoint.getFullAddress());
                            return;
                        }
                        Logging.error("Transaction {0} was rejected with code: {1}", Transaction.getTxIdString(rej.data), rej.code);
                        PendingTransactions.increaseRejectedCount(rej.data, endpoint.serverWalletAddress);
                        break;

                    case RejectedCode.TransactionDuplicate:
                        if (endpoint.presenceAddress.type != 'M'
                            && endpoint.presenceAddress.type != 'H'
                            && endpoint.presenceAddress.type != 'R')
                        {
                            Logging.error("Received 'rejected' message {0} {1} from non-master {2}", rej.code, Transaction.getTxIdString(rej.data), endpoint.getFullAddress());
                            return;
                        }
                        Logging.warn("Transaction {0} already sent.", Transaction.getTxIdString(rej.data), rej.code);
                        // All good
                        PendingTransactions.increaseReceivedCount(rej.data, endpoint.serverWalletAddress);
                        break;

                    default:
                        Logging.error("Received 'rejected' message with unknown code {0} {1}", rej.code, Crypto.hashToString(rej.data));
                        break;
                }
            }
            catch (Exception e)
            {
                throw new Exception(string.Format("Exception occured while processing 'rejected' message with code {0} {1}", data[0], Crypto.hashToString(data)), e);
            }
        }
    }
}
