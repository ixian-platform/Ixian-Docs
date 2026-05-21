using IXICore;
using IXICore.Meta;
using IXICore.Activity;
using System;
using System.Linq;

namespace IxianClient
{
    internal class ICTransactionInclusionCallbacks : TransactionInclusionCallbacks
    {
        public void transactionVerified(Transaction tx)
        {
            var bh = IxianHandler.getBlockHeader(tx.applied);
            Node.activityStorage.updateStatus(tx.id, ActivityStatus.Final, tx.applied, bh.timestamp);
        }

        public void transactionRejected(Transaction tx)
        {
            tx.applied = 0;
            Node.activityStorage.updateStatus(tx.id, ActivityStatus.Rejected, 0);
        }

        public void transactionExpired(Transaction tx)
        {
            tx.applied = 0;
            Node.activityStorage.updateStatus(tx.id, ActivityStatus.Expired, 0);
        }

        public void transactionCannotVerify(Transaction tx)
        {
            tx.applied = 0;
            Node.activityStorage.updateStatus(tx.id, ActivityStatus.Unknown, 0);
        }

        public void receivedBlockHeader(Block blockHeader, bool verified)
        {
            foreach (Balance balance in IxianHandler.balances.Values)
            {
                if (balance.blockChecksum != null && balance.blockChecksum.SequenceEqual(blockHeader.blockChecksum))
                {
                    balance.verified = true;
                }
            }

            IxianHandler.status = NodeStatus.ready;
        }

        public void blockReorg(Block blockHeader)
        {
            var revertedTransactions = Node.activityStorage.revertTransactionsByBlockHeight(blockHeader.blockNum);
            foreach(var revertedTxId in revertedTransactions)
            {
                var activity = Node.activityStorage.getActivityById(revertedTxId, null, true);
                PendingTransactions.addOutgoingTransaction(activity.transaction, activity.transaction.toList.TakeLast(2).Select(x => x.Key).ToList());
            }
        }
    }
}
