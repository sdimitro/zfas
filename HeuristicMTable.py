from Model import Pool, MetaslabGroup, LogSpaceMapGroup
import math

class MTable(object):
    def generateRunningSums(self):
        msrsum, blkrsum = 0, 0
        for sm in self.pool.logs.sms:
            blkrsum += sm.blocks
            msrsum += len(sm.metaslabs_flushed)
            self.blk_rsum.append(blkrsum)
            self.ms_rsum.append(msrsum)
        if not msrsum == self.pool.nmetaslabs:
            for sm in self.pool.logs.sms:
                print sm.txg, len(sm.metaslabs_flushed)
        assert msrsum == self.pool.nmetaslabs

    def __init__(self, p):
        self.pool = p
        self.ms_rsum = []
        self.blk_rsum = []
        self.generateRunningSums()

    def flushesNeededForBlocks(self, blocks):
        for msrsum, blkrsum in zip(self.ms_rsum, self.blk_rsum):
            if blkrsum >= blocks:
                return msrsum
        return self.ms_rsum[-1]

    def flushesPerTXG(self, incoming, txgs):
        res = []
        t = 1
        numberBlocksNeeded = incoming
        for i in range(txgs):
            if numberBlocksNeeded > 64:
                break
            res.append(int(math.ceil(self.flushesNeededForBlocks(numberBlocksNeeded) / t)))
            t += 1
            numberBlocksNeeded += incoming
        return res

    def getFlushesNeeded(self, incoming):
        return max(self.flushesPerTXG(incoming, 10))

#
# XXX: Explain Heuristic
#
class HeuristicMTable(object):
    def __init__(self, nmetaslabs):
        self.nmetaslabs = nmetaslabs
        self.pool = Pool(nmetaslabs)
        self.y_flushed = []
        self.y_blocks = []
        self.y_logs = []

    def condition_satisfied(self):
        if self.pool.mss.nmetaslabs() >= self.pool.logs.nblocks:
            return True
        return False

    def sync_cycle(self, incoming_blocks):
        if self.pool.syncing_txg == 0:
            all_metaslabs = self.pool.mss.ms_ordered_by_flushed
            self.pool.sync_new_changes(incoming_blocks, all_metaslabs)
            self.y_flushed.append(0)
            self.y_blocks.append(self.pool.logs.nblocks)
            self.y_logs.append(self.pool.logs.nlogs())
            self.pool.sync_done()
            return

        nflushed = 0
        if self.pool.syncing_txg > 768:
            mtable = MTable(self.pool)
            nflushed += mtable.getFlushesNeeded(incoming_blocks)
            print "M", nflushed
            self.pool.flush_n_metaslabs(nflushed)
        else:
            self.pool.flush_n_metaslabs(incoming_blocks)
            nflushed += incoming_blocks

        # while not self.condition_satisfied():
        #     self.pool.flush_n_metaslabs(1)
        #     nflushed += 1
        
        ms_flushed_this_txg = self.pool.mss.ms_ordered_by_flushed[-nflushed:]
        self.pool.sync_new_changes(incoming_blocks, ms_flushed_this_txg)

        print "DBG - ", self.pool.syncing_txg, " flushed", nflushed
        self.pool.sync_done()

        self.y_flushed.append(nflushed)
        self.y_blocks.append(self.pool.logs.nblocks)
        self.y_logs.append(self.pool.logs.nlogs())

    def simulate(self, y_incoming):
        for incoming in y_incoming:
            self.sync_cycle(incoming)
