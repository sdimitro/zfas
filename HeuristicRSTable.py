from Model import Pool, MetaslabGroup, LogSpaceMapGroup
import math

class MTable(object):
    def generateRunningSums(self):
        assert len(self.mrsum) == 0 and len(self.brsum) == 0
        assert self.pool.logs.nlogs() <= self.pool.nmetaslabs
        
        msum, bsum = 0, 0
        for i in range(self.pool.logs.nlogs()):
            msum += len(self.pool.logs.sms[i].metaslabs_flushed)
            bsum += self.pool.logs.sms[i].blocks
            self.mrsum.append(msum)
            self.brsum.append(bsum)

        # DBG START
        print "blks", "ms", "brsum", "mrsum"
        for sm, bsum, msum in zip(self.pool.logs.sms, self.brsum, self.mrsum):
            print sm.blocks, len(sm.metaslabs_flushed), bsum, msum
        # DBG END

        assert i <= self.pool.nmetaslabs
        assert msum <= self.pool.nmetaslabs

    def __init__(self, p):
        self.pool = p
        self.mrsum = []
        self.brsum = []
        self.generateRunningSums()

    def flushesNeededForBlocks(self, nblocks):
        assert self.brsum[-1] >= nblocks
        assert len(self.brsum) == len(self.mrsum)

        for bsum, msum in zip(self.brsum, self.mrsum):
            if nblocks <= bsum:
                assert msum <= self.pool.nmetaslabs
                return msum

        # Should never get here!
        assert False
        return -1

    def flushApproximately(self, current_incoming):
        assert current_incoming >= 0 and current_incoming <= 64
        roomLeft = self.pool.nmetaslabs - self.pool.logs.nblocks
        print "L", roomLeft

        n = self.pool.nmetaslabs + 1
        incoming = current_incoming
        for i in range(self.pool.nmetaslabs):
            afterwards = incoming - roomLeft
            if afterwards <= 0:
                print "X", incoming, roomLeft, afterwards
                incoming += current_incoming
                continue
            n = self.flushesNeededForBlocks(afterwards)
            print "A", (i + 1), afterwards, n
            break

        assert n <= self.pool.nmetaslabs
        return int(math.ceil(float(n) / float(i + 1)))


class HeuristicRSTable(object):
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
        mtable = MTable(self.pool)
        nflushed += mtable.flushApproximately(incoming_blocks)
        print "M", nflushed
        self.pool.flush_n_metaslabs(nflushed)

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
