from Model import Pool, MetaslabGroup, LogSpaceMapGroup
import math

class SummaryEntry(object):
    def __init__(self, b, mss, blks):
        assert b >= 0
        assert mss > 0
        assert blks > 0
        self.beginTXG = b
        self.endTXG = b
        self.mss = mss
        self.blks = blks

    def addTXG(self, m , b):
        assert self.beginTXG <= self.endTXG
        self.endTXG += 1
        self.mss += m
        self.blks += b
        assert self.beginTXG < self.endTXG

    def txgLength(self):
        return self.endTXG - self.beginTXG

    def isEmpty(self):
        if self.beginTXG == -1:
            assert self.mss == 0
            assert self.blks == 0
            assert self.endTXG == -1
            return True
        assert self.beginTXG <= self.endTXG
        assert self.mss > 0
        assert self.blks > 0
        return False

    def flushTXG(self, m ,b):
        assert self.mss > 0
        assert self.blks > 0
        assert not self.beginTXG == -1
        assert not self.endTXG == -1

        print "Q", m, b

        # self.beginTXG += 1
        self.mss -= m
        self.blks -= b

        # print self.beginTXG, self.endTXG
        # if self.beginTXG > self.endTXG:

        print "R", self.mss, self.blks
        if self.mss == 0:
            # XXX: Double checking
            assert self.blks == 0
            assert self.mss == 0
            assert self.beginTXG > self.endTXG
            self.beginTXG, self.endTXG = -1, -1

        assert self.mss >= 0
        assert self.blks >= 0
        assert self.beginTXG <= self.endTXG
        if self.blks == 0:
            assert self.mss == 0
            assert self.beginTXG == -1
            assert self.beginTXG == self.endTXG

    def isFilled(self):
        assert not self.txgLength() < 0
        if self.txgLength() < 10:
            return False
        assert self.txgLength() == 10
        return True


class Summary(object):
    def __init__(self, pool):
        self.p = pool
        self.tab = []

    def addEntry(self, entry):
        assert not entry.isEmpty()
        self.tab.append(entry)

    def trimStart(self):
        ntrim = 0
        for entry in self.tab:
            if not entry.isEmpty():
                break
            ntrim += 1
        self.tab = self.tab[ntrim:]

    def verifySummary(self):
        msum, bsum = 0, 0
        tab_idx = 0
        # ctxg = self.tab[tab_idx].beginTXG
        ctxg = self.p.logs.sms[0].txg

        for entry in self.tab:
            print "range: {}-{}".format(entry.beginTXG, entry.endTXG)
        print "=== DBG - PRINT SUMMARY - END ==="
        print "=== DBG - PRINT LOGS - START ==="
        for log in self.p.logs.sms:
            print "txg", log.txg, "ms", len(log.metaslabs_flushed), "blks", log.blocks
        print "=== DBG - PRINT LOGS - END ==="

        for log in self.p.logs.sms:
            print ctxg, log.txg
            assert ctxg == log.txg

            entry = self.tab[tab_idx]
            if entry.endTXG < ctxg:
                assert entry.mss == msum
                assert entry.blks == bsum
                tab_idx += 1
                entry = self.tab[tab_idx]
                msum, bsum = 0, 0

            msum += len(log.metaslabs_flushed)
            bsum += log.blocks
            ctxg += 1
        # XXX: recheck assertion
        # print "W", tab_idx, (len(self.tab)-1)
        # assert tab_idx == len(self.tab) - 1

    def metaslabsToFlush(self, incoming):
        assert incoming > 0
        assert len(self.tab) > 0

        fullness = self.p.logs.nblocks - self.p.nmetaslabs
        assert fullness <= 0
        prediction = incoming
        candidates = []

        txg = 1
        fullness += prediction
        if fullness < 0:
            advance_txgs = int(math.ceil(math.fabs(fullness) / incoming))
            fullness += (incoming * advance_txgs)
        assert fullness >= 0

        # for entry in self.tab:
        #     continue

        # XXX: for now
        # return self.tab[0].mss
        return 5

    def flushTXGEntries(self, msflushed, blkgone):
        for entry in self.tab:
            if msflushed > entry.mss:
                assert blkgone >= entry.blks
                msflushed -= entry.mss
                blkgone -= entry.blks
                entry.flushTXG(entry.mss, entry.blks)
                assert entry.isFilled()
            else:
                entry.flushTXG(msflushed, blkgone)
                break

    def updateSummary(self, msflushed, blkgone, incoming):
        assert msflushed > 0

        # XXX: HACK!
        if blkgone < 0:
            blkgone = 0

        assert blkgone >= 0
        assert incoming > 0
        assert len(self.tab) >= 0

        if len(self.tab) == 0:
            self.addEntry(SummaryEntry(1, msflushed, incoming))
        elif self.tab[-1].isFilled():
            self.addEntry(SummaryEntry(self.tab[-1].endTXG + 1, msflushed, incoming))
        else:
            self.tab[-1].addTXG(msflushed, incoming)

        self.trimStart()
        assert len(self.tab) > 0

class HeuristicSummary2(object):
    def __init__(self, nmetaslabs):
        self.nmetaslabs = nmetaslabs
        self.pool = Pool(nmetaslabs)
        self.y_flushed = []
        self.y_blocks = []
        self.y_logs = []
        self.summary = Summary(self.pool)

    def condition_satisfied(self):
        if self.pool.mss.nmetaslabs() >= self.pool.logs.nblocks:
            return True
        return False

    def printAllLogs(self):
        print "=== DBG - PRINT LOGS - START ==="
        for log in self.pool.logs.sms:
            print "txg", log.txg, "ms", len(log.metaslabs_flushed), "blks", log.blocks
        print "=== DBG - PRINT LOGS - END ==="

    def printSummary(self):
        print "=== DBG - PRINT SUMMARY - START ==="
        for entry in self.summary.tab:
            print "range: {}-{}".format(entry.beginTXG, entry.endTXG)
        print "=== DBG - PRINT SUMMARY - END ==="

    def sync_cycle(self, incoming_blocks):
        if self.pool.syncing_txg == 0:
            all_metaslabs = self.pool.mss.ms_ordered_by_flushed
            self.pool.sync_new_changes(incoming_blocks, all_metaslabs)
            self.summary.updateSummary(len(all_metaslabs), 0, incoming_blocks)
            self.y_flushed.append(0) # Not really
            self.y_blocks.append(self.pool.logs.nblocks)
            self.y_logs.append(self.pool.logs.nlogs())
            self.pool.sync_done()
            return

        self.summary.verifySummary()
        nflushed = 0
        nflushed += self.summary.metaslabsToFlush(incoming_blocks)
        print "M", nflushed
        self.pool.flush_n_metaslabs(nflushed)
        self.summary.verifySummary()
        
        ms_flushed_this_txg = self.pool.mss.ms_ordered_by_flushed[-nflushed:]
        self.pool.sync_new_changes(incoming_blocks, ms_flushed_this_txg)
        print "Y",self.y_blocks[-1], self.pool.logs.nblocks
        self.summary.updateSummary(nflushed, self.y_blocks[-1] - self.pool.logs.nblocks, incoming_blocks)

        self.printAllLogs()
        self.printSummary()
        self.summary.verifySummary()

        print "DBG - ", self.pool.syncing_txg, " flushed", nflushed
        self.pool.sync_done()

        self.y_flushed.append(nflushed)
        self.y_blocks.append(self.pool.logs.nblocks)
        self.y_logs.append(self.pool.logs.nlogs())

    def simulate(self, y_incoming):
        for incoming in y_incoming:
            self.sync_cycle(incoming)
