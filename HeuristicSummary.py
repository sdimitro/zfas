from Model import Pool, MetaslabGroup, LogSpaceMapGroup
import math

q = 100

class SummaryEntry(object):
    #
    # s - startTXG
    # e - endTXG
    # m - metaslabs needed to flush
    # b - blocks that would be freed
    #
    def __init__(self, s, m, b):
        self.s, self.e = s, s
        self.m, self.b = m, b

    def isObsolete(self):
        if self.m == 0:
            assert self.b == 0, "should be 0 but it is %r" % self.b
            return True
        assert self.m > 0
        assert self.b > 0
        return False

    def isFull(self):
        global q
        if self.b < q:
            return False
        return True

    def addToEntry(self, t, m, b):
        assert (self.e + 1) == t
        self.e += 1
        self.m += m
        self.b += b

    def removeFromEntry(self, m, b):
        self.m -= m
        self.b -= b

    def toS(self):
        return "[{} - {}] M - {} B - {}".format(self.s, self.e, self.m, self.b)

class Summary(object):
    def __init__(self, pool):
        self.p = pool
        self.tab = []

    def addData(self, txg, mflushed, nblocks):
        if len(self.tab) == 0 or self.tab[-1].isFull():
            self.tab.append(SummaryEntry(txg, mflushed, nblocks))
        else:
            #
            # The thing to note here is that the entry can
            # be full after wards (i.e. not a hard limit).
            #
            self.tab[-1].addToEntry(txg, mflushed, nblocks)

    def trimData(self, mflushed, blkgone):
        assert blkgone >= 0
        n = 0
        for entry in self.tab:
            if entry.m >= mflushed:
                assert entry.b >= blkgone
                entry.removeFromEntry(mflushed, blkgone)
                if entry.isObsolete():
                    n += 1
                break
            else:
                assert entry.b <= blkgone, "%r %r" % (entry.b, blkgone)
                mflushed -= entry.m
                blkgone  -= entry.b
                entry.removeFromEntry(entry.m, entry.b)
                n += 1
        for i in range(n):
            assert self.tab[i].isObsolete(), "about to delete entry %r which is not obsolete" % i
        self.tab = self.tab[n:]

    def adviceFlushing(self, incoming, blimit):
        print "=== DBG - PRINT FLUSH TAB - START ==="
        maxflush = 1

        futureTXGs = 1
        budget = blimit - self.p.logs.nblocks - incoming
        print "budget = {} , blks = {}, incom = {}".format(budget, self.p.logs.nblocks, incoming)

        deficit = 0
        if budget > 0:
            futureTXGs += int(math.ceil(budget / incoming))
            deficit = budget - (futureTXGs * incoming)
        else:
            deficit = budget
        print "deficit = {} futureTXG = {}".format(deficit, futureTXGs)

        assert deficit <= 0

        accms = 0
        accblks = deficit
        for e in self.tab:
            accms += e.m
            accblks += e.b
            print "b = {} m = {} - bsum = {} msum = {}".format(e.b, e.m, accms, accblks)
            if accblks < 0:
                continue
            toflush = int(math.ceil(accms / futureTXGs))
            print "cmax = {} c = {}".format(maxflush, toflush)
            maxflush = max(maxflush, toflush)
            futureTXGs += int(math.floor(accblks / incoming))
            print "futureTXGs = {}".format(futureTXGs)

        print "=== DBG - PRINT FLUSH TAB - END ==="
        return maxflush

    def printSummary(self):
        print "=== DBG - PRINT SUMMARY - START ==="
        for e in self.tab:
            print e.toS()
        print "=== DBG - PRINT SUMMARY - END ==="

    def crossVerifySummary(self):
        row, msum, bsum = 0, 0, 0
        for log in self.p.logs.sms:
            e = self.tab[row]
            assert e.s <= log.txg and log.txg <= e.e
            msum += len(log.metaslabs_flushed)
            bsum += log.blocks
            assert e.m <= msum and e.b <= bsum
            if e.e == log.txg:
                row += 1
                assert e.m == msum and e.b == bsum
                msum, bsum = 0, 0

class HeuristicSummary(object):
    def __init__(self, nmetaslabs, blimit):
        self.nmetaslabs = nmetaslabs
        self.pool = Pool(nmetaslabs)
        self.y_flushed, self.y_blocks, self.y_logs = [], [], []
        self.summary = Summary(self.pool)
        self.blimit = blimit

        #XXX: Hack
        global q
        q = int(blimit / 10)

    def printLogs(self):
        print "=== DBG - PRINT LOGS - START ==="
        for log in self.pool.logs.sms:
            print "[{}] - M {} - B {}".format(log.txg, len(log.metaslabs_flushed), log.blocks)
        print "=== DBG - PRINT LOGS - END ==="

    def condition_satisfied(self):
        if self.pool.mss.nmetaslabs() >= self.pool.logs.nblocks:
            return True
        return False

    def addGraphEntry(self, nflushed):
        self.y_flushed.append(nflushed)
        self.y_blocks.append(self.pool.logs.nblocks)
        self.y_logs.append(self.pool.logs.nlogs())

    def initializeState(self, incoming):
        all_metaslabs = self.pool.mss.ms_ordered_by_flushed
        self.pool.sync_new_changes(incoming, all_metaslabs)
        self.summary.addData(self.pool.syncing_txg, len(all_metaslabs), incoming)
        self.summary.trimData(0, self.pool.logs.nblocks - incoming)
        print "DBG - TXG: {} - Flushed: {} ".format(self.pool.syncing_txg, len(all_metaslabs))
        self.summary.printSummary()
        self.pool.sync_done()
        self.addGraphEntry(len(all_metaslabs))

    def sync_cycle(self, incoming_blocks):
        if self.pool.syncing_txg == 0:
            self.initializeState(incoming_blocks)
            return

        nflushed = self.summary.adviceFlushing(incoming_blocks, self.blimit)
        self.pool.flush_n_metaslabs(nflushed)
        ms_flushed_this_txg = self.pool.mss.ms_ordered_by_flushed[-nflushed:]
        self.pool.sync_new_changes(incoming_blocks, ms_flushed_this_txg)
        self.summary.addData(self.pool.syncing_txg, nflushed, incoming_blocks)
        self.summary.trimData(nflushed, self.y_blocks[-1] -self.pool.logs.nblocks +incoming_blocks)
        print "DBG - TXG: {} - Flushed: {} ".format(self.pool.syncing_txg, nflushed)
        self.summary.printSummary()
        self.pool.sync_done()
        self.addGraphEntry(nflushed)

    def simulate(self, y_incoming):
        for incoming in y_incoming:
            self.sync_cycle(incoming)
