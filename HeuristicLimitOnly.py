from Model import Pool, MetaslabGroup, LogSpaceMapGroup

#
# This heuristic works like this:
#   1] While # of log blocks >== # of metaslabs, keep flushing
#   2] If we have haven't flushed anything flush one metaslab
#
class HeuristicLimitOnly(object):
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
        while not self.condition_satisfied():
            self.pool.flush_n_metaslabs(1)
            nflushed += 1

        if nflushed == 0:
            self.pool.flush_n_metaslabs(1)
            nflushed += 1
        
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
