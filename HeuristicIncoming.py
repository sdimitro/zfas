from Model import Pool, MetaslabGroup, LogSpaceMapGroup

#
# This heuristic works like this:
#    Flush exactly as many metaslabs as many incoming blocks
#    we've had.
#
class HeuristicIncoming(object):
    def __init__(self, nmetaslabs):
        self.nmetaslabs = nmetaslabs
        self.pool = Pool(nmetaslabs)
        self.y_flushed = []
        self.y_blocks = []
        self.y_logs = []
        self.last_incoming = 0

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
            self.last_incoming = incoming_blocks
            return

        nflushed = 0
        if not self.last_incoming == 0:
            self.pool.flush_n_metaslabs(self.last_incoming)
            nflushed += self.last_incoming
        assert nflushed <= self.nmetaslabs

        while not self.condition_satisfied():
            print "1:", len(self.pool.logs.sms[0].metaslabs_flushed), self.pool.logs.nlogs()
            self.pool.flush_n_metaslabs(1)
            nflushed += 1
            print "2:", len(self.pool.logs.sms[0].metaslabs_flushed), self.pool.logs.nlogs()
        assert nflushed <= self.nmetaslabs
        assert self.condition_satisfied()
        
        ms_flushed_this_txg = self.pool.mss.ms_ordered_by_flushed[-nflushed:]
        self.pool.sync_new_changes(incoming_blocks, ms_flushed_this_txg)

        print "DBG - ", self.pool.syncing_txg, " incoming", incoming_blocks, " flushed", nflushed
        self.pool.sync_done()
        self.last_incoming = incoming_blocks

        self.y_flushed.append(nflushed)
        self.y_blocks.append(self.pool.logs.nblocks)
        self.y_logs.append(self.pool.logs.nlogs())

        print "N", self.nmetaslabs, self.pool.logs.nblocks

    def simulate(self, y_incoming):
        for incoming in y_incoming:
            self.sync_cycle(incoming)
