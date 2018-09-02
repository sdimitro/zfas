import math
import numpy as np

#
# Metaslab:
# The object being flushed to disk.
#
#   Its properties are:
#   1] ID - so it can be identified.
#   2] Last TXG flushed -
#      to identify when it was last flushed.
#
class Metaslab(object):
    def __init__(self, id):
        #
        # The ID is mainly used for debugging the simulator
        # and the model. It should not be used for any
        # explictiy control flow or decision making.
        #
        self.id = id

        #
        # @Assumption+
        # Metaslabs with that haven't been flushed yet have
        # a flushed TXG field of 0.
        # @Assumption-
        #
        self.flushed = 0

    def flush(self, txg):
        #
        # Assert that we are not setting the flushed TXG to
        # a past TXG.
        #
        # print self.flushed, txg
        assert self.flushed < txg
        self.flushed = txg

#
# Metaslab Pool Group:
# Not to be confused with Metaslab Group in ZFS terms.
# This is an object that holds all the metaslabs within
# a pool, not just a vdev. The reason of this object is
# to ensure extra assertion for the correctness of the
# simulator and potentially help in the model's validation.
#
class MetaslabGroup(object):
    def __init__(self, nmetaslabs):
        self.ms_ordered_by_flushed = [Metaslab(i) for i in range(nmetaslabs)]

    def nmetaslabs(self):
        return len(self.ms_ordered_by_flushed)

    #
    # @Assumption+
    # In a real-system we may skip flushing a metaslab if
    # it is currently loading and either skip to the next
    # one or just stop flushing. The simulator assumes
    # that this rarely happens and completely omits this
    # scenario.
    # @Assumption-
    #
    def update_flushed_order(self, nflushes, txg):
        prev_size = len(self.ms_ordered_by_flushed)

        #
        # Update the flushed TXG of the metaslabs
        # that were just flushed.
        #
        l = self.ms_ordered_by_flushed
        for ms in l[:nflushes]:
            ms.flush(txg)

        #
        # Rotate the order of the metaslabs.
        #
        self.ms_ordered_by_flushed = l[nflushes:] + l[:nflushes]

        #
        # Ensure we didn't screw up anything with the indexes
        # above.
        #
        assert prev_size == len(self.ms_ordered_by_flushed)

    def assert_flush_order(self):
        prev_flushed_txg = -1
        for ms in self.ms_ordered_by_flushed:
            assert prev_flushed_txg <= ms.flushed
            prev_flushed_txg = ms.flushed

    def assert_ms_uniqueness(self):
        ms_ids = [ms.id for ms in self.ms_ordered_by_flushed]
        assert np.unique(ms_ids).size == len(ms_ids)

#
# Log Space Map:
# Contains all incoming blocks for a given TXG. The simulator
# augments this structure to also include the metaslabs that
# were last flushed on this TXG.
#
class LogSpacemap(object):
    def __init__(self, b, t, m):
        self.blocks = b
        self.txg = t
        self.metaslabs_flushed = m

    def assert_ms_flushed_txg(self):
        for ms in self.metaslabs_flushed:
            assert self.txg == ms.flushed

    def flush_metaslabs(self, n):
        assert n <= len(self.metaslabs_flushed)
        self.metaslabs_flushed = self.metaslabs_flushed[n:]

    def isIrrelevant(self):
        if len(self.metaslabs_flushed) == 0:
            return True
        return False

#
# LogSpaceMapGroup:
# The set of Log Space Maps in a pool.
#
class LogSpaceMapGroup(object):
    def __init__(self):
        self.sms = []
        self.nblocks = 0
        self.nms = 0

    def nlogs(self):
        return len(self.sms)

    def assert_ms_flushed_txgs(self):
        for sm in self.sms:
            sm.assert_ms_flushed_txg()

    def assert_nblocks(self):
        n = 0
        for sm in self.sms:
            n += sm.blocks
        assert n == self.nblocks

    def assert_nms(self):
        n = 0
        for sm in self.sms:
            n += len(sm.metaslabs_flushed)
        assert n == self.nms

    def assert_log_txg_order(self):
        prev_txg = -1
        for sm in self.sms:
            assert prev_txg < sm.txg
            prev_txg = sm.txg

    def assert_no_irrelevant_logs(self):
        for sm in self.sms:
            assert not sm.isIrrelevant()

    def log_insert(self, log):
        self.sms.append(log)
        self.nblocks += log.blocks
        self.nms += len(log.metaslabs_flushed)

    def _log_pop(self):
        assert self.sms[0].isIrrelevant()

        self.nblocks -= self.sms[0].blocks
        assert self.nblocks >= 0

        self.nms -= len(self.sms[0].metaslabs_flushed)
        assert self.nms >= 0

        self.sms = self.sms[1:]

    def _flush_from_next_log(self, n):
        oldest_log = self.sms[0]
        to_flush = min(len(oldest_log.metaslabs_flushed), n)
        oldest_log.flush_metaslabs(to_flush)
        if oldest_log.isIrrelevant():
            self._log_pop()
        return (n - to_flush)

    def update_flushed_metaslab_data(self, nflushed):
        left = nflushed
        while left > 0:
           left = self._flush_from_next_log(left) 

    def avg_blocks_per_log(self):
        #
        # Note: We always round up
        #
        return math.ceil(self.nblocks / self.nlogs())

    def avg_ms_flushed_per_log(self):
        #
        # Note: We always round up
        #
        return math.ceil(self.nms / self.nlogs())

class Pool(object):
    def __init__(self, nmetaslabs):
        self.mss = MetaslabGroup(nmetaslabs)
        self.logs = LogSpaceMapGroup()
        self.syncing_txg = 0
        self.nmetaslabs = nmetaslabs

    def flush_n_metaslabs(self, n):
        assert n <= self.mss.nmetaslabs()
        self.mss.update_flushed_order(n, self.syncing_txg)
        self.logs.update_flushed_metaslab_data(n)

    def sync_done(self):
        self.syncing_txg += 1

    def sync_new_changes(self, incoming_blocks, metaslabs_flushed):
        new_log = LogSpacemap(incoming_blocks, self.syncing_txg, metaslabs_flushed)
        self.logs.log_insert(new_log)

    def assert_pool_metadata(self):
        self.logs.assert_ms_flushed_txgs()
        self.logs.assert_log_txg_order()
        self.logs.assert_no_irrelevant_logs()
        self.logs.assert_nblocks()
        self.logs.assert_nms()
        self.mss.assert_flush_order()
        self.mss.assert_ms_uniqueness()

    def set_state(self):
        # STUB: To be used for initial transient removal.
        pass
