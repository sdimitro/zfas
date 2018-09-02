#!/usr/bin/python

import argparse

from InputGenerator import InputGenerator
from Reporter import Reporter
from HeuristicLimitOnly import HeuristicLimitOnly
from HeuristicAvgBlock import HeuristicAvgBlock
from HeuristicAvgMetaslab import HeuristicAvgMetaslab
from HeuristicAvgMsBlockLog import HeuristicAvgMsBlockLog
from HeuristicIncoming import HeuristicIncoming
from HeuristicMTable import HeuristicMTable
from HeuristicRSTable import HeuristicRSTable
from HeuristicRSTable2 import HeuristicRSTable2
from HeuristicSummary import HeuristicSummary
from HeuristicSummary2 import HeuristicSummary2
from HeuristicSummary4 import HeuristicSummary4

def main():
    #
    # Parse arguments
    #
    parser = argparse.ArgumentParser(description='ZFS flushing algorithm simulator')
    parser.add_argument('--txgs', type=int, help='number of TXGs', default=1024)
    parser.add_argument('--txgs2', type=int, help='number of TXGs_2', default=1024)
    parser.add_argument('--heuristic', choices=['limit', 'avgblock', 'avgms',
        'avgmsblocklog', 'incoming', 'mtable', 'rstable', 'summary', 'summary2',
        'summary4', 'rstable2'],
        help='heuristic', default='summary4')
    parser.add_argument('--metaslabs', type=int, help='number of metaslabs',
        default=1024)
    parser.add_argument('--blocklimit', type=int,
        help='block limit (applicable only with summary{,2,4})',
        default=1024)
    parser.add_argument('--incomingrate', type=int,
        help='number of incoming blocks by TXG (applicable only with steady)',
        default=24)
    parser.add_argument('--incomingrate2', type=int,
        help='number of incoming blocks by TXG_2 (applicable only with singlestep)',
        default=48)
    parser.add_argument('--datagen', choices=['steady', 'sine', 'spikedsine',
        'discreteuni', 'singlespike', 'singlestep', 'file'],
        help='input data generator func', default='steady')
    parser.add_argument('--ifile', type=str, help='file to save test input data',
        default='test.input')
    parser.add_argument('--ofile', type=str, help='file to save test output data',
        default='test.output')
    parser.add_argument('--verbosity', type=int, help='verbosity level', default=1)
    parser.add_argument('--visuals', type=int, help='visualizations level', default=1)
    parser.add_argument('--spikediff', type=int,
        help='spike differential (applicable only with singlespike and spikedsine)',
        default=20)
    parser.add_argument('--iblockmin', type=int,
        help='incoming block minimum (applicable only with sine, spikedsine, and discreteuni)',
        default=1)
    # default value was chosen based on the assumption that ZFS can't do more writes than
    # the amount that fits within 64 log blocks (at least currently)
    parser.add_argument('--iblockmax', type=int,
        help='incoming block maximum (applicable only with sine, spikedsine, and discreteuni)',
        default=64)
    parser.add_argument('--spiketxg', type=int,
        help='txg of single spike (applicable only with singlespike)',
        default=64)
    args = parser.parse_args()

    #
    # Choose heuristic
    #
    if args.heuristic == 'limit':
        heuristic = HeuristicLimitOnly(args.metaslabs)
    elif args.heuristic == 'avgblock':
        heuristic = HeuristicAvgBlock(args.metaslabs)
    elif args.heuristic == 'avgms':
        heuristic = HeuristicAvgMetaslab(args.metaslabs)
    elif args.heuristic == 'avgmsblocklog':
        heuristic = HeuristicAvgMsBlockLog(args.metaslabs)
    elif args.heuristic == 'incoming':
        heuristic = HeuristicIncoming(args.metaslabs)
    elif args.heuristic == 'mtable':
        heuristic = HeuristicMTable(args.metaslabs)
    elif args.heuristic == 'rstable':
        heuristic = HeuristicRSTable(args.metaslabs)
    elif args.heuristic == 'rstable2':
        heuristic = HeuristicRSTable2(args.metaslabs)
    elif args.heuristic == 'summary':
        heuristic = HeuristicSummary(args.metaslabs, args.blocklimit)
    elif args.heuristic == 'summary2':
        heuristic = HeuristicSummary2(args.metaslabs, args.blocklimit)
    elif args.heuristic == 'summary4':
        heuristic = HeuristicSummary4(args.metaslabs, args.blocklimit)
    else:
        # Should never go here (see default heuristic)
        return

    #
    # Use or generate test input
    #
    txgs = args.txgs
    if args.datagen == 'steady':
        incoming_pertxg = InputGenerator.getSteady(args.incomingrate, txgs)
        InputGenerator.spitOutGeneratedInput(args.ifile, incoming_pertxg)
    elif args.datagen == 'singlestep':
        incoming_pertxg = InputGenerator.getSingleStep(args.incomingrate, txgs, args.incomingrate2, args.txgs2)
        txgs += args.txgs2
        InputGenerator.spitOutGeneratedInput(args.ifile, incoming_pertxg)
    elif args.datagen == 'discreteuni':
        incoming_pertxg = InputGenerator.getDiscreteUniform(args.iblockmin, args.iblockmax, txgs)
        InputGenerator.spitOutGeneratedInput(args.ifile, incoming_pertxg)
    elif args.datagen == 'sine':
        incoming_pertxg = InputGenerator.getSine(args.iblockmin, args.iblockmax, txgs)
        InputGenerator.spitOutGeneratedInput(args.ifile, incoming_pertxg)
    elif args.datagen == 'spikedsine':
        incoming_pertxg = InputGenerator.getSpikedSine(args.iblockmin, args.iblockmax, txgs, args.spikediff)
        InputGenerator.spitOutGeneratedInput(args.ifile, incoming_pertxg)
    elif args.datagen == 'singlespike': ### XXX
        incoming_pertxg = InputGenerator.getSingleSpike(args.iblockmin, args.iblockmax, txgs, args.spiketxg)
        InputGenerator.spitOutGeneratedInput(args.ifile, incoming_pertxg)
    elif args.datagen == 'file':
        incoming_pertxg = InputGenerator.readInGeneratedInput(args.ifile)
        txgs = len(incoming_pertxg)
    else:
        # Should never go here (see default datagen)
        return

    #
    # Run simulation
    #
    heuristic.simulate(incoming_pertxg) # XXX: maybe make it spit out the data?

    rep = Reporter(args.ofile)  # XXX: visuals and verbosity?
    x_txgs = [i for i in range(txgs)]
    rep.incoming_per_txg(x_txgs, incoming_pertxg)
    rep.flushes_per_txg(x_txgs, heuristic.y_flushed)
    rep.blocks_per_txg(x_txgs, heuristic.y_blocks)
    rep.logs_per_txg(x_txgs, heuristic.y_logs)
    rep.flush_stats(heuristic.y_flushed, heuristic.nmetaslabs)
    rep.show_and_close()
  
if __name__== "__main__":
    #
    # sample txgs = [4096, 8192, 16384] ...
    #
    # datagen samples:
    #    steady        (24)
    #    spikedsine    (2, 64, txgs, 50)
    #    spikedsine    (2, 64, txgs, 40)
    #    spikedsine    (2, 64, txgs, 30)
    #    spikedsine    (2, 64, txgs, 20)
    #    sine          (0, 64, txgs)
    #    sine          (2, 64, txgs)
    #    discreteuni   (0, 64, txgs)
    #    discreteuni   (0, 10, txgs)
    #    discreteuni   (1, 64, txgs)
    #
    # heuristic samples:
    #    LimitOnly     (1024)
    #    Incoming      (1024)
    #    AvgBlock      (1024)
    #    AvgMetaslab   (1024)
    #    AvgMetaslab   (2048)
    #    AvgMetaslab   (512)
    #    AvgMetaslab   (127)
    #    AvgMsBlockLog (1024)
    #    AvgMsBlockLog (2048)
    #    AvgMsBlockLog (8192)
    #    AvgMTable     (1024)
    #    RSTable       (1024)
    #    RSTable       (100)
    #    RSTable(2)    (1024)
    #    RSTable(2)    (200)
    #    Summary(2)    (1024)
    #    Summary(4)    (1024, 1024)
    #    Summary(4)    (1024, 200)
    #    Summary       (1024, 1024)
    #    Summary       (8192, 1024)
    #    Summary       (1024, 4096)
    #    ...
    #
    main()
