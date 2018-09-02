import datetime
import math
import matplotlib.pyplot as plt

class Reporter(object):
    def __init__(self, name):
        self.name = name
        self.plots = []

    def flushes_per_txg(self, x_txgs, y):
        fig, ax = plt.subplots()
        ax.set_ylabel('# of flushes', color='r')
        ax.set_ylim(0, max(y) + 1) # XXX: may not need that
        ax.set_xlabel('TXG')
        ax.set_xticks(x_txgs, minor=True)
        ax.plot(x_txgs, y, 'r')
        self.plots.append((fig, ax, "f"))

    def incoming_per_txg(self, x_txgs, y):
        fig, ax = plt.subplots()
        ax.set_ylabel('# of incoming log blocks', color='b')
        ax.set_ylim(0, max(y) + 1) # XXX: may not need that
        ax.set_xlabel('TXG')
        ax.set_xticks(x_txgs, minor=True)
        ax.plot(x_txgs, y, 'b')
        self.plots.append((fig, ax, "i"))

    def logs_per_txg(self, x_txgs, y):
        fig, ax = plt.subplots()
        ax.set_ylabel('# of logs', color='y')
        ax.set_ylim(0, max(y) + 1) # XXX: may not need that
        ax.set_xlabel('TXG')
        ax.set_xticks(x_txgs, minor=True)
        ax.plot(x_txgs, y, 'y')
        self.plots.append((fig, ax, "l"))

    def blocks_per_txg(self, x_txgs, y):
        fig, ax = plt.subplots()
        ax.set_ylabel('# of log blocks', color='g')
        ax.set_ylim(0, max(y) + 1) # XXX: may not need that
        ax.set_xlabel('TXG')
        ax.set_xticks(x_txgs, minor=True)
        ax.plot(x_txgs, y, 'g')
        self.plots.append((fig, ax, "b"))

    def _flush_stats_str(self, y_flushes, nmetaslabs):
        str_out = ""
        total_flushes = sum(y_flushes)
        str_out += "# of metaslabs: " + str(nmetaslabs)
        str_out += "\ntotal # of flushes: " + str(total_flushes)
        str_out += "\n# of flushes per TXG: " + str(math.ceil(total_flushes / len(y_flushes)))
        str_out += "\nmax # of flushes in TXG: " + str(max(y_flushes))
        return str_out

    def flush_stats(self, y_flushes, nmetaslabs):
        print self._flush_stats_str(y_flushes, nmetaslabs)

    def show_and_close(self):
        plt.show()
        plt.close()

    def spit_out_figures(self):
        for fig, ax, type in self.plots:
            fig.savefig(self.name + "-" + type + "-" + str(datetime.datetime.now()) + ".png")

    def spit_out_flush_results(self, y_flushes, nmetaslabs):
        with open(self.name + "-r-" + str(datetime.datetime.now()) + ".txt", "w") as f:
            f.write(self._flush_stats_str(y_flushes, nmetaslabs))

    def close(self):
        plt.close()
