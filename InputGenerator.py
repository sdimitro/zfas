import matplotlib.pyplot as plt
import numpy as np
import pylab
from random import *

class InputGenerator:
    @staticmethod
    def getSteady(incomingrate, txgs):
        return [incomingrate for i in range(txgs)]

    @staticmethod
    def getSingleStep(incomingrate, txgs, incomingrate2, txgs2):
        steady1 = [incomingrate for i in range(txgs)]
        steady2 = [incomingrate2 for i in range(txgs2)]
        return steady1 + steady2

    @staticmethod
    def getDiscreteUniform(min, max, n):
        return np.random.random_integers(min, max, n)

    @staticmethod
    def spitOutGeneratedInput(fn, arr):
        with open(fn, "w") as f:
            for elt in arr:
                f.write(str(elt)+ "\n")

    @staticmethod
    def readInGeneratedInput(fn):
        arr = []
        with open(fn, "r") as f:
            for line in f:
                arr.append(int(line))
        return arr

    @staticmethod
    def getSine(min, max, n):
        # x = pylab.arange(0, n, 0.1) - XXX parametrize this
        x = pylab.arange(0, n, 0.0060)
        ys = pylab.sin(x)
        final_y = []
        for y in ys:
            final_y.append(int(min + (max - min)/2 + y*(max - min)/2))
        return final_y[:n]

    @staticmethod
    def _spike(n, spikeness):
        spike = randint(0, spikeness) - int(spikeness / 2)
        newn = n - spike
        newn = max(newn, 1)
        newn = min(newn, 64)
        return newn

    @staticmethod
    def getSpikedSine(min, max, n, spikeness):
        ys = self.getSine(min, max, n)
        return [self._spike(y, spikeness) for y in ys]

    @staticmethod
    def getSingleSpike(min, max, n, spiketxg):
        assert spiketxg < n, "spiketxg cannot be more than # of txgs"
        incoming = [min for i in range(spiketxg)]
        incoming.append(max)
        for i in range(n - 1 - spiketxg):
            incoming.append(min)
        return incoming

