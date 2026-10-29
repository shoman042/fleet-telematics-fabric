import random
import numpy as np

class SimulationRNG:
    def __init__(self, seed=42):
        self.seed = seed
        self.random = random.Random(seed)
        self.np_random = np.random.default_rng(seed)

    def reseed(self, new_seed):
        self.seed = new_seed
        self.random.seed(new_seed)
        self.np_random = np.random.default_rng(new_seed)