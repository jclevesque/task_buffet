# Copyright (C) 2015 Julien-Charles Levesque

import numpy as np


class ParamGrid():
    def __init__(self, names, values, meshgrid=False):
        if meshgrid:
            param_grid = nd_meshgrid(*values)
            param_grid = [p.flatten() for p in param_grid]
        else:
            param_grid = values

        self.names = names
        self.values = param_grid
        self.nvals = len(self.values[0])
        assert(np.all([len(v) == self.nvals for v in self.values]))

        self.nparams = len(names)
        self.shape = (self.nparams, self.nvals)

    def __getitem__(self, i):
        # Returns a dictionary with wrapped argument for position i
        return {n:v[i] for n, v in zip(self.names, self.values)}

    def __iter__(self, i):
        for i in range(self.nvals):
            yield {k:v[i] for k,v in grid.items()}

    def __eq__(self, comp):
        comp_names = np.all(np.array(self.names) == 
                    np.array(comp.names))
        comp_values = [np.all(comp.values[i] == self.values[i]) 
                       for i in range(self.nparams)]
        return comp_names & comp_values


def nd_meshgrid(*arrs):
    arrs = tuple(reversed(arrs))
    lens = list(map(len, arrs))
    dim = len(arrs)

    sz = 1
    for s in lens:
        sz *= s

    ans = []
    for i, arr in enumerate(arrs):
        slc = [1]*dim
        slc[i] = lens[i]
        arr2 = np.asarray(arr, dtype='object').reshape(slc)
        for j, sz in enumerate(lens):
            if j != i:
                arr2 = arr2.repeat(sz, axis=j)
        ans.append(arr2)

    return tuple(ans[::-1])
