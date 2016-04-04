# Copyright (C) 2015 Julien-Charles Levesque
'''
Define and execute a series of tasks given distributed worker processes or nodes. The workers are said to pick tasks from a `buffet`, and they will keep eating tasks until there are no more to be found. Synchronization is based on file locks, so all worker must have access to the same filesystem.

First process to run and grab the buffet lock creates a task_status
 structure, and the subsequent steps are:

- lock the buffet and open it;
- pick something to execute;
- mark the task as being in execution;
- release the buffet for the next one in line;
- execute the task.

On a task's finished execution, the following steps are then executed:

- lock the buffet and open it;
- mark the previously executed task as done or failed;
- pick a new task to execute, mark it as so;
- release the buffet for the next one in line.

Note: since everything hangs on a file based locking mechanism, this probably
 will not scale up to hundreds of processes, or at least it will do so badly.
'''

import hashlib
import os
import pickle

import numpy as np

from . import grid
from . import file_lock

# constants
TASK_FAILED = -1
TASK_SUCCESS = 0
TASK_AVAILABLE = 1
TASK_RUNNING = 2


def run(task_function, task_param_names, task_param_values, buffet_name,
        build_grid=False, fail_on_exception=True):
    '''
    The scripts executing the task buffet should setup the description of the
     tasks to be executed and call this function when ready. This script should
     run on all worker nodes and be called with the same parameters to ensure
     proper execution.

    Parameters:
    -----------

    task_function: a function to call for the execution of a task, should take
        as input a list of parameters, described in `task_params`. The task
        function must return 0 if it succeeded and -1 if it failed.

    task_params: a list of parameters to draw upon, can be parameters used to
        build a grid, but the mode should then be set with `build_grid=True`

    build_grid: if true, build a mesh grid from the different parameters
        provided in `task_params`

    Notes:
    ------

    Parameter ordering in task_param_names & task_param_values will change the
     order in which tasks will be computed. First parameters are looped upon
     first, and the last parameter at the end.
    '''

    if build_grid:
        param_grid = grid.nd_meshgrid(*task_param_values)
        param_grid = [p.flatten() for p in param_grid]
        n = len(param_grid[0])
    else:
        n = len(task_param_values[0])
        assert(np.all([len(vals) == n for vals in task_param_values]))
        param_grid = task_param_values

    # buffet_params contains the raw data for the tasks to execute, whereas
    # buffet will contain the status of each task
    buffet_params = grid.ParamGrid(task_param_names, param_grid)

    #if buffet_name is None:
        # Determine a uid for the current task setup -- if param_grid contains dicts this will NOT work
        # all in all automatically determining this sounds like a bad idea.
        #buffet_name = 'buffet_' + hashlib.md5(pickle.dumps([task_param_names, param_grid])).hexdigest()

    task_i = 0
    while task_i >= 0:
        with TaskBuffet(buffet_name, buffet_params) as buffet:
            task_i, task_p = buffet.get_next_free()
            if task_i < 0:
                continue
            # Release buffet/lock

        print("running task with parameters: %s" % task_p)
        # might be a long function call, insert time managing stuff
        # around here
        try:
            status = task_function(**task_p)
            if status not in [TASK_FAILED, TASK_SUCCESS]:
                raise Exception("Wrong status returned.")
        except Exception as exc:
            if fail_on_exception:
                print("Caught exception in job %s, stopping." % task_p)
                raise
            else:
                print("Job %s failed with exception %s, marking as failed." %
                    (task_p, exc))
                status = TASK_FAILED

        with TaskBuffet(buffet_name, buffet_params) as buffet:
            buffet.update_task(task_i, status)

    print("Done executing all tasks in the buffet.")


class TaskBuffet:
    def __init__(self, buffet_name, task_params):
        self.name = buffet_name
        self.task_params = task_params
        self.lock = file_lock.Locker(self.name)

    def __enter__(self):
        self.lock.acquire()
        self.access_buffet()
        return self

    def __exit__(self, *_exc):
        self.lock.release()

    def access_buffet(self):
        # Check if the job running with lock is the first job to execute
        if not os.path.exists(self.name):
            # Arrange the buffet
            self.setup_buffet()
        else:
            self.open_buffet()

    def setup_buffet(self):
        self.task_status = np.ones(self.task_params.nvals, dtype=int) * TASK_AVAILABLE
        self.dump_buffet()

    def dump_buffet(self):
        f = open(self.name, 'wb')
        pickle.dump(self.task_status, f)
        f.close()

    def open_buffet(self):
        f = open(self.name, 'rb')
        self.task_status = pickle.load(f)
        f.close()

    def get_next_free(self):
        free = np.where(self.task_status == TASK_AVAILABLE)[0]
        if len(free) == 0:
            return -1, {}
        else:
            i = free[0]
            self.task_status[i] = TASK_RUNNING
            self.dump_buffet()
            return i, self.task_params[i]

    def update_task(self, task_i, status):
        self.task_status[task_i] = status
        self.dump_buffet()
