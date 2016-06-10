# Copyright (C) 2016 Julien-Charles Levesque
'''
Define and execute a series of tasks given distributed worker processes or
 nodes. The workers are said to pick tasks from a `buffet`, and they will
 keep eating tasks until there are no more to be found. Synchronization is
 based on file locks, so all worker must have access to the same filesystem.

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

import bz2
import os
import pickle
import time
import traceback

import numpy as np

from . import grid
from . import file_lock

# constants
TASK_FAILED = -1
TASK_SUCCESS = 0
TASK_AVAILABLE = 1
TASK_RUNNING = 2


def run(task_function, task_param_names, task_param_values, buffet_name,
        build_grid=False, fail_on_exception=True, time_budget=None):
    '''
    The scripts executing the task buffet should setup the description of the
     tasks to be executed and call this function when ready. This script should
     run on all worker nodes and be called with the same parameters to ensure
     proper execution.

    Parameters:
    -----------

    task_function: a function to call for the execution of a task, should take
        as input a list of parameters, described in task_param names and
        values. The task function must return 0 if it succeeded and -1
        if it failed.

    task_param_names: names of the parameters to draw upon. The ordering of
        parameters must match that of `task_param_values`.

    task_param_values: list of values for corresponding parameters.
        If `build_grid` is true, a mesh grid is built with each unique
        parameter value. If `build_grid` is False, must have a shape
        n_params x n_jobs, with different values for each parameter on each
        column.

    build_grid: if true, build a mesh grid from the different parameters
        provided in `task_params`

    Notes:
    ------

    In the case of a grid, parameter ordering in task_param_names &
     task_param_values will change the order in which tasks will be
     computed. First parameters are looped upon first, and the last
     parameter at the end.
    '''
    time_start = time.time()
    out_of_time = False

    task_i = 0
    while task_i >= 0:
        if time_budget is not None:
            time_left = time_budget - (time.time() - time_start)
            # if there is no time left, we can stop in here
            if time_left < 0:
                out_of_time = True
                break

        # Next call locks the buffet
        with TaskBuffet(buffet_name, task_param_names, task_param_values,
                build_grid=build_grid) as buffet:
            task_i, task_p = buffet.get_next_free()
            if task_i < 0:
                continue
        # Release buffet/lock

        print("running task with parameters: %s" % task_p)
        # Will not force a task to exit, because that would require a separate
        # process. Give the time left to the task_func and let it handle it
        if time_budget is not None:
            task_p['time_left'] = time_left

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
                print(traceback.format_exc())
                status = TASK_FAILED

        # Lock buffet again to update it
        with TaskBuffet(buffet_name) as buffet:
            buffet.update_task(task_i, status)

    if out_of_time:
        print("Ran out of time.")
    else:
        print("Done executing all tasks in the buffet.")


class TaskBuffet:
    def __init__(self, buffet_name, task_param_names=None,
            task_param_values=None, build_grid=False):

        self.name = buffet_name
        self.task_param_names = task_param_names
        self.task_param_values = task_param_values
        self.task_params = None

        self.build_grid = build_grid
        self.lock = file_lock.Locker(self.name)

    def __enter__(self):
        self.lock.acquire()
        self.access_buffet()
        return self

    def __exit__(self, *_exc):
        self.lock.release()

    def __del__(self,):
        if self.lock.i_am_locking():
            self.lock.release()

    def access_buffet(self):
        # Check if the job running with lock is the first job to execute
        if not os.path.exists(self.name):
            # Arrange the buffet
            self.setup_new_buffet()
        else:
            self.open_buffet()

    def setup_new_buffet(self):
        """ Initialize a new buffet, must have received task_param_values
         and names in constructor"""
        if self.task_param_names is None and self.task_param_values is None:
            raise Exception("Uninitialized task_param names and values, cannot"
            " setup a new buffet.")

        # task_params contains the raw data for the tasks to execute, whereas
        # buffet will contain the status of each task
        self.task_params = grid.ParamGrid(self.task_param_names,
            self.task_param_values, meshgrid=self.build_grid)

        self.task_status = np.ones(self.task_params.nvals, dtype=int) * TASK_AVAILABLE
        self.dump_buffet()

    def dump_buffet(self):
        f = bz2.open(self.name, 'wb')
        pickle.dump(self.task_status, f)
        pickle.dump(self.task_params, f)
        f.close()

    def open_buffet(self):
        # Find filetype
        try:
            f = bz2.open(self.name, 'rb')
            f.read()
            f.seek(0)
        except:
            f = open(self.name, 'rb')

        self.task_status = pickle.load(f)
        try:
            self.task_params = pickle.load(f)
        except:
            print("Warning: unable to load task params. could be an old"
                " buffet or something is wrong. Will not be able to"
                " launch new tasks.")
            self.task_params = None
        f.close()

        # Check for compatibility with whatever buffet was loaded
        self.check_merge_buffets()

    def check_merge_buffets(self, merge=True):
        '''
        See if the new buffet corresponds to whatever was saved.
        '''
        if self.task_param_names is not None:
            saved_g = self.task_params
            new_g = grid.ParamGrid(self.task_param_names,
                self.task_param_values, self.build_grid)

            if saved_g == new_g:
                # Task buffets identical, nothing to see here carry on
                return
            elif not merge:
                raise Exception("Task buffet saved in %s differs from"
                    " parameters passed right now. Saved grid: %s, new"
                    " grid: %s" % (self.name, saved_g, new_g))
            else:
                print("Merging experiments into new frame.")
                new_task_status = np.ones(new_g.nvals, dtype=int)\
                    * TASK_AVAILABLE

                for p in range(saved_g.nvals):
                    saved_p = saved_g[p]
                    i = 0
                    while saved_p != new_g[i]:
                        i += 1
                        if i == new_g.nvals:
                            raise Exception("Unable to find match for a task"
                                " while merging buffets: %s" % saved_p)
                    print("DEBUG: Match for saved_g %i is new_g %i" % (p, i))
                    new_task_status[i] = self.task_status[p]
                self.task_status = new_task_status
                self.task_params = new_g
                self.dump_buffet()

    def get_next_free(self):
        if self.task_params is None:
            raise Exception("Uninitialized task_params, cannot return free"
                " params.")

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

    def print_status(self, ):
        sz = self.get_size()
        print("%i tasks finished, %i tasks failed, %i tasks running,"
            " and %i tasks available out of a total of %i tasks." %
            (np.sum(self.task_status == TASK_SUCCESS),
            np.sum(self.task_status == TASK_FAILED),
            np.sum(self.task_status == TASK_RUNNING),
            np.sum(self.task_status == TASK_AVAILABLE),
            sz))

    def get_size(self, ):
        return len(self.task_status)
