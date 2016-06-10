
import argparse
import os
import shutil
import sys

import numpy as np

from task_buffet import task_buffet


def buffet_cli(buffet_filename, reset_failed, reset_running, no_backup):
    with task_buffet.TaskBuffet(buffet_filename) as buffet:
        if not no_backup:
            shutil.copy(buffet_filename, buffet_filename + '.bkp')

        if reset_failed:
            failed = np.where(buffet.task_status == task_buffet.TASK_FAILED)[0]
            print("Resetting failed jobs to available: %s" % failed)
            buffet.task_status[failed] = task_buffet.TASK_AVAILABLE
            buffet.dump_buffet()

        if reset_running:
            run = np.where(buffet.task_status == task_buffet.TASK_RUNNING)[0]
            print("Resetting running jobs to available: %s" % run)
            buffet.task_status[run] = task_buffet.TASK_AVAILABLE
            buffet.dump_buffet()

    return 0


def main():
    parser = argparse.ArgumentParser("task-buffet-cli")
    parser.add_argument("buffet_filename", help="Name of the file containing"
        " the buffet, will be locked and manipulated by this program.")
    parser.add_argument("--no-backup", action="store_true",
        help="Skip backup saving step.")
        #metavar='')
    parser.add_argument("-f", action="store_true",
        help="Reset failed tasks.")
    parser.add_argument("-r", action="store_true",
        help="Reset running tasks.")

    args = parser.parse_args()

    if not os.path.exists(args.buffet_filename):
        raise Exception("Given buffet %s does not exist." % args.buffet_filename)

    if not args.f and not args.r:
        raise Exception("No action specified, nothing to do.")

    buffet_cli(args.buffet_filename, args.f, args.r, args.no_backup)


if __name__ == '__main__':
    main()
