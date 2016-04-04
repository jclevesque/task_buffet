
# dirty workaround for quick testing
import multiprocessing
import sys
import time

import task_buffet


def sub_job_mp(a):
    print(a)

    if a == 8:
        raise Exception("Test mp exception.")

def my_task(a, b):
    time.sleep(0.1)
    c = a + b

    pool = multiprocessing.Pool(2)
    pool.map(sub_job_mp, range(a, b))
    pool.close()

    return task_buffet.TASK_SUCCESS


def main():
    A = list(range(5))
    B = list(range(5,10))
    task_buffet.run(my_task, ['a', 'b'], [A, B], build_grid=True,
        buffet_name='test_buffet_mp', fail_on_exception=False)

if __name__ == '__main__':
    main()
