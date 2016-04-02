
# dirty workaround for quick testing
import sys
import time

import task_buffet


def my_task(a, b):
    time.sleep(1)
    c = a + b
    return task_buffet.TASK_SUCCESS


def main():
    A = list(range(5))
    B = list(range(5,10))
    task_buffet.run(my_task, ['a', 'b'], [A, B], build_grid=True, buffet_name='test_buffet')

if __name__ == '__main__':
    main()
