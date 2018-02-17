import time

import task_buffet


def my_task(a, b):
    time.sleep(0.1)
    print(a + b)
    return task_buffet.TASK_SUCCESS


def main():
    A = list(range(5))
    B = list(range(5, 10))

    task_buffet.run_mp(2, my_task, ['a', 'b'], [A, B], build_grid=True,
        buffet_name='test_buffet_mp', fail_on_exception=False)


if __name__ == '__main__':
    main()
