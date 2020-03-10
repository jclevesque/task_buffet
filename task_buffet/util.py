import functools
import psutil

def kill_proc_tree(pid, including_parent=True):
    # util function to kill a process and its children
    parent = psutil.Process(pid)
    children = parent.children(recursive=True)
    for child in children:
        child.terminate()
    gone, alive = psutil.wait_procs(children, timeout=1)
    for p in alive:
        p.kill()
    if including_parent:
        parent.kill()


# try to determine if tasks are identical, basically if they have the
# same parameters they are deemed identical
# recursive function
def tasks_eq(a, b):
    # TODO JCL: this is a mess and it shows that this method of `comparing tasks`
    # is probably not viable, refactor this asap
    if isinstance(a, str) or isinstance(b, str):
        return a == b
    elif isinstance(a, dict):
        if not isinstance(b, dict):
            return False

        if len(a) != len(b):
            return False

        keys_a = list(a.keys())
        keys_b = list(b.keys())

        # sort keys to be sure
        keys_a.sort()
        keys_b.sort()

        # make sure keys are the same
        if not tasks_eq(keys_a, keys_b):
            return False

        # compare values
        for k in a:
            if not tasks_eq(a[k], b[k]):
                return False
        return True
    elif hasattr(a, '__len__'):
        if not hasattr(b, '__len__'):
            return False

        if len(a) != len(b):
            return False

        for i in range(len(a)):
            if not tasks_eq(a[i], b[i]):
                return False
        return True
    elif isinstance(a, functools.partial):
        if not isinstance(b, functools.partial):
            return False

        return tasks_eq(a.func, b.func) and tasks_eq(a.args, b.args)
    else:
        return a == b
