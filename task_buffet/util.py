
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
