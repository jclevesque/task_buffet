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
