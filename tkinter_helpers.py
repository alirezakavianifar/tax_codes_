import sys
import os
import psutil
import multiprocess


ALL_PROCESSES = {}
LST_ON = {'soratmoamelat': {'on': False, 'pid': [0, 0]},
          'arzeshafzoodeh': {'on': False, 'pid': [0, 0]},
          'mash': {'on': False, 'pid': [0, 0]},
          'sanim': {'on': False, 'pid': [0, 0]},
          }


def thread_start(method, name, *args, **kwargs):
    global ALL_PROCESSES
    p1 = multiprocess.Process(
        target=lambda: method(field=kwargs['field']), daemon=True)
    p1.start()
    ALL_PROCESSES[name] = p1
    return p1.pid, p1._parent_pid


def kill_proc_tree(pid, parent_pid, including_parent=False):
    parent = psutil.Process(pid)
    children = parent.children(recursive=True)
    if not children:
        including_parent = True
    for child in children:
        try:
            child.kill()
        except psutil.NoSuchProcess:
            continue
    gone, still_alive = psutil.wait_procs(children, timeout=5)
    if including_parent:
        parent.kill()
        parent.wait(5)


def thread_kill(pid, parent_pid, name='soratmoamelat_start'):

    if name in ALL_PROCESSES.keys():
        # pid = os.getpid()
        kill_proc_tree(pid, parent_pid, False)
        # ALL_PROCESSES['tgju_start'].terminate()
        ALL_PROCESSES.pop(name, None)


def btn_handler(method, typeof='soratmoamelat', *args, **kwargs):
    global ALL_PROCESSES
    global LST_ON
    # global btn_tgju_text_init

    for key, value in LST_ON.items():
        if typeof == key:

            if not LST_ON[key]['on']:
                LST_ON[key]['on'] = True
                LST_ON[key]['pid'][0], LST_ON[key]['pid'][1] = thread_start(
                    method, name=typeof, field=kwargs['field'])
            else:
                thread_kill(LST_ON[key]['pid'][0],
                            LST_ON[key]['pid'][1], name=typeof)
                LST_ON[key]['on'] = False
