import psutil

def get_child_processes(parent_pid):
    child_processes = []

    try:
        process = psutil.Process(parent_pid)
        for child in process.children(recursive=True):
            child_processes.append(child.pid)
    except psutil.NoSuchProcess:
        pass

    return child_processes

def cleanup_processes(pid):
    # Terminate all subprocesses when the application is closed
    # Note: This is a simple example; you may need to adapt it based on your specific subprocess handling
    print('cleaning up')
    procs = get_child_processes(pid)
    print(procs)
    for proc in psutil.process_iter(['pid', 'cmdline']):
        if proc.pid in procs:
            proc.terminate()
