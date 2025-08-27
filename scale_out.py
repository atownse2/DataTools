import os
import textwrap
import subprocess
import pickle
import uuid

from tools import cache
tools_dir = os.path.dirname(os.path.abspath(__file__))

condor_dir = cache.ensure_cache('condor')
submit_dir = cache.ensure_cache('condor/submit')
tasks_dir = cache.ensure_cache('condor/tasks')

run_in_cmssw = f"{tools_dir}/run_in_cmssw_env.sh"
run_in_mamba = f"{tools_dir}/run_in_mamba_env.sh"
task_worker = os.path.abspath(__file__)

class Task:
    def __init__(self, func, args, kwargs, condor_output_dir=None):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.condor_output_dir = condor_output_dir

# default_memory = 16 # GB
default_memory = 8  # GB
default_disk = 4 # GB

def run_tasks(
    tasks: list[Task],
    n_cores=8,
    merge_results_fn=None,
    use_condor: bool = False,
    condor_job_name: str = None,
    **submit_condor_tasks_kwargs
    ):
    if use_condor:
        assert condor_job_name is not None, "condor_job_name must be provided when use_condor is True"
        cluster_id, result_files = submit_condor_tasks(
            condor_job_name,
            tasks,
            **submit_condor_tasks_kwargs
        )
        # If merging wait for the condor job to finish
        if merge_results_fn is not None:
            print(f"Waiting for condor job to finish... (Cluster ID: {cluster_id})")
            while True:
                result = subprocess.run(
                    ['condor_q', str(cluster_id)],
                    capture_output=True,
                    text=True,
                )
                if str(cluster_id) not in result.stdout:
                    break
                # print("Job still running, waiting 10 seconds...")
                import time
                time.sleep(10)
            
            results = [pickle.load(open(f, 'rb')) for f in result_files]
            return merge_results_fn(results)
    else:
        from multiprocessing import Pool
        with Pool(n_cores) as p:
            results = [p.apply(task.func, task.args, task.kwargs) for task in tasks]
        if merge_results_fn is not None:
            return merge_results_fn(results)
        return results
    

def submit_condor_tasks(
    job_name: str,
    tasks: list[Task],
    clear_logs: bool = False,
    env_wrapper: str = run_in_cmssw,
    transfer_on_exit: bool = False,
    memory: int = default_memory,
    disk: int = default_disk,
    cache_results: bool = False,
    ):
    """
    Submits a list of tasks to the condor queue.
    """
    job_task_dir = f"{tasks_dir}/{job_name}_{uuid.uuid1()}"
    if os.path.exists(job_task_dir):
        os.system(f"rm -rf {job_task_dir}")
    os.makedirs(job_task_dir)
    os.chmod(job_task_dir, 0o777)

    task_files = []
    result_files = []
    output_dirs = []
    for i, task in enumerate(tasks):
        # Pickle the task
        task_file = f"{job_task_dir}/{i}.task"
        with open(task_file, 'wb') as file:
            pickle.dump(task, file)

        # Configure the result file if caching is enabled
        result_file = None
        if cache_results:
            result_file = f"{task_file}_result.pkl"

        # Format the arguments
        wrapper_args = [task_worker, task_file]
        if cache_results:
            wrapper_args.append(result_file)
    
        task_files.append(" ".join(wrapper_args))
        if cache_results:
            result_files.append(result_file)
        output_dirs.append(task.condor_output_dir)

    submit_file = create_condor_submission_file(
        job_name,
        env_wrapper,
        task_files,
        output_dirs=output_dirs,
        clear_logs=clear_logs,
        transfer_on_exit=transfer_on_exit,
        memory=memory,
        disk=disk,
    )

    # Submit the job
    try:
        result = subprocess.run(
            ['condor_submit', submit_file],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print("condor_submit failed with output:")
        print(e.stdout)
        print(e.stderr)
        raise

    # Parse the cluster ID
    for line in result.stdout.splitlines():
        if "cluster" in line:
            cluster_id = line.split()[-1].rstrip(".")

    return cluster_id, result_files


def create_condor_submission_file(
    job_name, executable, args,
    memory=default_memory,
    disk=default_disk,
    clear_logs=False,
    transfer_on_exit=False,
    output_dirs=None,
    ):

    log_dir = f"{condor_dir}/{job_name}"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        os.chmod(log_dir, 0o777)
    elif clear_logs:
        os.system(f"rm -rf {log_dir}/*")

    submission_script = textwrap.dedent(f"""\
        universe = vanilla
        executable = {executable}
        request_memory = {memory} GB
        request_disk = {disk} GB
        """)
    if transfer_on_exit:
        submission_script += textwrap.dedent(f"""\
            should_transfer_files = yes
            when_to_transfer_output = on_exit
            """)

    # Loop through each tuple in the list and append a job queue entry for each
    for idx, arg in enumerate(args):
        if '"' in arg:
            arg = arg.replace('"', '\\"')
        # Use per-task output_dir if provided, else default log_dir
        out_dir = log_dir
        if output_dirs and output_dirs[idx]:
            out_dir = output_dirs[idx]
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)
                os.chmod(out_dir, 0o777)
        submission_script += (
            f'output = {out_dir}/$(Cluster)_$(Process).out\n'
            f'error = {out_dir}/$(Cluster)_$(Process).err\n'
            f'log = {out_dir}/$(Cluster).log\n'
            f'arguments = {arg}\nqueue\n\n'
        )

    job_filename = f"{submit_dir}/{job_name}.sub"
    with open(job_filename, 'w') as file:
        file.write(submission_script)

    return job_filename


import pickle

def run_condor_task():
    task_file = sys.argv[1]
    with open(task_file, 'rb') as f:
        task = pickle.load(f)

    result = task.func(*task.args, **task.kwargs)

    if len(sys.argv) > 2:
        result_file = sys.argv[2]
        with open(result_file, 'wb') as f:
            pickle.dump(result, f)

if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    run_condor_task()