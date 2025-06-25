import os
import textwrap
import pickle

from . import storage_config as stor
import uuid

condor_dir = stor.ensure_cache('condor')
submit_dir = stor.ensure_cache('condor/submit')
tasks_dir = stor.ensure_cache('condor/tasks')

run_in_cmssw = f"{stor.scripts_dir}/run_in_cmssw_env.sh"
run_in_mamba = f"{stor.scripts_dir}/run_in_mamba_env.sh"
task_worker = f"{stor.scripts_dir}/worker.py"

class Task:
    def __init__(self, func, args, kwargs, condor_output_dir=None):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.condor_output_dir = condor_output_dir

# default_memory = 16 # GB
default_memory = 8  # GB
default_disk = 4 # GB

def submit_tasks(
    job_name: str,
    tasks: list[Task],
    clear_logs: bool = False,
    env_wrapper: str = run_in_cmssw,
    transfer_on_exit: bool = False,
    memory: int = default_memory,
    disk: int = default_disk,
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
    output_dirs = []
    for i, task in enumerate(tasks):
        task_file = f"{job_task_dir}/{i}.task"
        with open(task_file, 'wb') as file:
            pickle.dump(task, file)
        task_files.append(f"{task_worker} {task_file}")
        # Support both TaskSpec class and tuple/list for backward compatibility
        if hasattr(task, 'condor_output_dir') and task.condor_output_dir:
            output_dirs.append(task.condor_output_dir)
        else:
            output_dirs.append(None)

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
    os.system(f"condor_submit {submit_file}")


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