import os
import textwrap

from . import storage_config as stor

condor_dir = stor.ensure_cache('condor')
submit_dir = stor.ensure_cache('condor/submit')

def create_condor_submission_file(job_name, executable, args, clear_logs=False):

    log_dir = f"{condor_dir}/{job_name}"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        os.chmod(log_dir, 0o777)
    elif clear_logs:
        os.system(f"rm -rf {log_dir}/*")

    submission_script = textwrap.dedent(f"""\
        universe = vanilla
        executable = {executable}
        output = {log_dir}/$(Cluster)_$(Process).out
        error = {log_dir}/$(Cluster)_$(Process).err
        log = {log_dir}/$(Cluster).log
        """)

    # Loop through each tuple in the list and append a job queue entry for each
    for arg in args:
        if '"' in arg:
            arg = arg.replace('"', '\\"')
        submission_script += f'arguments = {arg}\nqueue\n\n'

    job_filename = f"{submit_dir}/{job_name}.sub"
    with open(job_filename, 'w') as file:
        file.write(submission_script)

    return job_filename