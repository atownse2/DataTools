import os

## Directories
tools_dir = os.path.dirname(os.path.abspath(__file__))
top_dir = os.path.dirname(os.path.dirname(tools_dir))
print(f"Top directory: {top_dir}")

cache_dir = f"{top_dir}/cache"
scripts_dir = f"{top_dir}/scripts"

def ensure_cache(relative_path, cache_dir=cache_dir):
    """Ensure that a directory exists in the cache directory."""
    path = os.path.join(cache_dir, relative_path)
    if not os.path.exists(path):
        os.makedirs(path)
    return path

## Local storage
USER = os.environ['USER']
vast_storage = f'/project01/ndcms/{USER}'

## Data Directories
def get_storage_dir(data_format, use_ceph=False, **kwargs):
    if use_ceph:
        return ceph_data_dirs[data_format]
    else:
        return vast_data_dirs[data_format]

ceph_data_dirs = {
    "MLNanoAODv9": "/cms/cephfs/data/store/user/atownse2/RSTriPhoton/data/MLNanoAODv9",
}

vast_data_dirs = {
    "MLNanoAODv9": "/project01/ndcms/atownse2/data/MLNanoAODv9",
    "skim_preselection": f"{top_dir}/data/skim_preselection",
    "skim_trigger_study": f"{top_dir}/data/skim_trigger_study",
}