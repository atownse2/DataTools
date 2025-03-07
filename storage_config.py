import os

top_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

## Directories
cache_dir = top_dir+"/cache"

def ensure_cache(relative_path):
    """Ensure that a directory exists in the cache directory."""
    path = os.path.join(cache_dir, relative_path)
    if not os.path.exists(path):
        os.makedirs(path)
    return path

## Local storage
USER = os.environ['USER']
vast_storage = f'/project01/ndcms/{USER}'

## Data Directories
data_dirs = {
    "MLNanoAODv9": "/project01/ndcms/atownse2/data/MLNanoAODv9",
    "skim_preselection": f"{top_dir}/data/skim_preselection",
    "skim_trigger_study": f"{top_dir}/data/skim_trigger_study",
}