import os
import sys

tools_dir = os.path.dirname(os.path.abspath(__file__))
top_dir = os.path.dirname(os.path.dirname(tools_dir))
cache_dir = os.path.join(top_dir, 'cache')
if not os.path.exists(cache_dir):
    os.makedirs(cache_dir)

def ensure_cache(relative_dir):
    full_path = os.path.join(cache_dir, relative_dir)
    if not os.path.exists(full_path):
        os.makedirs(full_path)
    return full_path