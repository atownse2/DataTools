import os

config_dir = os.path.dirname(os.path.abspath(__file__))
top_dir = os.path.dirname(config_dir)

## Directories
cache_dir = top_dir+"/cache"
condor_dir = '/scratch365/atownse2/RSTriPhoton/condor'

output_dir = top_dir+"/outputs"
plot_dir = output_dir+"/plots"

## Local storage
USER = os.environ['USER']
hadoop_storage = f'/hadoop/store/user/{USER}/RSTriPhoton'
vast_storage = f'/project01/ndcms/{USER}'

local_storage = vast_storage
# local_storage = top_dir
skim_storage = top_dir

all_data_storages = {'vast': vast_storage, 'hadoop': hadoop_storage}

# XRootD
hadoop_redirector = "root://deepthought.crc.nd.edu/"
nd_redirector = "root://ndcms.crc.nd.edu/"

redirector = nd_redirector