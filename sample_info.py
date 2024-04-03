import os
import sys

import yaml
import json
import re

top_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

hadoop_redirector = "root://deepthought.crc.nd.edu/"
nd_redirector = "root://ndcms.crc.nd.edu/"

## Configuration
hadoop_storage = '/hadoop/store/user/atownse2/RSTriPhoton'
vast_storage = '/project01/ndcms/atownse2/RSTriPhoton'

all_data_formats = ['MiniAODv2', 'NanoAODv9']
all_data_storages = {'vast': vast_storage, 'hadoop': hadoop_storage}

years = ["2016", "2017", "2018"]

def get_years_from_era(era):
    if "," in era: return era.split(',')
    elif era in years: return [era]
    elif era == "Run2" : return years
    else: raise ValueError(f'era {era} not recognized')

def get_sample_info():
    with open(f'{top_dir}/samples.yml') as f:
        return yaml.load(f, Loader=yaml.FullLoader)

def write_sample_info(sample_info):
    with open(f'{top_dir}/samples.yml', 'w') as f:
        yaml.dump(sample_info, f)

class SampleInfo:
    def __init__(self):
        self.sample_info = self.get()
    
    def get(self):
        with open(f'{top_dir}/samples.yml') as f:
            return yaml.load(f, Loader=yaml.FullLoader)
    
    def write(self):
        with open(f'{top_dir}/samples.yml', 'w') as f:
            yaml.dump(self.sample_info, f)
    
    def __getitem__(self, dType):
        if dType not in self.sample_info:
            self.sample_info[dType] = { 'das_queries' : [], 'datasets': {} }
            self.write_sample_info()
            raise ValueError(f"dType {dType} not configured, add a das query to samples.yml.")
        return self.sample_info[dType]
        
    def get_access(self, dType, dataset, data_format):
        datasets = self[dType]['datasets']
        if dataset not in datasets:
            raise ValueError(f"Dataset {dataset} not found in sample info")
        if data_format not in datasets[dataset]:
            raise ValueError(f"Data format {data_format} not found for dataset {dataset}")
        return datasets[dataset][data_format]


class Datasets:
    """High level class for intuitively and flexibly acessing datasets"""

    def __init__(self, dTypes, era, data_format, subset=None):

        if isinstance(dTypes, str): dTypes = dTypes.split(',')
        self.dTypes = dTypes

        self.era = era
        self.data_format = data_format
        self.subset = subset

        self.years = get_years_from_era(era)
        self.datasets = self.get_datasets()
    
    def __iter__(self):
        return iter(self.datasets)

    def __len__(self):
        return len(self.datasets)

    @property
    def name(self):
        return f"{'-'.join(self.dTypes)}_{self.era}_{self.data_format}"

    def get_datasets(self):
        sample_info = SampleInfo()
        
        datasets = []
        for dType in self.dTypes:
            for dataset_name, dataset_info in sample_info[dType]['datasets'].items():
                if self.subset is not None and dataset_name not in self.subset:
                    continue
                if self.data_format in dataset_info:
                    datasets.append(Dataset(dType, dataset_name, self.data_format, access=dataset_info[self.data_format]))
                else:
                    print(f"Data format {self.data_format} not found for dataset {dataset_name}")
        return datasets

class Dataset:
    def __init__(self, dType, dataset, data_format, access=None):
        self.dType = dType
        self.dataset = dataset
        self.data_format = data_format
        self.access = access

        self.isMC = dType != 'data'

    def update_sample_info(self, storage_base, test=False):

        if test: self.data_format += "_test"
        tag = "mc" if self.isMC else "data"
        output_dir = f"{storage_base}/{tag}"
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)

        self.access = f"local:{output_dir}/{self.name}"

        sample_info = SampleInfo()
        sample_info[self.dType]['datasets'][self.dataset].update({self.data_format: self.access})
        sample_info.write()

    def get_access_from_sample_info():
        sample_info = SampleInfo()
        return sample_info.get_access(self.dType, self.dataset, self.data_format)

    @property
    def name(self):
        return f'{self.dataset}_{self.data_format}'

    @property
    def files(self):

        if self.access is None:
            self.access = self.get_access_from_sample_info()
        access_method, access_string = self.access.split(':')
        if access_method == 'das':
            filelist = get_das_filelist(access_string)
        elif 'local' in access_method:
            filelist = get_local_filelist(access_string)
            # Assume MiniAOD files are CMSSW inputs for now
            if self.data_format == 'MiniAODv2':
                filelist = [f"file:{f}" for f in filelist]
        else:
            raise ValueError(f'Access method {access_method} not recognized')
        return filelist

    @property
    def fileset(self):
        return {self.name: {"files":{f: {'object_path': 'Events'} for f in self.files}}}


def query_das(query, outputfile):
  print('Submitting query: ' + query)
  os.system('dasgoclient --query "{}" >> {}'.format(query, outputfile))

def get_local_filelist(dataset_location):
    file_dir = os.path.dirname(dataset_location)
    file_tag = os.path.basename(dataset_location)
    return [f'{file_dir}/{f}' for f in os.listdir(file_dir) if file_tag in f]

def get_das_filelist(data_location, redirector=nd_redirector):
    '''Returns the filelist for the dataset'''
    filelist_dir = f'{top_dir}/cache/filelists'
    if not os.path.isdir(filelist_dir):
        os.makedirs(filelist_dir)

    dataset_name = data_location.replace('/', '_')[1:]
    filelist_name = f'{filelist_dir}/{dataset_name}_filelist.txt'
    if not os.path.isfile(filelist_name):
        query_das(f"file dataset={data_location}", filelist_name)
    filelist = [redirector+f.replace('\n','') for f in open(filelist_name).readlines()]
    return filelist

### For updating samples.yml from DAS
def dataset_name_from_das(self, das_name): 
    if self.isMC:
        name = das_name.split('/')[1].split('_TuneCP5_')[0]
        era_tag = 'RunIISummer20UL(\d{2})'
    else:
        name = das_name.split('/')[1]
        era_tag = 'Run20(\d{2})'
    
    match = re.search(era_tag, das_name)
    if match is None:
        raise ValueError(f'Could not find era in dataset name {das_name}')
                                                    
    era = f"20{match.group(1)}"
    if era == "2016":
        if "APV" in das_name:
            era += "APV"
        elif "HIPM" in das_name:
            era += "HIPM"

    return f"{name}_{era}"

if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description='Initialize the samples.yml file')
    parser.add_argument('--update', '-u', action='store_true', help='Update the datasets from DAS')
    parser.add_argument('--dType', '-d', type=str, default=None, help='Data type of the sample (e.g. data, GJets, QCD, etc.)')
    parser.add_argument('--format', '-f', type=str, default=None, help='Format of the sample (e.g. MiniAODv2, NanoAODv9, etc.)')

    args = parser.parse_args()
    
    if args.update:
        if args.dType is None or args.format is None:
            raise ValueError('dType and format must be specified when updating from DAS')
        # datasets = Datasets(args.dType, update=True).datasets
        raise NotImplementedError('Updating from DAS not implemented')


"""
class SampleInfo:
    def __init__(self):
        self.sample_info = self.get()
    
    def get(self):
        with open(f'{top_dir}/samples.yml') as f:
            return yaml.load(f, Loader=yaml.FullLoader)
    
    def write(self):
        with open(f'{top_dir}/samples.yml', 'w') as f:
            yaml.dump(self.sample_info, f)
    
    def update_from_das(self, dType):
        '''Updates the sample info from DAS'''
        cache_file = f'{top_dir}/cache/das_query.txt'
        for das_query in self[dType]['das_queries']:
            query_das(das_query, cache_file)
        das_datasets = [d.replace('\n','') for d in open(cache_file).readlines()]
        datasets = self[dType]['datasets']
        for das_dataset in das_datasets:
            dataset = Dataset(dType, das_dataset).name()
            
            # Parse the data format from the dataset name
            data_format = None
            for df in all_data_formats:
                if df in das_dataset:
                    data_format = df
                    break
            if data_format is None:
                raise ValueError(f'Could not find data format in dataset name {dataset}')

            if dataset not in datasets:
                datasets[dataset] = {}
            datasets[dataset].update({data_format: f'das:{das_dataset}'})
        self.write()

    def update_from_local(self, dType):
        print(f'update_from_local not implemented')
        pass

    def __getitem__(self, dType):
        if dType not in self.sample_info:
            self.sample_info[dType] = { 'das_queries' : [], 'datasets': {} }
            self.write_sample_info()
            raise ValueError(f"dType {dType} not configured, add a das query to samples.yml.")
        return self.sample_info[dType]

    def __setitem__(self, dType, value):
        self.sample_info[dType] = value
        self.write()
"""