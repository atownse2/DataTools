import os
import sys
import glob

import json
import re

config_dir = os.path.dirname(os.path.abspath(__file__))
top_dir = os.path.dirname(config_dir)

hadoop_redirector = "root://deepthought.crc.nd.edu/"
nd_redirector = "root://ndcms.crc.nd.edu/"

## Configuration
USER = os.environ['USER']
hadoop_storage = f'/hadoop/store/user/{USER}/RSTriPhoton'
vast_storage = f'/project01/ndcms/{USER}/RSTriPhoton'
local_storage = vast_storage

all_data_formats = ['MiniAODv2', 'NanoAODv9']
all_data_storages = {'vast': vast_storage, 'hadoop': hadoop_storage}

years = ["2016", "2017", "2018"]

samples_config = f'{config_dir}/samples.json'

def get_years_from_era(era):
    if "," in era: return era.split(',')
    elif era in years: return [era]
    elif era == "Run2" : return years
    else: raise ValueError(f'era {era} not recognized')


class SampleInfo:
    def __init__(self):
        self.sample_info = self.get()
    
    def get(self):
        with open(samples_config) as f:
            return json.load(f)
    
    def write(self):
        with open(samples_config, 'w') as f:
            json.dump(self.sample_info, f, indent=4)
    
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

    def remove_data_format(self, data_format):
        for dType, dInfo in self.sample_info.items():
            for dataset, dataset_info in dInfo['datasets'].items():
                if data_format in dataset_info:
                    del dataset_info[data_format]
        self.write()


class Dataset:
    """
    A Dataset is the smallest division of a sample that can be processed
    E.g. dataset tag : "GJets_HT-40To100_2018" with data format :"NanoAODv9"
    """

    def __init__(self, dType, dataset_tag, data_format, storage_base=vast_storage):
        self.dType = dType
        self.isMC = dType != 'data'
        self.dTag = 'mc' if self.isMC else 'data'
        
        self.dataset_tag = dataset_tag
        self.year = dataset_tag.split('_')[-1]

        self.data_format = data_format
        self.storage_base = storage_base

    def update_sample_info(self):

        if "ceph" in self.storage_base:
            raise NotImplementedError('File access for Ceph not implemented')
            access_method = 'ceph'
        else:
            access_method = 'local'

        self._access = f"{access_method}:{self.storage_dir}/{self.name}"
        if len(self.files) != 0:
            sample_info = SampleInfo()
            sample_info[self.dType]['datasets'][self.dataset_tag].update({self.data_format: self.access})
            sample_info.write()

        return self

    @property
    def access(self):
        if not hasattr(self, '_access'):
            sample_info = SampleInfo()
            self._access = sample_info.get_access(self.dType, self.dataset_tag, self.data_format)
        return self._access

    @property
    def name(self):
        return f'{self.dataset_tag}_{self.data_format}'

    @property
    def storage_dir(self):
        data_dir = f"{self.storage_base}/{self.dTag}/{self.data_format}"
        if not os.path.isdir(data_dir):
            os.makedirs(data_dir)
        return data_dir

    def reset_files(self):
        if hasattr(self, '_files'):
            del self._files

    @property
    def files(self):
        if hasattr(self, '_files'):
            return self._files

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
        
        if len(filelist) > 0:
            self._files = filelist
        return filelist

class Datasets:
    """High level class for intuitively and flexibly acessing datasets"""

    dataset_class = Dataset

    def __init__(self, dTypes, era, data_format, subset=None, storage_base=vast_storage,):
        if isinstance(dTypes, str): dTypes = dTypes.split(',')
        self.dTypes = dTypes
        self.era = era
        self.years = get_years_from_era(era)

        self.data_format = data_format
        self.storage_base = storage_base

        if isinstance(subset, str): subset = subset.split(',')
        self.subset = subset
        self.datasets = self.get_datasets()

    def __iter__(self):
        return iter(self.datasets.values())

    def __len__(self):
        return len(self.datasets.keys())

    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.datasets.values())[key]
        elif isinstance(key, str):
            if key in self.datasets:
                return self.datasets[key]
            else:
                _datasets = [d for d in self if key in d.dataset_tag]
                if len(_datasets) == 1:
                    return _datasets[0]
                elif len(_datasets) == 0:
                    raise ValueError(f"Dataset {key} not found")
                else:
                    raise ValueError(f"Multiple datasets found for {key}: {[d.dataset_tag for d in _datasets]}, don't know what to do with this yet")
        elif isinstance(key, slice):
            subset = [d.dataset_tag for d in self.datasets.values()][key]
            return type(self)(
                self.dTypes, self.era, self.data_format,
                subset=subset, 
                storage_base=self.storage_base
                )
        else:
            raise ValueError(f'Class Datasets does not support key type {type(key)}')

    @property
    def name(self):
        return f"{'-'.join(self.dTypes)}_{self.era}_{self.data_format}"

    def get_datasets(self):
        sample_info = SampleInfo()
        datasets = {}
        for dType in self.dTypes:
            for dataset_tag, dataset_info in sample_info[dType]['datasets'].items():
                if self.subset and dataset_tag not in self.subset:
                    continue

                dataset = self.dataset_class(
                    dType, dataset_tag, self.data_format,
                    storage_base=self.storage_base
                    )

                if self.data_format not in dataset_info:
                    dataset.update_sample_info()
                datasets[dataset.name] = dataset
        return datasets


def query_das(query, outputfile):
  print('Submitting query: ' + query)
  os.system('dasgoclient --query "{}" >> {}'.format(query, outputfile))

def get_local_filelist(dataset_location):
    return [os.path.abspath(f) for f in glob.glob(f"{dataset_location}*")]

    # file_dir = os.path.dirname(dataset_location)
    # file_tag = os.path.basename(dataset_location)
    # return [f'{file_dir}/{f}' for f in os.listdir(file_dir) if file_tag in f]

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
