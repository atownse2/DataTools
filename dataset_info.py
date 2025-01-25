import os
import glob

from typing import Union, List

import json
import re

import data_tools.storage_config as storage
from data_tools import signal_info


all_data_formats = ['MiniAODv2', 'NanoAODv9']

years = ["2016_HIPM", "2016", "2017", "2018"]

lumis = {
    "2016_HIPM" : 20.103495005, # fb^-1
    "2016" : 37.184259631,
    "2017" : 43.178270568,
    "2018": 62.448754676 
} # TODO check with someone that I used brilcalc correctly

def get_years_from_era(era):
    if "," in era: return era.split(',')
    elif era in years: return [era]
    elif era == "Run2" : return years
    else: raise ValueError(f'era {era} not recognized')

def format_access(access_str):
    if 'LOCAL_STORAGE' in access_str:
        return access_str.replace('LOCAL_STORAGE', storage.local_storage)
    if 'SKIM_STORAGE' in access_str:
        return access_str.replace('SKIM_STORAGE', storage.skim_storage)
    return access_str

dataset_config = f'{storage.top_dir}/data_tools/dataset_config.json'
class DatasetInfo:
    """Class for handling Dataset information and access"""

    def __init__(self):
        self.dataset_info = self.get()
    
    def get(self):
        with open(dataset_config) as f:
            return json.load(f)
    
    def write(self):
        with open(dataset_config, 'w') as f:
            json.dump(self.dataset_info, f, indent=4)

    def __getitem__(self, dType):
        if dType not in self.dataset_info:
            self.add_dType(dType)
            raise ValueError(f"dType {dType} not configured, add a das query to dataset_config.json.")
        return self.dataset_info[dType]

    def add_dType(self, dType):
        if dType not in self.dataset_info:
            self.dataset_info[dType] = { 'das_queries' : [], "datasets": {} }
            self.write()    

    def add_dataset(self, dType, sample_name, data_format, access):
        if dType not in self:
            self.add_dType(dType)
        if sample_name not in self[dType]['datasets']:
            self[dType]['datasets'][sample_name] = {data_format: access}
        else:
            print(f"Warning: Dataset {sample_name}_{data_format} already exists, overwriting...")
            self[dType]['datasets'][sample_name][data_format] = access
        self.write()

    def get_access(self, dType, sample_name, data_format):
        datasets = self[dType]['datasets']
        if sample_name not in datasets or data_format not in datasets[sample_name]:
            return None
        return datasets[sample_name][data_format]

    def remove_data_format(self, data_format):
        for dType, dInfo in self.dataset_info.items():
            for dataset, info in dInfo['datasets'].items():
                if data_format in info:
                    del info[data_format]
        self.write()

class Dataset:
    """Class for handling dataset information and access"""
    file_extension = '.root'

    def __init__(
            self,
            dType: str,
            sample_name: str,
            data_format: str,
            acess: str = None,
            storage_base: str = None,
            update_dataset_info: bool = False,
            **kwargs
            ):
        """
        Initialize a dataset object

        Args:
            dType (str): Data type of the Dataset (e.g. data, GJets, signal, etc.)
            sample_name (str): Name of the sample (e.g. GJets_HT-40To100_2018)
            data_format (str): Format of the Dataset (e.g. MiniAODv2, NanoAODv9, etc.)
            acess (str): Specification for dataset access (e.g. das:/GJets_HT-40To100_2018)
            storage_base (str): Base storage directory for the dataset (if stored locally)
        """

        self.dType = dType
        self.isMC = dType != 'data'
        self.dTag = 'mc' if self.isMC else 'data'
        if dType == 'signal':
            self.signal_point = signal_info.SignalPoint(tag=sample_name)
            self.M_BKK = self.signal_point.M_BKK
            self.M_R = self.signal_point.M_R
        
        self.sample_name = sample_name
        self.data_format = data_format
        
        self.kwargs = kwargs
        
        if update_dataset_info:
            DatasetInfo().add_dataset(dType, sample_name, data_format, acess)

        self.access = acess
        if self.access is None: # Try to get the access from the Dataset info
            self.access = DatasetInfo().get_access(dType, sample_name, data_format)
            # if storage_base is not None and self.access is not None:
            #     assert storage_base in self.access, f"Storage base: {storage_base} does not match access specification: {self.access}"
        if self.access is None: # If still no access, use the storage base
            assert storage_base is not None, "Storage base must be specified if access specification is not"
            print(f"Warning: Access not specified for dataset {sample_name}_{data_format}, using {storage_base} instead. Consider updating Datasets.json somehow.")
            self.access = f"local:{storage_base}/{self.dTag}/{self.data_format}/{self.sample_name}"


    def __getitem__(self, key):
        if key in self.__dict__:
            return self.__dict__[key]
        elif hasattr(self, 'get'):
            return self.get(key)
        raise KeyError(f'Key {key} not found in {self.__class__}')

    @property
    def year(self):
        for y in years:
            if y in self.sample_name:
                return y
        print(f"Warning: Year not found in dataset tag {self.sample_name}, defaulting to 2018")
        return "2018"
        # raise ValueError(f'Year not found in dataset tag {self.sample_name}')

    @property
    def era(self):
        return self.year

    @property
    def name(self):
        return f'{self.sample_name}_{self.data_format}'

    @property
    def storage_dir(self):
        storage_dir = os.path.dirname(self.access.split(':')[1])
        storage_dir = format_access(storage_dir)
        if not os.path.isdir(storage_dir):
            os.makedirs(storage_dir)
        return storage_dir

    def reset_files(self):
        if hasattr(self, '_files'):
            del self._files

    @property
    def files(self):
        if hasattr(self, '_files'):
            return self._files

        access_method, access_string = self.access.split(':')
        access_string = format_access(access_string)
        if access_method == 'das':
            filelist = get_das_filelist(access_string)
        elif 'local' in access_method:
            filelist = get_local_filelist(access_string)

            # Assume MiniAOD files are CMSSW inputs
            if self.data_format == 'MiniAODv2':
                filelist = [f"file:{f}" for f in filelist]
        else:
            raise ValueError(f'Access method {access_method} not recognized')
        
        # Filter for the correct file extension
        filelist = [f for f in filelist if f.endswith(self.file_extension)]

        if len(filelist) > 0:
            self._files = filelist
        return filelist

class Datasets:
    """Class for intuitively and flexibly acessing multiple datasets"""

    dataset_class = Dataset

    def __init__(
            self,
            dTypes: Union[str, List[str]],
            era: str,
            data_format: str,
            subset: Union[str, List[str], List[Dataset]] = None,
            storage_base: str = None,
            combine_eras: bool = False,
            test: bool = False,
            **kwargs
            ):
        """
        Initialize a Datasets object

        Args:
            dTypes (str or list): Data types of the Datasets (e.g. data, GJets, signal, etc.)
            era (str): Era of the Datasets (e.g. 2018, Run2, etc.)
            data_format (str): Format of the Datasets (e.g. MiniAODv2, NanoAODv9, etc.)
            subset (str, list): Subset of datasets to include can be a list of strings
                or a list of Dataset objects. String specification can be a comma separated,
                and should follow the pattern "dType/sample_name" (e.g. "GJets_HT-40To100_2018")
            storage_base (str): Base storage directory for the datasets (if stored locally)
        """

        if isinstance(dTypes, str): dTypes = dTypes.split(',')
        self.dTypes = dTypes
        self.era = era
        self.years = get_years_from_era(era)

        self.data_format = data_format

        self.kwargs = kwargs

        self.datasets = self.get_datasets(subset, storage_base)

    @property
    def signal_points(self):
        assert all([dType == 'signal' for dType in self.dTypes]), "Signal points only available for signal datasets"
        return set([d.signal_point for d in self.datasets.values() if hasattr(d, 'signal_point')])

    @property
    def signal_point(self):
        signal_points = self.signal_points
        if len(signal_points) == 1:
            return signal_points.pop()
        elif len(signal_points) > 1:
            raise ValueError(f"Multiple signal points found in Datasets: {signal_points}")

    def __iter__(self):
        return iter(self.datasets.values())

    def __len__(self):
        return len(self.datasets.keys())

    def get(self, key):
        raise NotImplementedError(f'Getting {key} from {self.dataset_class} not implemented') 

    def copy(self, **kwargs):
        return type(self)(
            self.dTypes, self.era, self.data_format,
            **self.kwargs,
            **kwargs
        )


    def __getitem__(self, key):
        """Return a subset of the datasets or other objects through self.get()"""
        if isinstance(key, signal_info.SignalPoint):
            subset = [d for d in self.datasets.values() if d.signal_point == key]
            if len(subset) == 1:
                return subset[0]
            elif len(subset) > 1:
                return self.copy(subset=subset)
        if isinstance(key, int):
            return list(self.datasets.values())[key]
        elif isinstance(key, str):
            subset = [d for name, d in self.datasets.items() if key in name]
            # print([s.name for s in subset])
            if len(subset) == 1:
                return subset[0]
            elif len(subset) > 1:
                return self.copy(subset=subset)
            else:
                return self.get(key)
        elif isinstance(key, slice):
            subset = list(self.datasets.values())[key]
            return self.copy(subset=subset)
        elif isinstance(key, list):
            subset = []
            for k in key:
                if isinstance(k, str):
                    _subset = [d for name, d in self.datasets.items() if k in name]
                    subset.extend(_subset)
                elif isinstance(k, int):
                    subset.append(list(self.datasets.values())[k])
            return self.copy(subset=subset)
        else:
            raise ValueError(f'Class Datasets does not support key type {type(key)}')
        

    @property
    def name(self):
        return f"{'-'.join(self.dTypes)}_{self.era}_{self.data_format}"

    def get_datasets(self, subset, storage_base):

        if isinstance(subset, str): subset = subset.split(',')

        if subset:
            if all([isinstance(d, Dataset) for d in subset]):
                return {d.name: d for d in subset}
            else:
                _subset = [
                    self.dataset_class(
                        s.dType, s.sample_name, self.data_format,
                        storage_base=storage_base,
                        **self.kwargs
                        ) for s in subset
                    ]
                return {d.name: d for d in _subset}

        dataset_info = DatasetInfo()
        subset = []

        for dType in self.dTypes:
            datasets = dataset_info[dType]['datasets'].keys()
            for sample_name in datasets:
                subset.append(
                    self.dataset_class(
                        dType, sample_name, self.data_format,
                        storage_base=storage_base,
                        **self.kwargs
                    )
                )

        return {d.name: d for d in subset}

def query_xrootd(path, redirector=storage.redirector, top_dir=storage.top_dir):
    cache_file = f'{top_dir}/cache/filelists/{path.replace("/","_")}_filelist.txt'
    if not os.path.isfile(cache_file):
        import XRootD.client
        dirlist = []
        fs = XRootD.client.FileSystem(redirector)
        status, listing = fs.dirlist(path)
        for f in listing:
            dirlist.append(f.name)
        with open(cache_file, 'w') as f:
            f.write('\n'.join(dirlist))
    
    with open(cache_file) as f:
        dirlist = f.readlines()

    return dirlist

def query_das(query, outputfile):
  print('Submitting query: ' + query)
  os.system('dasgoclient --query "{}" >> {}'.format(query, outputfile))

def get_local_filelist(glob_pattern):
    '''Returns the filelist for the dataset'''
    return [os.path.abspath(f) for f in glob.glob(glob_pattern+"*")]

def get_das_filelist(data_location, redirector=storage.redirector, top_dir=storage.top_dir):
    '''Returns the filelist for the dataset'''
    filelist_dir = f'{top_dir}/cache/filelists'
    if not os.path.isdir(filelist_dir):
        os.makedirs(filelist_dir)

    sample_name = data_location.replace('/', '_')[1:]
    filelist_name = f'{filelist_dir}/{sample_name}_filelist.txt'
    if not os.path.isfile(filelist_name):
        query_das(f"file dataset={data_location}", filelist_name)
    filelist = [redirector+f.replace('\n','') for f in open(filelist_name).readlines()]
    return filelist

### For updating dataset config from DAS
def sample_name_from_das(self, das_name): 
    if self.isMC:
        name = das_name.split('/')[1].split('_TuneCP5_')[0]
        era_tag = r'RunIISummer20UL(\d{2})'
    else:
        name = das_name.split('/')[1]
        era_tag = r'Run20(\d{2})'
    
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
#### End Data Tools

if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description='Initialize the dataset config file')
    parser.add_argument('--update', '-u', action='store_true', help='Update the datasets from DAS')
    parser.add_argument('--dType', '-d', type=str, default=None, help='Data type of the Dataset (e.g. data, GJets, QCD, etc.)')
    parser.add_argument('--format', '-f', type=str, default=None, help='Format of the Dataset (e.g. MiniAODv2, NanoAODv9, etc.)')

    args = parser.parse_args()
    
    if args.update:
        if args.dType is None or args.format is None:
            raise ValueError('dType and format must be specified when updating from DAS')
        # datasets = Datasets(args.dType, update=True).datasets
        raise NotImplementedError('Updating from DAS not implemented')
