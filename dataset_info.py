import os

from typing import Union, List

import json
import subprocess
import textwrap

import analysis.tools.storage_config as stor

import random

# Cache directories
cache_dir = stor.ensure_cache("dataset_info")
dataset_cache = stor.ensure_cache("dataset_info/datasets")
xs_cache = stor.ensure_cache("dataset_info/xs")

# Redirectors
redirector = "root://hactar01.crc.nd.edu/"

# Constants
all_data_formats = ['MiniAODv2', 'NanoAODv9']

years = ["2016preVFP", "2016postVFP", "2017", "2018"]

lumis_in_fb = { # fb^-1
    "2016" : 36.31, # From twiki (need to divide into pre and post VFP)
    "2016preVFP" : 19.5, # TODO need to verify this
    "2016postVFP" : 16.4, # TODO need to verify this
    # "2016preVFP" : 37.184259631, # From brilcalc without GoldenJSON applied
    # "2016postVFP" : 20.103495005, # From brilcalc without GoldenJSON applied
    # "2017" : 43.178270568, # From brilcalc without GoldenJSON applied
    "2017" : 41.48, # From twiki
    # "2018": 62.448754676#*0.1 # From brilcalc without GoldenJSON applied
    "2018": 59.83 # From twiki
} # TODO check with someone that I used brilcalc correctly

signal_processes = [
'BkkToGRadionToGGG',
'BkkToGRadionJetsToGGGJets'
]

dataset_name_include_all = {
    'data': ["DoubleEG", "EGamma"],
    'data_trigger_study': ["SingleElectron", "EGamma"],
    'signal': signal_processes,
    'GJets': ["GJets_HT"],
    "QCD": ["QCD_Pt-", "EMEnriched"]
}

# Functions
def tag_dataset(dType, years, data_format, sample_name_or_filename):
    if dType in dataset_name_include_all.keys():
        pass_tag = any([name in sample_name_or_filename for name in dataset_name_include_all[dType]])
    else:
        pass_tag = dType in sample_name_or_filename
    
    pass_year = False
    for year in years:
        if f"{year}_{data_format}" in sample_name_or_filename:
            pass_year = True
            break

    return pass_tag and pass_year

def get_years_from_era(era):
    if "," in era: return era.split(',')
    elif era in years: return [era]
    elif era == "Run2" : return years
    else: raise ValueError(f'era {era} not recognized')

# Signal
class SignalPoint:

    def __init__(
        self,
        M_BKK: Union[int, float] = None,
        M_R: Union[int, float] = None,
        MOE: Union[int, float] = None,
        Mass_Ratio: Union[int, float] = None,
        name: str = None,
        ):

        if name is not None:
            M_BKK, M_R = self.from_name(name)
        elif M_BKK and M_R:
            M_BKK = float(M_BKK)
            M_R = float(M_R)
        elif M_BKK and MOE:
            M_BKK = float(M_BKK)
            M_R = round((M_BKK/2)*MOE, 4)
        elif M_BKK and Mass_Ratio:
            M_BKK = float(M_BKK)
            M_R = round(Mass_Ratio*M_BKK, 4)
        else:
            raise ValueError(f"Inputs are not valid: M_BKK={M_BKK}, M_R={M_R}, MOE={MOE}, Mass_Ratio={Mass_Ratio}")

        # Remove decimal if integer
        if type(M_BKK) != int and M_BKK.is_integer():
            M_BKK = int(M_BKK)
        if type(M_R) != int and M_R.is_integer():
            M_R = int(M_R)

        self.M_BKK = round(M_BKK, 6)
        self.M_R = round(M_R, 6)
        self.MOE = round(M_R/(M_BKK/2), 6)
        self.Mass_Ratio = round(M_R/M_BKK, 6)

    @property
    def observables(self):
        return self.M_BKK, self.Mass_Ratio

    def __eq__(self, other):
        return self.M_BKK == other.M_BKK and self.M_R == other.M_R

    def __hash__(self):
        return hash((self.M_BKK, self.M_R))

    def from_name(self, tag):
        import re

        M_BKK, M_R = None, None
        if 'M' not in tag or 'R0' not in tag:
            raise ValueError('Fragment does not contain mass point')
        else:
            M_BKK = float(
                re.search(r'M1-(\d+p\d+|\d+)', tag
                ).group(1).replace('p', '.'))

            M_R = float(
                re.search(r'R0-(\d+p\d+|\d+)', tag
                ).group(1).replace('p', '.'))
        
        return M_BKK, M_R

    @property
    def short_name(self):
        return f'{self.M_BKK}_{self.M_R}'
    
    @property
    def name(self):
        return f'M1-{self.M_BKK}_R0-{self.M_R}'.replace('.', 'p')


# Dataset Classes
class Dataset:
    """Class for handling dataset information and access"""
    file_extension = '.root'

    def __init__(
        self,
        dType: str,
        sample_name: str,
        data_format: str,
        storage_base: str = None,
        update_dataset_info: bool = False,
        **kwargs
        ):

        self.dType = dType
        self.isMC = 'data' not in dType

        self.sample_name = sample_name
        self.data_format = data_format

        if dType == 'signal':
            self.signal_point = SignalPoint(name=sample_name)
            self.M_BKK = self.signal_point.M_BKK
            self.M_R = self.signal_point.M_R
            self.signal_process = self.sample_name.split('_')[0]
        
        self.kwargs = kwargs

    @property
    def name(self):
        return f'{self.sample_name}_{self.data_format}'

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
    def files(self):
        if hasattr(self, '_files'): return self._files

        file_dir = stor.get_storage_dir(self.data_format, **self.kwargs)
        file_tag = f"{self.sample_name}_{self.data_format}"

        files = []
        for f in os.listdir(file_dir):
            if file_tag in f:
                files.append(f"{file_dir}/{f}")

        self._files = files
        return self._files

    def __getitem__(self, key):
        if key in self.__dict__:
            return self.__dict__[key]
        elif hasattr(self, 'get'):
            return self.get(key)
        raise KeyError(f'Key {key} not found in {self.__class__}')

    @property
    def xs_info(self, batch_mode=False):
        """Cross section in pb"""
        if not self.isMC:
            raise ValueError(f'Cross section not defined for data datasets')

        xs_file = f"{xs_cache}/{self.sample_name}_xs.txt"
        if not os.path.exists(xs_file):
            print(f"Cross section for {self.sample_name} does not exist, trying to get it")
        
            # Need to find one miniAOD file.
            datasets_file = f"{dataset_cache}/{self.dType}.json"
            if not os.path.exists(datasets_file):
                raise FileNotFoundError(f"Dataset file {datasets_file} not found.")

            datasets_in_dType = json.load(open(datasets_file))['datasets']

            tags = self.sample_name
            tags = tags.replace("2016preVFP", "RunIISummer20UL16MiniAODv2")
            tags = tags.replace("2016postVFP", "RunIISummer20UL16MiniAODAPVv2")
            tags = tags.replace("2017", "RunIISummer20UL17MiniAODv2")
            tags = tags.replace("2018", "RunIISummer20UL18MiniAODv2")
            tags = ["_".join(tags.split('_')[:-1]), tags.split('_')[-1]]

            matches = [d for d in datasets_in_dType if all(tag in d for tag in tags)]
            if not matches:
                if self.dType == "signal" and "Jets" in self.sample_name:
                    # This is a special case for the BKK signal, these samples are locally produced
                    # TODO this is a hack, need to find a better way to do this
                    dir_base = "/cms/cephfs/data/store/user/atownse2/mc/signal_postGEN_2016preVFP+2016postVFP+2017"
                    subdir_name = "maod_step_" + self.sample_name.replace("-", "_")
                    dataset_dir = f"{dir_base}/{subdir_name}"
                    if not os.path.exists(dataset_dir):
                        raise ValueError(f"Dataset not found for {self.sample_name} in {dataset_dir}")

                    file = os.path.join(dataset_dir, os.listdir(dataset_dir)[0]).replace("/cms/cephfs/data", "")
                else:
                    raise ValueError(f"Dataset not found for {self.sample_name}")
            elif len(matches) > 1:
                raise ValueError(f"Multiple datasets found for {self.sample_name}: {matches}")
            else:
                dataset = matches[0]

                dataset_file = "_".join(dataset.split('/')[1:])
                files_dict = json.load(open(f"{dataset_cache}/{dataset_file}.json"))

                success = False
                n_tries = 0
                while not success:
                    try:
                        file = files_dict['files'][random.randint(0, len(files_dict['files']) - 1)]
                        write_ana_output(file, xs_file)
                        parse_xs(xs_file)
                        success = True
                    except ValueError as e:
                        print(f"Error parsing cross section for {self.sample_name}: {e}")
                        print(f"Trying again with a different file")
                        n_tries += 1
                        if n_tries > 10:
                            raise ValueError(f"Failed to parse cross section for {self.sample_name} after 10 tries")

            if batch_mode:
                return (file, xs_file)

            write_ana_output(file, xs_file)
            print(f"Cross section written to {xs_file}")
        
        if batch_mode:
            return None

        return parse_xs(xs_file)

class Datasets:
    dataset_class = Dataset

    def __init__(
        self,
        dType: str,
        era: str,
        data_format: str,
        subset: Union[str, List[str], List[Dataset]] = None,
        **kwargs
        ):

        self.dType = dType
        self.era = era
        self.years = get_years_from_era(era)

        self.data_format = data_format

        if subset is not None and not isinstance(subset, list):
            subset = [subset]

        self.subset = subset

        self.kwargs = kwargs

        self.set_up_datasets()

    def set_up_datasets(self):
        self.datasets = {}
        for f in os.listdir(stor.get_storage_dir(self.data_format, **self.kwargs)):
            if tag_dataset(self.dType, self.years, self.data_format, f):
                sample_name = f.split(f"_{self.data_format}")[0]

                _dataset = self.dataset_class(self.dType, sample_name, self.data_format, **self.kwargs)
                self.datasets[_dataset.name] = _dataset

        if self.subset is not None:
            self.datasets = {d.name: d for d in self[self.subset]}

    @property
    def name(self):
        if self.dType == 'signal':
            if len(self.signal_points) == 1:
                return f"{'-'.join(self.signal_processes)}_{self.signal_point.name}_{self.era}_{self.data_format}"
        return f"{self.dType}_{self.era}_{self.data_format}"

    @property
    def files(self):
        return [f for d in self.datasets.values() for f in d.files]

    @property
    def year(self):
        years = set([d.year for d in self.datasets.values()])
        if len(years) == 1:
            return years.pop()
        elif len(years) > 1:
            for d in self:
                print(f"Warning: Multiple years found in datasets: {d.name}")
            raise ValueError(f"Multiple years found in Datasets: {years}")
    
    # Signal methods
    @property
    def signal_points(self):
        assert self.dType == 'signal', "Signal points only available for signal datasets"
        return set([d.signal_point for d in self.datasets.values() if hasattr(d, 'signal_point')])

    @property
    def signal_point(self):
        assert self.dType == 'signal', "Signal point only available for signal datasets"
        signal_points = self.signal_points
        if len(signal_points) == 1:
            return signal_points.pop()
        elif len(signal_points) > 1:
            raise ValueError(f"Multiple signal points found in Datasets: {signal_points}")

    @property
    def signal_processes(self):
        assert self.dType == 'signal', "Signal processes only available for signal datasets"
        return set([d.signal_process for d in self.datasets.values() if hasattr(d, 'signal_process')])

    @property
    def signal_process(self):
        assert self.dType == 'signal', "Signal process only available for signal datasets"
        signal_processes = set([d.signal_process for d in self.datasets.values() if hasattr(d, 'signal_process')])
        if len(signal_processes) == 1:
            return signal_processes.pop()
        elif len(signal_processes) > 1:
            return "-".join(signal_processes)
        else:
            raise ValueError(f"Signal process not found in Datasets: {self.datasets.keys()}")

    # Access methods
    def __iter__(self):
        return iter(self.datasets.values())

    def __len__(self):
        return len(self.datasets.keys())

    def __getitem__(self, key):
        """Return a subset of the datasets"""
        if not isinstance(key, list): key = [key]

        subset = []
        for k in key:
            if isinstance(k, self.dataset_class):
                subset.append(k)
            elif isinstance(k, int):
                subset.append(self.datasets.values()[k])
            elif isinstance(k, slice):
                subset.extend(list(self.datasets.values())[k])
            elif isinstance(k, str):
                subset += [d for name, d in self.datasets.items() if k in name or k == name]
            elif isinstance(k, SignalPoint):
                subset += [d for d in self.datasets.values() if d.signal_point == k]
            else:
                raise ValueError(f'Class Datasets does not support key type {type(k)}')
        
        if len(subset) == 0:
            raise KeyError(f'Key {key} not found in {self.__class__}')
        elif len(subset) == 1:
            return subset[0]
        else:
            copy =  self.copy()
            copy.datasets = {d.name: d for d in subset}
            return copy

    def copy(self, **kwargs):
        return type(self)(
            self.dType, self.era, self.data_format,
            **self.kwargs,
            **kwargs
        )

### Cross Sections
### TODO: Somehow need to specify the MiniAOD files to run on - right now I will use the existing framework
def write_ana_output(input_file, output_file):
    command = textwrap.dedent(f"""
        source /cvmfs/cms.cern.ch/cmsset_default.sh
        cd /afs/crc.nd.edu/user/a/atownse2/Public/RSTriPhoton/preprocessing/lobster/releases/CMSSW_15_0_0_pre3/src
        cmsenv
        cmsRun ana.py inputFiles={input_file} maxEvents=-1
        """) # TODO: maybe need to move this release

    # Open a log file to write both stdout and stderr
    with open(output_file, "w") as logfile:
        # Start the process in bash
        process = subprocess.Popen(
            ["bash", "-c", command],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1  # Line-buffered
        )

        # Stream the output line by line
        for line in process.stdout:
            # print(line, end="")        # Optional: also print to terminal
            logfile.write(line)        # Write to log file

        process.wait()  # Wait for the process to finish

def parse_xs(xs_file):
    """Parse the cross section file and return the cross section and error"""
    xs_txt = open(xs_file).readlines()

    # Find the line starting with
    the_line_starts_with = "After filter: final cross section ="
    lines = [l for l in xs_txt if the_line_starts_with in l]
    if len(lines) == 0:
        raise ValueError(f"Cross section not found in {xs_file}")
    if len(lines) > 1:
        raise ValueError(f"Multiple cross sections found in {xs_file}: {lines}")
    line = lines[0]
    xs, xs_err = line.split("=")[1].replace("pb", "").split("+-")
    xs = float(xs.strip())
    xs_err = float(xs_err.strip())
    return {"xs": xs, "xs_error": xs_err}

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
