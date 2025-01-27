import os
import json

from typing import List, Union
from . import storage_config as stor
from . import dataset_info

import uproot
##

## Signal mass grid and functions
# mass_grids = {
#     'old' : {
#         'M_BKK' : [180, 250, 500, 1000, 3000],
#         'MOE' : [0.04, 0.02, 0.01, 0.005, 0.0025]
#     },
#     'current' : {
#         'M_BKK' : [180, 250, 500, 1000, 1500, 2000, 2500, 3000],
#         'MOE' : [0.04, 0.02, 0.01, 0.005, 0.0025]
#     },
# }

# # Calculate mass grid from list of BKK masses and MOEs
# def get_mass_grid(version):
#     '''Get mass grid from list of BKK masses and MOEs'''
#     BKKs = mass_grids[version]['M_BKK']
#     MOEs = mass_grids[version]['MOE']
#     return [SignalPoint(M_BKK=M_BKK, MOE=MOE) for M_BKK in BKKs for MOE in MOEs]

def get_n_events(signal_point, year, data_format='MLNanoAODv9'):
    name = signal_point.name() + f'_{year}_{data_format}'

    n_events_dict = {}

    cache_file = stor.cache_dir+'/signal_n_events.json'
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as f:
            n_events_dict = json.load(f)
        
        if name in n_events_dict:
            return n_events_dict[name]
    
    n_events = 0
    data_dir = "/project01/ndcms/atownse2/mc/MLNanoAODv9"
    for file in os.listdir(data_dir):
        if name not in file:
            continue

        with uproot.open(f'{data_dir}/{file}') as f:
            n_events += f['Events'].num_entries

    n_events_dict[name] = n_events

    with open(cache_file, 'w') as f:
        json.dump(n_events_dict, f, indent=4)
    
    return n_events

class SignalPoint:
    tag = 'BkkToGRadionToGGG'

    def __init__(
        self,
        M_BKK: Union[int, float] = None,
        M_R: Union[int, float] = None,
        MOE: Union[int, float] = None,
        tag: str = None,
        ):

        if tag is not None:
            M_BKK, M_R = self.from_tag(tag)

        assert M_BKK is not None and (M_R is not None or MOE is not None), "Must specify M_BKK and either M_R or MOE"
        assert M_R is None or MOE is None, "Cannot specify both M_R and MOE"

        self.M_BKK = M_BKK

        if M_R is not None:
            self.M_R = M_R
            self.MOE = round(M_R/(M_BKK/2), 4)
        else:
            self.M_R = round((M_BKK/2)*MOE, 4)
            self.MOE = MOE
        self.Mass_Ratio = self.M_R/self.M_BKK

    def __eq__(self, other):
        return self.M_BKK == other.M_BKK and self.M_R == other.M_R

    def __hash__(self):
        return hash((self.M_BKK, self.M_R))

    def from_tag(self, tag):
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
    
    def name(self, decimal=False):
        M_BKK = self.M_BKK
        M_R = self.M_R

        # Remove decimal if integer
        if M_BKK/int(M_BKK) == 1:
            M_BKK = int(M_BKK)
        if int(M_R) != 0 and M_R/int(M_R) == 1:
            M_R = int(M_R)
        
        tag = f'{self.tag}_M1-{M_BKK}_R0-{M_R}'
        if decimal == False:
            tag = tag.replace('.', 'p')

        return tag

    def n_events(self, era):
        years = dataset_info.get_years_from_era(era)
        return sum([get_n_events(self, year) for year in years])

    @property
    def xs(self):
        xs_pb = get_signal_xs(self.M_BKK, self.M_R)['xs']
        xs_fb = xs_pb*1000
        return xs_fb
    
    @property
    def xs_error(self):
        xs_error_pb = get_signal_xs(self.M_BKK, self.M_R)['error']
        xs_error_fb = xs_error_pb*1000
        return xs_error_fb

gridpack_dir = '/cms/cephfs/data/store/user/atownse2/RSTriPhoton/gridpacks/'
gridpack_name = lambda M_BKK, M_R: f'BkkToGRadionToGGG_M1-{M_BKK}_R0-{M_R}_slc7_amd64_gcc10_CMSSW_12_4_8_tarball.tar.xz'

def get_signal_xs(M_BKK, M_R):

    signal_point = SignalPoint(M_BKK=M_BKK, M_R=M_R)
    _entry = f"{signal_point.M_BKK}_{signal_point.M_R}"

    xs_dict = {}
    xs_file = stor.cache_dir+'/signal_xs_pb.json'
    if os.path.exists(xs_file):
        with open(xs_file, 'r') as f:
            xs_dict = json.load(f)
    
    if _entry in xs_dict:
        return xs_dict[_entry]
    
    gridpack = f'{signal_point.name()}_slc7_amd64_gcc10_CMSSW_12_4_8_tarball.tar.xz'

    # Make workspace
    tmpdir = f'/tmp/{stor.USER}'
    if os.path.exists(tmpdir):
        os.system(f'rm -rf {tmpdir}')
    os.mkdir(tmpdir)
    os.chdir(tmpdir)

    # Unpack gridpack
    os.system(f'tar -xf {gridpack_dir+gridpack}')

    # Read cross section from .log file
    log_file = 'gridpack_generation.log'
    with open(log_file, 'r') as f:
        for line in f:
            if 'Cross-section :' in line:
                split_line = line.split()
                xs = float(split_line[2])
                error = float(split_line[4])
                if xs > 0:
                    xs_dict[_entry] = {'xs': xs, 'error': error}
    
    # Clean up
    os.system(f'rm -rf  {tmpdir}')

    # Save cross sections to yaml file
    with open(xs_file, 'w') as f:
        json.dump(xs_dict, f, indent=4)
    
    return xs_dict[_entry]


# Unpack gridpack in temporary directoy and read cross section from .log file
# import os
# import sys

# import json

# top_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# sys.path.append(top_dir)

# import argparse

# parser = argparse.ArgumentParser()
# parser.add_argument('--test', '-t', action='store_true', help='Run on one gridpack')

# args = parser.parse_args()

# gridpack_dir = '/hadoop/store/user/atownse2/RSTriPhoton/gridpacks/'
# tmpdir = '/tmp/atownse2'

# # Clean up tmpdir
# os.system(f'rm -rf {tmpdir}/*')

# log_file = 'gridpack_generation.log'
# xs_file = f'{top}/analysis/metadata/json/signal_xs.json'

# # Load existing cross sections
# if os.path.exists(xs_file):
#     with open(xs_file, 'r') as f:
#         xs_dict = json.load(f)
# else:
#     xs_dict = {}

# for point in sample_info.mass_grid:

#     if point in xs_dict:
#         continue

#     filetag = sample_info.get_signal_filetag(point)

#     # Get gridpack name
#     gridpack = [f for f in os.listdir(gridpack_dir) if filetag in f][0]

#     # Make workspace
#     os.chdir(tmpdir)
#     os.system(f'mkdir {filetag}')
#     os.chdir(f'{tmpdir}/{filetag}')

#     # Unpack gridpack
#     os.system(f'tar -xf {gridpack_dir+gridpack}')

#     # Read cross section from .log file
#     with open(log_file, 'r') as f:
#         for line in f:
#             if 'Cross-section :' in line:
#                 split_line = line.split()
#                 xs = float(split_line[2])
#                 error = float(split_line[4])
#                 if xs > 0:
#                     print(line)
#                     print(f'{filetag}: {xs} +/- {error}')
#                     xs_dict[filetag] = {'xs': xs, 'error': error}
    
#     # Clean up
#     os.system(f'rm -rf {tmpdir}/{filetag}')

#     if args.test:
#         break


# # Save cross sections to yaml file
# with open(xs_file, 'w') as f:
#     json.dump(xs_dict, f, indent=4)