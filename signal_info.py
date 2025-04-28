import os
import json

from typing import List, Union
from . import storage_config as stor
from . import dataset_info

import uproot
##

signal_processes = [
'BkkToGRadionToGGG',
'BkkToGRadionJetsToGGGJets'
]

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

    # @property
    # def xs(self, signal_process):
    #     xs_pb = get_signal_xs(signal_process, self.M_BKK, self.M_R)['xs']
    #     xs_fb = xs_pb*1000
    #     return xs_fb
    
    # @property
    # def xs_error(self):
    #     xs_error_pb = get_signal_xs(self.M_BKK, self.M_R)['error']
    #     xs_error_fb = xs_error_pb*1000
    #     return xs_error_fb

gridpack_dir = '/cms/cephfs/data/store/user/atownse2/RSTriPhoton/signal/gridpacks/'
gridpack_name = lambda M_BKK, M_R: f'BkkToGRadionToGGG_M1-{M_BKK}_R0-{M_R}_slc7_amd64_gcc10_CMSSW_12_4_8_tarball.tar.xz'

def get_signal_xs_pb(signal_process, signal_point: SignalPoint):

    M_BKK = signal_point.M_BKK
    M_R = signal_point.M_R

    signal_point = SignalPoint(M_BKK=M_BKK, M_R=M_R)
    
    gridpack = [f for f in os.listdir(gridpack_dir) if signal_point.name in f and signal_process in f][0]

    # Make workspace
    import random
    random_number = random.randint(0, 1000000) 
    tmpdir = f'/tmp/{stor.USER}_signal_xs_{signal_process}_{signal_point.short_name}_{random_number}'
    if os.path.exists(tmpdir):
        os.system(f'rm -rf {tmpdir}')
    os.mkdir(tmpdir)
    os.chdir(tmpdir)

    # Unpack gridpack
    os.system(f'tar -xf {gridpack_dir+gridpack}')

    # Read cross section from .log file
    log_file = 'gridpack_generation.log'
    entry = None
    with open(log_file, 'r') as f:
        for line in f:
            if 'Cross-section :' in line:
                split_line = line.split()
                xs = float(split_line[2])
                error = float(split_line[4])
                if xs > 0:
                    entry = {'xs': xs, 'error': error}


    # Clean up
    os.system(f'rm -rf  {tmpdir}')
    
    if entry is None:
        raise ValueError(f"Could not find cross section for {signal_process} {signal_point.name} in {log_file}")

    return entry