import os

import multiprocessing as mp

import numpy as np
import pandas as pd
import ROOT

from analysis import modeling as md

from analysis.tools import dataset_info as di
from analysis.tools import storage_config as stor
from analysis.tools import condor

import textwrap
# import pickle
import joblib

# combine_cache = stor.ensure_cache("combine")

scripts_dir = f"{stor.top_dir}/scripts"

NCPUS = 8

def run_in_cmssw(workspace_dir, command):
    command = textwrap.dedent(f"""
        source /cvmfs/cms.cern.ch/cmsset_default.sh
        cd /project01/ndcms/atownse2/Combine/CMSSW_14_1_0_pre4/src
        eval `scramv1 runtime -sh`
        cd {workspace_dir}
        {command}
        """)
    os.system(command)

def run_combine(datacard, method, workspace_dir, extra_args=""):

    if extra_args != "":
        command = f"combine -M {method} -d {datacard} {extra_args}\n"
    else:
        command = f"combine -M {method} {datacard}\n"
    run_in_cmssw(workspace_dir, command)
