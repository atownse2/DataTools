# This script takes a python script and any number of arguments and runs the script in the virtual environment
unset PYTHONPATH
unset PYTHONHOME
unset PERL5LIB

# Use CMSSW env
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd /project01/ndcms/atownse2/Combine/CMSSW_14_1_0_pre4/src
eval `scramv1 runtime -sh`
cd -

python3 $1 "${@:2}"