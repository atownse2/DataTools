# This script takes a python script and any number of arguments and runs the script in the virtual environment

unset PYTHONPATH
unset PYTHONHOME
unset PERL5LIB

# # Use mamba env
# # !! Contents within this block are managed by 'micromamba shell init' !!
# export MAMBA_EXE='/project01/ndcms/atownse2/.local/bin/micromamba';
# export MAMBA_ROOT_PREFIX='/project01/ndcms/atownse2/micromamba';
# __mamba_setup="$("$MAMBA_EXE" shell hook --shell bash --root-prefix "$MAMBA_ROOT_PREFIX" 2> /dev/null)"
# if [ $? -eq 0 ]; then
#     eval "$__mamba_setup"
# else
#     alias micromamba="$MAMBA_EXE"  # Fallback on help from micromamba activate
# fi
# unset __mamba_setup

# micromamba activate triphoton-env

# # Run the python script with the arguments passed to this script
# which python

# Use CMSSW env
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd /project01/ndcms/atownse2/Combine/CMSSW_14_1_0_pre4/src
eval `scramv1 runtime -sh`
cd -

python3 $1 "${@:2}"