#!/bin/bash

# Unset variables that might interfere with micromamba
unset PYTHONPATH PYTHONHOME PERL5LIB

export MAMBA_EXE='/project01/ndcms/atownse2/.local/bin/micromamba';
export MAMBA_ROOT_PREFIX='/project01/ndcms/atownse2/micromamba';
__mamba_setup="$("$MAMBA_EXE" shell hook --shell bash --root-prefix "$MAMBA_ROOT_PREFIX" 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__mamba_setup"
else
    alias micromamba="$MAMBA_EXE"  # Fallback on help from micromamba activate
fi
unset __mamba_setup

# Activate the environment
micromamba activate triphoton-env

# Run the provided Python script with all additional arguments
exec python "$@"
