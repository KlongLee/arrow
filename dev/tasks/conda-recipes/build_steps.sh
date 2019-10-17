#!/usr/bin/env bash

# PLEASE NOTE: This script has been automatically generated by conda-smithy. Any changes here
# will be lost next time ``conda smithy rerender`` is run. If you would like to make permanent
# changes to this script, consider a proposal to conda-smithy so that other feedstocks can also
# benefit from the improvement.

set -xeuo pipefail

output_dir=${1}

export PYTHONUNBUFFERED=1
export FEEDSTOCK_ROOT="${FEEDSTOCK_ROOT:-/home/conda/feedstock_root}"
export CI_SUPPORT="${FEEDSTOCK_ROOT}/.ci_support"
export CONFIG_FILE="${CI_SUPPORT}/${CONFIG}.yaml"

cat >~/.condarc <<CONDARC

conda-build:
 root-dir: ${output_dir}

CONDARC

conda install --yes --quiet conda-forge-ci-setup=2 conda-build -c conda-forge

# set up the condarc
setup_conda_rc "${FEEDSTOCK_ROOT}" "${FEEDSTOCK_ROOT}" "${CONFIG_FILE}"

source run_conda_forge_build_setup

# make the build number clobber
make_build_number "${FEEDSTOCK_ROOT}" "${FEEDSTOCK_ROOT}" "${CONFIG_FILE}"

export CONDA_BLD_PATH="${output_dir}"

conda build \
    "${FEEDSTOCK_ROOT}/arrow-cpp" \
    "${FEEDSTOCK_ROOT}/parquet-cpp" \
    "${FEEDSTOCK_ROOT}/pyarrow" \
    -m "${CI_SUPPORT}/${CONFIG}.yaml" \
    --clobber-file "${CI_SUPPORT}/clobber_${CONFIG}.yaml" \
    --output-folder "${output_dir}"

touch "${output_dir}/conda-forge-build-done-${CONFIG}"
