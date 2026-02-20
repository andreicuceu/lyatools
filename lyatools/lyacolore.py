import os
from pathlib import Path
from . import submit_utils

__DIR__ = os.path.dirname(os.path.realpath(__file__))


def create_lyacolore_script(
    colore_out_loc, lyacolore_out_loc, lyacolore_path, config_file, conda_environment
):
    script_content = f"""
################################################################################
## USER DEFINED PARAMS.

CONFIG_FILE="{config_file}"

CONDA_ENVIRONMENT={conda_environment} # Conda environment to use

# Set where your CoLoRe output is located, and where you would like your
# LyaCoLoRe output to be located.
COLORE_OUT_LOC="{colore_out_loc}"
LYACOLORE_OUT_LOC="{lyacolore_out_loc}"
export LYACOLORE_PATH="{lyacolore_path}"

# Specify the settings for LyaCoLoRe.
NNODES=16
NCORES=128
TIME="00:15:00" #hh:mm:ss

## END OF USER DEFINED PARAMS.
################################################################################

# Change to the required directory
cd {lyacolore_path}

## Echo the settings and outputs chosen for each realization.

echo " "
echo "################################################################################"
echo " "
echo "CoLoRe input will be taken from {colore_out_loc}"
INPUT_FILES=$(ls -1 {colore_out_loc}/out_srcs_*.fits)
NFILES=$(echo $INPUT_FILES | wc -w)
echo " -> $NFILES input files have been found"
echo "Output will be written to {lyacolore_out_loc}"
if [ ! -d {lyacolore_out_loc} ]; then
    mkdir -p {lyacolore_out_loc}
fi
echo " -> Output logs will be saved to {lyacolore_out_loc}/logs"
if [ ! -d {lyacolore_out_loc}/logs ]; then
    mkdir -p {lyacolore_out_loc}/logs
fi

################################################################################
## Make the master file and the new file structure.
echo " "
echo "Starting LyaCoLoRe..."
echo " "
echo " 1. Make master file"
echo " "
source $(conda info --base)/etc/profile.d/conda.sh
{conda_environment}
{lyacolore_path}/scripts/make_master.py -c {config_file} -i {colore_out_loc} -o {lyacolore_out_loc} --nproc 128

################################################################################
## Create transmission skewers.
echo " "
echo " 2. Create transmission skewers"
echo " "

umask 0002
export OMP_NUM_THREADS=2

PIXDIRS=$(ls -tr1d {lyacolore_out_loc}/[0-9]*/* | sort -R)
NPIXELS=$(echo $PIXDIRS | wc -w)
PIXDIRS_list=($PIXDIRS)

PIXELS=()
for PIXDIR in $PIXDIRS; do
    PIX=${{PIXDIR##*/}}
    PIXELS=("${{PIXELS[@]}}" $PIX)
done

NPIXELS_PER_NODE=$(( ($NPIXELS + 8 - 1) / 8 ))
START_INDEX=0
STOP_INDEX=$(( $START_INDEX + $NPIXELS_PER_NODE - 1 ))
FINAL_NODE=0

for NODE in $(seq 8); do
    echo "starting node $NODE"
    NODE_PIXELS=${{PIXELS[@]:$START_INDEX:$NPIXELS_PER_NODE}}
    echo "looking at pixels: $NODE_PIXELS"
    command="srun -N 1 -n 1 -c 128 {lyacolore_path}/scripts/make_transmission.py -c {config_file} -i {colore_out_loc} -o {lyacolore_out_loc} --nproc 128 --pixels $NODE_PIXELS"
    echo $command
    $command >& {lyacolore_out_loc}/logs/node-$NODE.log &

    if (( $FINAL_NODE == 1)); then
        echo "all pixels allocated, no more nodes needed"
        break
    fi

    START_INDEX=$(( $STOP_INDEX + 1 ))
    STOP_INDEX=$(( $START_INDEX + $NPIXELS_PER_NODE - 1 ))

    if (( $STOP_INDEX >= ($NPIXELS - 1) )); then
        STOP_INDEX=$NPIXELS-1
        FINAL_NODE=1
    fi
done

wait
date

# Copy the config file to the output location for clarity
cp {config_file} {lyacolore_out_loc}

echo " "
echo "Done!"
echo " "
echo "################################################################################"
"""
    return script_content


def run_lyacolore(lyacolore_config, skewers_path, seed, job, dependency_ids=None):
    submit_utils.set_umask()

    output_dir = skewers_path
    config_file = os.path.join(__DIR__, "input_files/lyacolore/config_v9.0.ini")
    lyacolore_install_path = lyacolore_config.get('lyacolore_install_path')
    input_box_dir = Path(lyacolore_config.get('input_box_dir'))
    mock_box_type = lyacolore_config.get('mock_box_type', 'colore')
    input_box_path = input_box_dir / mock_box_type / f'box-{seed}' / 'results'
    env_command = job.get('env_command')
    lyacolore_script = create_lyacolore_script(input_box_path, output_dir, lyacolore_install_path, 
                                               config_file, env_command)

    # Write the script to file and submit it.
    num_nodes = lyacolore_config.getint('num_nodes', 8)
    slurm_hours = lyacolore_config.getfloat('slurm_hours', 0.25)
    header = submit_utils.make_header(
        job.get('nersc_machine'), 'regular', nodes=num_nodes, time=slurm_hours,
        omp_threads=2, job_name=f'lyacolore_{seed}', 
        err_file=output_dir/'logs/run-%j.err', 
        out_file=output_dir/'logs/run-%j.out'
        )
    header += f'{env_command}\n\n'
    full_script = header + lyacolore_script
    script_path = output_dir / 'scripts' / 'run_lyacolore.sh'
    submit_utils.write_script(script_path, full_script)
    job_id = submit_utils.run_job(
        script_path, dependency_ids=dependency_ids,
        no_submit=job.getboolean('no_submit')
    )
    return job_id
