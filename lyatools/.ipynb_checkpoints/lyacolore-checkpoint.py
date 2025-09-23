import os
import subprocess
from pathlib import Path

__DIR__ = os.path.dirname(os.path.realpath(__file__))

def create_sbatch_script(colore_out_loc, lyacolore_out_loc, lyacolore_path, config_file, output_file, conda_environment, nnodes=8, job_dependency=None):
    dependency_str = f'#SBATCH --dependency=afterok:{job_dependency}\n' if job_dependency else ''

    script_content = f"""#!/bin/bash -l

#SBATCH --qos regular
#SBATCH --job-name=lyacolore          # Job name
#SBATCH --output={lyacolore_out_loc}/lyacolore_job.out    # Standard output and error log
#SBATCH --error={lyacolore_out_loc}/lyacolore_job.err     # Error log
#SBATCH --time=00:15:00               # Time limit hrs:min:sec
#SBATCH --nodes=8              # Number of nodes
#SBATCH -C cpu                        # Constraint to specify CPU nodes
#SBATCH -A desi                       # Account
{dependency_str}
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
<<<<<<< HEAD
{conda_environment}
=======
conda activate {conda_environment}
>>>>>>> 277b2c1 (1st version of adding lyacolore option for raw mocks)
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

    with open(output_file, 'w') as f:
        f.write(script_content)
#            self.skewers_path = raw_path / skewers_name / skewers_version / f'skewers-{mock_seed}'

def run_sbatch_script(box, mock_setup, lyacolore_setup, job, output_file, nnodes=8, job_dependency=None):
    print(f'Starting lyacolore skewers for box {box}.')
    no_submit=job.getboolean('no_submit')
    #defining the inputs of the slurm file
    mock_type = lyacolore_setup.get('mock_box_type')
    skewers_name = mock_setup.get('skewers_name')
    skewers_version = mock_setup.get('skewers_version')
<<<<<<< HEAD
    lyacolore_path=lyacolore_setup.get('lyacolore_env')
    colore_out_loc = Path(lyacolore_setup.get('boxes_dir')) / mock_type / f'box-{box}' / 'results'
    lyacolore_out_loc = Path(lyacolore_setup.get('lyacolore_dir')) / skewers_name / skewers_version / f'skewers-{box}'
    conda_environment = job.get('env_command')
=======
    lyacolore_path=lyacolore_setup.get('lyacolore_path')
    colore_out_loc = Path(lyacolore_setup.get('boxes_dir')) / mock_type / f'box-{box}' / 'results'
    lyacolore_out_loc = Path(lyacolore_setup.get('lyacolore_dir')) / skewers_name / skewers_version / f'skewers-{box}'
    conda_environment = job.get('env_lyacolore')
>>>>>>> 277b2c1 (1st version of adding lyacolore option for raw mocks)
    config_file = os.path.join(__DIR__, "input_files/lyacolore/config_v9.0.ini")
    output_file=Path(lyacolore_setup.get('lyacolore_dir'))  / skewers_name / skewers_version / f'skewers-{box}'/'scripts'/'lyacolore_script.sh'
    scripts_dir=Path(lyacolore_setup.get('lyacolore_dir'))  / skewers_name / skewers_version / f'skewers-{box}'/'scripts'
    # Ensure the lyacolore output directory exists
    if not os.path.exists(lyacolore_out_loc):
        os.makedirs(Path(lyacolore_out_loc))   
    if not os.path.exists(scripts_dir):
        os.makedirs(scripts_dir)    

    # Call the function to create the sbatch script
    create_sbatch_script(colore_out_loc, lyacolore_out_loc, lyacolore_path, config_file, output_file, conda_environment, nnodes=8, job_dependency=job_dependency)

    # Print the dependency information
    if job_dependency:
        print(f"Job {box} depends on job {job_dependency}")

    if no_submit is False:    
    # Submit the sbatch script and return the job ID
        result = subprocess.run(['sbatch', output_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            print(f"Error submitting sbatch script: {result.stderr}")
            return None

        job_id = result.stdout.strip().split()[-1]
        print(f"Submitted batch job {job_id}")
    else:
        job_id=None
        print("Skipping LyaCoLoRe")
    return job_id

def run_lyacolore(mock_setup, lyacolore_setup, job, dependency_ids=None):
    #Get box list
    
    no_submit=job.getboolean('no_submit')
    boxes=mock_setup.get('mock_seeds')
    boxes_str = mock_setup.get('mock_seeds')
    if '-' in boxes_str:
        start, end = map(int, boxes_str.split('-', 1))
        # usa fin INCLUSIVO; si quieres exclusivo, quita el +1
        box_list = list(range(start, end + 1))
    else:
        box_list = [int(boxes_str)]
        
    jobids = []
    for box in box_list:
        #Create job dependency on the CoLoRe run with the same seed 
        job_dependency = dependency_ids.get(box) if dependency_ids else None
        #Call function to submit the scripts and return jobids 
        jobid = run_sbatch_script(box, mock_setup, lyacolore_setup, job, nnodes=8, output_file=f'lyacolore_script_{box}.sbatch', job_dependency=job_dependency)
        if no_submit is False:
            jobids.append(int(jobid))
        else:
            jobids = ""
    return jobids



