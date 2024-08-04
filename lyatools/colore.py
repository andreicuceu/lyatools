from pathlib import Path
from subprocess import run # To send runs 
from multiprocessing import Pool # To run multiple seeds in parallel
import numpy as np
from os import mkdir
from astropy.io import fits  # Import fits from astropy

from lyatools.tasker import get_Tasker # To organize runs outs and logs
from lyatools.dict_utils import DictUtils # To check if two dicts are the same 
from lyatools import submit_utils
import libconf # To read CoLoRe config file.

import os 

__DIR__ = os.path.dirname(os.path.realpath(__file__))

        
def create_empty_fits(file_path):
    hdu = fits.PrimaryHDU()
    hdul = fits.HDUList([hdu])
    hdul.writeto(file_path, overwrite=True)        

def compute_colore(box, analysis_struct, mock_version, colore_setup, job, version): # We define function to then run multiple seeds using multiprocessing
    no_submit=job.getboolean('no_submit')
    n_grid = 4096 # Grid size
    overwrite_colore = True # Overwrite if already exists.
    overwrite_corrf = False 
    overwrite_config=False
    # We need the path to the CoLoRe executable to run it.
    colore_executables = colore_setup.get('colore_path')
    env_colore= job.get('env_colore')
    colore_box = Path(colore_setup.get('colore_dir'))/'colore'/ f'box-{box}'
    (colore_box / "results").mkdir(exist_ok=True, parents=True) # Create folder structure
    
    starting_seed=colore_setup.get('starting_seed')
    colore_seed=box+int(starting_seed)
    # Now we create the param.cfg for CoLoRe in a dict, so it can be customize
    param_cfg = {
        "global": {
            "prefix_out": f"{colore_box}/results/out",
            "output_format": "FITS",
            "output_density": False,
            "pk_filename": os.path.join(__DIR__, "input_files/colore/smoothed_mu0_NL_matter_powerspectrum_DR12.dat"),
            "z_min": 1.6,
            "z_max": 3.79,
            "write_pred": False,
            "just_write_pred": False,
            "seed": colore_seed,
            "pred_dz": 0.1,
        },
        "field_par": {
            "r_smooth": 2.0,
            "smooth_potential": True,
            "n_grid": n_grid,
            "dens_type": 0,
            "lpt_buffer_fraction": 0.6,
            "lpt_interp_type": 1,
            "output_lpt": 0,
        },
        "cosmo_par": {
            "omega_M": 0.3147,
            "omega_L": 0.6853,
            "omega_B": 0.04904,
            "h": 0.6731,
            "w": -1.0,
            "ns": 0.9655,
            "sigma_8": 0.83,
        },
        "srcs1": { # here we are adding multiple sources with different choices of bias and threshold.
            "nz_filename" : os.path.join(__DIR__, "input_files/colore/Nz.txt"),
            "bias_filename": os.path.join(__DIR__, "input_files/colore//bias_101.txt"),
            "threshold_filename": os.path.join(__DIR__, "input_files/colore/threshold_101.txt"),
            "include_shear": False,
            "include_lensing": False,
            "store_skewers": True,
            "gaussian_skewers": True,
        },

            
    }
    
    # If paramcfg already exists, we  need to check that the content is the same
    if (colore_box / "param.cfg").is_file():
        with open(colore_box / "param.cfg") as f:
            existing_config = libconf.load(f) # We use libconf to read the file
        
        diff = DictUtils.remove_empty(
            DictUtils.diff_dicts(existing_config, param_cfg)
        )
        if diff != dict():
            raise ValueError("Different param provided", diff)        
            
    with open(colore_box/'results' / "params.cfg", "w") as f:
        libconf.dump(param_cfg, f) # Write configuration to file.
        
    args = {
        "": str(colore_box /'results' / "params.cfg"), # This is the only terminal arg needed to run CoLoRe
    }
    
    # Create the logs directory
    colore_logs_dir = colore_box / "logs"
    colore_logs_dir.mkdir(exist_ok=True)
    
    # This is to appropiate set the output and error files
    # j will be subtituted by the time of execution
    # all of this is handled by picca_bookkeeper
    slurm_header_args = dict(
        qos="regular",
        nodes=8,
        time = "00:15:00",
        output=str(colore_logs_dir / "CoLoRe-%j.out"),
        error=str(colore_logs_dir / "CoLoRe-%j.err"),
        constraint = "cpu",
        account = "desi",
    )
    slurm_header_args["job-name"] = "CoLoRe"
    slurm_header_args["ntasks-per-node"] = 8
    
    # Create the scripts directory
    colore_scripts_dir = colore_box / "scripts"
    colore_scripts_dir.mkdir(exist_ok=True)
    
    # Create the tasker instance that will be responsible of sending the job.
    tasker = get_Tasker("slurm_perlmutter")( # bash means: do not run it in a computing node.
        command = colore_executables,
        command_args = args,
        environment = env_colore, # Name or path to the conda environment to be activated through ``source/conda activate``
        slurm_header_args = slurm_header_args,
        jobid_log_file = colore_logs_dir / "jobids.log", # This is only used for chaining slurm jobs, not needed here.
        run_file = colore_scripts_dir.resolve() / "run_colore.sh", # This is the file that will be executed
        force_OMP_threads = 128
    )
    
    for i in range(0, 64):  # Create 10 empty FITS files for demonstration
        empty_fits_file = colore_box / 'results' / f"out_srcs_{i}.fits"
        create_empty_fits(empty_fits_file)
    
    if len(list((colore_box/"results").glob("out_srcs*fits"))) == 0 or overwrite_colore:
        # If there are no results, we just run the job
        tasker.write_job()
        print(tasker._make_body())
        
        if no_submit is False:
            tasker.send_job()
            print(f"Sending CoLoRe job for box {box} --> seed {colore_seed}")

        else:
            print("Skipping CoLoRe")

    else:
        print("Skipping CoLoRe")
           
    return box, tasker.jobid



def run_colore(analysis_struct, mock_version, colore_setup, job, nproc_colore=1, version="base"):
    boxes=colore_setup.get('colore_boxes')
    boxes_range = list(map(int, boxes.split('-')))
    if len(boxes_range) == 1:
        box_list = boxes_range
    else:
        box_list=list(range(boxes_range[0], boxes_range[1]))
    jobids = dict()
    job_ids=[]
    # Create a multiprocessing Pool
    with Pool(nproc_colore) as pool:
        # Create a list of asynchronous tasks
        tasks = [pool.apply_async(
            compute_colore,
            args=(box, analysis_struct, mock_version, colore_setup, job, version)
        ) for box in box_list for version in ["thres_bias",]]

        # Iterate over the tasks to get the results
        for result in tasks:
            box, jobid = result.get()
            jobids[box] = jobid
            job_ids.append(jobids)
   
    return jobids