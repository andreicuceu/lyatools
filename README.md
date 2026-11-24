# Lyatools
This package started with some tools and scripts I use for Lyman-alpha forest analyses, but has since evolved into the mock production and analysis pipeline for the DESI Lyman-alpha BAO measurements. It contains functions and scripts for running the different parts of the mock production process which involves the QuickQuasars script in [desisim](https://github.com/desihub/desisim), and the mock analysis using [picca](https://github.com/igmhub/picca.git), [QSOnic](https://github.com/p-slash/qsonic), and/or [lya2pt](https://github.com/igmhub/lya_2pt).

* Free software: GPL-3.0 License

## Installation
Lyatools has similar dependencies with picca, and even directly calls some picca code. Therefore I recommend installing it in the same conda environment with picca. To do this, first follow the installation instructions in [picca](https://github.com/igmhub/picca/tree/master#installation), and then do:

    git clone https://github.com/andreicuceu/lyatools.git
    cd lyatools
    pip install -e .

## Usage
Lyatools has a number of scripts (checkout [scripts](https://github.com/andreicuceu/lyatools/tree/main/lyatools/scripts) to see all of them), however the most common one is `lyatools-run`, which requires a configuration file to run. You can find example config files [here](https://github.com/andreicuceu/lyatools/tree/main/examples). To run, simply do:

    lyatools-run -i path/to/config.ini

Note that Lyatools has a `no_submit` option in the config file, which controls the submission of jobs to NERSC. I strongly recommend first setting this to `True` and running once to check whether all the scripts that Lyatools produces are correct for your desired run.

To run Lyatools, you will also need two environment commands, one for picca, and one for the DESI environment. These could either be a bash function or the name of an alias. For picca, I recommend to add something like this to you `bashrc` file:

    piccaenv () {
    module load python
    conda activate picca
    }

You would then need to set the name of this function in the Lyatools config file: `env_command = piccaenv`. For the DESI environment, you can do the same if you want to have your own custom environment (see example below), but you can also comment out the `desi_env_command` option, which will just use the default DESI master branch at NERSC.

## Seed catalogs: the Lya coverage of a release

`gen_qso_catalog` (desisim) gives the mock Lya quasar targets the footprint, completeness (QSO targets and non-QSO targets, i.e. quasars selected
as ELG or other classes) and exposure times of a data release (whole nominal passes of 1000 s). By default (`selection_method = randoms` in
`[quickquasars]`) it uses the **Lya coverage** of the release, `lya_<release>_coverage.fits`, made by `build_lya_coverage` from the LSS randoms.

lyatools builds the coverage first, once per run, in the directory where the mocks are stored
(`<mock_start_path>/<survey_name>/<skewers_version>.<qq_version>/coverage/`, one file per release), and every seed catalog waits for that job. An existing coverage file is reused,
and `lya_coverage = /path/to/coverage.fits` points to one elsewhere. Set `lss_randoms_dir` (the LSS directory with `rancomb_*dark*wdupspec_zdone.fits`)
and `input_data_catalog`; the SLURM environment (`desi_env_command`) must provide a desisim with `build_lya_coverage`. With
`run_mocks_individually = False` all the seed catalogs of the run are made in one job, 64 at the same time (they are independent and take about 1.7 GB each; a wave takes about
a minute, so 300 mocks take about 5 minutes). The walltime of that job grows with the number of mocks (2 minutes plus 3 per wave of 64, at least 10 minutes);
`seed_catalog_hours` in `[quickquasars]` sets it by hand.
`selection_method = tiles` gives back the previous tile-circle method. The skewers folders are only created when `run_lyacolore = True`.

## quickquasars options (lyatools 1.1, new desisim)

lyatools 1.1 needs the desisim with the Lya coverage, DLA+, `--no-lya-transmission` and the quickquasars cleanup. That quickquasars always writes the zbest files
and no longer has `--zbest`, `--bbflux`, `--zmin` or the other options of the old footprint and density modes, so configs and run types of older
lyatools versions do not run with it (use the previous lyatools and desisim to reproduce older mocks). The digits of `qq_run_type` (e.g. `matterhorn-124`) are:

| digit | quickquasars options |
|---|---|
| 1 | `--dla file`: DLAs of the transmission files, always with the higher Lyman lines (DLA+; desisim PR #606 removed `--dlaplus`) |
| 2 | `--metals ...` with `metal_strengths` (or the named tuning `metal_strengths_label`, e.g. `lyacolore-2lpt`) |
| 3 | `--metals-from-file all` |
| 4 | `--balprob 0.16` |
| 6 | `--no-lya-transmission`: no Lya absorption; with 1 and/or 2, DLA-only, metal-only or both (not with 3) |

The DLA catalog used for masking keeps only DLAs whose Lya is observed, 1215.67 (1 + z_DLA) >= `dla_mask_lambda_min` (default 3600 A, the start
of the DESI spectra); `dla_mask_lambda_min = 0` gives the catalogs of the DR2 mocks. See `examples/desi_dr3_mocks.ini`.

## Instructions for running DESI Y1 Lyman-alpha mocks at NERSC
Here are instructions to setup the same environment as the one used to run the set of Y1 mocks for Key Project 6 in DESI Y1. 

Warning: Make sure your bashrc script does not activate anything automatically. Not even `module load python`. Otherwise, the scripts may not work in some cases. It is best to keep it clean of direct calls and keep any environment activations in bash functions or in aliases.

First you need to setup your own custom DESI environment. You will need three packages: [desisim](https://github.com/desihub/desisim), [specsim](https://github.com/desihub/specsim), and [desimodel](https://github.com/desihub/desimodel). For each of these make note of the path where you cloned these packages, as it will be needed later.

Starting with specsim, do:

    git clone https://github.com/desihub/specsim.git
    cd specsim
    git fetch
    git checkout binning

Moving on to desimodel, do:

    git clone https://github.com/desihub/desimodel.git
    cd desimodel
    svn export https://desi.lbl.gov/svn/code/desimodel/tags/0.19.0/data
    cd data
    cp -r /global/cfs/cdirs/desi/users/dkirkby/desimodel-data/* .

For more details on why these two are needed, see [this pull request](https://github.com/desihub/specsim/pull/125).

Then moving to desisim, do:

    git clone https://github.com/HiramHerrera/desisim.git
    cd desisim
    git fetch
    git checkout y1mocks_v3
    
Once you have done these three steps, you can setup your DESI environment command. Open your bashrc file and add this function:

    mydesienv () {
    source /global/common/software/desi/desi_environment.sh master
    
    module unload desimodel
    export PYTHONPATH=/path/to/desimodel/py:$PYTHONPATH
    export PATH=/path/to/desimodel/bin:$PATH
    export DESIMODEL=/path/to/desimodel/
    
    module unload specsim
    export PYTHONPATH=/path/to/specsim:$PYTHONPATH
    export PATH=/path/to/specsim/bin:$PATH
    export SPECSIM=/path/to/specsim
    
    module unload desisim
    export PYTHONPATH=/path/to/desisim/py:$PYTHONPATH
    export PATH=/path/to/desisim/bin:$PATH
    export DESISIM=/path/to/desisim
    export DESI_BASIS_TEMPLATES=/global/cfs/cdirs/desi/spectro/templates/basis_templates/v3.2
    }

Make sure to replace all the paths in the function above with the path where you installed the three packages. Once this is done, you can now set `desi_env_command = mydesienv` in the Lyatools config file.

Finally, you will need a picca environment with Lyatools installed. For the first part, just follow the instructions in [picca](https://github.com/igmhub/picca/tree/master#installation), or if you have a picca environment already, just go to picca, check you are on the master branch, and do `git pull`. To install Lyatools follow the instructions above, and then put this function in your bashrc file:

    piccaenv () {
    module load python
    conda activate picca
    }
and add `env_command = piccaenv` to your config file.

You are now ready to run DESI Lyman-alpha Y1 mocks. There are two example Y1 config files you can checkout for running the two types of mocks: [LyaCoLoRe](https://github.com/andreicuceu/lyatools/blob/main/examples/desi_lyacolore_y1.ini) and [Saclay](https://github.com/andreicuceu/lyatools/blob/main/examples/desi_saclay_y1.ini).
