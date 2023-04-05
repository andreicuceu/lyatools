import numpy as np
from pathlib import Path
from subprocess import run
from configparser import ConfigParser

from . import dir_handlers, submit_utils


def multi_run_delta_extraction(args):
    if args.test_run:
        print('test run enabled, overriding "--qq_run_type" to set to "desi-test"')
        args.qq_run_type = 'desi-test'
        print('test run enabled, overriding "--slurm-queue" to set to "debug"')
        args.slurm_queue = 'debug'
        submit_utils.print_spacer_line()

    # Get list of seeds
    run_seeds = []
    for seed in args.qq_seeds:
        seed_range = seed.split('-')

        if len(seed_range) == 1:
            run_seeds.append(int(seed_range[0]))
        elif len(seed_range) == 2:
            run_seeds += list(np.arange(int(seed_range[0]), int(seed_range[1])))
        else:
            raise ValueError(f'Unknown seed type {seed}. Must be int or range (e.g. 0-5)')

    run_seeds.sort()

    for seed in run_seeds:
        qq_dir = Path(args.input_dir) / f'v9.0.{seed}' / f'{args.qq_run_type}'
        print(f'Making catalogues for quickquasars run in {qq_dir}')

        # Make the zcat if it does not exist already
        zcat_file = qq_dir / 'zcat.fits'
        if not zcat_file.is_file():
            print('Making DESI zcat')
            command = f'make_zcat.py --qq-dir {qq_dir} --out zcat.fits'
            process = run(command, shell=True)
            if process.returncode != 0:
                raise ValueError(f'Running command "{command}" returned non-zero exitcode '
                                 f'with error {process.stderr}')
            submit_utils.print_spacer_line()

        # Make the drq if it does not exist already
        drq_file = qq_dir / 'drq_qso.fits'
        if not drq_file.is_file():
            print('Making picca drq')
            command = f'make_cooked_drqs.py --in-dir {qq_dir} --out-dir {qq_dir} '
            command += '--zcat-name zcat.fits --drq-name drq_qso.fits'
            process = run(command, shell=True)
            if process.returncode != 0:
                raise ValueError(f'Running command "{command}" returned non-zero exitcode '
                                 f'with error {process.stderr}')
            submit_utils.print_spacer_line()

        submit_utils.print_spacer_line()

        if args.run_true_continuum:
            if args.run_lya_region:
                make_delta_analysis(args, qq_dir, seed, zcat_file, region_name='lya',
                                    lambda_rest_min=1040., lambda_rest_max=1200.,
                                    true_continuum=True)

            if args.run_lyb_region:
                make_delta_analysis(args, qq_dir, seed, zcat_file, region_name='lyb',
                                    lambda_rest_min=920., lambda_rest_max=1020.,
                                    true_continuum=True)

        if not args.no_run_continuum_fitting:
            if args.run_lya_region:
                make_delta_analysis(args, qq_dir, seed, zcat_file, region_name='lya',
                                    lambda_rest_min=1040., lambda_rest_max=1200.)

            if args.run_lyb_region:
                make_delta_analysis(args, qq_dir, seed, zcat_file, region_name='lyb',
                                    lambda_rest_min=920., lambda_rest_max=1020.)


def make_delta_analysis(args, qq_dir, seed, zcat_file, region_name='lya',
                        lambda_rest_min=1040., lambda_rest_max=1200., true_continuum=False):
    print(f'Submitting job to make continuum fitted {region_name} deltas')

    spectra_dir = qq_dir / 'spectra-16'
    main_path = Path(args.output_dir) / f'v9.0.{seed}'
    if not true_continuum:
        analysis_dir = dir_handlers.AnalysisDir(str(main_path), args.qq_run_type)
    else:
        analysis_dir = dir_handlers.AnalysisDir(str(main_path), args.qq_run_type,
                                                deltas_lya_dirname='true_deltas_lya',
                                                deltas_lyb_dirname='true_deltas_lyb')

    if region_name == 'lya':
        deltas_dirname = analysis_dir.deltas_lya_dirname
    elif region_name == 'lyb':
        deltas_dirname = analysis_dir.deltas_lyb_dirname
    else:
        raise ValueError('Unkown region name. Choose from ["lya", "lyb"].')

    run_delta_extraction(spectra_dir, zcat_file, analysis_dir.data_dir, deltas_dirname,
                         region_name, lambda_rest_min, lambda_rest_max, true_continuum, args)


def run_delta_extraction(spectra_dir, catalogue, output_dir, deltas_dirname, region_name,
                         lambda_rest_min, lambda_rest_max, true_continuum, args):
    submit_utils.set_umask()

    run_files_dir = Path(output_dir) / 'run_files'
    scripts_dir = Path(output_dir) / 'scripts'

    dir_handlers.check_dir(run_files_dir)
    dir_handlers.check_dir(scripts_dir)

    # Create the path and name for the config file
    type = 'true' if true_continuum else 'fitted'
    config_path = scripts_dir / f'deltas_{region_name}_{type}.ini'

    # Create the config file for running picca_delta_extraction
    create_config(config_path, spectra_dir, catalogue, output_dir, deltas_dirname,
                  lambda_rest_min, lambda_rest_max, true_continuum, args)

    run_name = f'picca_delta_extraction_{region_name}_{type}'
    slurm_script_path = scripts_dir / f'run_{run_name}.sh'
    time = submit_utils.convert_job_time(args.slurm_hours)

    # Make the header
    header = submit_utils.make_header(args.nersc_machine, args.slurm_queue, time=time,
                                      omp_threads=args.nproc, job_name=run_name,
                                      err_file=run_files_dir/f'{run_name}-%j.err',
                                      out_file=run_files_dir/f'{run_name}-%j.out')

    # Create the script
    text = header
    text += f'{args.env_command}\n\n'
    text += f'srun -n 1 -c {args.nproc} picca_delta_extraction.py {config_path}\n'

    # Write the script.
    with open(slurm_script_path, 'w') as f:
        f.write(text)
    submit_utils.make_file_executable(slurm_script_path)

    # Submit the job.
    if not args.no_submit:
        run(['sbatch', slurm_script_path])


def create_config(config_path, spectra_dir, catalogue, output_dir, deltas_dirname,
                  lambda_rest_min, lambda_rest_max, true_continuum, args):
    """Create picca_delta_extraction config file.
    See https://github.com/igmhub/picca/blob/master/tutorials/
    /delta_extraction/picca_delta_extraction_configuration_tutorial.ipynb
    """
    config = ConfigParser()

    deltas_dir = Path(output_dir) / deltas_dirname
    config['general'] = {'out dir': deltas_dir, 'num processors': str(args.nproc),
                         'overwrite': 'True'}

    config['data'] = {'type': 'DesisimMocks',
                      'input directory': spectra_dir,
                      'catalogue': catalogue,
                      'wave solution': 'lin',
                      'delta lambda': str(args.delta_lambda),
                      'lambda min': str(args.lambda_min),
                      'lambda max': str(args.lambda_max),
                      'lambda min rest frame': str(lambda_rest_min),
                      'lambda max rest frame': str(lambda_rest_max),
                      'minimum number pixels in forest': str(args.num_pix_min)}

    if args.max_num_spec is not None:
        config['data']['max num spec'] = str(args.max_num_spec)

    config['corrections'] = {'num corrections': '0'}
    config['masks'] = {'num masks': '0'}

    force_stack_delta_to_zero = not args.no_force_stack_delta_to_zero
    if true_continuum:
        config['expected flux'] = {'type': 'TrueContinuum',
                                   'input directory': spectra_dir,
                                   'force stack delta to zero': str(force_stack_delta_to_zero)}
        if args.raw_stats_file is not None:
            config['expected flux']['raw statistics file'] = args.raw_stats_file
    else:
        config['expected flux'] = {'type': 'Dr16FixedEtaFudgeExpectedFlux',
                                   'iter out prefix': 'delta_attributes',
                                   'limit var lss': '0.0,1.0',
                                   'force stack delta to zero': str(force_stack_delta_to_zero)}

    with open(config_path, 'w') as configfile:
        config.write(configfile)
