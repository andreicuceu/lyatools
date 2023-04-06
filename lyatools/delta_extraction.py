from subprocess import run
from configparser import ConfigParser

from . import dir_handlers, submit_utils


def run_true_continuum(args, qq_dir, main_path, zcat_file):
    assert args.run_true_continuum

    name = 'true_cont'
    if args.analysis_name is not None:
        name += f'_{args.analysis_name}'
    analysis_dir = dir_handlers.AnalysisDir(main_path, args.qq_run_type, name)

    make_delta_runs(args, qq_dir, zcat_file, analysis_dir, true_continuum=True)


def run_continuum_fitting(args, qq_dir, main_path, zcat_file):
    assert not args.no_run_continuum_fitting

    name = 'baseline'
    if args.analysis_name is not None:
        name = args.analysis_name
    analysis_dir = dir_handlers.AnalysisDir(main_path, args.qq_run_type, name)

    make_delta_runs(args, qq_dir, zcat_file, analysis_dir)


def make_delta_runs(args, qq_dir, zcat_file, analysis_dir, true_continuum=False):
    if args.run_lya_region:
        run_delta_extraction(args, qq_dir, analysis_dir, zcat_file, region_name='lya',
                             lambda_rest_min=1040., lambda_rest_max=1200.,
                             true_continuum=true_continuum)

    if args.run_lyb_region:
        run_delta_extraction(args, qq_dir, analysis_dir, zcat_file, region_name='lyb',
                             lambda_rest_min=920., lambda_rest_max=1020.,
                             true_continuum=true_continuum)


def run_delta_extraction(args, qq_dir, analysis_dir, catalogue, region_name='lya',
                         lambda_rest_min=1040., lambda_rest_max=1200., true_continuum=False):
    print(f'Submitting job to make continuum fitted {region_name} deltas')
    submit_utils.set_umask()

    if region_name == 'lya':
        deltas_dirname = analysis_dir.deltas_lya_dir
    elif region_name == 'lyb':
        deltas_dirname = analysis_dir.deltas_lyb_dir
    else:
        raise ValueError('Unkown region name. Choose from ["lya", "lyb"].')

    # Create the path and name for the config file
    type = 'true' if true_continuum else 'fitted'
    config_path = analysis_dir.scripts_dir / f'deltas_{region_name}_{type}.ini'

    # Create the config file for running picca_delta_extraction
    spectra_dir = qq_dir / 'spectra-16'
    create_config(args, config_path, spectra_dir, catalogue, deltas_dirname,
                  lambda_rest_min, lambda_rest_max, true_continuum)

    run_name = f'picca_delta_extraction_{region_name}_{type}'
    slurm_script_path = analysis_dir.scripts_dir / f'run_{run_name}.sh'
    time = submit_utils.convert_job_time(args.slurm_hours)

    # Make the header
    header = submit_utils.make_header(args.nersc_machine, args.slurm_queue, time=time,
                                      omp_threads=args.nproc, job_name=run_name,
                                      err_file=analysis_dir.logs_dir/f'{run_name}-%j.err',
                                      out_file=analysis_dir.logs_dir/f'{run_name}-%j.out')

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


def create_config(args, config_path, spectra_dir, catalogue, deltas_dir,
                  lambda_rest_min, lambda_rest_max, true_continuum):
    """Create picca_delta_extraction config file.
    See https://github.com/igmhub/picca/blob/master/tutorials/
    /delta_extraction/picca_delta_extraction_configuration_tutorial.ipynb
    """
    config = ConfigParser()

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
