from configparser import ConfigParser

from . import submit_utils


def make_delta_runs(config, job, qq_dir, zcat_file, analysis_struct, mask_dla_cat=None,
                    zcat_job_id=None, true_continuum=False):
    job_ids = []
    if config.getboolean('run_lya_region'):
        id = run_delta_extraction(config, job, qq_dir, analysis_struct, zcat_file, mask_dla_cat,
                                  region_name='lya', lambda_rest_min=1040., lambda_rest_max=1200.,
                                  zcat_job_id=zcat_job_id, true_continuum=true_continuum)
        job_ids += [id]

    if config.getboolean('run_lyb_region'):
        id = run_delta_extraction(config, job, qq_dir, analysis_struct, zcat_file, mask_dla_cat,
                                  region_name='lyb', lambda_rest_min=920., lambda_rest_max=1020.,
                                  zcat_job_id=zcat_job_id, true_continuum=true_continuum)
        job_ids += [id]

    return job_ids


def run_delta_extraction(config, job, qq_dir, analysis_struct, catalogue, mask_dla_cat=None,
                         region_name='lya', lambda_rest_min=1040., lambda_rest_max=1200.,
                         zcat_job_id=None, true_continuum=False):
    print(f'Submitting job to run delta extraction on {region_name} region')
    submit_utils.set_umask()

    if region_name == 'lya':
        deltas_dirname = analysis_struct.deltas_lya_dir
    elif region_name == 'lyb':
        deltas_dirname = analysis_struct.deltas_lyb_dir
    else:
        raise ValueError('Unkown region name. Choose from ["lya", "lyb"].')

    # Create the path and name for the config file
    type = 'true' if true_continuum else 'fitted'
    config_path = analysis_struct.scripts_dir / f'deltas_{region_name}_{type}.ini'

    # Create the config file for running picca_delta_extraction
    nproc = config.getint('nproc', 64)
    create_config(config, config_path, qq_dir, catalogue, mask_dla_cat, deltas_dirname,
                  lambda_rest_min, lambda_rest_max, true_continuum, nproc)

    run_name = f'picca_delta_extraction_{region_name}_{type}'
    script_path = analysis_struct.scripts_dir / f'run_{run_name}.sh'

    slurm_hours = config.getfloat('slurm_hours', None)
    if slurm_hours is None:
        slurm_hours = 0.5 if true_continuum else 1.

    # Make the header
    header = submit_utils.make_header(
        job.get('nersc_machine'), job.get('slurm_queue'),
        time=slurm_hours, omp_threads=nproc, job_name=run_name,
        err_file=analysis_struct.logs_dir/f'{run_name}-%j.err',
        out_file=analysis_struct.logs_dir/f'{run_name}-%j.out')

    # Create the script
    text = header
    env_command = job.get('env_command')
    text += f'{env_command}\n\n'
    text += f'srun -n 1 -c {nproc} picca_delta_extraction.py {config_path}\n'

    submit_utils.write_script(script_path, text)

    job_id = submit_utils.run_job(script_path, dependency_ids=zcat_job_id,
                                  no_submit=job.getboolean('no_submit'))

    return job_id


def create_config(config, config_path, qq_dir, catalogue, mask_dla_cat, deltas_dir,
                  lambda_rest_min, lambda_rest_max, true_continuum, nproc):
    """Create picca_delta_extraction config file.
    See https://github.com/igmhub/picca/blob/master/tutorials/
    /delta_extraction/picca_delta_extraction_configuration_tutorial.ipynb
    """
    spectra_dir = qq_dir / 'spectra-16'

    out_config = ConfigParser()
    out_config['general'] = {'out dir': deltas_dir, 'num processors': str(nproc),
                             'overwrite': 'True'}

    out_config['data'] = {'type': 'DesisimMocks',
                          'input directory': spectra_dir,
                          'catalogue': catalogue,
                          'wave solution': 'lin',
                          'delta lambda': config.get('delta_lambda'),
                          'lambda min': config.get('lambda_min'),
                          'lambda max': config.get('lambda_max'),
                          'lambda min rest frame': str(lambda_rest_min),
                          'lambda max rest frame': str(lambda_rest_max)}

    num_pix_min = config.getint('num_pix_min', None)
    if num_pix_min is not None and num_pix_min >= 0:
        out_config['data']['minimum number pixels in forest'] = config.get('num_pix_min')

    if config.getint('max_num_spec', -1) > 0:
        out_config['data']['max num spec'] = config.get('max_num_spec')

    out_config['corrections'] = {'num corrections': '0'}

    mask_dla_flag = config.getboolean('mask_DLAs')
    if mask_dla_flag:
        assert mask_dla_cat is not None
        print(f'Asked for DLA masking. Assuming dla mask catalog exists: {mask_dla_cat}')
        out_config['masks'] = {'num masks': '1',
                               'type 0': 'DlaMask'}
        out_config['mask arguments 0'] = {'filename': str(mask_dla_cat),
                                          'los_id name': 'TARGETID'}
    else:
        out_config['masks'] = {'num masks': '0'}

    force_stack_delta_to_zero = config.getboolean('force_stack_delta_to_zero', True)
    recompute_var_lss = config.getboolean('recompute_var_lss', True)
    if true_continuum:
        out_config['expected flux'] = {'type': 'TrueContinuum',
                                       'input directory': spectra_dir,
                                       'recompute var lss': recompute_var_lss,
                                       'var lss mod': config.get('var_lss_mod', '1'),
                                       'force stack delta to zero': str(force_stack_delta_to_zero)}

        raw_stats_file = config.get('raw_stats_file', None)
        if raw_stats_file is not None:
            out_config['expected flux']['raw statistics file'] = raw_stats_file
    else:
        out_config['expected flux'] = {'type': 'Dr16FixedEtaFudgeExpectedFlux',
                                       'iter out prefix': 'delta_attributes',
                                       'limit var lss': '0.0,1.0',
                                       'var lss mod': config.get('var_lss_mod', '1'),
                                       'force stack delta to zero': str(force_stack_delta_to_zero)}

    with open(config_path, 'w') as configfile:
        out_config.write(configfile)
