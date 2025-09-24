from . import submit_utils
from . import dir_handlers

CORR_TYPES = {
    'cf_lya_lya': 'dmat_lya_lya', 'cf_lya_lyb': 'dmat_lya_lyb',
    'xcf_lya_qso': 'xdmat_lya_qso', 'xcf_lyb_qso': 'xdmat_lyb_qso'
}


def make_export_runs(corr_paths, analysis_tree, config, job, corr_job_ids=None, run_local=True):
    subtract_shuffled = config.getboolean('subtract_shuffled')

    corr_dict = {}
    export_commands = []
    for cf_path in corr_paths:
        exp_string = config.get('exp_string')

        if exp_string is not None:
            exp_file = submit_utils.append_string_to_correlation_path(cf_path, f'_{exp_string}-exp')
        else:
            exp_file = submit_utils.append_string_to_correlation_path(cf_path, '-exp')

        shuffled_path = None
        if subtract_shuffled and 'xcf' in cf_path.name:
            shuffled_path = submit_utils.append_string_to_correlation_path(cf_path, '_shuffled')
            if not shuffled_path.is_file() and corr_job_ids is None:
                raise ValueError(
                    'Asked to subtract shuffled correlation, but could not '
                    f'find the shuffled correlation at {shuffled_path}. '
                    'Make sure it was run by activating "compute_shuffled".'
                )

            exp_file = submit_utils.append_string_to_correlation_path(exp_file, '-shuff')

        corr_name_split = cf_path.name.split('.')
        corr_type = None
        for key in CORR_TYPES:
            if key in corr_name_split[0]:
                corr_type = key

        if corr_type is None:
            raise ValueError(f'Unknown correlation type {corr_name_split[0]}')

        corr_dict[corr_type] = (cf_path, exp_file)
        if not exp_file.is_file():
            # Do the exporting
            command = f'picca_export.py --data {cf_path} --out {exp_file} '

            if shuffled_path is not None:
                command += f'--remove-shuffled-correlation {shuffled_path} '

            if config.get(f'corr-mat-{corr_type}') is not None:
                corr_mat = config.get(f'corr-mat-{corr_type}')
                command += f'--cor {corr_mat} '

            export_commands += [command]

    if len(export_commands) < 1:
        print(f'No individual mock export needed for seed {analysis_tree.full_mock_seed}.')
        return corr_dict, None, None
    elif not run_local:
        return corr_dict, None, export_commands

    # Make the header
    header = submit_utils.make_header(
        job.get('nersc_machine'), time=0.2,
        omp_threads=64, job_name=f'export_{analysis_tree.full_mock_seed}',
        err_file=analysis_tree.logs_dir/f'export-{analysis_tree.full_mock_seed}-%j.err',
        out_file=analysis_tree.logs_dir/f'export-{analysis_tree.full_mock_seed}-%j.out'
    )

    # Create the script
    text = header
    env_command = job.get('env_command')
    text += f'{env_command}\n\n'
    for command in export_commands:
        text += command + '\n'

    # Write the script.
    script_path = analysis_tree.scripts_dir / 'export.sh'
    submit_utils.write_script(script_path, text)

    job_id = corr_job_ids
    job_id = submit_utils.run_job(
        script_path, dependency_ids=corr_job_ids, no_submit=job.getboolean('no_submit'))

    return corr_dict, job_id, None


def export_full_cov(corr_paths, analysis_tree, config, job, corr_job_ids=None, run_local=True):
    subtract_shuffled = config.getboolean('subtract_shuffled')
    ordered_cf_paths = {}
    block_types = []

    for cf_path in corr_paths:
        for key in CORR_TYPES:
            if key in cf_path.name:
                ordered_cf_paths[key] = cf_path
                block_types += ['cross'] if 'xcf' in key else ['auto']

                if subtract_shuffled and 'xcf' in cf_path.name:
                    shuffled_path = submit_utils.append_string_to_correlation_path(
                        cf_path, '_shuffled')
                    if not shuffled_path.is_file() and corr_job_ids is None:
                        raise ValueError(
                            'Asked to subtract shuffled correlation, but could not '
                            f'find the shuffled correlation at {shuffled_path}. '
                            'Make sure it was run by activating "compute_shuffled".'
                        )

                    ordered_cf_paths[key + '-shuff'] = shuffled_path

    exp_string = config.get('exp_string')
    cov_string = config.get('cov_string')
    if cov_string is None and exp_string is not None:
        cov_string = exp_string
    name = 'full_cov' if cov_string is None else f'full_cov_{cov_string}'
    output_path = corr_paths[0].parent / f'{name}.fits'
    output_path_smoothed = corr_paths[0].parent / f'{name}_smooth.fits'
    block_types_str = ' '.join(block_types)

    commands = []
    if not output_path.is_file():
        command = '/global/cfs/projectdirs/desi/science/lya/y1-kp6/iron-tests'
        command += '/correlations/scripts/write_full_covariance_matrix_flex_size_shuffled.py '
        for key, path in ordered_cf_paths.items():
            type = key.split('_')
            field = f'{type[1]}-{type[2]}'
            if len(type) > 3:
                field += f'-{type[3]}'
            command += f'--{field} {path} '

        command += f'-o {output_path}\n'
        commands += [command]

    if not output_path_smoothed.is_file():
        command = '/global/cfs/cdirs/desicollab/science/lya/y1-kp6/iron-tests/'
        command += 'correlations/scripts/write_smooth_covariance_flex_size.py '
        command += f'--input-cov {output_path} --output-cov {output_path_smoothed} '
        command += f'--block-types {block_types_str}\n'
        commands += [command]

    # stacked_cov_flag = config.getboolean('stacked_cov_flag', False)
    # if stacked_cov_flag:
    #     stacked_cov_path = config.get('stacked_cov_path')
    #     output_stacked_cov_path = corr_paths[0].parent / 'full_cov_stacked.fits'
    #     if not output_stacked_cov_path.is_file():
    #         command = '/global/homes/a/acuceu/desi_acuceu/notebooks_perl'
    #         command += '/mocks/covariance/export_full_cov.py '
    #         command += f'-i {stacked_cov_path} -c {cf_paths_str} -o {output_stacked_cov_path}\n'
    #         commands += [command]

    if len(commands) < 1:
        print(f'Full covariance already exists for seed {analysis_tree.full_mock_seed}.')
        return None, None
    elif not run_local:
        return None, commands

    # Make the header
    export_cov_time = config.getfloat('export-cov-slurm-hours', 0.5)
    header = submit_utils.make_header(
        job.get('nersc_machine'), time=export_cov_time,
        omp_threads=64, job_name=f'export-cov_{analysis_tree.full_mock_seed}',
        err_file=analysis_tree.logs_dir/f'export-cov-{analysis_tree.full_mock_seed}-%j.err',
        out_file=analysis_tree.logs_dir/f'export-cov-{analysis_tree.full_mock_seed}-%j.out'
    )

    # Create the script
    text = header
    env_command = job.get('env_command')
    text += f'{env_command}\n\n'

    for command in commands:
        text += command + '\n'

    # Write the script.
    script_path = analysis_tree.scripts_dir / 'export-cov.sh'
    submit_utils.write_script(script_path, text)

    job_id = corr_job_ids
    if not config.getboolean('mpi_export_flag', False):
        job_id = submit_utils.run_job(
            script_path, dependency_ids=corr_job_ids, no_submit=job.getboolean('no_submit'))

    return job_id, None


def stack_correlations(
    corr_dict, stack_tree, job, shuffled=False, name_string=None, corr_job_ids=None
):
    # Stack correlations from different seeds
    export_commands = []
    for cf_name, (cf_list, _) in corr_dict.items():
        if len(cf_list) < 1:
            continue

        str_list = [str(cf) for cf in cf_list]
        in_files = ' '.join(str_list)

        shuffled_files = None
        if shuffled:
            shuffled_list = []
            for cf in cf_list:
                if 'xcf' not in cf.name:
                    continue
                shuffled_path = submit_utils.append_string_to_correlation_path(cf, '_shuffled')
                if not shuffled_path.is_file():
                    raise ValueError('Asked to subtract shuffled correlation, but could not '
                                     f'find the shuffled correlation at {shuffled_path}. '
                                     'Make sure it was run by activating "compute_shuffled".')

                shuffled_list.append(str(shuffled_path))

            shuffled_files = ' '.join(shuffled_list)

        # Infer name from first correlation. Automatically inherits name_string in correlations
        # Avoids different redshift ranges having the same stack filename
        corr_filename = cf_list[0].name
        exp_out_file = stack_tree.corr_dir / f'{corr_filename}'
        name_ext = '-exp' if name_string is None else f'_{name_string}-exp'
        exp_out_file = submit_utils.append_string_to_correlation_path(exp_out_file, name_ext)

        if shuffled_files is not None:
            exp_out_file = submit_utils.append_string_to_correlation_path(exp_out_file, '-shuff')
        if exp_out_file.is_file():
            print(f'Exported correlation already exists: {exp_out_file}. Skipping.')
            continue

        command = f'lyatools-stack-export --data {in_files} --out {exp_out_file} '

        if shuffled_files is not None:
            command += f'--shuffled-correlations {shuffled_files} '

        export_commands += [command]

    # Make the header
    header = submit_utils.make_header(
        job.get('nersc_machine'), time=0.2,
        omp_threads=64, job_name='stack_export',
        err_file=stack_tree.logs_dir/'stack_export-%j.err',
        out_file=stack_tree.logs_dir/'stack_export-%j.out'
    )

    # Create the script
    text = header
    env_command = job.get('env_command')
    text += f'{env_command}\n\n'
    for command in export_commands:
        text += command + '\n'

    # Write the script.
    script_path = stack_tree.scripts_dir / 'stack_export.sh'
    submit_utils.write_script(script_path, text)

    job_id = submit_utils.run_job(
        script_path, dependency_ids=corr_job_ids, no_submit=job.getboolean('no_submit'))

    return job_id

def stack_full_covariance(corr_dict, stack_tree, job, smooth_covariance_flag,
                          corr_config, name_string=None, corr_job_ids=None):
    # Make correlation file lists
    lyaxlya_files = []
    lyaxlyb_files = []
    lyaxqso_files = []
    lybxqso_files = []
    correlation_types = []
    
    for cf_name, (cf_list, _) in corr_dict.items():
        if len(cf_list) < 1:
            continue

        if 'cf_lya_lya' in cf_name:
            lyaxlya_files += [str(cf) for cf in cf_list]
            correlation_types += ['auto']
        elif 'cf_lya_lyb' in cf_name:
            lyaxlyb_files += [str(cf) for cf in cf_list]
            correlation_types += ['auto']
        elif 'xcf_lya_qso' in cf_name:
            lyaxqso_files += [str(cf) for cf in cf_list]
            correlation_types += ['cross']
        elif 'xcf_lyb_qso' in cf_name:
            lybxqso_files += [str(cf) for cf in cf_list]
            correlation_types += ['cross']
        else:
            raise ValueError(f'Unknown correlation type {cf_name}')
    if (len(lyaxlya_files) + len(lyaxlyb_files) + len(lyaxqso_files) + len(lybxqso_files)) < 1:
        print('No correlation files provided for full covariance. Skipping.')
        return None

    name_ext = '' if name_string is None else f'_{name_string}'
    out_file = stack_tree.corr_dir / f'full_cov{name_ext}.fits'
    out_file_smoothed = stack_tree.corr_dir / f'full_cov{name_ext}_smooth.fits'
    nproc = corr_config.getint('nproc', 128)

    header = submit_utils.make_header(
        job.get('nersc_machine'), time=0.25,
        omp_threads=64, job_name=f'stack_full_cov',
        err_file=stack_tree.logs_dir/'stack_full_cov-%j.err',
        out_file=stack_tree.logs_dir/'stack_full_cov-%j.out'
    )

    env_command = job.get('env_command')
    text = header
    text += f'{env_command}\n\n'

    if out_file.is_file() and out_file_smoothed.is_file():
        print(f'Full covariance and smoothed full covariance already exist: {out_file}, {out_file_smoothed}. Skipping.')
        return None

    if not out_file.is_file():
        text += f'lyatools-stack-fullcov '
        if len(lyaxlya_files) > 0:
            text += '--lyaxlya ' + ' '.join(lyaxlya_files) + ' '
        if len(lyaxlyb_files) > 0:
            text += '--lyaxlyb ' + ' '.join(lyaxlyb_files) + ' '
        if len(lyaxqso_files) > 0:
            text += '--lyaxqso ' + ' '.join(lyaxqso_files) + ' '
        if len(lybxqso_files) > 0:
            text += '--lybxqso ' + ' '.join(lybxqso_files) + ' '
        text += f'--outfile {out_file} --nproc {nproc}\n\n'

    if smooth_covariance_flag:
        if not out_file_smoothed.is_file():
            rp_min = corr_config.getint('rp_min', 0)
            rp_max = corr_config.getint('rp_max', 200)
            rt_min = corr_config.getint('rt_min', 0)
            rt_max = corr_config.getint('rt_max', 200)
            num_bins_rp = corr_config.getint('num_bins_rp', 50)
            num_bins_rt = corr_config.getint('num_bins_rt', 50)

            text += f'picca_write_smooth_covariance.py --input-cov {out_file} --output-cov {out_file_smoothed} '
            text += f'--rp-min-auto {rp_min} --rp-max-auto {rp_max} --np-auto {num_bins_rp} '
            text += f'--rt-min-auto {rt_min} --rt-max-auto {rt_max} --nt-auto {num_bins_rt} '
            text += f'--rp-min-cross {-rp_max} --rp-max-cross {rp_max} --np-cross {2*num_bins_rp} '
            text += f'--rt-min-cross {rt_min} --rt-max-cross {rt_max} --nt-cross {num_bins_rt} '
            text += f'--correlation-types {" ".join(correlation_types)}\n'


    # Write the script.
    script_path = stack_tree.scripts_dir / f'stack_full_cov{name_ext}.sh'
    submit_utils.write_script(script_path, text)
    job_id = submit_utils.run_job(
        script_path, dependency_ids=corr_job_ids, no_submit=job.getboolean('no_submit'))
    return job_id


def mpi_export(export_commands, export_cov_commands, analysis_tree, job, corr_job_ids=None):

    # Filter Nones
    # export_commands = [command for mock_commands in export_commands for command in mock_commands]
    export_commands = [command for command in export_commands if command is not None]
    export_cov_commands = [command for command in export_cov_commands if command is not None]

    export_job_id = None
    if len(export_commands) > 1:
        export_job_id = mpi_export_correlations(
            export_commands, analysis_tree, job, corr_job_ids=corr_job_ids)

    # Restructure the export_cov_commands
    individual_cov_commands = []
    smooth_cov_commands = []
    for command in export_cov_commands:
        if 'write_full_covariance_matrix_flex_size_shuffled' in command:
            individual_cov_commands += [command]
        elif 'write_smooth_covariance_flex_size' in command:
            smooth_cov_commands += [command]
        else:
            raise ValueError(f'Unknown covariance command: {command}')

    cov_job_id = None
    if len(individual_cov_commands) > 1:
        ntasks_per_node = min(len(individual_cov_commands), 32)
        cov_job_id = mpi_export_covariances(
            individual_cov_commands, analysis_tree, job, script_name='individual_cov',
            num_nodes=1, ntasks_per_node=ntasks_per_node, corr_job_ids=corr_job_ids
        )

    cov_smooth_job_id = None
    if cov_job_id is None:
        cov_job_id = corr_job_ids
    if len(smooth_cov_commands) > 1:
        num_nodes = max(len(smooth_cov_commands) // 32, 1)
        ntasks_per_node = min(len(smooth_cov_commands), 32)
        cov_smooth_job_id = mpi_export_covariances(
            smooth_cov_commands, analysis_tree, job, script_name='smooth_cov',
            num_nodes=num_nodes, ntasks_per_node=ntasks_per_node, corr_job_ids=cov_job_id
        )

    return export_job_id, cov_smooth_job_id


def mpi_export_correlations(export_commands, analysis_tree, job, corr_job_ids=None):
    # Make the header
    header = submit_utils.make_header(
        job.get('nersc_machine'), time=1.0,
        omp_threads=2, job_name='export_mocks',
        err_file=analysis_tree.logs_dir/'mpi_export-%j.err',
        out_file=analysis_tree.logs_dir/'mpi_export-%j.out'
    )

    # Create the script
    text = header
    env_command = job.get('env_command')
    text += f'{env_command}\n\n'

    logs = analysis_tree.logs_dir / 'export'
    dir_handlers.check_dir(logs)

    text_commands = '"' + '" "'.join(export_commands) + '"'
    num_cores = min(len(export_commands), 64)
    text += f'srun --ntasks-per-node={num_cores} lyatools-mpi-export -i {text_commands} '
    text += f'-l {logs}\n'

    # Write the script.
    script_path = analysis_tree.scripts_dir / 'mpi_export.sh'
    submit_utils.write_script(script_path, text)

    job_id = submit_utils.run_job(
        script_path, dependency_ids=corr_job_ids, no_submit=job.getboolean('no_submit'))

    return job_id


def mpi_export_covariances(
        commands, analysis_tree, job, script_name,
        num_nodes=1, ntasks_per_node=64, corr_job_ids=None
):
    # Make the header
    header = submit_utils.make_header(
        job.get('nersc_machine'), time=job.getfloat('mpi-export-time', 4.0),
        nodes=int(num_nodes), omp_threads=2, job_name=script_name,
        err_file=analysis_tree.logs_dir/f'mpi_export-cov-{script_name}-%j.err',
        out_file=analysis_tree.logs_dir/f'mpi_export-cov-{script_name}-%j.out'
    )

    # Create the script
    text = header
    env_command = job.get('env_command')
    text += f'{env_command}\n\n'

    text_commands = '"' + '" "'.join(commands) + '"'

    logs = analysis_tree.logs_dir / f'export-cov-{script_name}'
    dir_handlers.check_dir(logs)

    text += f'srun --ntasks-per-node={ntasks_per_node} lyatools-mpi-export -i {text_commands} '
    text += f'-l {logs}\n'

    # Write the script.
    script_path = analysis_tree.scripts_dir / f'mpi_export_{script_name}.sh'
    submit_utils.write_script(script_path, text)

    job_id = submit_utils.run_job(
        script_path, dependency_ids=corr_job_ids, no_submit=job.getboolean('no_submit'))

    return job_id
