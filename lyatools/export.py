from pathlib import Path

from . import submit_utils

CORR_TYPES = {'cf_lya_lya': 'dmat_lya_lya', 'cf_lya_lyb': 'dmat_lya_lyb',
              'xcf_lya_qso': 'xdmat_lya_qso', 'xcf_lyb_qso': 'xdmat_lyb_qso'}


def find_dmat(dmat_path, corr_type):
    if dmat_path is None:
        raise ValueError('Asked for distortion matrix, but did not provide a path.')

    dmat_dir = Path(dmat_path)
    dmat_dir_files = dmat_dir.glob('*.fits*')
    dmat_type = CORR_TYPES[corr_type]
    name_list = [file.name for file in dmat_dir_files if dmat_type in file.name]

    if len(name_list) < 1:
        raise ValueError(f'No distortion matrix of type "{dmat_type}" '
                         f'found in {dmat_dir}')
    elif len(name_list) > 1:
        raise ValueError(f'Found more than one distortion matrix of type "{dmat_type}" '
                         f'in {dmat_dir}. Please move test files to a separate folder.')

    return dmat_dir / name_list[0]


def make_export_runs(seed, analysis_struct, corr_paths, job, config, corr_job_ids=None):
    add_dmat = config.getboolean('add_dmat')
    dmat_path = config.get('dmat_path')
    shuffled = config.getboolean('subtract_shuffled')

    corr_dict = {}
    export_commands = []
    for cf_path in corr_paths:
        exp_string = config.get('exp_string')

        if exp_string is not None:
            exp_file = submit_utils.append_string_to_correlation_path(cf_path, f'_{exp_string}-exp')
        else:
            exp_file = submit_utils.append_string_to_correlation_path(cf_path, '-exp')

        shuffled_path = None
        if shuffled:
            shuffled_path = submit_utils.append_string_to_correlation_path(cf_path, '_shuffled')
            if not shuffled_path.is_file():
                raise ValueError('Asked to subtract shuffled correlation, but could not '
                                 f'find the shuffled correlation at {shuffled_path}. '
                                 'Make sure it has the correct name as in the link here.')

            exp_file = submit_utils.append_string_to_correlation_path(exp_file, '-shuff')

        corr_name_split = cf_path.name.split('.')
        corr_type = None
        for key in CORR_TYPES:
            if key in corr_name_split[0]:
                corr_type = key

        if corr_type is None:
            raise ValueError(f'Unknown correlation type {corr_name_split[0]}')

        corr_dict[corr_type] = cf_path
        if not exp_file.is_file():
            # Do the exporting
            command = f'picca_export.py --data {cf_path} --out {exp_file} '

            if add_dmat:
                dmat_file = find_dmat(dmat_path, corr_type)
                command += f'--dmat {dmat_file} '

            if shuffled_path is not None:
                command += f'--remove-shuffled-correlation {shuffled_path} '

            if config.get(f'corr-mat-{corr_type}') is not None:
                corr_mat = config.get(f'corr-mat-{corr_type}')
                command += f'--cor {corr_mat} '

            export_commands += [command]

    if len(export_commands) < 1:
        print(f'No individual mock export needed for seed {seed}.')
        return corr_dict, None, None

    # Make the header
    header = submit_utils.make_header(job.get('nersc_machine'), time=0.2,
                                      omp_threads=64, job_name=f'export_{seed}',
                                      err_file=analysis_struct.logs_dir/f'export-{seed}-%j.err',
                                      out_file=analysis_struct.logs_dir/f'export-{seed}-%j.out')

    # Create the script
    text = header
    env_command = job.get('env_command')
    text += f'{env_command}\n\n'
    for command in export_commands:
        text += command + '\n'

    # Write the script.
    script_path = analysis_struct.scripts_dir / f'export-{seed}.sh'
    submit_utils.write_script(script_path, text)

    job_id = corr_job_ids
    if not config.getboolean('mpi_export_flag', False):
        job_id = submit_utils.run_job(script_path, dependency_ids=corr_job_ids,
                                      no_submit=job.getboolean('no_submit'))

    return corr_dict, job_id, export_commands


def export_full_cov(seed, analysis_struct, corr_paths, job, config, corr_job_ids=None):
    if len(corr_paths) != 4:
        raise ValueError(f'Expected 4 correlation files, but got {len(corr_paths)}')

    # Put the correlations in the right order
    order = ['cf_lya_lya', 'cf_lya_lyb', 'xcf_lya_qso', 'xcf_lyb_qso']
    ordered_cf_paths = [None] * 4
    for i, corr_type in enumerate(order):
        for cf_path in corr_paths:
            if corr_type in cf_path.name:
                ordered_cf_paths[i] = cf_path

    output_path = corr_paths[0].parent / 'full_cov.fits'
    output_path_smoothed = corr_paths[0].parent / 'full_cov_smooth.fits'
    cf_paths_str = ' '.join([str(cf_path) for cf_path in ordered_cf_paths])

    commands = []

    if not output_path.is_file():
        command = '/global/homes/a/acuceu/desi_acuceu/notebooks_perl'
        command += '/mocks/covariance/export_individual_cov.py '
        command += f'-i {cf_paths_str} -o {output_path}\n'
        commands += [command]

    if not output_path_smoothed.is_file():
        command = '/global/homes/a/acuceu/desi_acuceu/notebooks_perl'
        command += '/mocks/covariance/smoothit.py '
        command += f'-i {output_path} -o {output_path_smoothed}\n'
        commands += [command]

    stacked_cov_flag = config.getboolean('stacked_cov_flag', False)
    if stacked_cov_flag:
        stacked_cov_path = config.get('stacked_cov_path')
        output_stacked_cov_path = corr_paths[0].parent / 'full_cov_stacked.fits'
        if not output_stacked_cov_path.is_file():
            command = '/global/homes/a/acuceu/desi_acuceu/notebooks_perl'
            command += '/mocks/covariance/export_full_cov.py '
            command += f'-i {stacked_cov_path} -c {cf_paths_str} -o {output_stacked_cov_path}\n'
            commands += [command]

    if len(commands) < 1:
        print(f'Full covariance already exists for seed {seed}.')
        return None, None

    # Make the header
    header = submit_utils.make_header(job.get('nersc_machine'), time=0.2,
                                      omp_threads=64, job_name=f'export-cov_{seed}',
                                      err_file=analysis_struct.logs_dir/f'export-cov-{seed}-%j.err',
                                      out_file=analysis_struct.logs_dir/f'export-cov-{seed}-%j.out')

    # Create the script
    text = header
    env_command = job.get('env_command')
    text += f'{env_command}\n\n'

    for command in commands:
        text += command + '\n'

    # Write the script.
    script_path = analysis_struct.scripts_dir / f'export-cov-{seed}.sh'
    submit_utils.write_script(script_path, text)

    job_id = corr_job_ids
    if not config.getboolean('mpi_export_flag', False):
        job_id = submit_utils.run_job(script_path, dependency_ids=corr_job_ids,
                                      no_submit=job.getboolean('no_submit'))

    return job_id, commands


def stack_correlations(corr_dict, global_struct, job, add_dmat=False, dmat_path=None,
                       shuffled=False, name_string=None, exp_job_ids=None):
    # Stack correlations from different seeds
    export_commands = []
    for cf_name, cf_list in corr_dict.items():
        if len(cf_list) < 1:
            continue

        str_list = [str(cf) for cf in cf_list]
        in_files = ' '.join(str_list)

        shuffled_files = None
        if shuffled:
            shuffled_list = []
            for cf in cf_list:
                shuffled_path = submit_utils.append_string_to_correlation_path(cf, '_shuffled')
                if not shuffled_path.is_file():
                    raise ValueError('Asked to subtract shuffled correlation, but could not '
                                     f'find the shuffled correlation at {shuffled_path}. '
                                     'Make sure it has the correct name as in the link here.')

                shuffled_list.append(str(shuffled_path))

            shuffled_files = ' '.join(shuffled_list)

        name_ext = '' if name_string is None else '_' + name_string
        exp_out_file = global_struct.corr_dir / f'{cf_name}{name_ext}-exp.fits.gz'

        if shuffled_files is not None:
            exp_out_file = submit_utils.append_string_to_correlation_path(exp_out_file, '-shuff')
        if exp_out_file.is_file():
            raise ValueError(f'Exported correlation already exists: {exp_out_file}')

        command = f'lyatools-stack-export --data {in_files} --out {exp_out_file} '

        if add_dmat:
            dmat_file = find_dmat(dmat_path, cf_name)
            command += f'--dmat {dmat_file} '

        if shuffled_files is not None:
            command += f'--shuffled-correlations {shuffled_files} '

        export_commands += [command]

    # Make the header
    header = submit_utils.make_header(job.get('nersc_machine'), time=0.2,
                                      omp_threads=64, job_name='stack_export',
                                      err_file=global_struct.logs_dir/'stack_export-%j.err',
                                      out_file=global_struct.logs_dir/'stack_export-%j.out')

    # Create the script
    text = header
    env_command = job.get('env_command')
    text += f'{env_command}\n\n'
    for command in export_commands:
        text += command + '\n'

    # Write the script.
    script_path = global_struct.scripts_dir / 'stack_export.sh'
    submit_utils.write_script(script_path, text)

    job_id = submit_utils.run_job(script_path, dependency_ids=exp_job_ids,
                                  no_submit=job.getboolean('no_submit'))

    return job_id


def mpi_export(export_dict, job, analysis_struct, corr_job_ids=None):
    export_commands = export_dict['export_commands']
    export_cov_commands = export_dict['export_cov_commands']

    # Filter Nones
    export_commands = [command for command in export_commands if command is not None]
    export_cov_commands = [command for command in export_cov_commands if command is not None]

    if len(export_commands) > 1:
        mpi_export_correlations(export_commands, job, analysis_struct, corr_job_ids=corr_job_ids)

    # Restructure the export_cov_commands
    individual_cov_commands = []
    for cov_commands in export_cov_commands:
        if len(cov_commands) >= 1:
            individual_cov_commands += [cov_commands[0]]

    smooth_cov_commands = []
    for cov_commands in export_cov_commands:
        if len(cov_commands) >= 2:
            smooth_cov_commands += [cov_commands[1]]

    stacked_cov_commands = []
    for cov_commands in export_cov_commands:
        if len(cov_commands) >= 3:
            stacked_cov_commands += [cov_commands[2]]

    if len(individual_cov_commands) > 1:
        mpi_export_covariances(
            individual_cov_commands, job, analysis_struct, num_nodes=1,
            ntasks_per_node=32, corr_job_ids=corr_job_ids)

    if len(smooth_cov_commands) > 1:
        num_nodes = max(len(smooth_cov_commands) // 32, 1)
        mpi_export_covariances(
            smooth_cov_commands, job, analysis_struct, num_nodes=num_nodes,
            ntasks_per_node=32, corr_job_ids=corr_job_ids)

    if len(stacked_cov_commands) > 1:
        mpi_export_covariances(
            stacked_cov_commands, job, analysis_struct, num_nodes=1,
            ntasks_per_node=32, corr_job_ids=corr_job_ids)


def mpi_export_correlations(export_commands, job, analysis_struct, corr_job_ids=None):
    # Make the header
    header = submit_utils.make_header(job.get('nersc_machine'), time=1.0,
                                      omp_threads=64, job_name='stack_export',
                                      err_file=analysis_struct.logs_dir/'mpi_export-%j.err',
                                      out_file=analysis_struct.logs_dir/'mpi_export-%j.out')

    # Create the script
    text = header
    env_command = job.get('env_command')
    text += f'{env_command}\n\n'

    text += f'srun --ntasks-per-node=64 lyatools-mpi-export -i {export_commands} '
    text += f'-l {analysis_struct.logs_dir}\n'

    # Write the script.
    script_path = analysis_struct.scripts_dir / 'mpi_export.sh'
    submit_utils.write_script(script_path, text)

    _ = submit_utils.run_job(script_path, dependency_ids=corr_job_ids,
                             no_submit=job.getboolean('no_submit'))


def mpi_export_covariances(
        commands, job, analysis_struct, num_nodes=1, ntasks_per_node=64, corr_job_ids=None):
    # Make the header
    header = submit_utils.make_header(
        job.get('nersc_machine'), time=job.get('mpi-export-time', 4.0), nodes=int(num_nodes),
        omp_threads=64, job_name='stack_export',
        err_file=analysis_struct.logs_dir/'mpi_export-cov-%j.err',
        out_file=analysis_struct.logs_dir/'mpi_export-cov-%j.out')

    # Create the script
    text = header
    env_command = job.get('env_command')
    text += f'{env_command}\n\n'

    text_commands = '"' + '" "'.join(commands) + '"'

    text += f'srun --ntasks-per-node={ntasks_per_node} lyatools-mpi-export -i {text_commands} '
    text += f'-l {analysis_struct.logs_dir}\n'

    # Write the script.
    script_path = analysis_struct.scripts_dir / 'mpi_export.sh'
    submit_utils.write_script(script_path, text)

    _ = submit_utils.run_job(script_path, dependency_ids=corr_job_ids,
                             no_submit=job.getboolean('no_submit'))
