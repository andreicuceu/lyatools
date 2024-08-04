from . import submit_utils
from lyatools import qq_run_args


def create_qq_catalog(qq_tree, seed_cat_path, config, job, seed, run_local=True):
    submit_utils.set_umask()

    release = config.get('release', 'jura')
    master_cat_path = f"{qq_tree.skewers_path}/master.fits"
    input_data_catalog = config.get('input_data_catalog')

    command = f'gen_qso_catalog -i {master_cat_path} -o {seed_cat_path} --seed {seed}'
    command += f' --release {release} --input-data {input_data_catalog}'
    if config.getboolean('invert_cat_seed', False):
        command += ' --invert'
    if config.getboolean('include_nonqso_targets', True):
        command += ' --include-nonqso-targets'
    command += '\n\n'

    if not run_local:
        return command

    # Make the header
    header = submit_utils.make_header(
        job.get('nersc_machine'), 'regular', 1, time=0.2,
        omp_threads=128, job_name=f'qq_cat_{seed}',
        err_file=qq_tree.runfiles_dir/'run-%j.err',
        out_file=qq_tree.runfiles_dir/'run-%j.out'
    )

    env_text = '\n\n'
    if job.get('desi_env_command', None) is None:
        env_text += 'source /global/common/software/desi/desi_environment.sh master'
    else:
        env_text += job.get('desi_env_command')
    env_text += '\n\n'

    # Write the script to file and run it.
    script_path = qq_tree.scripts_dir / 'run_qq_seed_cat.sh'
    full_text = header + env_text + command

    job_id = None
    if not seed_cat_path.is_file():
        submit_utils.write_script(script_path, full_text)
        job_id = submit_utils.run_job(script_path, no_submit=job.getboolean('no_submit'))

    return job_id


def run_qq(qq_tree, config, job, seed_cat_path, qq_seed, mock_type, prev_job_id=None):
    """Create a QQ run and submit it

    Parameters
    ----------
    qq_tree : QQTree
        The QQ directory tree object.
    config : dict
        The configuration dictionary.
    job : dict
        The job dictionary.
    """
    # Print run config
    print(f'Submitting quickquasars runs with configuration {qq_tree.qq_run_name}')

    split_qq_run_type = qq_tree.qq_run_name.split('-')
    if '_' in split_qq_run_type[1]:
        raise ValueError(
            f'Cannot have underscores in middle part of qq_run_type: {split_qq_run_type[1]}'
            ' Please separate the control digits from the rest of the name with dash lines.'
            ' E.g. Use jura-124-test instead of jura-124_test'
        )

    qq_args = ' '.join(qq_run_args.QQ_DEFAULTS)
    qq_args += f' --seed {qq_seed}'
    qq_args += f' --from-catalog {seed_cat_path}'
    for digit in split_qq_run_type[1]:
        if digit not in qq_run_args.QQ_RUN_CODES:
            raise ValueError(
                f'Invalid digit {digit} in qq_run_type: {split_qq_run_type[1]}.'
                f' Only the following digits are currently recognized: {qq_run_args.QQ_RUN_CODES}.'
                ' Please add the new digit to the QQ_RUN_CODES dictionary in qq_run_args.py'
            )

        qq_args += f' {qq_run_args.QQ_RUN_CODES[digit]}'

        if digit == '2':
            metal_strengths = config.get('metal_strengths')
            if metal_strengths is None:
                metal_strengths = qq_run_args.QQ_DEFAULT_METAL_STRENGTHS[mock_type]
            qq_args += f' --metal-strengths {metal_strengths}'

    dla_flag = '1' in split_qq_run_type[1]
    bal_flag = '4' in split_qq_run_type[1]

    print('Found the following arguments to pass to quickquasars:')
    print(qq_args)

    qq_script = create_qq_script(qq_tree, config, job, qq_args, qq_seed)

    job_id = submit_utils.run_job(
        qq_script, dependency_ids=prev_job_id, no_submit=job.getboolean('no_submit'))

    return job_id, dla_flag, bal_flag


def create_qq_script(qq_tree, config, job, qq_args, qq_seed):
    submit_utils.set_umask()

    slurm_queue = job.get('slurm_queue', 'regular')
    nodes = config.getint('nodes', 8)
    nproc = config.getint('nproc', 32)
    slurm_hours = config.getfloat('slurm_hours', 0.5)

    # Make the header
    time = submit_utils.convert_job_time(slurm_hours)
    header = submit_utils.make_header(
        job.get('nersc_machine'), slurm_queue, nodes, time=time,
        omp_threads=nproc, job_name=f'qq_{qq_seed}',
        err_file=qq_tree.runfiles_dir/'run-%j.err',
        out_file=qq_tree.runfiles_dir/'run-%j.out'
    )

    # Create the main qq run command
    qq_run = f'    command="srun -N 1 -n 1 -c {nproc} '
    qq_run += f'quickquasars -i $tfiles --nproc {nproc} '
    qq_run += f'--outdir {qq_tree.spectra_dir} {qq_args}"\n'

    # Make the text body of the script.
    text = '\n\n'
    if job.get('desi_env_command', None) is None:
        text += 'source /global/common/software/desi/desi_environment.sh master'
    else:
        text += job.get('desi_env_command')

    text += '\n\n'
    text += 'echo "get list of skewers to run ..."\n\n'

    if job.getboolean('test_run'):
        text += 'echo "test run enabled, selecting only first 10 files"\n'
        text += f'files=`ls -1 {qq_tree.skewers_path}/*/*/transmission*.fits* | head -10`\n'
    else:
        text += f'files=`ls -1 {qq_tree.skewers_path}/*/*/transmission*.fits*`\n'

    text += 'nfiles=`echo $files | wc -w`\n'
    text += f'nfilespernode=$(( $nfiles / {nodes} + 1 ))\n\n'
    text += 'echo "n files =" $nfiles\n'
    text += 'echo "n files per node =" $nfilespernode\n\n'

    text += 'first=1\n'
    text += 'last=$nfilespernode\n'
    text += f'for node in `seq {nodes}` ; do\n'
    text += '    echo "starting node $node"\n\n'
    text += '    # list of files to run\n'
    text += f'    if (( $node == {nodes} )) ; then\n'
    text += '        last=""\n'
    text += '    fi\n\n'
    text += '    echo ${first}-${last}\n'
    text += '    tfiles=`echo $files | cut -d " " -f ${first}-${last}`\n'
    text += '    first=$(( first + nfilespernode ))\n'
    text += '    last=$(( last + nfilespernode ))\n'

    text += qq_run

    text += '    echo $command\n'
    text += f'    echo "log in {qq_tree.logs_dir}/node-$node.log"\n\n'
    text += f'    $command >& {qq_tree.logs_dir}/node-$node.log &\n\n'
    text += 'done\n\n'
    text += 'wait\n\n'

    zcat_file = qq_tree.qq_dir / 'zcat.fits'
    text += f'desi_zcatalog -i {qq_tree.spectra_dir} -o {zcat_file} '
    text += '--minimal --prefix zbest\n\n'

    text += 'echo "END"\n\n'

    full_text = header + text

    # Write the script to file and run it.
    script_path = qq_tree.scripts_dir / 'run_quickquasars.sh'
    submit_utils.write_script(script_path, full_text)

    return script_path


def make_contaminant_catalogs(qq_tree, config, job, dla_flag, bal_flag, qq_job_id, run_local=True):
    assert dla_flag or bal_flag

    command = ''
    if dla_flag:
        command += f'lyatools-make-dla-cat -i {qq_tree.spectra_dir} -o {qq_tree.qq_dir} '
        mask_nhi_cut = config.getfloat('dla_mask_nhi_cut')
        command += f'--mask-nhi-cut {mask_nhi_cut} --nproc {128}\n\n'

    if bal_flag:
        command += f'lyatools-make-bal-cat -i {qq_tree.spectra_dir} -o {qq_tree.qq_dir} '

        ai_cut = config.getint('bal_ai_cut', None)
        if ai_cut is not None:
            command += f'--ai-cut {ai_cut} '

        bi_cut = config.getint('bal_bi_cut', None)
        if bi_cut is not None:
            command += f'--bi-cut {bi_cut} '

        command += f'--nproc {128}\n\n'

    if not run_local:
        return command

    print('Submitting Contaminant catalog job')
    header = submit_utils.make_header(
        job.get('nersc_machine'), nodes=1, time=0.5,
        omp_threads=128, job_name=f'cont_cat_{qq_tree.mock_seed}',
        err_file=qq_tree.runfiles_dir/'run-cont-cat-%j.err',
        out_file=qq_tree.runfiles_dir/'run-cont-cat-%j.out'
    )

    env_command = job.get('env_command')
    text = header + f'{env_command}\n\n' + command

    script_path = qq_tree.scripts_dir / 'make_cont_cat.sh'
    submit_utils.write_script(script_path, text)

    job_id = submit_utils.run_job(
        script_path, dependency_ids=qq_job_id, no_submit=job.getboolean('no_submit'))

    return job_id
