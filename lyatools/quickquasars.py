from . import dir_handlers, submit_utils
from lyatools.qq_run_args import QQ_RUN_ARGS


def run_qq(config, job, qq_run_type, seed, input_dir, output_dir):
    """Create a QQ run and submit it

    Parameters
    ----------
    qq_run_type : str
        Run type. Must be a key in the QQ_RUN_ARGS dict.
    test_run : bool
        Test run flag
    no_submit : bool
        Submit flag
    args : list
        List with args passed to create_qq_script
    """

    # Check if it is a test run and update args accordingly
    if job.getboolean('test_run'):
        print('Test run enabled, overriding arguments to setup it up.')
        qq_run_type = 'desi-test'

    # Print run config
    print(f'Submitting quickquasars runs with configuration {qq_run_type}')

    run_args = QQ_RUN_ARGS[qq_run_type]

    print('Found the following arguments to pass to quickquasars:')
    qq_args = ''
    dla_flag = False
    for key, val in run_args.items():
        qq_args += f' --{key} {val}'

        if 'dla' in key:
            dla_flag = True

    qq_args += f' --seed {seed}'
    print(qq_args)

    qq_script = create_qq_script(config, job, qq_run_type, qq_args, seed, input_dir, output_dir)

    job_id = submit_utils.run_job(qq_script, no_submit=job.getboolean('no_submit'))

    return job_id, dla_flag


def create_qq_script(config, job, qq_dirname, qq_args, seed, input_dir, output_dir):

    submit_utils.set_umask()

    if job.getboolean('test_run'):
        print('Test run enabled, only using first 10 transmission files.')
        slurm_queue = 'debug'
        nodes = 1
        nproc = 2
        slurm_hours = 0.25
    else:
        slurm_queue = job.get('slurm_queue', 'regular')
        nodes = config.getint('nodes', 8)
        nproc = config.getint('nproc', 32)
        slurm_hours = config.getfloat('slurm_hours', 0.5)

    # Set up the directory structure to put everything into.
    qq_dir = dir_handlers.QQDir(output_dir, qq_dirname)

    # Make the header
    time = submit_utils.convert_job_time(slurm_hours)
    header = submit_utils.make_header(job.get('nersc_machine'), slurm_queue, nodes, time=time,
                                      omp_threads=nproc, job_name=f'qq_{seed}',
                                      err_file=qq_dir.run_dir/'run-%j.err',
                                      out_file=qq_dir.run_dir/'run-%j.out')

    # Create the main qq run command
    qq_run = f'    command="srun -N 1 -n 1 -c {nproc} '
    qq_run += f'quickquasars -i $tfiles --nproc {nproc} '
    qq_run += f'--outdir {qq_dir.spectra_dir} {qq_args}"\n'

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
        text += f'files=`ls -1 {input_dir}/*/*/transmission*.fits* | head -10`\n'
    else:
        text += f'files=`ls -1 {input_dir}/*/*/transmission*.fits*`\n'

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
    text += f'    echo "log in {qq_dir.log_dir}/node-$node.log"\n\n'
    text += f'    $command >& {qq_dir.log_dir}/node-$node.log &\n\n'
    text += 'done\n\n'
    text += 'wait\n'
    text += 'echo "END"\n\n'

    full_text = header + text

    # Write the script to file and run it.
    script_path = qq_dir.scripts_dir / 'run_quickquasars.sh'

    submit_utils.write_script(script_path, full_text)

    return script_path
