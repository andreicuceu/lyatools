import numpy as np
from pathlib import Path
from subprocess import call

from . import dir_handlers, submit_utils

QQ_RUN_ARGS = {
    'desi-test': {
        'exptime': 4000,
        'downsampling': 0.4,
        'sigma_kms_fog': 0.0,
        'zbest': '',
        'bbflux': '',
        'save-continuum': '',
        'desi-footprint': '',
    },
    'desi-3.0-4': {
        'dn_dzdm': 'lyacolore',
        'exptime': 4000,
        'zbest': '',
        'bbflux': '',
        'save-continuum': '',
        'desi-footprint': '',
        'zmin': '1.7',
        'add-LYB': ''
    },
    'desi-3.5-4': {
        'dn_dzdm': 'lyacolore',
        'exptime': 4000,
        'zbest': '',
        'bbflux': '',
        'save-continuum': '',
        'desi-footprint': '',
        'zmin': '1.7',
        'add-LYB': '',
        'sigma_kms_fog': '0'
    },
    'desi-3.12-4': {
        'dn_dzdm': 'lyacolore',
        'exptime': 4000,
        'zbest': '',
        'bbflux': '',
        'save-continuum': '',
        'desi-footprint': '',
        'zmin': '1.7',
        'add-LYB': '',
        'dla': 'file',
        'metals': 'LYB LY3 LY4 LY5 SiII(1260) SiIII(1207) SiII(1193) SiII(1190)'
    },
}


def multi_run_qq(input_dir_all, output_dir_all, qq_seeds, qq_run_type, test_run, no_submit, *args):
    """Create and submit QQ runs for multiple mock realizations

    Parameters
    ----------
    input_dir_all : str
        Raw directory with all v9.0 lyacolore runs
    output_dir_all : str
        Directory that contains all of the qq realisations (dir with v9.0.x).
    qq_seeds : str
        QQ seeds to run. Either a single string (e.g. '0') or a range (e.g. '0-5').
    qq_run_type : str
        Run type. Must be a key in the QQ_RUN_ARGS dict.
    test_run : bool
        Test run flag
    no_submit : bool
        Submit flag
    args : list
        List with args passed to create_qq_script
    """
    # Get list of seeds
    run_seeds = []
    for seed in qq_seeds:
        seed_range = seed.split('-')

        if len(seed_range) == 1:
            run_seeds.append(int(seed_range[0]))
        elif len(seed_range) == 2:
            run_seeds += list(np.arange(int(seed_range[0]), int(seed_range[1])))
        else:
            raise ValueError(f'Unknown seed type {seed}. Must be int or range (e.g. 0-5)')

    run_seeds.sort()

    # Submit QQ run for each seed
    for seed in run_seeds:
        input_dir = Path(input_dir_all) / f'v9.0.{seed}'
        output_dir = Path(output_dir_all) / f'v9.0.{seed}'
        print(f'Submitting QQ run for mock v9.0.{seed}')

        run_qq(qq_run_type, test_run, no_submit, input_dir, output_dir, *args)


def run_qq(qq_run_type, test_run, no_submit, *args):
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
    if test_run:
        print('Test run enabled, overriding arguments to setup it up.')
        qq_run_type = 'desi-test'

    submit_utils.print_spacer_line()

    # Print run config
    print(f'Submitting quickquasars runs with configuration {qq_run_type}')

    run_args = QQ_RUN_ARGS[qq_run_type]
    qq_args = ''
    for key, val in run_args.items():
        qq_args += f' --{key} {val}'

    qq_script = create_qq_script(qq_run_type, qq_args, test_run, *args)

    if not no_submit:
        print(f'Submitting script {qq_script}')
        call(f'sbatch {qq_script}', shell=True)


def create_qq_script(qq_dirname, qq_args, test_run, input_dir, output_dir, nersc_machine='perl',
                     slurm_hours=0.5, slurm_queue='regular', nodes=8, nproc=32, env_command=None):

    submit_utils.set_umask()

    if test_run:
        print('INFO: test run enabled, only using first 10 transmission files.')
        slurm_queue = 'debug'
        nodes = 1
        nproc = 2
        slurm_hours = 0.25

    qq_string = ''
    for arg in qq_args:
        qq_string += (' ' + arg)
    print('INFO: Found the following arguments to pass to quickquasars:')
    print(qq_string)

    # Set up the directory structure to put everything into.
    qq_dir = dir_handlers.QQDir(output_dir, qq_dirname)

    # Make the header
    time = submit_utils.convert_job_time(slurm_hours)
    header = submit_utils.make_header(nersc_machine, slurm_queue, nodes, time=time,
                                      omp_threads=nproc, job_name='run_quickquasars',
                                      err_file=qq_dir.run_dir/'run-%j.err',
                                      out_file=qq_dir.run_dir/'run-%j.out')

    # Create the main qq run command
    qq_run = f'    command="srun -N 1 -n 1 -c {nproc} '
    qq_run += f'quickquasars -i $tfiles --nproc {nproc} '
    qq_run += f'--outdir {qq_dir.spectra_dir} {qq_string}"\n'

    # Make the text body of the script.
    text = '\n\n'
    if env_command is None:
        text += 'source /global/common/software/desi/desi_environment.sh master'
    else:
        text += env_command

    text += '\n\n'
    text += 'echo "get list of skewers to run ..."\n\n'

    if test_run:
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
    with open(script_path, 'w') as f:
        f.write(full_text)

    submit_utils.make_file_executable(script_path)

    return script_path
