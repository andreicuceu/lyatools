from subprocess import run
from lyatools import submit_utils
from pathlib import Path


def make_catalog(spec_dir, name):
    """Build a redshift catalog from spectra using desi_zcatalog.

    Parameters
    ----------
    spec_dir : str or Path
        Directory containing the spectra files.
    name : str
        Name of the output catalog file.
    """
    # Make the text of the script
    text = '#!/bin/bash -l\n\n'
    text += 'source /global/common/software/desi/desi_environment.sh\n'

    qq_dir = Path(spec_dir).parents[0]
    zcat = qq_dir / name
    text += f'desi_zcatalog -i {spec_dir} -o {zcat} --minimal --prefix zbest\n'

    # Write it to file
    script = qq_dir / 'make_zcat.sh'
    with open(script, 'w') as f:
        f.write(text)
    submit_utils.make_file_executable(script)

    # Execute it
    process = run(script)
    if process.returncode != 0:
        raise ValueError(f'Running script "{script}" returned non-zero exitcode '
                         f'with error {process.stderr}')
