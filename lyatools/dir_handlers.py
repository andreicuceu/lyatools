from os import mkdir
from pathlib import Path
from subprocess import call
from dataclasses import dataclass, field


def make_permission_group_desi(dir: Path):
    """
    Makes a directory and all its contents have DESI as their permission group.
    Args:
        dir: Path
            Directory to deal with.
    """
    call(f'chgrp -R desi {dir}', shell=True)
    call(f'chmod -R g+rx-w {dir}', shell=True)
    call(f'chmod -R g+s {dir}', shell=True)


def check_dir(dir: Path):
    """
    Checks that a directory exists, and that its permission group is DESI.
    Args:
        dir: Path
            Directory to check
    """
    if not dir.is_dir():
        mkdir(dir)
        make_permission_group_desi(dir)


@dataclass
class QQDir:
    """
    An object that contains the directory structure for a quickquasars run.
    """
    main_path: str
    qq_dirname: str
    spectra_dirname: str = 'spectra-16'
    run_dirname: str = 'run_files'
    log_dirname: str = 'logs'
    scripts_dirname: str = 'scripts'

    qq_dir: Path = field(init=False)
    spectra_dir: Path = field(init=False)
    run_dir: Path = field(init=False)
    log_dir: Path = field(init=False)
    scripts_dir: Path = field(init=False)

    def __post_init__(self):
        main_path = Path(self.main_path)
        check_dir(main_path)

        self.qq_dir = main_path / self.qq_dirname
        check_dir(self.qq_dir)

        self.spectra_dir = self.qq_dir / self.spectra_dirname
        check_dir(self.spectra_dir)

        self.run_dir = self.qq_dir / self.run_dirname
        check_dir(self.run_dir)

        self.log_dir = self.qq_dir / self.log_dirname
        check_dir(self.log_dir)

        self.scripts_dir = self.qq_dir / self.scripts_dirname
        check_dir(self.scripts_dir)


@dataclass
class AnalysisDir:
    """
    An object that contains the directory structure for our analysis.
    """
    main_path: str
    analysis_name: str
    data_dirname: str = 'data'
    corr_dirname: str = 'correlations'
    fits_dirname: str = 'fits'
    deltas_lya_dirname: str = 'deltas_lya'
    deltas_lyb_dirname: str = 'deltas_lyb'

    analysis_dir: Path = field(init=False)
    data_dir: Path = field(init=False)
    corr_dir: Path = field(init=False)
    fits_dir: Path = field(init=False)
    deltas_lya_dir: Path = field(init=False)
    deltas_lyb_dir: Path = field(init=False)

    def __post_init__(self):
        main_path = Path(self.main_path)
        check_dir(main_path)

        self.analysis_dir = main_path / self.analysis_name
        check_dir(self.analysis_dir)

        self.data_dir = self.analysis_dir / self.data_dirname
        check_dir(self.data_dir)

        self.corr_dir = self.analysis_dir / self.corr_dirname
        check_dir(self.corr_dir)

        self.fits_dir = self.analysis_dir / self.fits_dirname
        check_dir(self.fits_dir)

        self.deltas_lya_dir = self.data_dir / self.deltas_lya_dirname
        check_dir(self.deltas_lya_dir)

        self.deltas_lyb_dir = self.data_dir / self.deltas_lyb_dirname
        check_dir(self.deltas_lyb_dir)


@dataclass
class CorrDir:
    """
    An object that contains the directory structure for an individual correlation.
    """
    main_path: str
    corr_dirname: str = 'correlations'
    results_dirname: str = 'measurements'
    run_dirname: str = 'run_files'
    scripts_dirname: str = 'scripts'

    corr_dir: Path = field(init=False)
    results_dir: Path = field(init=False)
    run_dir: Path = field(init=False)
    scripts_dir: Path = field(init=False)

    def __post_init__(self):
        main_path = Path(self.main_path)
        check_dir(main_path)

        self.corr_dir = main_path / self.corr_dirname
        check_dir(self.corr_dir)

        self.results_dir = self.corr_dir / self.results_dirname
        check_dir(self.results_dir)

        self.run_dir = self.corr_dir / self.run_dirname
        check_dir(self.run_dir)

        self.scripts_dir = self.corr_dir / self.scripts_dirname
        check_dir(self.scripts_dir)
