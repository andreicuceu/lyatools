from os import mkdir
from typing import Union
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
    main_path: Union[str, Path]
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
    main_path: Union[str, Path]
    qq_run_name: str
    analysis_version: str = 'baseline'
    deltas_lya_dirname: str = 'deltas_lya'
    deltas_lyb_dirname: str = 'deltas_lyb'

    analysis_dir: Path = field(init=False)
    corr_dir: Path = field(init=False)
    deltas_lya_dir: Path = field(init=False)
    deltas_lyb_dir: Path = field(init=False)
    qsonic_deltas_lya_dir: Path = field(init=False)
    qsonic_deltas_lyb_dir: Path = field(init=False)
    fits_dir: Path = field(init=False)
    logs_dir: Path = field(init=False)
    scripts_dir: Path = field(init=False)

    def __post_init__(self):
        self.main_path = Path(self.main_path)
        check_dir(self.main_path)

        main_analysis_dir = self.main_path / self.qq_run_name
        check_dir(main_analysis_dir)

        self.analysis_dir = main_analysis_dir / self.analysis_version
        check_dir(self.analysis_dir)

        self.corr_dir = self.analysis_dir / 'correlations'
        check_dir(self.corr_dir)

        self.deltas_lya_dir = self.analysis_dir / self.deltas_lya_dirname
        check_dir(self.deltas_lya_dir)

        self.deltas_lyb_dir = self.analysis_dir / self.deltas_lyb_dirname
        check_dir(self.deltas_lyb_dir)

        self.qsonic_deltas_lya_dir = self.analysis_dir / ('qsonic_' + self.deltas_lya_dirname)
        check_dir(self.deltas_lya_dir)

        self.qsonic_deltas_lyb_dir = self.analysis_dir / ('qsonic_' + self.deltas_lyb_dirname)
        check_dir(self.deltas_lyb_dir)

        self.fits_dir = self.analysis_dir / 'fits'
        check_dir(self.fits_dir)

        self.logs_dir = self.analysis_dir / 'logs'
        check_dir(self.logs_dir)

        self.scripts_dir = self.analysis_dir / 'scripts'
        check_dir(self.scripts_dir)


# @dataclass
# class CorrDir:
#     """
#     An object that contains the directory structure for an individual correlation.
#     """
#     main_path: str
#     corr_dirname: str = 'correlations'
#     results_dirname: str = 'measurements'
#     run_dirname: str = 'run_files'
#     scripts_dirname: str = 'scripts'

#     corr_dir: Path = field(init=False)
#     results_dir: Path = field(init=False)
#     run_dir: Path = field(init=False)
#     scripts_dir: Path = field(init=False)

#     def __post_init__(self):
#         main_path = Path(self.main_path)
#         check_dir(main_path)

#         self.corr_dir = main_path / self.corr_dirname
#         check_dir(self.corr_dir)

#         self.results_dir = self.corr_dir / self.results_dirname
#         check_dir(self.results_dir)

#         self.run_dir = self.corr_dir / self.run_dirname
#         check_dir(self.run_dir)

#         self.scripts_dir = self.corr_dir / self.scripts_dirname
#         check_dir(self.scripts_dir)
