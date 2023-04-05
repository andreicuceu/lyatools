    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--deltas-dir-lya', type=str, default=None, required=True,
                        help='Directory containing deltas for lya region')

    parser.add_argument('--deltas-dir-lyb', type=str, default=None, required=False,
                        help='Directory containing deltas for lyb region')

    parser.add_argument('-c', '--catalogue', type=str, default=None, required=False,
                        help='Quasar catalogue')

    parser.add_argument('-o', '--analysis-dir', type=str, default=None, required=True,
                        help='Analysis directory where the correlations folder will be stored')

    parser.add_argument('--corr-types', type=str, default=None, required=False,
                        nargs='*', choices=corr_types,
                        help=f'Type of correlations to compute. Choose from {corr_types}')

    parser.add_argument('--compute-dmat', action='store_true', required=False,
                        help='Flag for computing distortion matrices')

    parser.add_argument('--name-string', type=str, default=None, required=False,
                        help='Optional name string to append to the end of the filenames')

    parser.add_argument('--rp-min', type=float, default=0., required=False,
                        help='Min r-parallel [h^-1 Mpc]')

    parser.add_argument('--rp-max', type=float, default=200., required=False,
                        help='Max r-parallel [h^-1 Mpc]')

    parser.add_argument('--rt-max', type=float, default=200., required=False,
                        help='Mix r-transverse [h^-1 Mpc]')

    parser.add_argument('--num-bins-rp', type=int, default=50, required=False,
                        help='Number of bins in r-parallel [h^-1 Mpc]')

    parser.add_argument('--num-bins-rt', type=int, default=50, required=False,
                        help='Number of bins in r-transverse [h^-1 Mpc]')

    parser.add_argument('--z-limits', type=float, nargs=2, default=[0, 10], required=False,
                        help='Bin edges in redshift')

    parser.add_argument('--fid-Om', type=float, default=0.31457, required=False,
                        help='Fiducial Omega_matter value at z=0.')

    parser.add_argument('--no-project', action='store_true', required=False,
                        help='Do not project out continuum fitting modes')

    parser.add_argument('--no-remove-mean-lambda-obs', action='store_true', required=False,
                        help='Do not remove mean delta versus lambda_obs')

    parser.add_argument('--dmat-rejection', type=float, default=0.99, required=False,
                        help='Fraction of pairs to ignore when computing distortion matrix')

    parser.add_argument('--rebin-factor', type=int, default=None, required=False,
                        help='Rebin factor for deltas. If not None, deltas will '
                             'be rebinned by that factor')

    parser.add_argument('--nersc-machine', type=str, default='perl', choices=['perl', 'cori'],
                        required=False, help='Whether script is to be run on Perlmutter or Cori')

    parser.add_argument('--slurm-hours', type=float, default=0.5, required=False,
                        help='Number of hours for slurm job')

    parser.add_argument('--slurm-queue', type=str, default='regular', required=False,
                        help='Slurm queue to use')

    parser.add_argument('--nproc', type=int, default=64, required=False,
                        help='Number of processors')

    parser.add_argument('--env-command', type=str, default='piccaenv', required=False,
                        help='Command to activate the anaconda environment that contains picca.')

    parser.add_argument('--no-submit', action="store_true", default=False, required=False,
                        help='make the run scripts but do not submit the jobs')

    args = parser.parse_args()