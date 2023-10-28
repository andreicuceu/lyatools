#!/usr/bin/env python3

import argparse

from survey_release import SurveyRelease


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--input-cat", type=str, required=False,
                        default=str(
                            '/global/cfs/cdirs/desi/science/lya/y1-kp6/iron-tests/catalogs/'
                            'QSO_cat_iron_main_dark_healpix_zlya-altbal_zwarn_cut_20230918.fits'
                        ),
                        help="Input Iron catalog")

    parser.add_argument("-o", "--output", type=str, required=True,
                        help="Path of output catalog")

    parser.add_argument("--mock-type", type=str, default='london', required=False,
                        choices=('london', 'saclay'), help='Target mock type.')

    parser.add_argument("--mock-version", type=int, default='v9.0', required=False,
                        help='Mock version (do not include last digit, which is the seed)')

    parser.add_argument("--seed", type=int, default=None, required=True,
                        help='Mock seed')

    args = parser.parse_args()

    survey = SurveyRelease(mockteam=args.mock_type, subversion=f'{args.mock_version}.{args.seed}',
                           seed=args.seed, qso_only=True, data_file=args.input_cat)

    # Apply redshift distribution
    # Note: For Y1 mocks (and probably Y5 too) the target selection redshift distribution
    # from Chaussidon et al. 2022 works better to match QSO targets Iron catalog.
    # The option distribution='from_data' should be a better option once I finish implementing it.
    survey.apply_redshift_dist(distribution='target_selection', zmin=1.8)

    # Apply NPASS geometry:
    survey.apply_data_geometry(release='iron')  # Pass release = None for Y5 mocks.

    # Assign magnitudes
    # Pass from_data = False for Y5 mocks. Unless you want to use the Y1 magnitude distributions.
    survey.assign_rband_magnitude(from_data=True)

    # Assign exposures
    survey.assign_exposures(exptime=None)  # Pass exptime = 4000 for Y5 mocks.

    # Write mock catalog
    survey.mockcatalog.write(args.output)


if __name__ == '__main__':
    main()
