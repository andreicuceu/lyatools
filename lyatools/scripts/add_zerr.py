#!/usr/bin/env python3
import argparse

import fitsio
import numpy as np
import scipy as sp
from scipy.constants import speed_of_light

from lyatools import submit_utils


def gen_lorentzian(loc, scale, size, cut):
    samples = sp.stats.cauchy.rvs(loc=loc, scale=scale, size=3*size)
    samples = samples[abs(samples) < cut]
    if len(samples) >= size:
        samples = samples[:size]
    else:
        # Only added for the very unlikely case that there are not enough samples after the cut.
        samples = gen_lorentzian(loc, scale, size, cut)
    return samples


def main():
    submit_utils.set_umask()
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--input", type=str, required=True,
                        help="Input catalog")

    parser.add_argument("-o", "--output", type=str, required=True,
                        help="Ouput catalog with redshift errors")

    parser.add_argument("-a", "--amplitude", type=float, required=True,
                        help="Amplitude of redshift errors in km/s")

    parser.add_argument("-t", "--type", type=str, required=True, choices=['gauss', 'lorentz'],
                        help="Type of distribution to draw redshift errors from")

    parser.add_argument("-s", "--seed", type=int, required=False, default=0,
                        help="Seed for drawing random redshift errors")

    args = parser.parse_args()

    print(f'Creating new catalog with redshift errors from {args.input}.')
    print(f'Distribution: {args.type}, Amplitude: {args.amplitude}')

    hdul = fitsio.FITS(args.input)
    header = hdul[1].read_header()
    data = hdul[1].read()
    size = data['Z'].size

    np.random.seed(args.seed)

    dv = np.zeros(size)
    if args.type == 'gauss':
        dv = np.random.normal(0, args.amplitude, size)
    elif args.type == 'lorentz':
        dv = gen_lorentzian(0, args.amplitude, size, cut=2000)
    else:
        raise ValueError(f'Unkown distribution "{args.type}".')

    data['Z'] += dv / (speed_of_light / 1e3) * (1 + data['Z'])

    results = fitsio.FITS(args.output, 'rw', clobber=True)
    results.write(data, header=header)
    results.close()

    print('Done')


if __name__ == '__main__':
    main()
