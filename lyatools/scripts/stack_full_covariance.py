#!/usr/bin/env python3
import fitsio
import argparse
import numpy as np
from functools import reduce
from multiprocessing import Pool
from lyatools import submit_utils
from picca.utils import compute_cov

def read_corr(files):
    xi = []
    weights = []
    hp_ids = []

    for file in files:
        with fitsio.FITS(file) as hdul:
            txi=None
            if 'DA' in hdul[2].get_colnames():
                txi=hdul[2]['DA'][:]
            else:
                txi=hdul[2]['DA_BLIND'][:]
            print(file,"correlation shape=",txi.shape)
            weights.append(hdul[2]['WE'][:])
            hp_ids.append(hdul[2]["HEALPID"][:])        
        xi.append(txi)
    
    common_hp = reduce(np.intersect1d, hp_ids)
    masks = [np.in1d(hp_ids_i, common_hp) for hp_ids_i in hp_ids]

    xi = np.hstack([xi_i[mask] for xi_i, mask in zip(xi, masks)])
    weights = np.hstack([weights_i[mask] for weights_i, mask in zip(weights, masks)])

    return xi, weights

def read_all(files, nproc):
    with Pool(processes=nproc) as pool:
        results = list(pool.imap(read_corr, files))
    xi = np.vstack([res[0] for res in results])
    weights = np.vstack([res[1] for res in results])
    return xi, weights

def main():
    submit_utils.set_umask()
    parser = argparse.ArgumentParser()

    parser.add_argument("--lyaxlya", type=str, nargs="*", default = None,
                        help="Correlation files for lyaxlya.")
    parser.add_argument("--lyaxlyb", type=str, nargs="*", default = None,
                        help="Correlation files for lyaxlyb.")
    parser.add_argument("--lyaxqso", type=str, nargs="*", default = None,
                        help="Correlation files for lyaxqso.")
    parser.add_argument("--lybxqso", type=str, nargs="*", default = None,
                        help="Correlation files for lybxqso.")
    parser.add_argument("--outfile", type=str, required=True, help="name of output file")
    parser.add_argument("--no-smooth-cov", action="store_true", default=False,
                        help="Whether to turn off smoothing of the covariance matrix")
    parser.add_argument("--nproc", type=int, default=128, required=False, help="Number of processes")

    args = parser.parse_args()
    
    all_files = []
    if args.lyaxlya is not None:
        all_files.append(args.lyaxlya)
    if args.lyaxlyb is not None:
        all_files.append(args.lyaxlyb)
    if args.lyaxqso is not None:
        all_files.append(args.lyaxqso)
    if args.lybxqso is not None:
        all_files.append(args.lybxqso)
    if len(all_files) == 0:
        raise ValueError("No correlation files provided.")
    # Check that all lists have the same length
    if not all(len(f) == len(all_files[0]) for f in all_files):
        raise ValueError("All correlation file lists must have the same length.")
    all_files = list(zip(*all_files))

    print(f'Reading {len(all_files)} mocks...')
    xi, weights = read_all(all_files, nproc=args.nproc)
    print('Done reading')

    cov = compute_cov(xi, weights)

    print('Writing covariance')
    results = fitsio.FITS(args.outfile, 'rw', clobber=True)
    results.write([cov], names=['COV'], units=[''], extname='COVMAT')
    results.close()

    print('Done')
