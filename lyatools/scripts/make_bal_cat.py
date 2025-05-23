#!/usr/bin/env python3

import argparse
import fitsio
import numpy as np
from multiprocessing import Pool

from lyatools import submit_utils


def read_bals_from_truth(truth_file):
    with fitsio.FITS(truth_file) as hdul:
        data = hdul['BAL_META'].read()

    if data.shape[0] < 1:
        return None

    return data


def make_bal_catalog(input_dir, output_dir, ai_cut=None, bi_cut=None, nproc=1):
    spec_dir = submit_utils.find_path(input_dir)
    truth_files = spec_dir.glob("*/*/truth-*.fits*")

    # Read BALs from truth files
    print("Iterating over files")
    bal_chunks = []
    with Pool(processes=nproc) as pool:
        imap_it = list(pool.imap(read_bals_from_truth, truth_files))

        for arr in imap_it:
            if arr is None:
                continue

            bal_chunks.append(arr)

    num_bals = np.sum([chunk.size for chunk in bal_chunks])

    print(f"There are {num_bals} BALs.")

    output_catalog = np.empty(num_bals, dtype=bal_chunks[0].dtype)

    i = 0
    for chunk in bal_chunks:
        nrows = chunk.size
        if nrows == 0:
            continue

        output_catalog[i:i+nrows] = chunk
        i += nrows

    # Write full BAL catalog
    output_path = submit_utils.find_path(output_dir)
    output_file = output_path / 'bal_cat.fits'
    if not output_file.is_file():
        print('Writing catalog with all BALs')
        with fitsio.FITS(output_file, 'rw') as file:
            file.write(output_catalog, extname='ZCATALOG')

    if ai_cut is None and bi_cut is None:
        return

    # Find BALs that are below the AI/BI cuts and make a second BAL catalog with them
    mask = np.ones(output_catalog.size, dtype=bool)
    if ai_cut is not None:
        mask &= output_catalog['AI_CIV'] < ai_cut

    if bi_cut is not None:
        mask &= output_catalog['AI_CIV'] < bi_cut

    output_file = output_path / f'bal_cat_AI_{ai_cut}_BI_{bi_cut}.fits'
    if not output_file.is_file():
        print('Writing catalog with cuts in AI/BI')
        with fitsio.FITS(output_file, 'rw') as file:
            file.write(output_catalog[mask], extname='ZCATALOG')

    # Read QSO catalog and remove BAL QSOs with AI/BI larger than cuts
    with fitsio.FITS(output_path / 'zcat.fits') as hdul_qso:
        header = hdul_qso[1].read_header()
        qso_cat = hdul_qso[1].read()

    _, common_idx, __ = np.intersect1d(
        qso_cat['TARGETID'], output_catalog[~mask]['TARGETID'], return_indices=True)

    qso_mask = np.ones(qso_cat.size, dtype=bool)
    qso_mask[common_idx] = False

    output_file = output_path / f'zcat_masked_AI_{ai_cut}_BI_{bi_cut}.fits'
    if not output_file.is_file():
        with fitsio.FITS(output_file, 'rw') as file:
            file.write(output_catalog[mask], header=header, extname='ZCATALOG')


def main():
    submit_utils.set_umask()
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--input-dir", type=str, required=True,
                        help="the spectra-16 directory containing truth files")

    parser.add_argument("-o", "--output-dir", type=str, required=True,
                        help="Name of output directory")

    parser.add_argument("--ai-cut", type=int, default=None, required=False,
                        help='Cut threshold in AI')

    parser.add_argument("--bi-cut", type=int, default=None, required=False,
                        help='Cut threshold in BI')

    parser.add_argument("--nproc", type=int, default=None, required=False,
                        help='Number cores for parallelization')

    args = parser.parse_args()

    make_bal_catalog(
        args.input_dir, args.output_dir, ai_cut=args.ai_cut, bi_cut=args.bi_cut, nproc=args.nproc
    )


if __name__ == '__main__':
    main()
