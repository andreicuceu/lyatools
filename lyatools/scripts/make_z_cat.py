#!/usr/bin/env python3

import argparse
import fitsio
from astropy.io import fits
import numpy as np
from multiprocessing import Pool
from pathlib import Path

from lyatools import submit_utils


FINAL_DTYPE = np.dtype([
    ('CHI2', 'f8'), ('COEFF', 'f8', 4), ('Z', 'f8'), ('ZERR', 'f8'),
    ('ZWARN', 'i8'), ('TARGETID', 'i8'),
    ('TARGET_RA', 'f8'), ('TARGET_DEC', 'f8'),
    ('FLUX_G', 'f4'), ('FLUX_R', 'f4'), ('FLUX_Z', 'f4')
])


def one_zcatalog(fzbest):
    fts = fitsio.FITS(fzbest)

    hdr1 = fts['ZBEST'].read_header()

    nrows = hdr1['NAXIS2']
    if nrows == 0:
        fts.close()
        return None

    newdata = np.empty(nrows, dtype=FINAL_DTYPE)

    data = fts['ZBEST'].read()
    n1 = list(set(data.dtype.names).intersection(FINAL_DTYPE.names))
    newdata[n1] = data[n1]

    data = fts['FIBERMAP'].read()
    n1 = list(set(data.dtype.names).intersection(FINAL_DTYPE.names))
    newdata[n1] = data[n1]

    fts.close()

    return newdata


def make_z_catalog(input_dir, output_file, prefix='zbest', nproc=None, only_qso_targets=False):
    spec_dir = submit_utils.find_path(input_dir)
    zbest_files = spec_dir.glob(f"*/*/{prefix}-*.fits*")

    # Read quasars from truth files
    print("Iterating over files")
    zcat_list = []
    with Pool(processes=nproc) as pool:
        imap_it = list(pool.imap(one_zcatalog, zbest_files))

        for arr in imap_it:
            if arr is None:
                continue

            zcat_list.append(arr)

    final_data = np.concatenate(zcat_list)
    
    if only_qso_targets:
        seed_zcat=fits.open(Path(output_file).parents[0] / 'seed_zcat.fits')
        final_data = final_data[seed_zcat[1].data['IS_QSO_TARGET']]
        
    
    print(f"There are {final_data.size} QSOs.")

    with fitsio.FITS(output_file, 'rw', clobber=True) as fts:
        fts.write(final_data, extname="ZCATALOG")

    print('Done')


def main():
    submit_utils.set_umask()
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--input-dir", type=str, required=True,
                        help="the spectra-16 directory containing truth files")

    parser.add_argument("-o", "--output-file", type=str, required=True,
                        help="Name of output file")

    parser.add_argument("--prefix", default='zbest',
                        help='Healpix zbest file prefix.')

    parser.add_argument("--nproc", type=int, default=None, required=False,
                        help='Number cores for parallelization')
    
    parser.add_argument("--only_qso_targets", action="store_true",
                        help='Only qso targets')

    args = parser.parse_args()

    make_z_catalog(
        args.input_dir, args.output_file, args.prefix, args.nproc, args.only_qso_targets
    )


if __name__ == '__main__':
    main()
