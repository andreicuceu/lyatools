#!/usr/bin/env python3

import argparse
import fitsio
import numpy as np
from multiprocessing import Pool

from lyatools import submit_utils

FINAL_DTYPE = np.dtype([('NHI', 'f8'), ('Z', 'f8'), ('TARGETID', 'i8'), ('DLAID', 'i8'), ('SNR','f8')])


def make_dla_catalog(input_dir, output_dir, mask_nhi_cut=None, sigma_nhi_errors=None,
                     snr_catalog_dir=None, mask_snr_cut=None, completeness=1.0, seed=0, nproc=None):

    spec_dir = submit_utils.find_path(input_dir)
    truth_files = spec_dir.glob("*/*/truth-*.fits*")

    dla_chunks = []

    print("Iterating over files")
    with Pool(processes=nproc) as pool:
        imap_it = pool.imap(_get_dla_catalog, truth_files)

        for arr in imap_it:
            if arr is None:
                continue

            dla_chunks.append(arr)

    num_hcds = np.sum([chunk.size for chunk in dla_chunks])

    print(f"There are {num_hcds} HCDs.")

    output_catalog = np.empty(num_hcds, dtype=FINAL_DTYPE)

    i = 0
    for chunk in dla_chunks:
        nrows = chunk.size
        if nrows == 0:
            continue

        output_catalog[i:i+nrows] = chunk
        i += nrows

    snr_catalog = _read_snr_catalog(snr_catalog_dir, output_catalog['TARGETID'])
    output_catalog['SNR'] = snr_catalog['SNR_REDSIDE']

    output_file = submit_utils.find_path(output_dir) / 'hcd_truth_cat.fits'
    if not output_file.is_file():
        print('Writing catalog with all HCDs')
        with fitsio.FITS(output_file, 'rw') as file:
            file.write(output_catalog, extname='DLACAT')

    # insert random errors in NHI
    np.random.seed(seed)
    if sigma_nhi_errors is not None:
        nhi_error = np.random.normal(0, sigma_nhi_errors, num_hcds)
        output_catalog['NHI'] += nhi_error

    # Mask DLAs with NHI > mask_nhi_cut
    mask = output_catalog['NHI'] > mask_nhi_cut
    mask_catalog = output_catalog[mask]

    # Mask DLAs with SNR < mask_snr_cut
    mask = mask_catalog['SNR'] > mask_snr_cut
    mask_catalog = mask_catalog[mask]

    # Reduce catalog completeness
    mask_catalog = _reduce_completeness(mask_catalog, completeness)

    output_file = submit_utils.find_path(output_dir) / f'dla_cat_nhi_{mask_nhi_cut:.2f}_snr_{mask_snr_cut:.1f}_completeness_{completeness:.2f}.fits'
    if not output_file.is_file():
        print(f'Writing catalog for masking DLAs with NHI > {mask_nhi_cut:.2f}, SNR > {mask_snr_cut:.1f} and completeness {completeness:.2f}')
        with fitsio.FITS(output_file, 'rw') as file:
            file.write(mask_catalog, extname='DLACAT')

def _get_dla_catalog(truth_file):
    hdul = fitsio.FITS(truth_file)

    hdr_dla = hdul['DLA_META'].read_header()

    nrows = hdr_dla['NAXIS']
    if nrows == 0:
        hdul.close()
        return None

    dat_dla = hdul['DLA_META'].read()
    nrows = len(dat_dla)
    hdul.close()

    newdata = np.empty(nrows, dtype=FINAL_DTYPE)
    newdata['NHI'] = dat_dla['NHI']
    newdata['Z'] = dat_dla['Z_DLA']
    newdata['TARGETID'] = dat_dla['TARGETID']
    newdata['DLAID'] = dat_dla['DLAID']

    return newdata

def _read_snr_catalog(path,targetids):
    path = submit_utils.find_path(path)
    if not path.is_file():
        raise FileNotFoundError('SNR catalog not found.')

    snr_catalog = fitsio.read(path, ext='SNRCAT',columns=['TARGETID','SNR_REDSIDE'])
    in_catalog = np.isin(targetids, snr_catalog['TARGETID'])
    if not np.all(in_catalog):
        raise ValueError('There are some TARGETIDs in the DLA catalog that are not in the SNR catalog. This should not happen.')

    # Sort SNR catalog to match the order of the DLA catalog.
    snr_catalog_sorted = np.sort(snr_catalog, order='TARGETID')
    sort_indices = np.searchsorted(snr_catalog_sorted['TARGETID'], targetids)
    snr_catalog = snr_catalog_sorted[sort_indices]
    assert np.all(snr_catalog['TARGETID'] == targetids)

    return snr_catalog

def _reduce_completeness(catalog, completeness):
    if completeness > 1 or completeness < 0:
        raise ValueError('Completeness fraction must be between 0 and 1')
    rand = np.random.rand(catalog.size)
    mask = rand < completeness
    # Placeholder for a more complex completeness downsampling strategy as a function of SNR and NHI.

    return catalog[mask]
    

def main():
    submit_utils.set_umask()
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--input-dir", type=str, required=True,
                        help="the spectra-16 directory containing truth files")

    parser.add_argument("-o", "--output-dir", type=str, required=True,
                        help="Name of output directory")

    parser.add_argument('--snr-catalog-dir', type=str, required=True,
                        help=('Path to SNR catalog'))

    parser.add_argument('--mask-nhi-cut', type=float, default=20.3, required=False,
                        help=('NHI cut used to select DLAs for masking. '
                              '(Only DLAs with NHI larger than this will be masked)'))

    parser.add_argument("--nhi-error-amplitude", type=float, default=None, required=False,
                        help="Amplitude of NHI errors")

    parser.add_argument("--completeness", type=float, default=1.0, required=False, 
                        help="Fraction of DLAs to keep in the catalog")
    
    parser.add_argument('--mask-snr-cut', type=float, default=0., required=False,
                        help=('SNR cut used to select DLAs for masking. '
                              '(Only QSOs with SNR larger than this will have their DLAs masked)'))
    
    parser.add_argument("-s", "--seed", type=int, required=False, default=0,
                        help="Seed for random implementations")
    
    parser.add_argument("--nproc", type=int, default=None, required=False,
                        help='Number cores for parallelization')

    args = parser.parse_args()


    make_dla_catalog(args.input_dir, args.output_dir, mask_nhi_cut=args.mask_nhi_cut, sigma_nhi_errors=args.nhi_error_amplitude, 
                     snr_catalog_dir=args.snr_catalog_dir, mask_snr_cut=args.mask_snr_cut, completeness=args.completeness, 
                     seed=args.seed, nproc=args.nproc) 


if __name__ == '__main__':
    main()
