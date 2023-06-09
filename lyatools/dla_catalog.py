import fitsio
import numpy as np
from multiprocessing import Pool

from lyatools.submit_utils import find_path


FINAL_DTYPE = np.dtype([('NHI', 'f8'), ('Z', 'f8'), ('TARGETID', 'i8'), ('DLAID', 'i8')])


def make_dla_catalog(input_dir, output_dir, mask_nhi_cut=None, nproc=1):

    spec_dir = find_path(input_dir)
    truth_files = spec_dir.glob("*/*/truth-*.fits*")

    dla_chunks = []

    print("Iterating over files")
    with Pool(processes=nproc) as pool:
        imap_it = pool.imap(_get_dla_catalog, truth_files)

        for arr in imap_it:
            if arr is None:
                continue

            dla_chunks.append(arr)

    num_dlas = np.sum([chunk.size for chunk in dla_chunks])

    print(f"There are {num_dlas} DLAs.")

    output_catalog = np.empty(num_dlas, dtype=FINAL_DTYPE)

    i = 0
    for chunk in dla_chunks:
        nrows = chunk.size
        if nrows == 0:
            continue

        output_catalog[i:i+nrows] = chunk
        i += nrows

    output_file = find_path(output_dir) / 'dla_cat.fits'
    if not output_file.is_file():
        print('Writing catalog with all DLAs')
        with fitsio.FITS(output_file, 'rw') as file:
            file.write(output_catalog, extname='DLACAT')

    if mask_nhi_cut is not None:
        mask = output_catalog['NHI'] > mask_nhi_cut
        mask_catalog = output_catalog[mask]

        output_file = find_path(output_dir) / f'dla_cat_mask_{mask_nhi_cut:.2f}.fits'
        if not output_file.is_file():
            print(f'Writing catalog for masking DLAs with NHI > {mask_nhi_cut:.2f}')
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
