import fitsio
import numpy as np
from multiprocessing import Pool

from lyatools.submit_utils import find_path


def read_bals_from_truth(truth_file):
    hdul = fitsio.FITS(truth_file)

    header = hdul['BAL_META'].read_header()
    nrows = header['NAXIS']
    if nrows == 0:
        hdul.close()
        return None

    data = hdul['BAL_META'].read()
    hdul.close()

    return data


def make_bal_catalog(input_dir, output_dir, ai_cut=int(500), bi_cut=None, nproc=1):
    spec_dir = find_path(input_dir)
    truth_files = spec_dir.glob("*/*/truth-*.fits*")

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

    output_file = find_path(output_dir) / 'bal_cat.fits'
    if not output_file.is_file():
        print('Writing catalog with all BALs')
        with fitsio.FITS(output_file, 'rw') as file:
            file.write(output_catalog, extname='BALCAT')

    mask = np.ones(output_catalog.size, dtype=bool)
    if ai_cut is not None:
        mask &= output_catalog['AI_CIV'] < ai_cut

    if bi_cut is not None:
        mask &= output_catalog['AI_CIV'] < bi_cut

    output_file = find_path(output_dir) / f'bal_cat_AI_{ai_cut}_BI_{bi_cut}.fits'
    if not output_file.is_file():
        print('Writing catalog with cuts in AI/BI')
        with fitsio.FITS(output_file, 'rw') as file:
            file.write(output_catalog[mask], extname='BALCAT')
