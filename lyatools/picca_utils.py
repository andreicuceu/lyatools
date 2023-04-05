import fitsio
import numpy as np
from scipy.constants import speed_of_light


def desi_from_ztarget_to_drq(in_path, out_path, spec_type='QSO', downsampling_z_cut=None,
                             downsampling_num=None, gauss_redshift_error=None):
    """Transforms a catalog of object in desi format to a catalog in DRQ format
    Args:
        in_path: string
            Full path filename containing the catalogue of objects
        out_path: string
            Full path filename where the fits DLA catalogue will be written to
        spec_type: string
            Spectral type of the objects to include in the catalogue
        downsampling_z_cut: float or None - default: None
            Minimum redshift to downsample the data. 'None' for no downsampling
        downsampling_num: int
            Target number of object above redshift downsampling-z-cut.
            'None' for no downsampling
        gauss_redshift_error: int
            Gaussian random error to be added to redshift (in km/s)
            Mimics uncertainties in estimation of z in classifiers
            'None' for no error
    """
    # Info of the primary observation
    hdul = fitsio.FITS(in_path)
    spec_type_list = np.char.strip(hdul[1]['SPECTYPE'][:].astype(str))

    # apply cuts
    print(f" start               : nb object in cat = {spec_type_list.size}")
    w = hdul[1]['ZWARN'][:] == 0.
    print(f' and zwarn==0        : nb object in cat = {w.sum()}')
    w &= spec_type_list == spec_type
    print(f' and spectype=={spec_type}    : nb object in cat = {w.sum()}')

    # load the arrays
    cat = {}
    from_desi_key_to_picca_key = {
        'RA': 'RA',
        'DEC': 'DEC',
        'Z': 'Z',
        'THING_ID': 'TARGETID',
        'PLATE': 'TARGETID',
        'MJD': 'TARGETID',
        'FIBERID': 'TARGETID'
    }
    for key, value in from_desi_key_to_picca_key.items():
        cat[key] = hdul[1][value][:][w]
    hdul.close()

    for key in ['RA', 'DEC']:
        cat[key] = cat[key].astype('float64')

    # apply error to z
    if gauss_redshift_error is not None:
        SPEED_LIGHT = speed_of_light / 1000  # [km/s]
        np.random.seed(0)
        dz = gauss_redshift_error/SPEED_LIGHT*(1.+cat['Z'])*np.random.normal(0, 1, cat['Z'].size)
        cat['Z'] += dz

    # apply downsampling
    if downsampling_z_cut is not None and downsampling_num is not None:
        if cat['RA'].size < downsampling_num:
            print(f"WARNING:: Trying to downsample, when nb cat = {cat['RA'].size} and "
                  f"nb downsampling = {downsampling_num}")
        else:
            z_cut_num = (cat['Z'] > downsampling_z_cut).sum()
            select_fraction = (downsampling_num / z_cut_num)
            if select_fraction < 1.0:
                np.random.seed(0)
                w = np.random.choice(np.arange(cat['RA'].size),
                                     size=int(cat['RA'].size * select_fraction),
                                     replace=False)
                for key in cat:
                    cat[key] = cat[key][w]
                print((f" and downsampling : nb object in cat = {cat['RA'].size}, nb z > "
                       f"{downsampling_z_cut} = {z_cut_num}"))
            else:
                print((f"WARNING::Trying to downsample, when nb QSOs with z > {downsampling_z_cut}"
                       f" = {z_cut_num} and downsampling = {downsampling_num}"))

    # sort by THING_ID
    w = np.argsort(cat['THING_ID'])
    for key in cat:
        cat[key] = cat[key][w]

    # save catalogue
    results = fitsio.FITS(out_path, 'rw', clobber=True)
    cols = list(cat.values())
    names = list(cat)
    results.write(cols, names=names, extname='CAT')
    results.close()
