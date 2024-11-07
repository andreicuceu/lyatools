#!/usr/bin/env python3

from astropy.table import Table, vstack
import numpy as np
import multiprocessing as mp
import desispec.io
from desispec.coaddition import resample_spectra_lin_or_log
from scipy.constants import speed_of_light

import os
import argparse
import time
import fitsio

# constants for masking broad absorption lines 
# line centers identical to those defined in igmhub/picca
bal_lines={
    "CIV" : 1549.,
    "SiIV2" : 1403.,
    "SiIV1" : 1394.,
    "NV" : 1240.81,
    "Lya" : 1216.1,
    "CIII" : 1175.,
    "PV2" : 1128.,
    "PV1" : 1117.,
    "SIV2" : 1074.,
    "SIV1" : 1062.,
    "OIV" : 1031.,
    "OVI" : 1037.,
    "OI" : 1039.,
    "Lyb" : 1025.7,
    "Ly3" : 972.5,
    "CIII" : 977.0,
    "NIII" : 989.9,
    "Ly4" : 949.7
    }

c = speed_of_light/1000. # m/s -> km/s

# set the wave windows for SNR computation
redsnr_min = 1420
redsnr_max = 1480
bluesnr_min = 1040
bluesnr_max = 1205

def parse(options=None):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description="""compute snr values for lya mock data set""")

    parser.add_argument('--path', type = str, default = None, required = True,
                        help='path to mock directory containing spectra-16/, zcat, and bal-cat')
    
    parser.add_argument('--balmask', default = False, required = False, action='store_true',
                        help='should BALs be masked using AI_CIV?')
    
    parser.add_argument('-o', '--out', type=str, default = None, required = True,
                        help='output path for SNR catalog, e.g. .../NAME.fits')

    if options is None:
        args  = parser.parse_args()
    else:
        args  = parser.parse_args(options)

    return args


def read_mock_catalog(mockpath, balmask):
    """
    read quasar catalog

    Arguments
    ---------
    mockpath (str) : path to mock data

    Returns
    -------
    table of relevant attributes 

    """
    
    qsocat = os.path.join(mockpath, 'zcat.fits')
    # read the following columns from qsocat
    cols = ['TARGETID', 'RA', 'DEC', 'Z']
    catalog = Table(fitsio.read(qsocat, ext=1, columns=cols))

    if balmask:
        try:
            # open bal catalog
            balcat = os.path.join(mockpath,'bal_cat.fits')
            cols = ['TARGETID', 'AI_CIV', 'NCIV_450', 'VMIN_CIV_450', 'VMAX_CIV_450']
            balcat = Table(fitsio.read(balcat, ext=1, columns=cols))


            # add columns to catalog
            ai = np.full(len(catalog), 0.)
            nciv = np.full(len(catalog), 0)
            vmin = np.full((len(catalog), balcat['VMIN_CIV_450'].shape[1]), -1.)
            vmax = np.full((len(catalog),balcat['VMIN_CIV_450'].shape[1]), -1.)

            for i,tid in enumerate(catalog['TARGETID']):
                if np.any(tid == balcat['TARGETID']):
                    match = balcat[balcat['TARGETID'] == tid]
                    ai[i] = match['AI_CIV']
                    nciv[i] = match['NCIV_450']
                    vmin[i] = match['VMIN_CIV_450']
                    vmax[i] = match['VMAX_CIV_450']

            catalog.add_columns([ai, nciv, vmin, vmax], names=['AI_CIV', 'NCIV_450', 'VMIN_CIV_450', 'VMAX_CIV_450'])

        except:
            exit(1)
    
    return( catalog )

def getsnr(specfile, catalog):

    if os.path.exists(specfile):

        # open spectra file fibermap only
        fm = desispec.io.read_fibermap(specfile)

        # pare catalog to match spectra file fibermap
        tidmask = np.in1d(catalog['TARGETID'], fm['TARGETID'])
        scat = catalog[tidmask]
        
        if len(scat) < 1:
            # no objects
            return()

        specobj = desispec.io.read_spectra(specfile, targetids=scat['TARGETID'], skip_hdus=['EXP_FIBERMAP', 'SCORES', 'EXTRA_CATALOG'])
        truthfile = specfile.replace('spectra-16-', 'truth-16-')
        specobj.resolution_data = {}
        for cam in ['b', 'r', 'z']:
            tres = fitsio.read(truthfile, ext=f'{cam}_RESOLUTION')
            tresdata = np.empty([specobj.flux[cam].shape[0], tres.shape[0], specobj.flux[cam].shape[1]], dtype=float)
            for i in range(specobj.flux[cam].shape[0]):
                tresdata[i] = tres
            specobj.resolution_data[cam] = tresdata
        specobj = resample_spectra_lin_or_log(specobj, linear_step=0.8, wave_min = np.min(specobj.wave['b']),
                                              wave_max = np.max(specobj.wave['z']), fast = True)

        wave = specobj.wave['brz']

        tidlist = []
        redsnrlist, bluesnrlist = [], []

        for entry in range(len(scat)):

            tid = scat['TARGETID'][entry]
            zqso = scat['Z'][entry]
            idx = np.nonzero(specobj.fibermap['TARGETID']==tid)[0][0]

            flux = specobj.flux['brz'][idx]
            ivar = specobj.ivar['brz'][idx]
            wave_rf = wave / (1 + zqso)
               
            # apply mask to BAL features, if available
            if 'NCIV_450' in scat.columns:
                nbal = scat['NCIV_450'][entry]
                bal_locs = []
                for n in range(nbal):
                    # Compute velocity ranges
                    v_max = -scat[entry]['VMAX_CIV_450'][n] / c + 1.
                    v_min = -scat[entry]['VMIN_CIV_450'][n] / c + 1.

                    for line, lam in bal_lines.items():
                        # Mask wavelengths within the velocity ranges
                        mask = np.logical_and(wave_rf > lam * v_max,
                                              wave_rf < lam * v_min)

                        # Update ivar = 0
                        ivar[mask] = 0

            # average signal to noise computation
            mask = np.logical_and(ivar != 0, np.ma.masked_inside(wave_rf, bluesnr_min, bluesnr_max).mask)
            if np.sum(mask) > 0:
                bluesnr = np.mean((flux[mask]*np.sqrt(ivar[mask])))
            else:
                bluesnr = np.nan
                
            mask = np.logical_and(ivar != 0, np.ma.masked_inside(wave_rf, redsnr_min, redsnr_max).mask)
            if np.sum(mask) > 0:
                redsnr = np.mean((flux[mask]*np.sqrt(ivar[mask])))
            else:
                redsnr = np.nan
                
            bluesnrlist.append(bluesnr)
            redsnrlist.append(redsnr)
            tidlist.append(tid)

        t = Table(data =(tidlist, bluesnrlist, redsnrlist), names=['TARGETID', 'SNR_FOREST', 'SNR_REDSIDE'], dtype=('int', 'float64', 'float64'))

        return(t)
    
def _getsnr(arguments):
    return( getsnr(**arguments) )

def main(args=None):

    if isinstance(args, (list, tuple, type(None))):
        args = parse(args)
        
    tini = time.time()

    # read mock catalog 
    catalog = read_mock_catalog(args.path, args.balmask)

    # find path to all spectra files
    datapath = f'{args.path}/spectra-16'

    speclist = []
    for level1 in os.listdir(f'{datapath}'):
        for level2 in os.listdir(f'{datapath}/{level1}'):
            if os.path.exists(f'{datapath}/{level1}/{level2}/spectra-16-{level2}.fits'):
                speclist.append(f'{datapath}/{level1}/{level2}/spectra-16-{level2}.fits')

    arguments = [ {"specfile": specfile , \
                   "catalog": catalog, \
                   } for ih,specfile in enumerate(speclist) ]

    with mp.Pool() as pool:
        tid_snr = pool.map(_getsnr, arguments)

    # removes empty entries
    results = vstack(tid_snr)
    if 'col0' in results.columns:
        results.remove_column('col0')

    results.meta['EXTNAME'] = 'SNRCAT'

    results.write(args.out, overwrite=True)
    
    tfin = time.time()
    total_time = tfin-tini

    print(f'total run time: {np.round(total_time/60,1)} minutes')

if __name__ == "__main__":
    main()
