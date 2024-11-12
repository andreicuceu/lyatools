import fitsio
import numpy as np
import scipy.linalg
from picca.utils import smooth_cov, compute_cov


def get_shuffled_correlations(files, headers_to_check_match_values):
    xi_shuffled_list = []
    for file in files:
        with fitsio.FITS(file) as hdul:
            for entry, value in headers_to_check_match_values.items():
                head = hdul[1].read_header()
                assert head[entry] == value

            xi_shuffled = hdul[2]['DA'][:]
            weight_shuffled = hdul[2]['WE'][:]
            xi_shuffled = (xi_shuffled * weight_shuffled).sum(axis=1)
            weight_shuffled = weight_shuffled.sum(axis=1)
            w = weight_shuffled > 0.
            xi_shuffled[w] /= weight_shuffled[w]
            xi_shuffled_list.append(xi_shuffled)

    xi_shuffled = np.hstack(xi_shuffled_list)
    return xi_shuffled[:, None]


def stack_export_correlations(
        input_files, output_file, smooth_cov_flag=True, dmat_path=None, shuffled_correlations=None):
    """Stacks correlation functions measured in different mocks.
    Parameters
    ----------
    input_files : list
        List of the paths to the input correlations to be stacked.
    output_file : string
        Path to the output file.
    dmat_path : string
        Path to distortion matrix file, by default None
    """
    # specify which header entries we want to check are consistent across all files being stacked
    headers_to_check_match = ['NP', 'NT', 'OMEGAM', 'OMEGAR', 'OMEGAK', 'WL', 'NSIDE']

    # initialize stack arrays, fill them with zeros
    with fitsio.FITS(input_files[0]) as hdul:
        r_par = hdul[1]['RP'][:] * 0
        r_trans = hdul[1]['RT'][:] * 0
        num_pairs = hdul[1]['NB'][:] * 0
        z = hdul[1]['Z'][:] * 0
        weights_total = r_par * 0

        # get values of header entries to check with other files
        header = hdul[1].read_header()
        headers_to_check_match_values = {h: header[h] for h in headers_to_check_match}

    xi_shuffled = None
    if shuffled_correlations is not None:
        assert len(shuffled_correlations) == len(input_files)
        xi_shuffled = get_shuffled_correlations(shuffled_correlations,
                                                headers_to_check_match_values)

    # initialise header quantities
    r_par_min = 1.e6
    r_par_max = -1.e6
    r_trans_max = -1.e6
    z_cut_min = 1.e6
    z_cut_max = -1.e6

    xi = []
    weights = []
    healpixs = []

    for file in input_files:
        # Open the file and read the header
        print("coadding file {}".format(file))
        hdul = fitsio.FITS(file)
        header = hdul[1].read_header()

        # Check that the header properties match those from the first file
        for entry in headers_to_check_match:
            assert header[entry] == headers_to_check_match_values[entry]

        # Add weighted contributions from this file to stack variables
        weights_aux = hdul[2]['WE'][:]
        weights_total_aux = weights_aux.sum(axis=0)
        r_par += hdul[1]['RP'][:] * weights_total_aux
        r_trans += hdul[1]['RT'][:] * weights_total_aux
        z += hdul[1]['Z'][:] * weights_total_aux
        num_pairs += hdul[1]['NB'][:]
        weights_total += weights_total_aux

        # Update values to go in stack header
        r_par_min = np.min([r_par_min, header['RPMIN']])
        r_par_max = np.max([r_par_max, header['RPMAX']])
        r_trans_max = np.max([r_trans_max, header['RTMAX']])
        z_cut_min = np.min([z_cut_min, header['ZCUTMIN']])
        z_cut_max = np.max([z_cut_max, header['ZCUTMAX']])

        # Add the correlations to the stack
        xi.append(hdul[2]["DA"][:])
        weights.append(weights_aux)
        healpixs.append(hdul[2]["HEALPID"][:])

        hdul.close()

    xi = np.vstack(xi)
    weights = np.vstack(weights)
    healpixs = np.hstack(healpixs)

    if xi_shuffled is not None:
        xi -= xi_shuffled

    # normalize all other quantities by total weights
    w = weights_total > 0
    r_par[w] /= weights_total[w]
    r_trans[w] /= weights_total[w]
    z[w] /= weights_total[w]

    delta_r_par = (r_par_max - r_par_min) / headers_to_check_match_values['NP']
    delta_r_trans = (r_trans_max - 0.) / headers_to_check_match_values['NT']

    if smooth_cov_flag:
        print("INFO: The covariance will be smoothed")
        covariance = smooth_cov(
            xi, weights, r_par, r_trans, delta_r_trans=delta_r_trans, delta_r_par=delta_r_par)
    else:
        covariance = compute_cov(xi, weights)

    xi = (xi * weights).sum(axis=0)
    weights = weights.sum(axis=0)
    w = weights > 0
    xi[w] /= weights[w]

    try:
        scipy.linalg.cholesky(covariance)
    except scipy.linalg.LinAlgError:
        print("WARNING: Matrix is not positive definite")

    if dmat_path is not None:
        hdul = fitsio.FITS(dmat_path)
        dmat = hdul[1]['DM'][:]

        try:
            r_par_dmat = hdul[2]['RP'][:]
            r_trans_dmat = hdul[2]['RT'][:]
            z_dmat = hdul[2]['Z'][:]
        except IOError:
            r_par_dmat = r_par.copy()
            r_trans_dmat = r_trans.copy()
            z_dmat = z.copy()
        if dmat.shape == (xi.size, xi.size):
            r_par_dmat = r_par.copy()
            r_trans_dmat = r_trans.copy()
            z_dmat = z.copy()
        hdul.close()
    else:
        dmat = np.eye(len(xi))
        r_par_dmat = r_par.copy()
        r_trans_dmat = r_trans.copy()
        z_dmat = z.copy()

    results = fitsio.FITS(output_file, 'rw', clobber=True)
    header = [{
        'name': "BLINDING",
        'value': 'none',
        'comment': 'String specifying the blinding strategy'
    }, {
        'name': 'RPMIN',
        'value': r_par_min,
        'comment': 'Minimum r-parallel'
    }, {
        'name': 'RPMAX',
        'value': r_par_max,
        'comment': 'Maximum r-parallel'
    }, {
        'name': 'RTMAX',
        'value': r_trans_max,
        'comment': 'Maximum r-transverse'
    }, {
        'name': 'NP',
        'value': headers_to_check_match_values['NP'],
        'comment': 'Number of bins in r-parallel'
    }, {
        'name': 'NT',
        'value': headers_to_check_match_values['NT'],
        'comment': 'Number of bins in r-transverse'
    }, {
        'name': 'OMEGAM',
        'value': headers_to_check_match_values['OMEGAM'],
        'comment': 'Omega_matter(z=0) of fiducial LambdaCDM cosmology'
    }, {
        'name': 'OMEGAR',
        'value': headers_to_check_match_values['OMEGAR'],
        'comment': 'Omega_radiation(z=0) of fiducial LambdaCDM cosmology'
    }, {
        'name': 'OMEGAK',
        'value': headers_to_check_match_values['OMEGAK'],
        'comment': 'Omega_k(z=0) of fiducial LambdaCDM cosmology'
    }, {
        'name': 'WL',
        'value': headers_to_check_match_values['WL'],
        'comment': 'Equation of state of dark energy of fiducial LambdaCDM cosmology'
    }]
    comment = [
        'R-parallel', 'R-transverse', 'Redshift', 'Correlation',
        'Covariance matrix', 'Distortion matrix', 'Number of pairs'
    ]
    results.write([xi, r_par, r_trans, z, covariance, dmat, num_pairs],
                  names=['DA', 'RP', 'RT', 'Z', 'CO', 'DM', 'NB'],
                  comment=comment,
                  header=header,
                  extname='COR')
    comment = ['R-parallel model', 'R-transverse model', 'Redshift model']
    results.write([r_par_dmat, r_trans_dmat, z_dmat],
                  names=['DMRP', 'DMRT', 'DMZ'],
                  comment=comment,
                  extname='DMATTRI')
    results.close()
