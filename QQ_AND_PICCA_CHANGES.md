# lyatools: faster quickquasars jobs and optional picca `--gpu` (branch `gpu-port`)

These changes go with the `gpu-port` branches of desisim (`QUICKQUASARS_CHANGES.md`) and picca (`GPU_PORT.md`).
None of them changes the content of the spectra or of the correlations. Some change how the generated job
scripts run (marked "default changed" below); every one can be set back from the ini file.

## quickquasars job script

| Setting in `[quickquasars]` | Default | What it does |
|---|---|---|
| `cpus_per_task` | 256 (**default changed**; the script used `-c <nproc>`) | CPUs of the `srun` step of each node. On Perlmutter CPU nodes `-c` counts logical CPUs (2 per core), so `-c 48` with 48 workers left them 24 physical cores. Measured on one CPU node with the same 32 healpixes and 32 workers: `-c 32` 172 s, `-c 256` 62 s. |
| `omp_threads` | 1 (**default changed**; it was `nproc`) | `OMP_NUM_THREADS` of the job. Each worker is a single process; 48 threads each made the workers about 60% larger (8.8 vs 5.4 GB in a pool of 8) with no change in speed or output. |
| `only_pixels_with_quasars` | True (**default changed**) | Runs only the skewer files whose healpixel holds quasars of the input catalogue (1152 of 3072 for mock-15; the others produce no output). Any failure of the check keeps all files. |
| `mem_per_worker` | not set | Passes `--mem-per-worker` (lowers `--nproc` to what the job's memory fits). |
| `fiber_chunk` | not set | Passes `--fiber-chunk` (needs the desisim branch; same spectra, about half the memory per worker). |

Also **default changed**: the skewer files are dealt out to the nodes round-robin (every `nodes`-th file) instead
of in contiguous slices. The healpixes with quasars are not spread evenly over the sorted list, so contiguous
slices gave the nodes 63k to 413k quasars (mock-15); round-robin gives 299k to 308k.

## picca jobs

`gpu = True` in `[picca_corr]` adds `--gpu` to `picca_cf`, `picca_xcf`, `picca_dmat`, `picca_xdmat` (and
`picca_metal_dmat`) in the generated scripts. Needs the picca `gpu-port` branch and a GPU node.

## numpy 2.5 fixes in `scripts/`

- `make_snr_cat.py`: the BAL catalogue merge compared every quasar with the whole BAL catalogue and assigned a
  one-row column to a scalar, which is an error in numpy 2.5 (a bare `except` then exited silently with
  status 1). It is now a sorted search (same values on a 20,000-quasar sample; 143 s instead of 494 s on a full mock).
- `make_snr_cat.py`, `stack_full_covariance.py`: `np.in1d` (removed in numpy 2.5) became `np.isin`.

## What was checked

- Generated scripts for a 1024^3 mock run through `lyatools-run` (also submitted for real with `sbatch` on the
  debug queue: the seed catalogue, quickquasars on 2 nodes, the QSO/BAL, SNR and DLA catalogues and the redshift
  errors, with their dependencies): the spectra are identical to those of the same files run by hand.
- A whole mock (1151 healpixes, 1.21M quasars) on one CPU node: 3934 s with the stock script, 752 s with these
  settings plus the desisim branch.

## Commits

`67a44c6` round-robin split; `cbdff24` `gpu`; `5d4e223` only pixels with quasars; `70717cf` `mem_per_worker`;
`e8b425d` `cpus_per_task`; `49f7285` `fiber_chunk`; `4bc88b9`, `1973751` `omp_threads` and its default;
`b4573c4`, `80206e7` numpy 2.5 fixes.
