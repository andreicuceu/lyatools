# The seed catalog selects the targets and their redshifts, and quickquasars always writes the zbest files
# (desisim with the quickquasars cleanup: --zbest, --bbflux and --zmin no longer exist).
QQ_DEFAULTS = [
    '--save-continuum',
]


# Digits of the qq_run_type (e.g. jura-124): 1 DLAs from the transmission files with the higher Lyman lines (DLA+),
# 2 metals, 3 metals from the files, 4 BALs, 6 no Lya absorption (with 1 and/or 2: DLA-only, metal-only or both).
QQ_RUN_CODES = {
    '1': '--dla file',
    '2': '--metals LYB LY3 LY4 LY5 SiII(1260) SiIII(1207) SiII(1193) SiII(1190)',
    '3': '--metals-from-file all',
    '4': '--balprob 0.16',
    '6': '--no-lya-transmission',
}


QQ_DEFAULT_METAL_STRENGTHS = {
    'lyacolore': '0.1901 0.0697 0.0335 0.0187 1.3e-03 3.5e-03 0.7e-03 1.4e-03',
    'saclay': '0.1901 0.0697 0.0335 0.0187 5.7e-04 1.6e-03 5.3e-04 6.8e-04'
}
