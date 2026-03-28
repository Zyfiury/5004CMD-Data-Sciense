# 5004CMD — BTS mobility analysis

**Module:** 5004CMD Data Science  

**Repository:** [https://github.com/Zyfiury/5004CMD-Data-Science](https://github.com/Zyfiury/5004CMD-Data-Science)

This repository contains the coursework **PDF report** (`5004CMD_Project_Report.pdf`), analysis script, datasets, timing output, and figure PNGs.

## Contents

| Item | Description |
|------|-------------|
| `5004CMD_Project_Report.pdf` | Final coursework report (includes repository link in the Introduction). |
| `analysis.py` | Loads data, cleans National rows, plots figures, threshold analysis, regression, serial vs Dask timings. |
| `Trips_by_Distance.csv` | Distance-bin trip counts (Git LFS — large file). |
| `Trips_Full Data.csv` | Full-data distance columns and related fields. |
| `timing_results.csv` | Serial and parallel runtimes. |
| `figures/` | Six PNG charts produced by the script. |

## Requirements

```bash
pip install pandas numpy matplotlib scikit-learn dask distributed pyarrow
```

## Run

From the repository root:

```bash
python analysis.py
```

If Matplotlib shows threading errors on Windows:

```powershell
$env:MPLBACKEND = "Agg"
python analysis.py
```

## Clone (large CSV)

Install [Git LFS](https://git-lfs.github.com/), then:

```bash
git clone https://github.com/Zyfiury/5004CMD-Data-Science.git
cd 5004CMD-Data-Science
git lfs pull
```

If the remote still uses the previous repository name, use the URL shown on your GitHub **Code** button.
