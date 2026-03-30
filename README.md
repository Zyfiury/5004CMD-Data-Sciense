# 5004CMD — BTS mobility analysis

Coursework repository: Word report, analysis script, datasets, timings, and figures.

**Repository:** [github.com/Zyfiury/5004CMD-Data-Sciense](https://github.com/Zyfiury/5004CMD-Data-Sciense)

## Contents

| File / folder | Description |
|---------------|-------------|
| `5004CMD_Project_Report.docx` | Final report (APA 7 in-text citations and reference list). |
| `analysis.py` | Serial Pandas and parallel Dask workflow, plots, regression, timings. |
| `Trips_by_Distance.csv` | Distance-bin trips (large file — **Git LFS**). |
| `Trips_Full Data.csv` | Full-data distance columns. |
| `timing_results.csv` | Serial vs parallel runtimes. |
| `figures/` | Six PNG figures from the script. |

## Setup

```bash
pip install pandas numpy matplotlib scikit-learn dask distributed pyarrow
```

## Run

```bash
python analysis.py
```

On Windows, if Matplotlib errors:

```powershell
$env:MPLBACKEND = "Agg"; python analysis.py
```

## Clone (LFS)

```bash
git clone https://github.com/Zyfiury/5004CMD-Data-Sciense.git
cd 5004CMD-Data-Sciense
git lfs pull
```

Export an updated PDF from Word (**File → Save As → PDF**) if you need a `5004CMD_Project_Report.pdf` copy locally; it is not stored in this repository.
