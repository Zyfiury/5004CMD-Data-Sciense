# BTS Mobility Data Analysis (5004CMD)

**Module:** 5004CMD Data Science  

Coursework project analysing Bureau of Transportation Statistics (BTS)–style national mobility extracts using **sequential Pandas** and **parallel Dask** workflows, with exploratory plots, threshold screening, linear regression, and runtime benchmarking.

---

## Datasets

| File | Description |
|------|-------------|
| **Trips_by_Distance.csv** | Daily records with `Level`, `Date`, `Week`, population staying at home / not staying at home, aggregate trip counts, and binned fields such as `Number of Trips 5-10`, `Number of Trips 10-25`, and related distance bands. |
| **Trips_Full Data.csv** | Daily national records with distance-stratified columns (`Trips <1 Mile` through `Trips 500+ Miles`), plus population and aggregate trip fields used for distance summaries and regression inputs. |

Both files are filtered to **National** level for consistent geography; dates are parsed as datetimes; duplicates are removed; redundant geographic columns are dropped where applicable before aggregation.

---

## Analysis workflow (`analysis.py`)

1. **Load** both CSVs (Pandas for serial; Dask for parallel).  
2. **Clean** to National rows, drop duplicates, and remove unnecessary location columns from the distance file.  
3. **Weekly aggregation** — group by `Week` and compute mean population staying at home and not staying at home; save bar charts (**Figure 1**, **Figure 2**).  
4. **Distance means** — column means of distance bands on National rows; save **Figure 3** and **Figure 6**.  
5. **Threshold analysis** — select days where `Number of Trips 10-25` or `Number of Trips 50-100` exceed **10,000,000**; scatter plot (**Figure 4**).  
6. **Regression** — inner merge on `Date`: predictor `Trips 1-25 Miles` (full data), response `Number of Trips 5-10` (distance file); fit `LinearRegression`; report **R²**, **RMSE**, coefficient, intercept; save **Figure 5**.  
7. **Timing** — record wall-clock time for full serial run vs Dask runs with **10** and **20** workers; write `timing_results.csv`.

Outputs: `figures/*.png`, `timing_results.csv`, and console summaries.

---

## Threshold analysis

A fixed cutoff of **ten million trips** highlights extremely high–volume national days for selected bins (`10–25` and `50–100` mile categories in the distance extract). This supports comparison of how often **medium-distance** movement reaches exceptional daily counts relative to a longer band, without fitting a full extreme-value model.

---

## Linear regression

The model relates **Trips 1-25 Miles** to **Number of Trips 5-10** on matched calendar dates. **Ordinary least squares** yields a slope and intercept; **R²** measures explained variance; **RMSE** measures typical absolute error in trip-count units. The fitted line and scatter are saved as **Figure 5**.

---

## Parallel processing: Pandas vs Dask

- **Pandas (serial):** single-process execution—low coordination overhead, suitable baseline.  
- **Dask (distributed):** local cluster with multiple workers—parallel CSV reads and partitioned operations; introduces **scheduler and communication overhead**, which can dominate for **moderate** data sizes.  

Reported timings (example coursework run) are written to `timing_results.csv` and discussed in the submitted report.

---

## Software requirements

- Python **3.10+** recommended  
- **pandas**, **numpy**, **matplotlib**, **scikit-learn**, **dask**, **distributed**  
- **pyarrow** (often required by recent Dask dataframe stacks)  
- Optional (to rebuild the Word report): **python-docx**

Install dependencies:

```bash
pip install pandas numpy matplotlib scikit-learn dask distributed pyarrow
```

Optional:

```bash
pip install python-docx
```

---

## Running the analysis

From the repository root (same directory as `analysis.py` and the CSV files):

```bash
python analysis.py
```

On Windows, if Matplotlib raises threading errors in some environments, use:

```powershell
$env:MPLBACKEND = "Agg"
python analysis.py
```

After a successful run you should see serial and parallel timings, regression metrics, and threshold counts in the terminal, with PNGs under `figures/` and `timing_results.csv` updated.

### Rebuilding the coursework Word report

If `python-docx` and Microsoft Word (for PDF export on Windows) are available:

```bash
python build_5004cmd_report.py
```

This regenerates `5004CMD_Project_Report.docx` and attempts to export `5004CMD_Project_Report.pdf`.

---

## Repository layout (submission-related)

| Item | Role |
|------|------|
| `analysis.py` | Main analytics and benchmarking script |
| `build_5004cmd_report.py` | Generates the structured Word/PDF report |
| `5004CMD_Project_Report.docx` | Submitted report (binary) |
| `figures/` | Six coursework figures (PNG) |
| `timing_results.csv` | Serial vs parallel timing summary |
| `pseudocode_appendix.txt` / `flowchart_appendix.txt` | Appendix source text |

`report_skeleton.docx` is intentionally **not** tracked (see `.gitignore`).

---

## Publishing this folder to GitHub

1. On GitHub, create a **public** repository named **`5004CMD-BTS-Mobility-Analysis`** (empty, no README).  
2. From this project directory (if Git is not yet initialised, run `git init` and commit as below), add the remote and push:

```bash
git branch -M main
git remote add origin https://github.com/<your-username>/5004CMD-BTS-Mobility-Analysis.git
git push -u origin main
```

Replace `<your-username>` with your GitHub username. After the first push, replace `INSERT_REPOSITORY_LINK_HERE` in `5004CMD_Project_Report.docx` (end of the Introduction) with your public repository URL, then save the document.

---

## Licence / academic use

This repository is provided for **academic coursework** submission. Dataset rights remain with their original publishers; use the mobility extracts only in line with your module and data-provider terms.
