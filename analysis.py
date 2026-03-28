import os
import time
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import dask.dataframe as dd
from distributed import Client, LocalCluster
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

TRIPS_DISTANCE_FILE = "Trips_by_Distance.csv"
TRIPS_FULL_FILE = "Trips_Full Data.csv"
FIG_DIR = "figures"

os.makedirs(FIG_DIR, exist_ok=True)

DISTANCE_COLS_FULL = [
    "Trips <1 Mile",
    "Trips 1-3 Miles",
    "Trips 3-5 Miles",
    "Trips 5-10 Miles",
    "Trips 10-25 Miles",
    "Trips 25-50 Miles",
    "Trips 50-100 Miles",
    "Trips 100-250 Miles",
    "Trips 250-500 Miles",
    "Trips 500+ Miles",
]


def load_pandas_data():
    df_distance = pd.read_csv(TRIPS_DISTANCE_FILE)
    df_full = pd.read_csv(TRIPS_FULL_FILE)
    df_distance["Date"] = pd.to_datetime(df_distance["Date"])
    df_full["Date"] = pd.to_datetime(df_full["Date"])
    return df_distance, df_full


def load_dask_data():
    # Explicit dtypes avoid Dask inferring float64 from first CSV chunk then failing on later string rows.
    ddf_distance = dd.read_csv(
        TRIPS_DISTANCE_FILE,
        assume_missing=True,
        dtype={"County Name": "object", "State Postal Code": "object"},
    )
    ddf_full = dd.read_csv(TRIPS_FULL_FILE, assume_missing=True)
    ddf_distance["Date"] = dd.to_datetime(ddf_distance["Date"])
    ddf_full["Date"] = dd.to_datetime(ddf_full["Date"])
    return ddf_distance, ddf_full


def clean_distance_df(df):
    df = df.copy()
    df = df[df["Level"] == "National"]
    drop_cols = ["State FIPS", "State Postal Code", "County FIPS", "County Name", "Row ID"]
    existing = [c for c in drop_cols if c in df.columns]
    df = df.drop(columns=existing)
    df = df.drop_duplicates()
    return df


def clean_full_df(df):
    df = df.copy()
    df = df[df["Level"] == "National"]
    df = df.drop_duplicates()
    return df


def question1_serial(df_distance, df_full):
    distance_national = clean_distance_df(df_distance)
    full_national = clean_full_df(df_full)

    weekly_home = (
        distance_national.groupby("Week")[["Population Staying at Home", "Population Not Staying at Home"]]
        .mean()
        .reset_index()
        .sort_values("Week")
    )

    plt.figure(figsize=(10, 5))
    plt.bar(weekly_home["Week"], weekly_home["Population Staying at Home"])
    plt.xlabel("Week")
    plt.ylabel("Average population staying at home")
    plt.title("Average Population Staying at Home per Week")
    plt.tight_layout()
    plt.savefig(f"{FIG_DIR}/fig1_weekly_home.png", dpi=300)
    plt.close()

    plt.figure(figsize=(10, 5))
    plt.bar(weekly_home["Week"], weekly_home["Population Not Staying at Home"])
    plt.xlabel("Week")
    plt.ylabel("Average population not staying at home")
    plt.title("Average Population Not Staying at Home per Week")
    plt.tight_layout()
    plt.savefig(f"{FIG_DIR}/fig2_weekly_not_home.png", dpi=300)
    plt.close()

    distance_means = full_national[DISTANCE_COLS_FULL].mean().sort_values(ascending=False)

    plt.figure(figsize=(12, 6))
    plt.bar(distance_means.index, distance_means.values)
    plt.xticks(rotation=45, ha="right")
    plt.xlabel("Distance category")
    plt.ylabel("Average trips")
    plt.title("Average Number of Trips by Distance Category")
    plt.tight_layout()
    plt.savefig(f"{FIG_DIR}/fig3_distance_means.png", dpi=300)
    plt.close()

    return weekly_home, distance_means


def question2_serial(df_distance):
    distance_national = clean_distance_df(df_distance)

    set1 = distance_national[distance_national["Number of Trips 10-25"] > 10_000_000]
    set2 = distance_national[distance_national["Number of Trips 50-100"] > 10_000_000]
    set3 = distance_national[
        (distance_national["Number of Trips 10-25"] > 10_000_000) &
        (distance_national["Number of Trips 50-100"] > 10_000_000)
    ]

    counts = {
        "10_25_over_10m": len(set1),
        "50_100_over_10m": len(set2),
        "both_over_10m": len(set3),
    }

    plt.figure(figsize=(12, 6))
    plt.scatter(set1["Date"], set1["Number of Trips 10-25"], label="10-25 trips > 10m", alpha=0.7)
    plt.scatter(set2["Date"], set2["Number of Trips 50-100"], label="50-100 trips > 10m", alpha=0.7)
    plt.xlabel("Date")
    plt.ylabel("Number of trips")
    plt.title("Dates with More Than 10 Million Trips")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{FIG_DIR}/fig4_trip_threshold_scatter.png", dpi=300)
    plt.close()

    return set1[["Date", "Number of Trips 10-25"]], set2[["Date", "Number of Trips 50-100"]], counts


def question3_serial(df_distance, df_full):
    distance_national = clean_distance_df(df_distance)
    full_national = clean_full_df(df_full)

    start_date = full_national["Date"].min()
    end_date = full_national["Date"].max()

    distance_subset = distance_national[
        (distance_national["Date"] >= start_date) & (distance_national["Date"] <= end_date)
    ].copy()

    distance_subset = distance_subset.sort_values("Date")
    full_national = full_national.sort_values("Date")

    merged = pd.merge(
        full_national[["Date", "Trips 1-25 Miles"]],
        distance_subset[["Date", "Number of Trips 5-10"]],
        on="Date",
        how="inner",
    ).dropna()

    X = merged[["Trips 1-25 Miles"]]
    y = merged["Number of Trips 5-10"]

    model = LinearRegression()
    model.fit(X, y)
    y_pred = model.predict(X)

    rmse = math.sqrt(mean_squared_error(y, y_pred))
    r2 = r2_score(y, y_pred)

    plt.figure(figsize=(10, 6))
    plt.scatter(X, y, alpha=0.7, label="Actual")
    plt.plot(X, y_pred, label="Regression line")
    plt.xlabel("Trips 1-25 Miles")
    plt.ylabel("Number of Trips 5-10")
    plt.title("Linear Regression for Travel Frequency Prediction")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{FIG_DIR}/fig5_regression.png", dpi=300)
    plt.close()

    results = {
        "intercept": float(model.intercept_),
        "coefficient": float(model.coef_[0]),
        "rmse": float(rmse),
        "r2": float(r2),
        "rows_used": int(len(merged)),
    }
    return merged, results


def question4_serial(df_full):
    full_national = clean_full_df(df_full)

    distance_daily_means = full_national[DISTANCE_COLS_FULL].mean()

    plt.figure(figsize=(12, 6))
    plt.bar(distance_daily_means.index, distance_daily_means.values)
    plt.xticks(rotation=45, ha="right")
    plt.xlabel("Distance category")
    plt.ylabel("Average number of travellers")
    plt.title("Average Travellers by Distance-Trip Category")
    plt.tight_layout()
    plt.savefig(f"{FIG_DIR}/fig6_travellers_by_distance.png", dpi=300)
    plt.close()

    return distance_daily_means


def question1_dask(ddf_distance, ddf_full):
    ddf_distance = ddf_distance[ddf_distance["Level"] == "National"]
    ddf_full = ddf_full[ddf_full["Level"] == "National"]
    weekly_home = (
        ddf_distance.groupby("Week")[["Population Staying at Home", "Population Not Staying at Home"]]
        .mean()
        .compute()
    )
    distance_means = ddf_full[DISTANCE_COLS_FULL].mean().compute()
    return weekly_home, distance_means


def question2_dask(ddf_distance):
    ddf_distance = ddf_distance[ddf_distance["Level"] == "National"]
    set1 = ddf_distance[ddf_distance["Number of Trips 10-25"] > 10_000_000].compute()
    set2 = ddf_distance[ddf_distance["Number of Trips 50-100"] > 10_000_000].compute()
    set3 = ddf_distance[
        (ddf_distance["Number of Trips 10-25"] > 10_000_000) &
        (ddf_distance["Number of Trips 50-100"] > 10_000_000)
    ].compute()
    return set1, set2, set3


def question3_dask(ddf_distance, ddf_full):
    df_distance = ddf_distance.compute()
    df_full = ddf_full.compute()
    return question3_serial(df_distance, df_full)


def question4_dask(ddf_full):
    ddf_full = ddf_full[ddf_full["Level"] == "National"]
    return ddf_full[DISTANCE_COLS_FULL].mean().compute()


def run_serial_all():
    start = time.time()
    df_distance, df_full = load_pandas_data()
    weekly_home, distance_means = question1_serial(df_distance, df_full)
    set1, set2, counts = question2_serial(df_distance)
    merged, model_results = question3_serial(df_distance, df_full)
    travellers_by_distance = question4_serial(df_full)
    elapsed = time.time() - start
    return {
        "time_seconds": elapsed,
        "weekly_home": weekly_home,
        "distance_means": distance_means,
        "set1": set1,
        "set2": set2,
        "counts": counts,
        "model_results": model_results,
        "travellers_by_distance": travellers_by_distance,
    }


def run_parallel_all(n_workers):
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=1)
    client = Client(cluster)
    start = time.time()
    ddf_distance, ddf_full = load_dask_data()
    q1 = question1_dask(ddf_distance, ddf_full)
    q2 = question2_dask(ddf_distance)
    q3 = question3_dask(ddf_distance, ddf_full)
    q4 = question4_dask(ddf_full)
    elapsed = time.time() - start
    client.close()
    cluster.close()
    return {
        "workers": n_workers,
        "time_seconds": elapsed,
        "q1": q1,
        "q2": q2,
        "q3": q3,
        "q4": q4,
    }


if __name__ == "__main__":
    print("Running serial version...")
    serial_results = run_serial_all()
    print("Serial time:", serial_results["time_seconds"])

    print("Running parallel version with 10 workers...")
    parallel_10 = run_parallel_all(10)
    print("Parallel 10 time:", parallel_10["time_seconds"])

    print("Running parallel version with 20 workers...")
    parallel_20 = run_parallel_all(20)
    print("Parallel 20 time:", parallel_20["time_seconds"])

    timing_df = pd.DataFrame([
        {"Mode": "Serial", "Workers": 1, "Time (s)": serial_results["time_seconds"]},
        {"Mode": "Parallel", "Workers": 10, "Time (s)": parallel_10["time_seconds"]},
        {"Mode": "Parallel", "Workers": 20, "Time (s)": parallel_20["time_seconds"]},
    ])

    print("\nTiming comparison:")
    print(timing_df)

    print("\nQuestion 2 counts:")
    print(serial_results["counts"])

    print("\nModel results:")
    print(serial_results["model_results"])

    timing_df.to_csv("timing_results.csv", index=False)
