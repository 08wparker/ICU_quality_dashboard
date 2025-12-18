import marimo

__generated_with = "0.10.12"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # SAT & SBT Analysis Pipeline

        This notebook loads CLIF data and runs the SAT (Spontaneous Awakening Trial) and SBT (Spontaneous Breathing Trial) SQL scripts to generate event-level and day-level outputs.

        ## Data Sources
        - **resp_p**: Waterfall-processed respiratory support (`output/intermediate/{site}_resp_processed_bf.parquet`)
        - **CLIF tables**: Raw tables from the data directory specified in `config/config.json`
        """
    )
    return


@app.cell
def _():
    import os
    import json
    import pandas as pd
    import duckdb
    from pathlib import Path

    # Change to project root
    os.chdir(Path(__file__).parent.parent)
    print(f"Working directory: {os.getcwd()}")
    return Path, duckdb, json, os, pd


@app.cell
def _(json, os):
    # Load configuration
    CONFIG_PATH = "config/config.json"

    with open(CONFIG_PATH) as f:
        config = json.load(f)

    SITE_NAME = config["site_name"].lower()
    DATA_DIR = config["data_directory"]
    FILETYPE = config["filetype"]
    TIMEZONE = config["timezone"]

    print(f"Site: {SITE_NAME}")
    print(f"Data directory: {DATA_DIR}")
    return CONFIG_PATH, DATA_DIR, FILETYPE, SITE_NAME, TIMEZONE, config


@app.cell
def _(Path):
    def run_query_from_file(sql_file_path: str, con=None) -> "duckdb.DuckDBPyRelation":
        """
        Loads a query from a .sql file and executes it using DuckDB.
        """
        import duckdb
        print(f"--- Loading and executing query from {sql_file_path} ---")
        query = Path(sql_file_path).read_text()
        if con is None:
            result = duckdb.sql(query)
        else:
            result = con.sql(query)
        print("Query executed successfully.")
        return result
    return (run_query_from_file,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Load Base Tables""")
    return


@app.cell
def _(DATA_DIR, SITE_NAME, pd):
    # Load resp_p (waterfall-processed respiratory support)
    resp_p_path = f"output/intermediate/{SITE_NAME}_resp_processed_bf.parquet"
    resp_p = pd.read_parquet(resp_p_path)
    resp_p['tracheostomy'] = resp_p['tracheostomy'].fillna(0).astype(int)
    print(f"Loaded resp_p: {len(resp_p):,} rows")
    resp_p.head()
    return resp_p, resp_p_path


@app.cell
def _(DATA_DIR, pd):
    # Load hospitalization table
    hosp_path = f"{DATA_DIR}/clif_hospitalization.parquet"
    hosp_df = pd.read_parquet(hosp_path)
    print(f"Loaded hosp_df: {len(hosp_df):,} rows")
    hosp_df.head()
    return hosp_df, hosp_path


@app.cell
def _(DATA_DIR, pd):
    # Load ADT table
    adt_path = f"{DATA_DIR}/clif_adt.parquet"
    adt_df = pd.read_parquet(adt_path)
    print(f"Loaded adt_df: {len(adt_df):,} rows")
    adt_df.head()
    return adt_df, adt_path


@app.cell
def _(DATA_DIR, pd):
    # Load code status table
    cs_path = f"{DATA_DIR}/clif_code_status.parquet"
    cs_df = pd.read_parquet(cs_path)
    print(f"Loaded cs_df: {len(cs_df):,} rows")
    cs_df.head()
    return cs_df, cs_path


@app.cell
def _(DATA_DIR, duckdb, pd):
    # Load vitals - get last vitals for each hospitalization (needed for SBT)
    vitals_path = f"{DATA_DIR}/clif_vitals.parquet"
    q = f"""
    FROM '{vitals_path}'
    SELECT hospitalization_id
        , MAX(recorded_dttm) AS recorded_dttm
    GROUP BY hospitalization_id
    """
    last_vitals_df = duckdb.sql(q).df()
    print(f"Loaded last_vitals_df: {len(last_vitals_df):,} rows")
    last_vitals_df.head()
    return last_vitals_df, q, vitals_path


@app.cell
def _(DATA_DIR, duckdb, pd):
    # Load patient assessments - filter to RASS only
    assessments_path = f"{DATA_DIR}/clif_patient_assessments.parquet"
    q_rass = f"""
    FROM '{assessments_path}'
    WHERE LOWER(assessment_category) = 'rass'
    SELECT hospitalization_id, recorded_dttm
        , rass: assessment_value::FLOAT
    """
    rass_df = duckdb.sql(q_rass).df()
    print(f"Loaded rass_df: {len(rass_df):,} rows")
    rass_df.head()
    return assessments_path, q_rass, rass_df


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Load and Pivot Medications (Wide Format)""")
    return


@app.cell
def _(DATA_DIR, duckdb, pd):
    # Load continuous medications and pivot to wide format
    meds_path = f"{DATA_DIR}/clif_medication_admin_continuous.parquet"

    # Define the medications we need for SAT
    sedation_meds = ['fentanyl', 'propofol', 'lorazepam', 'midazolam', 'hydromorphone', 'morphine']
    paralytic_meds = ['cisatracurium', 'vecuronium', 'rocuronium']
    all_meds = sedation_meds + paralytic_meds

    q_meds = f"""
    WITH filtered AS (
        FROM '{meds_path}'
        WHERE LOWER(med_category) IN ({', '.join([f"'{m}'" for m in all_meds])})
        SELECT hospitalization_id
            , admin_dttm AS recorded_dttm
            , LOWER(med_category) AS med_category
            , med_dose
    )
    PIVOT filtered
    ON med_category
    USING MAX(med_dose)
    ORDER BY hospitalization_id, recorded_dttm
    """
    meds_df = duckdb.sql(q_meds).df()
    print(f"Loaded meds_df (pivoted wide): {len(meds_df):,} rows")
    print(f"Columns: {list(meds_df.columns)}")
    meds_df.head()
    return all_meds, meds_df, meds_path, paralytic_meds, q_meds, sedation_meds


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Run SBT SQL Script

        The SBT script requires:
        - `resp_p`: Waterfall-processed respiratory support
        - `cs_df`: Code status table
        - `hosp_df`: Hospitalization table
        - `last_vitals_df`: Last vitals timestamp per hospitalization
        """
    )
    return


@app.cell
def _(cs_df, duckdb, hosp_df, last_vitals_df, resp_p, run_query_from_file):
    # Register tables for SBT query
    duckdb.register("resp_p", resp_p)
    duckdb.register("cs_df", cs_df)
    duckdb.register("hosp_df", hosp_df)
    duckdb.register("last_vitals_df", last_vitals_df)

    # Run SBT SQL
    sbt_result = run_query_from_file("code/sbt.sql")
    sbt_events = sbt_result.df()
    print(f"SBT events: {len(sbt_events):,} rows")
    sbt_events.head()
    return sbt_events, sbt_result


@app.cell
def _(duckdb, sbt_events):
    # Aggregate SBT to day level
    q_sbt_daily = """
    FROM sbt_events
    SELECT hospitalization_id
        , event_dttm::DATE AS event_date
        , CONCAT(hospitalization_id, '_', event_dttm::DATE) AS hosp_id_day_key
        , MAX(sbt_done) AS sbt_done
        , MAX(_extub_1st) AS extub_1st
        , MAX(_success_extub) AS success_extub
        , MAX(_trach_1st) AS trach_1st
        , MAX(_fail_extub) AS fail_extub
        , MIN(CASE WHEN sbt_done = 1 THEN event_dttm END) AS sbt_first_dttm
    GROUP BY hospitalization_id, event_date, hosp_id_day_key
    ORDER BY hospitalization_id, event_date
    """
    sbt_days = duckdb.sql(q_sbt_daily).df()
    print(f"SBT days: {len(sbt_days):,} rows")
    sbt_days.head()
    return q_sbt_daily, sbt_days


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Run SAT SQL Script

        The SAT script requires:
        - `resp_df`: Respiratory support (using resp_p)
        - `meds_df`: Pivoted wide medications table
        - `rass_df`: RASS assessments
        - `adt_df`: ADT with location_category
        """
    )
    return


@app.cell
def _(adt_df, duckdb, meds_df, rass_df, resp_p, run_query_from_file):
    # Register tables for SAT query
    duckdb.register("resp_df", resp_p)
    duckdb.register("meds_df", meds_df)
    duckdb.register("rass_df", rass_df)
    duckdb.register("adt_df", adt_df)

    # Run SAT SQL
    sat_result = run_query_from_file("code/sat.sql")
    sat_days = sat_result.df()
    print(f"SAT days (eligible): {len(sat_days):,} rows")
    sat_days.head()
    return sat_days, sat_result


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Summary Statistics""")
    return


@app.cell
def _(duckdb, sat_days, sbt_days):
    # Join SAT and SBT day-level results
    q_merged = """
    FROM sbt_days sbt
    LEFT JOIN sat_days sat USING (hosp_id_day_key)
    SELECT sbt.*
        , sat.sat_eligible
        , sat.SAT_EHR_delivery
        , sat.SAT_modified_delivery
        , sat.SAT_rass_nonneg_30
        , sat.SAT_med_halved_rass_pos
        , sat.SAT_no_meds_rass_pos_45
        , sat.SAT_rass_first_neg_30_last45_nonneg
    ORDER BY sbt.hospitalization_id, sbt.event_date
    """
    merged_days = duckdb.sql(q_merged).df()
    print(f"Merged day-level data: {len(merged_days):,} rows")
    merged_days.head()
    return merged_days, q_merged


@app.cell
def _(merged_days, mo):
    # Summary statistics
    stats = {
        "Total patient-days": len(merged_days),
        "Unique hospitalizations": merged_days["hospitalization_id"].nunique(),
        "Days with SBT done": merged_days["sbt_done"].sum(),
        "SBT rate (%)": round(merged_days["sbt_done"].mean() * 100, 1),
        "Days SAT eligible": merged_days["sat_eligible"].sum() if "sat_eligible" in merged_days.columns else "N/A",
        "SAT_EHR_delivery": merged_days["SAT_EHR_delivery"].sum() if "SAT_EHR_delivery" in merged_days.columns else "N/A",
        "SAT_modified_delivery": merged_days["SAT_modified_delivery"].sum() if "SAT_modified_delivery" in merged_days.columns else "N/A",
        "Successful extubations": merged_days["success_extub"].sum(),
    }

    mo.md(f"""
    ### Summary

    | Metric | Value |
    |--------|-------|
    {"".join([f"| {k} | {v} |" + chr(10) for k, v in stats.items()])}
    """)
    return (stats,)


@app.cell
def _(SITE_NAME, merged_days, os, sbt_events, sat_days):
    # Save outputs
    os.makedirs("output/intermediate", exist_ok=True)

    sbt_events.to_parquet(f"output/intermediate/{SITE_NAME}_sbt_events.parquet", index=False)
    sat_days.to_parquet(f"output/intermediate/{SITE_NAME}_sat_days.parquet", index=False)
    merged_days.to_parquet(f"output/intermediate/{SITE_NAME}_sat_sbt_merged_days.parquet", index=False)

    print(f"Saved outputs to output/intermediate/")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Data Quality Checks""")
    return


@app.cell
def _(duckdb, merged_days):
    # Check SAT flag distributions
    q_sat_flags = """
    SELECT
        COUNT(*) AS total_days,
        SUM(CASE WHEN sat_eligible = 1 THEN 1 ELSE 0 END) AS sat_eligible_days,
        SUM(CASE WHEN SAT_EHR_delivery = 1 THEN 1 ELSE 0 END) AS sat_ehr_delivery,
        SUM(CASE WHEN SAT_modified_delivery = 1 THEN 1 ELSE 0 END) AS sat_modified_delivery,
        SUM(CASE WHEN SAT_rass_nonneg_30 = 1 THEN 1 ELSE 0 END) AS sat_rass_nonneg_30,
        SUM(CASE WHEN SAT_med_halved_rass_pos = 1 THEN 1 ELSE 0 END) AS sat_med_halved_rass_pos,
        SUM(CASE WHEN SAT_no_meds_rass_pos_45 = 1 THEN 1 ELSE 0 END) AS sat_no_meds_rass_pos_45,
        SUM(CASE WHEN SAT_rass_first_neg_30_last45_nonneg = 1 THEN 1 ELSE 0 END) AS sat_rass_transition
    FROM merged_days
    """
    sat_summary = duckdb.sql(q_sat_flags).df()
    sat_summary.T
    return q_sat_flags, sat_summary


@app.cell
def _(duckdb, merged_days):
    # Check SBT and extubation outcomes
    q_sbt_outcomes = """
    SELECT
        COUNT(*) AS total_days,
        SUM(sbt_done) AS sbt_done_days,
        SUM(extub_1st) AS first_extubations,
        SUM(success_extub) AS successful_extubations,
        SUM(fail_extub) AS failed_extubations,
        SUM(trach_1st) AS tracheostomies
    FROM merged_days
    """
    sbt_summary = duckdb.sql(q_sbt_outcomes).df()
    sbt_summary.T
    return q_sbt_outcomes, sbt_summary


if __name__ == "__main__":
    app.run()
