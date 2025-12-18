import marimo

__generated_with = "0.17.6"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # ICU Quality Dashboard Backend

    This notebook calculates 3 metrics for the ICU quality dashboard:

    1. **SAT** - Spontaneous Awakening Trial metrics

    2. **SBT** - Spontaneous Breathing Trial metrics

    3. **LPV** - Low Tidal Volume Ventilation metrics

    ## Cohort Definitions

    - **SAT cohort**: Patients on IMV in ICU at 7 AM

    - **SBT/LPV cohort**: Patients on controlled-mode IMV in ICU at 7 AM
    """)
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
    return duckdb, json, os, pd


@app.cell
def _(json):
    # Load configuration
    CONFIG_PATH = "config/config.json"

    with open(CONFIG_PATH) as f:
        config = json.load(f)

    SITE_NAME = config["site_name"].lower()
    DATA_DIR = config["data_directory"]

    print(f"Site: {SITE_NAME}")
    print(f"Data directory: {DATA_DIR}")
    return DATA_DIR, SITE_NAME


@app.cell
def _():
    # User-defined cohort parameters
    # These can be overridden by the dashboard UI
    START_DATE = "2100-01-01"
    END_DATE = "2200-12-31"
    LOCATION_NAMES = None  # None = all ICU locations, or list like ['MICU', 'SICU']

    # Controlled modes for SBT/LPV cohort
    CONTROLLED_MODES = [
        'assist control-volume control',
        'pressure control',
        'pressure-regulated volume control'
    ]

    print(f"Date range: {START_DATE} to {END_DATE}")
    print(f"Locations: {LOCATION_NAMES or 'All ICU locations'}")
    return CONTROLLED_MODES, END_DATE, LOCATION_NAMES, START_DATE


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Load Base Tables (with filtering)
    """)
    return


@app.cell
def _(DATA_DIR, END_DATE, LOCATION_NAMES, START_DATE, duckdb):
    # Load ADT table with date and location filtering
    adt_path = f"{DATA_DIR}/clif_adt.parquet"

    location_filter = ""
    if LOCATION_NAMES:
        location_list = ', '.join([f"'{loc}'" for loc in LOCATION_NAMES])
        location_filter = f"AND location_name IN ({location_list})"

    q_adt = f"""
    FROM '{adt_path}'
    SELECT *
    WHERE location_category = 'icu'
        AND in_dttm >= '{START_DATE}'::DATE
        AND in_dttm <= '{END_DATE}'::DATE + INTERVAL 1 DAY
        {location_filter}
    """
    adt_df = duckdb.sql(q_adt).df()
    print(f"Loaded adt_df: {len(adt_df):,} rows")
    adt_df.head()
    return (adt_df,)


@app.cell
def _(END_DATE, SITE_NAME, START_DATE, duckdb):
    # Load resp_p (waterfall-processed respiratory support) with date filtering
    resp_p_path = f"output/intermediate/{SITE_NAME}_resp_processed_bf.parquet"

    q_resp = f"""
    FROM '{resp_p_path}'
    SELECT *
    WHERE recorded_dttm >= '{START_DATE}'::DATE
        AND recorded_dttm <= '{END_DATE}'::DATE + INTERVAL 1 DAY
    """
    resp_p = duckdb.sql(q_resp).df()
    resp_p['tracheostomy'] = resp_p['tracheostomy'].fillna(0).astype(int)
    print(f"Loaded resp_p: {len(resp_p):,} rows")
    resp_p.head()
    return (resp_p,)


@app.cell
def _(DATA_DIR, pd):
    # Load hospitalization table
    hosp_path = f"{DATA_DIR}/clif_hospitalization.parquet"
    hosp_df = pd.read_parquet(hosp_path)
    print(f"Loaded hosp_df: {len(hosp_df):,} rows")
    hosp_df.head()
    return (hosp_df,)


@app.cell
def _(DATA_DIR, duckdb):
    # Load patient table for patient_id
    patient_path = f"{DATA_DIR}/clif_patient.parquet"
    q_patient = f"""
    FROM '{patient_path}'
    SELECT patient_id, sex_category
    """
    patient_df = duckdb.sql(q_patient).df()
    print(f"Loaded patient_df: {len(patient_df):,} rows")
    return (patient_df,)


@app.cell
def _(DATA_DIR, duckdb):
    # Load height from vitals for IBW calculation
    vitals_path = f"{DATA_DIR}/clif_vitals.parquet"
    q_height = f"""
    WITH height_vitals AS (
        FROM '{vitals_path}'
        SELECT hospitalization_id, recorded_dttm, vital_value AS height_cm
        WHERE LOWER(vital_category) = 'height_cm'
            AND vital_value IS NOT NULL
    )
    SELECT hospitalization_id, height_cm
    FROM height_vitals
    QUALIFY ROW_NUMBER() OVER (PARTITION BY hospitalization_id ORDER BY recorded_dttm DESC) = 1
    """
    height_df = duckdb.sql(q_height).df()
    print(f"Loaded height_df: {len(height_df):,} rows")
    return (height_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Define Cohorts at 7 AM
    """)
    return


@app.cell
def _(CONTROLLED_MODES, adt_df, duckdb, hosp_df, patient_df, resp_p):
    # Register base tables
    duckdb.register("resp_p", resp_p)
    duckdb.register("adt_df", adt_df)
    duckdb.register("hosp_df", hosp_df)
    duckdb.register("patient_df", patient_df)

    controlled_modes_str = ', '.join([f"'{m}'" for m in CONTROLLED_MODES])

    # SAT Cohort: IMV + ICU at 7 AM
    q_sat_cohort = f"""
    WITH resp_at_7am AS (
        -- Get respiratory status at 7 AM each day using forward-fill
        FROM resp_p
        SELECT hospitalization_id
            , recorded_dttm::DATE AS day
            , device_category
            , mode_category
        WHERE EXTRACT(HOUR FROM recorded_dttm) = 7
            AND EXTRACT(MINUTE FROM recorded_dttm) < 30
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY hospitalization_id, recorded_dttm::DATE
            ORDER BY recorded_dttm DESC
        ) = 1
    )
    , adt_at_7am AS (
        -- Get location at 7 AM each day
        FROM adt_df
        SELECT hospitalization_id
            , in_dttm::DATE AS day
            , location_name
            , location_category
        WHERE location_category = 'icu'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY hospitalization_id, in_dttm::DATE
            ORDER BY in_dttm DESC
        ) = 1
    )
    SELECT r.hospitalization_id
        , h.patient_id
        , r.day
        , a.location_name
        , r.device_category
        , r.mode_category
    FROM resp_at_7am r
    INNER JOIN adt_at_7am a USING (hospitalization_id, day)
    LEFT JOIN hosp_df h USING (hospitalization_id)
    WHERE LOWER(r.device_category) = 'imv'
    """
    sat_cohort = duckdb.sql(q_sat_cohort).df()
    print(f"SAT cohort (IMV + ICU at 7 AM): {len(sat_cohort):,} patient-days")

    # SBT/LPV Cohort: Controlled-mode IMV + ICU at 7 AM
    q_sbt_cohort = f"""
    FROM sat_cohort
    SELECT *
    WHERE LOWER(mode_category) IN ({controlled_modes_str})
    """
    duckdb.register("sat_cohort", sat_cohort)
    sbt_cohort = duckdb.sql(q_sbt_cohort).df()
    print(f"SBT/LPV cohort (controlled IMV + ICU at 7 AM): {len(sbt_cohort):,} patient-days")

    sat_cohort.head()
    return sat_cohort, sbt_cohort


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Load Medications for SAT
    """)
    return


@app.cell
def _(DATA_DIR, END_DATE, START_DATE, duckdb, sat_cohort):
    # Load continuous medications and pivot to wide format
    meds_path = f"{DATA_DIR}/clif_medication_admin_continuous.parquet"

    # Get hospitalization IDs in SAT cohort
    sat_hosp_ids = sat_cohort['hospitalization_id'].unique().tolist()
    hosp_id_list = ', '.join([f"'{h}'" for h in sat_hosp_ids])

    # Medications for SAT: sedation + analgesia
    sedation_meds = ['propofol', 'lorazepam', 'midazolam']  # Sedatives (non-opioid)
    analgesia_meds = ['fentanyl', 'hydromorphone', 'morphine']  # Opioids/analgesia
    all_meds = sedation_meds + analgesia_meds

    q_meds = f"""
    WITH filtered AS (
        FROM '{meds_path}'
        SELECT hospitalization_id
            , admin_dttm AS recorded_dttm
            , LOWER(med_category) AS med_category
            , med_dose
        WHERE hospitalization_id IN ({hosp_id_list})
            AND admin_dttm >= '{START_DATE}'::DATE
            AND admin_dttm <= '{END_DATE}'::DATE + INTERVAL 1 DAY
            AND LOWER(med_category) IN ({', '.join([f"'{m}'" for m in all_meds])})
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
    return analgesia_meds, meds_df, sedation_meds


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Calculate SAT Metrics

    - **sat_complete_cessation_N_7AM_7PM**: Complete cessation of all analgesia and sedation (7 AM - 7 PM)

    - **sat_sedation_cessation_N_7AM_7PM**: Cessation of sedation only - propofol + benzos stopped (7 AM - 7 PM)

    - **sat_dose_reduction_N_7AM_7PM**: 50% dose reduction of sedation (7 AM - 7 PM)
    """)
    return


@app.cell
def _(analgesia_meds, duckdb, meds_df, sat_cohort, sedation_meds):
    # Register meds for SAT calculation
    duckdb.register("meds_df", meds_df)
    duckdb.register("sat_cohort", sat_cohort)

    sedation_cols = [m for m in sedation_meds if m in meds_df.columns]
    analgesia_cols = [m for m in analgesia_meds if m in meds_df.columns]
    all_med_cols = sedation_cols + analgesia_cols

    # Build COALESCE expressions for checking zero doses
    sedation_zero_check = ' AND '.join([f"COALESCE({m}, 0) = 0" for m in sedation_cols]) if sedation_cols else "TRUE"
    all_zero_check = ' AND '.join([f"COALESCE({m}, 0) = 0" for m in all_med_cols]) if all_med_cols else "TRUE"

    # Build sum expressions for dose reduction
    sedation_sum = ' + '.join([f"COALESCE({m}, 0)" for m in sedation_cols]) if sedation_cols else "0"

    q_sat_metrics = f"""
    WITH meds_with_day AS (
        FROM meds_df
        SELECT *
            , recorded_dttm::DATE AS day
            , EXTRACT(HOUR FROM recorded_dttm) AS hour
        WHERE EXTRACT(HOUR FROM recorded_dttm) >= 7
            AND EXTRACT(HOUR FROM recorded_dttm) < 19
    )
    , meds_with_flags AS (
        FROM meds_with_day
        SELECT *
            -- Complete cessation: all meds are 0
            , CASE WHEN {all_zero_check} THEN 1 ELSE 0 END AS _all_meds_zero
            -- Sedation cessation: sedation meds are 0 (opioids can continue)
            , CASE WHEN {sedation_zero_check} THEN 1 ELSE 0 END AS _sedation_zero
            -- Total sedation dose for reduction calculation
            , ({sedation_sum}) AS _sedation_total
    )
    , meds_with_prior AS (
        FROM meds_with_flags
        SELECT *
            -- Get max sedation in prior 30 min for dose reduction
            , MAX(_sedation_total) OVER (
                PARTITION BY hospitalization_id
                ORDER BY recorded_dttm
                RANGE BETWEEN INTERVAL 30 MINUTE PRECEDING AND INTERVAL 1 SECOND PRECEDING
            ) AS _prior_30min_max_sedation
    )
    , cessation_events AS (
        FROM meds_with_prior
        SELECT hospitalization_id, day
            -- Check if cessation sustained for 30 min forward
            , CASE WHEN _all_meds_zero = 1 AND NOT EXISTS (
                SELECT 1 FROM meds_with_flags m2
                WHERE m2.hospitalization_id = meds_with_prior.hospitalization_id
                    AND m2.recorded_dttm > meds_with_prior.recorded_dttm
                    AND m2.recorded_dttm <= meds_with_prior.recorded_dttm + INTERVAL 30 MINUTE
                    AND m2._all_meds_zero = 0
            ) THEN 1 ELSE 0 END AS _complete_cessation
            , CASE WHEN _sedation_zero = 1 AND NOT EXISTS (
                SELECT 1 FROM meds_with_flags m2
                WHERE m2.hospitalization_id = meds_with_prior.hospitalization_id
                    AND m2.recorded_dttm > meds_with_prior.recorded_dttm
                    AND m2.recorded_dttm <= meds_with_prior.recorded_dttm + INTERVAL 30 MINUTE
                    AND m2._sedation_zero = 0
            ) THEN 1 ELSE 0 END AS _sedation_cessation
            -- Dose reduction: current dose <= 50% of prior 30 min max
            , CASE WHEN _prior_30min_max_sedation > 0
                AND _sedation_total <= 0.5 * _prior_30min_max_sedation
            THEN 1 ELSE 0 END AS _dose_reduction
    )
    , sat_by_day AS (
        FROM cessation_events
        SELECT hospitalization_id, day
            , MAX(_complete_cessation) AS sat_complete_cessation_N_7AM_7PM
            , MAX(_sedation_cessation) AS sat_sedation_cessation_N_7AM_7PM
            , MAX(_dose_reduction) AS sat_dose_reduction_N_7AM_7PM
        GROUP BY hospitalization_id, day
    )
    -- Join with cohort to get patient_id and location_name
    FROM sat_cohort c
    LEFT JOIN sat_by_day s USING (hospitalization_id, day)
    SELECT c.patient_id
        , c.hospitalization_id
        , c.location_name
        , c.day
        , COALESCE(s.sat_complete_cessation_N_7AM_7PM, 0) AS sat_complete_cessation_N_7AM_7PM
        , COALESCE(s.sat_sedation_cessation_N_7AM_7PM, 0) AS sat_sedation_cessation_N_7AM_7PM
        , COALESCE(s.sat_dose_reduction_N_7AM_7PM, 0) AS sat_dose_reduction_N_7AM_7PM
    ORDER BY c.hospitalization_id, c.day
    """
    sat_intermediate = duckdb.sql(q_sat_metrics).df()
    print(f"SAT intermediate: {len(sat_intermediate):,} rows")
    sat_intermediate.head()
    return (sat_intermediate,)


@app.cell
def _(duckdb, sat_intermediate):
    # Aggregate SAT to final (location + day level)
    duckdb.register("sat_intermediate", sat_intermediate)

    q_sat_final = """
    FROM sat_intermediate
    SELECT location_name
        , day
        , COUNT(*) AS total_IMV_patients_7AM
        , SUM(sat_complete_cessation_N_7AM_7PM) AS sat_complete_cessation_N_7AM_7PM
        , SUM(sat_sedation_cessation_N_7AM_7PM) AS sat_sedation_cessation_N_7AM_7PM
        , SUM(sat_dose_reduction_N_7AM_7PM) AS sat_dose_reduction_N_7AM_7PM
    GROUP BY location_name, day
    ORDER BY location_name, day
    """
    sat_final = duckdb.sql(q_sat_final).df()
    print(f"SAT final: {len(sat_final):,} rows")
    sat_final.head()
    return (sat_final,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Calculate SBT Metrics

    - **sbt_pressure_support_7AM_7PM**: SBT done (pressure support/CPAP with PEEP ≤8, PS ≤8 for ≥30 min) between 7 AM - 7 PM

    - **any_extubation_7AM_7PM**: Any extubation event between 7 AM - 7 PM

    - **sbt_successful_extubation_7PM**: Extubation that remains successful at 7 PM
    """)
    return


@app.cell
def _(duckdb, sbt_cohort):
    # Register tables for SBT
    duckdb.register("sbt_cohort", sbt_cohort)

    q_sbt_metrics = """
    WITH resp_7am_7pm AS (
        -- Filter to 7 AM - 7 PM window
        FROM resp_p
        SELECT *
            , recorded_dttm::DATE AS day
            , EXTRACT(HOUR FROM recorded_dttm) AS hour
        WHERE EXTRACT(HOUR FROM recorded_dttm) >= 7
            AND EXTRACT(HOUR FROM recorded_dttm) < 19
    )
    , sbt_state AS (
        -- Define SBT state: pressure support/CPAP with low settings
        FROM resp_7am_7pm
        SELECT *
            , CASE
                WHEN LOWER(mode_category) IN ('pressure support/cpap', 'pressure support', 'cpap')
                    AND COALESCE(peep_set, 0) <= 8
                    AND COALESCE(pressure_support_set, 0) <= 8
                THEN 1 ELSE 0
              END AS _sbt_state
            -- Extubation: transition from IMV to non-IMV
            , CASE
                WHEN LAG(LOWER(device_category)) OVER w = 'imv'
                    AND LOWER(device_category) IS DISTINCT FROM 'imv'
                THEN 1 ELSE 0
              END AS _extub
        WINDOW w AS (PARTITION BY hospitalization_id ORDER BY recorded_dttm)
    )
    , sbt_blocks AS (
        -- Identify contiguous SBT blocks using gaps-and-islands
        FROM sbt_state
        SELECT *
            , CASE WHEN _sbt_state IS DISTINCT FROM LAG(_sbt_state) OVER w THEN 1 ELSE 0 END AS _block_change
        WINDOW w AS (PARTITION BY hospitalization_id ORDER BY recorded_dttm)
    )
    , sbt_block_ids AS (
        FROM sbt_blocks
        SELECT *
            , SUM(_block_change) OVER w AS _block_id
        WINDOW w AS (PARTITION BY hospitalization_id ORDER BY recorded_dttm)
    )
    , sbt_block_durations AS (
        -- Calculate duration of each SBT block
        FROM sbt_block_ids
        SELECT hospitalization_id, day, _block_id
            , MIN(recorded_dttm) AS block_start
            , MAX(recorded_dttm) AS block_end
        WHERE _sbt_state = 1
        GROUP BY hospitalization_id, day, _block_id
    )
    , sbt_block_with_next AS (
        FROM sbt_block_durations
        SELECT *
            , LEAD(block_start) OVER w AS next_block_start
            , DATE_DIFF('minute', block_start, COALESCE(next_block_start, block_end)) AS duration_mins
        WINDOW w AS (PARTITION BY hospitalization_id ORDER BY _block_id)
    )
    , sbt_done_by_day AS (
        -- SBT is done if any block >= 30 minutes
        FROM sbt_block_with_next
        SELECT DISTINCT hospitalization_id, day
            , 1 AS sbt_pressure_support_7AM_7PM
        WHERE duration_mins >= 30
    )
    , extub_by_day AS (
        -- Any extubation in 7 AM - 7 PM window
        FROM sbt_state
        SELECT DISTINCT hospitalization_id, day
            , 1 AS any_extubation_7AM_7PM
        WHERE _extub = 1
    )
    , status_at_7pm AS (
        -- Get device status closest to 7 PM to check if still extubated
        FROM resp_p
        SELECT hospitalization_id
            , recorded_dttm::DATE AS day
            , device_category
        WHERE EXTRACT(HOUR FROM recorded_dttm) >= 18
            AND EXTRACT(HOUR FROM recorded_dttm) <= 20
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY hospitalization_id, recorded_dttm::DATE
            ORDER BY ABS(EXTRACT(HOUR FROM recorded_dttm) * 60 + EXTRACT(MINUTE FROM recorded_dttm) - 19 * 60)
        ) = 1
    )
    , successful_extub AS (
        -- Successful if extubated AND still not on IMV at 7 PM
        FROM extub_by_day e
        INNER JOIN status_at_7pm s USING (hospitalization_id, day)
        SELECT e.hospitalization_id, e.day
            , 1 AS sbt_successful_extubation_7PM
        WHERE LOWER(s.device_category) IS DISTINCT FROM 'imv'
    )
    -- Join all metrics to cohort
    FROM sbt_cohort c
    LEFT JOIN sbt_done_by_day sbt USING (hospitalization_id, day)
    LEFT JOIN extub_by_day ext USING (hospitalization_id, day)
    LEFT JOIN successful_extub suc USING (hospitalization_id, day)
    SELECT c.patient_id
        , c.hospitalization_id
        , c.location_name
        , c.day
        , COALESCE(sbt.sbt_pressure_support_7AM_7PM, 0) AS sbt_pressure_support_7AM_7PM
        , COALESCE(ext.any_extubation_7AM_7PM, 0) AS any_extubation_7AM_7PM
        , COALESCE(suc.sbt_successful_extubation_7PM, 0) AS sbt_successful_extubation_7PM
    ORDER BY c.hospitalization_id, c.day
    """
    sbt_intermediate = duckdb.sql(q_sbt_metrics).df()
    print(f"SBT intermediate: {len(sbt_intermediate):,} rows")
    sbt_intermediate.head()
    return (sbt_intermediate,)


@app.cell
def _(duckdb, sbt_intermediate):
    # Aggregate SBT to final (location + day level)
    duckdb.register("sbt_intermediate", sbt_intermediate)

    q_sbt_final = """
    FROM sbt_intermediate
    SELECT location_name
        , day
        , COUNT(*) AS total_controlled_IMV_patients_7AM
        , SUM(sbt_pressure_support_7AM_7PM) AS sbt_pressure_support_7AM_7PM
        , SUM(any_extubation_7AM_7PM) AS any_extubation_7AM_7PM
        , SUM(sbt_successful_extubation_7PM) AS sbt_successful_extubation_7PM
    GROUP BY location_name, day
    ORDER BY location_name, day
    """
    sbt_final = duckdb.sql(q_sbt_final).df()
    print(f"SBT final: {len(sbt_final):,} rows")
    sbt_final.head()
    return (sbt_final,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Calculate LPV Metrics

    - **total_controlled_IMV_hours**: Total hours on controlled-mode IMV per patient-day

    - **ltv_hours**: Hours with low tidal volume (< 8 cc/kg IBW)
    """)
    return


@app.cell
def _(duckdb, height_df):
    # Calculate IBW for LPV
    duckdb.register("height_df", height_df)

    q_ibw = """
    WITH hosp_patient AS (
        FROM hosp_df h
        LEFT JOIN patient_df p USING (patient_id)
        SELECT h.hospitalization_id, h.patient_id, p.sex_category
    )
    , hosp_with_height AS (
        FROM hosp_patient hp
        LEFT JOIN height_df ht USING (hospitalization_id)
        SELECT hp.hospitalization_id
            , hp.patient_id
            , hp.sex_category
            , ht.height_cm
    )
    SELECT hospitalization_id
        , patient_id
        , sex_category
        , height_cm
        , CASE
            WHEN LOWER(sex_category) = 'female' THEN 45.5 + 0.9 * (height_cm - 152)
            WHEN LOWER(sex_category) = 'male' THEN 50.0 + 0.9 * (height_cm - 152)
            ELSE NULL
          END AS ibw_kg
    FROM hosp_with_height
    WHERE height_cm IS NOT NULL
    """
    ibw_df = duckdb.sql(q_ibw).df()
    print(f"Computed IBW for {len(ibw_df):,} hospitalizations")
    return (ibw_df,)


@app.cell
def _(CONTROLLED_MODES, duckdb, ibw_df):
    # Calculate LPV metrics per patient-day
    duckdb.register("ibw_df", ibw_df)
    controlled_modes_lpv = ', '.join([f"'{m}'" for m in CONTROLLED_MODES])

    q_lpv_metrics = f"""
    WITH resp_controlled AS (
        -- Filter to controlled-mode IMV
        FROM resp_p r
        LEFT JOIN ibw_df i USING (hospitalization_id)
        SELECT r.hospitalization_id
            , i.patient_id
            , r.recorded_dttm
            , r.recorded_dttm::DATE AS day
            , r.tidal_volume_set
            , i.ibw_kg
            , LEAD(r.recorded_dttm) OVER w AS next_recorded_dttm
        WHERE LOWER(r.device_category) = 'imv'
            AND LOWER(r.mode_category) IN ({controlled_modes_lpv})
        WINDOW w AS (PARTITION BY r.hospitalization_id ORDER BY r.recorded_dttm)
    )
    , resp_with_duration AS (
        FROM resp_controlled
        SELECT *
            -- Duration in hours until next measurement (capped at 4 hours to avoid gaps)
            , LEAST(
                DATE_DIFF('minute', recorded_dttm, COALESCE(next_recorded_dttm, recorded_dttm + INTERVAL 1 HOUR)) / 60.0,
                4.0
              ) AS duration_hours
            -- Low TV flag
            , CASE
                WHEN ibw_kg IS NOT NULL AND tidal_volume_set IS NOT NULL
                    AND (tidal_volume_set / ibw_kg) < 8
                THEN 1 ELSE 0
              END AS is_low_tv
    )
    , lpv_by_patient_day AS (
        FROM resp_with_duration
        SELECT hospitalization_id, patient_id, day
            , SUM(duration_hours) AS total_controlled_IMV_hours
            , SUM(CASE WHEN is_low_tv = 1 THEN duration_hours ELSE 0 END) AS ltv_hours
        GROUP BY hospitalization_id, patient_id, day
    )
    -- Join with SBT cohort to get location_name
    FROM sbt_cohort c
    LEFT JOIN lpv_by_patient_day l USING (hospitalization_id, day)
    SELECT c.patient_id
        , c.hospitalization_id
        , c.location_name
        , c.day
        , COALESCE(l.total_controlled_IMV_hours, 0) AS total_controlled_IMV_hours
        , COALESCE(l.ltv_hours, 0) AS ltv_hours
    ORDER BY c.hospitalization_id, c.day
    """
    lpv_intermediate = duckdb.sql(q_lpv_metrics).df()
    print(f"LPV intermediate: {len(lpv_intermediate):,} rows")
    lpv_intermediate.head()
    return (lpv_intermediate,)


@app.cell
def _(duckdb, lpv_intermediate):
    # Aggregate LPV to final (location + day level)
    duckdb.register("lpv_intermediate", lpv_intermediate)

    q_lpv_final = """
    FROM lpv_intermediate
    SELECT location_name
        , day
        , SUM(total_controlled_IMV_hours) AS total_controlled_IMV_hours
        , SUM(ltv_hours) AS ltv_hours
    GROUP BY location_name, day
    ORDER BY location_name, day
    """
    lpv_final = duckdb.sql(q_lpv_final).df()
    print(f"LPV final: {len(lpv_final):,} rows")
    lpv_final.head()
    return (lpv_final,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Save Outputs
    """)
    return


@app.cell
def _(
    lpv_final,
    lpv_intermediate,
    os,
    sat_final,
    sat_intermediate,
    sbt_final,
    sbt_intermediate,
):
    # Create output directories
    os.makedirs("output/intermediate", exist_ok=True)
    os.makedirs("output/final", exist_ok=True)

    # Save SAT outputs
    sat_intermediate.to_csv("output/intermediate/sat_metrics.csv", index=False)
    sat_final.to_csv("output/final/sat_metrics.csv", index=False)
    print("Saved SAT metrics")

    # Save SBT outputs
    sbt_intermediate.to_csv("output/intermediate/sbt_metrics.csv", index=False)
    sbt_final.to_csv("output/final/sbt_metrics.csv", index=False)
    print("Saved SBT metrics")

    # Save LPV outputs
    lpv_intermediate.to_csv("output/intermediate/lpv_metrics.csv", index=False)
    lpv_final.to_csv("output/final/lpv_metrics.csv", index=False)
    print("Saved LPV metrics")

    print("\nAll outputs saved successfully!")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Summary Statistics
    """)
    return


@app.cell
def _(lpv_final, mo, sat_final, sbt_final):
    # Display summary
    sat_total = sat_final['total_IMV_patients_7AM'].sum()
    sat_complete = sat_final['sat_complete_cessation_N_7AM_7PM'].sum()
    sat_sedation = sat_final['sat_sedation_cessation_N_7AM_7PM'].sum()
    sat_reduction = sat_final['sat_dose_reduction_N_7AM_7PM'].sum()

    sbt_total = sbt_final['total_controlled_IMV_patients_7AM'].sum()
    sbt_ps = sbt_final['sbt_pressure_support_7AM_7PM'].sum()
    sbt_extub = sbt_final['any_extubation_7AM_7PM'].sum()
    sbt_success = sbt_final['sbt_successful_extubation_7PM'].sum()

    lpv_hours = lpv_final['total_controlled_IMV_hours'].sum()
    ltv_hours = lpv_final['ltv_hours'].sum()

    mo.md(f"""
    ### Summary

    **SAT Metrics** (n = {sat_total:,.0f} patient-days on IMV at 7 AM)

    | Metric | Count | Rate |
    |--------|-------|------|
    | Complete cessation | {sat_complete:,.0f} | {100*sat_complete/sat_total:.1f}% |
    | Sedation cessation | {sat_sedation:,.0f} | {100*sat_sedation/sat_total:.1f}% |
    | Dose reduction | {sat_reduction:,.0f} | {100*sat_reduction/sat_total:.1f}% |

    **SBT Metrics** (n = {sbt_total:,.0f} patient-days on controlled IMV at 7 AM)

    | Metric | Count | Rate |
    |--------|-------|------|
    | SBT pressure support | {sbt_ps:,.0f} | {100*sbt_ps/sbt_total:.1f}% |
    | Any extubation | {sbt_extub:,.0f} | {100*sbt_extub/sbt_total:.1f}% |
    | Successful extubation | {sbt_success:,.0f} | {100*sbt_success/sbt_total:.1f}% |

    **LPV Metrics**

    | Metric | Value |
    |--------|-------|
    | Total controlled IMV hours | {lpv_hours:,.1f} |
    | Low tidal volume hours | {ltv_hours:,.1f} |
    | LPV rate | {100*ltv_hours/lpv_hours:.1f}% |
    """)
    return


if __name__ == "__main__":
    app.run()
