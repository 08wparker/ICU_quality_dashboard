import marimo

__generated_with = "0.17.6"
app = marimo.App(width="columns")


@app.cell(column=0)
def _():
    import marimo as mo
    import datetime
    import json
    import pandas as pd
    from pathlib import Path
    return Path, datetime, json, mo, pd


@app.cell
def _(Path, json):
    # Load configuration
    config_path = Path(__file__).parent.parent / "config" / "config.json"

    with open(config_path) as f:
        config = json.load(f)

    site_name = config["site_name"]
    tables_path = config["tables_path"]
    file_type = config["file_type"]
    timezone = config["timezone"]

    print(f"Site: {site_name}")
    print(f"Tables path: {tables_path}")
    print(f"File type: {file_type}")
    print(f"Timezone: {timezone}")
    return file_type, tables_path


@app.cell
def _(Path, file_type, json, tables_path):
    from clifpy.tables import Adt, Hospitalization

    # Get timezone from config
    _cfg_path = Path(__file__).parent.parent / "config" / "config.json"
    with open(_cfg_path) as _cfg_file:
        _cfg = json.load(_cfg_file)
    site_timezone = _cfg.get("timezone", "America/Chicago")

    # Load ADT table using clifpy (handles datetime conversion automatically)
    adt_table = Adt.from_file(
        data_directory=tables_path,
        filetype=file_type,
        timezone=site_timezone
    )
    adt_df = adt_table.df

    # Load hospitalization table using clifpy
    hosp_table = Hospitalization.from_file(
        data_directory=tables_path,
        filetype=file_type,
        timezone=site_timezone
    )
    hosp_df = hosp_table.df

    # Get unique ICU locations
    location_df = adt_df[['location_name', 'location_category', 'location_type']].drop_duplicates()
    icu_locations = location_df[location_df['location_category'].str.lower() == 'icu'].copy()

    print(f"Loaded {len(adt_df)} ADT records, {len(hosp_df)} hospitalizations")
    print(f"Found {len(icu_locations)} unique ICU locations")
    return adt_df, hosp_df, icu_locations


@app.cell
def _(adt_df, datetime):
    # Get min and max dates from ADT table (already converted to datetime)
    min_date = min(adt_df['in_dttm'].min(), adt_df['out_dttm'].min())
    max_date = max(adt_df['in_dttm'].max(), adt_df['out_dttm'].max())

    # Convert to date objects
    min_date_obj = datetime.date(2024, 1, 1)
    max_date_obj = datetime.date(2024, 12, 31)

    print(f"\nDate range in database:")
    print(f"  Min date: {min_date_obj}")
    print(f"  Max date: {max_date_obj}")
    return max_date_obj, min_date_obj


@app.cell
def _(datetime, icu_locations, max_date_obj, min_date_obj, mo):
    # Get location options from ICU locations
    location_options = sorted(icu_locations['location_name'].unique().tolist())

    # Default date range (1 week from min_date)
    default_start = min_date_obj
    default_end = min(default_start + datetime.timedelta(days=7), max_date_obj)

    # Create UI elements for the top section
    unit_dropdown = mo.ui.dropdown(
        options=location_options,
        value=location_options[0] if location_options else None,
        label="Unit"
    )

    date_range = mo.ui.date_range(
        start=min_date_obj,
        stop=max_date_obj,
        value=(default_start, default_end),
        label="Reporting Period"
    )
    return date_range, unit_dropdown


@app.cell
def _(adt_df, date_range, hosp_df, pd, unit_dropdown):
    # Generate overall_summary dataframe based on selected location and date range
    selected_location = unit_dropdown.value
    start_date = pd.Timestamp(date_range.value[0])
    end_date = pd.Timestamp(date_range.value[1])

    # Filter ADT data for selected location
    location_adt = adt_df[adt_df['location_name'] == selected_location].copy()

    # Get timezone from ADT data (set by clifpy)
    adt_tz = adt_df['in_dttm'].dt.tz

    # Create a date range for iteration
    date_list = pd.date_range(start=start_date, end=end_date, freq='D')

    summary_records = []

    for day in date_list:
        # Day is defined as 7 AM to 7 AM next day (timezone-aware)
        day_start = pd.Timestamp(day.date()).replace(hour=7).tz_localize(adt_tz)
        day_end = pd.Timestamp((day + pd.Timedelta(days=1)).date()).replace(hour=7).tz_localize(adt_tz)
        census_7am = pd.Timestamp(day.date()).replace(hour=7).tz_localize(adt_tz)
        census_7pm = pd.Timestamp(day.date()).replace(hour=19).tz_localize(adt_tz)

        # Total admissions: in_dttm within this day (7 AM to 7 AM next day)
        admissions = location_adt[
            (location_adt['in_dttm'] >= day_start) &
            (location_adt['in_dttm'] < day_end)
        ]
        day_admissions_count = len(admissions)

        # Census at 7 AM: patients present at 7 AM
        census_7am_count = len(location_adt[
            (location_adt['in_dttm'] <= census_7am) &
            ((location_adt['out_dttm'].isna()) | (location_adt['out_dttm'] > census_7am))
        ])

        # Census at 7 PM: patients present at 7 PM
        census_7pm_count = len(location_adt[
            (location_adt['in_dttm'] <= census_7pm) &
            ((location_adt['out_dttm'].isna()) | (location_adt['out_dttm'] > census_7pm))
        ])

        # Total discharges from ICU: out_dttm within this day (7 AM to 7 AM next day)
        icu_discharges = location_adt[
            (location_adt['out_dttm'] >= day_start) &
            (location_adt['out_dttm'] < day_end)
        ].copy()
        day_discharges_count = len(icu_discharges)

        # Merge with hospitalization data to get discharge categories
        icu_discharges_with_hosp = icu_discharges.merge(
            hosp_df[['hospitalization_id', 'discharge_category', 'discharge_dttm']],
            on='hospitalization_id',
            how='left'
        )

        # Floor transfers: ICU discharges where next ADT location is ward or stepdown
        # Get the next location for each ICU discharge
        floor_transfers_count = 0
        for idx, row in icu_discharges.iterrows():
            hosp_id = row['hospitalization_id']
            out_time = row['out_dttm']

            # Find next ADT location after ICU discharge
            next_location = adt_df[
                (adt_df['hospitalization_id'] == hosp_id) &
                (adt_df['in_dttm'] >= out_time) &
                (adt_df['in_dttm'] <= out_time + pd.Timedelta(hours=24))
            ].sort_values('in_dttm').head(1)

            if not next_location.empty:
                next_loc_category = next_location.iloc[0]['location_category'].lower()
                if next_loc_category in ['ward', 'stepdown']:
                    floor_transfers_count += 1

        # Deaths in ICU: discharge_category = 'Expired' and discharge occurred this day
        deaths_in_icu_count = len(icu_discharges_with_hosp[
            (icu_discharges_with_hosp['discharge_category'] == 'Expired') &
            (icu_discharges_with_hosp['discharge_dttm'] >= day_start) &
            (icu_discharges_with_hosp['discharge_dttm'] < day_end)
        ])

        # Discharges to hospice
        discharges_to_hospice_count = len(icu_discharges_with_hosp[
            (icu_discharges_with_hosp['discharge_category'] == 'Hospice') &
            (icu_discharges_with_hosp['discharge_dttm'] >= day_start) &
            (icu_discharges_with_hosp['discharge_dttm'] < day_end)
        ])

        # Discharges to facility: SNF, LTACH, Rehab, Assisted Living
        facility_categories = [
            'Skilled Nursing Facility (SNF)',
            'Long Term Care Hospital (LTACH)',
            'Acute Inpatient Rehab Facility',
            'Assisted Living'
        ]
        discharges_to_facility_count = len(icu_discharges_with_hosp[
            (icu_discharges_with_hosp['discharge_category'].isin(facility_categories)) &
            (icu_discharges_with_hosp['discharge_dttm'] >= day_start) &
            (icu_discharges_with_hosp['discharge_dttm'] < day_end)
        ])

        summary_records.append({
            'location_name': selected_location,
            'day': day.strftime('%m/%d/%y'),
            'total_admissions': day_admissions_count,
            'census_7AM': census_7am_count,
            'census_7PM': census_7pm_count,
            'total_discharges': day_discharges_count,
            'floor_transfers': floor_transfers_count,
            'deaths_in_icu': deaths_in_icu_count,
            'discharges_to_hospice': discharges_to_hospice_count,
            'discharges_to_facility': discharges_to_facility_count,
            'sofa_median': None,  # To be calculated later
            'sofa_q1': None,
            'sofa_q3': None
        })

    overall_summary_df = pd.DataFrame(summary_records)

    # Compute weekly summary metrics from overall_summary_df

    # Column 1 metrics
    total_admissions = int(overall_summary_df['total_admissions'].sum())
    daily_census = round(overall_summary_df['census_7AM'].mean(), 1)

    # Column 2 metrics
    total_discharges = int(overall_summary_df['total_discharges'].sum())
    floor_transfers = int(overall_summary_df['floor_transfers'].sum())
    deaths_in_icu = int(overall_summary_df['deaths_in_icu'].sum())
    discharges_to_hospice = int(overall_summary_df['discharges_to_hospice'].sum())
    discharges_to_facility = int(overall_summary_df['discharges_to_facility'].sum())

    # Column 3 metrics (placeholders - need additional data)
    bed_strain_pct = "N/A"  # Requires max bed capacity
    sofa_median = "N/A"  # Not yet implemented
    sofa_q1 = "N/A"
    sofa_q3 = "N/A"

    # Lung-Protective Ventilation metrics (placeholders - not yet implemented)
    lpv_adherence_pct = "N/A"
    median_vt = "N/A"

    # Spontaneous Awakening Trials metrics (placeholders - not yet implemented)
    sat_complete_cessation_pct = "N/A"
    sat_sedation_cessation_pct = "N/A"
    sat_dose_reduction_pct = "N/A"

    # Spontaneous Breathing Trials metrics (placeholders - not yet implemented)
    sbt_pressure_support_pct = "N/A"
    sbt_successful_extubation_pct = "N/A"
    return (
        bed_strain_pct,
        daily_census,
        deaths_in_icu,
        discharges_to_facility,
        discharges_to_hospice,
        floor_transfers,
        lpv_adherence_pct,
        median_vt,
        sat_complete_cessation_pct,
        sat_dose_reduction_pct,
        sat_sedation_cessation_pct,
        sbt_pressure_support_pct,
        sbt_successful_extubation_pct,
        sofa_median,
        sofa_q1,
        sofa_q3,
        total_admissions,
        total_discharges,
    )


@app.cell(column=1)
def _(mo):
    mo.md("""
    # CLIF ICU Quality Report
    """)
    return


@app.cell
def _(
    bed_strain_pct,
    daily_census,
    date_range,
    deaths_in_icu,
    discharges_to_facility,
    discharges_to_hospice,
    floor_transfers,
    mo,
    sofa_median,
    sofa_q1,
    sofa_q3,
    total_admissions,
    total_discharges,
    unit_dropdown,
):
    # Top section: Three columns with key metrics
    mo.hstack([
        # Column 1: Unit selection and admissions
        mo.vstack([
            unit_dropdown,
            mo.md("**Total Admissions**"),
            mo.md(f"## {total_admissions}"),
            mo.md("**Daily Census** (7 AM snapshot)"),
            mo.md(f"## {daily_census}"),
        ], align="start"),

        # Column 2: Reporting period and discharges
        mo.vstack([
            date_range,
            mo.md("**Total Discharges**"),
            mo.md(f"## {total_discharges}"),
            mo.md(f"""
    - Floor transfers: {floor_transfers}
    - Deaths in ICU: {deaths_in_icu}
    - Discharges to hospice: {discharges_to_hospice}
    - Discharges to facility: {discharges_to_facility}
            """),
        ], align="start"),

        # Column 3: Bed strain and SOFA-2
        mo.vstack([
            mo.md("**Bed Strain**"),
            mo.md(f"## {bed_strain_pct}%"),
            mo.md("*(% of max beds occupied)*"),
            mo.md("**SOFA-2 Score**"),
            mo.md(f"## {sofa_median} (IQR: {sofa_q1}-{sofa_q3})"),
            mo.md("*(Median and IQR of max daily SOFA)*"),
        ], align="start"),
    ], gap=2, widths=[1, 1, 1], justify="space-between")
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    """)
    return


@app.cell
def _(
    lpv_adherence_pct,
    median_vt,
    mo,
    sat_complete_cessation_pct,
    sat_dose_reduction_pct,
    sat_sedation_cessation_pct,
    sbt_pressure_support_pct,
    sbt_successful_extubation_pct,
):
    # Quality Metrics Section: Three columns
    mo.hstack([
        # Column 1: Lung-Protective Ventilation
        mo.vstack([
            mo.md("### Lung-Protective Ventilation (LPV)"),
            mo.md("""
    **Definition:**
    - Tidal volume ≤ 8 mL/kg PBW
    - Applied during invasive mechanical ventilation in **controlled** modes only
    - Modes: Assist Control-Volume Control, Pressure Control, or Pressure-Regulated Volume Control
            """),
            mo.md(f"""
    | Metric | Value |
    |--------|-------|
    | **LPV adherence (%)** | **{lpv_adherence_pct}%** |
    | Median VT (mL/kg PBW) | {median_vt} |
            """),
            mo.md("*Derived from CLIF Respiratory Support table (hourly resolution).*"),
        ], align="start"),

        # Column 2: Spontaneous Awakening Trials
        mo.vstack([
            mo.md("### Spontaneous Awakening Trials (SAT)"),
            mo.md("""
    For all patients receiving invasive mechanical ventilation at 7 AM, the daily rate of SAT are:
            """),
            mo.md(f"""
    | Outcome | Rate |
    |---------|------|
    | Complete cessation of all analgesia and sedation | **{sat_complete_cessation_pct}%** |
    | Cessation of sedation (stop propofol and benzodiazepine drips) | **{sat_sedation_cessation_pct}%** |
    | Dose reduction of sedation | **{sat_dose_reduction_pct}%** |
            """),
        ], align="start"),

        # Column 3: Spontaneous Breathing Trials
        mo.vstack([
            mo.md("### Spontaneous Breathing Trials (SBT)"),
            mo.md("""
    For all patients receiving a controlled mode of invasive mechanical ventilation at 7 AM, the daily rate of SBT and extubation:
            """),
            mo.md(f"""
    | Outcome | Rate |
    |---------|------|
    | Any switch to pressure support < 10 cmH2O | **{sbt_pressure_support_pct}%** |
    | Successful extubation (remains extubated at 7 PM) | **{sbt_successful_extubation_pct}%** |
            """),
        ], align="start"),
    ], gap=2, widths=[1, 1, 1], justify="space-between")
    return


if __name__ == "__main__":
    app.run()
