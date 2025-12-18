import marimo

__generated_with = "0.17.6"
app = marimo.App(width="columns")


@app.cell(column=0)
def _():
    import marimo as mo
    import datetime
    return datetime, mo


@app.cell
def _(datetime, mo):
    # Placeholder data for dropdowns
    location_options = ["ICU-1", "ICU-2", "ICU-3", "MICU", "SICU", "CCU"]

    # Default date range (1 week)
    ## change default for today to jan 1st 2025
    today = datetime.date(2025, 1, 1)
    week_ahead = today + datetime.timedelta(days=7)

    # Create UI elements for the top section
    unit_dropdown = mo.ui.dropdown(
        options=location_options,
        value=location_options[0],
        label="Unit"
    )

    date_range = mo.ui.date_range(
        start= today,
        stop=week_ahead,
        label="Reporting Period"
    )
    return date_range, unit_dropdown


@app.cell
def _():
    # Placeholder variables for all dashboard metrics
    # These will be populated by the backend

    # Column 1 metrics
    total_admissions = 156
    daily_census = 23

    # Column 2 metrics
    total_discharges = 142
    floor_transfers = 98
    deaths_in_icu = 18
    discharges_to_hospice = 12
    discharges_to_facility = 14

    # Column 3 metrics
    bed_strain_pct = 87.3
    sofa_median = 6
    sofa_q1 = 4
    sofa_q3 = 9

    # Lung-Protective Ventilation metrics
    lpv_adherence_pct = 78.5
    median_vt = 6.8

    # Spontaneous Awakening Trials metrics
    sat_complete_cessation_pct = 34.2
    sat_sedation_cessation_pct = 52.8
    sat_dose_reduction_pct = 23.5

    # Spontaneous Breathing Trials metrics
    sbt_pressure_support_pct = 41.3
    sbt_successful_extubation_pct = 28.7
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
