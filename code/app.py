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
    today = datetime.date.today()
    week_ago = today - datetime.timedelta(days=7)

    # Create UI elements for the top section
    unit_dropdown = mo.ui.dropdown(
        options=location_options,
        value=location_options[0],
        label="Unit"
    )

    date_range = mo.ui.date_range(
        start=week_ago,
        stop=today,
        label="Reporting Period"
    )
    return date_range, unit_dropdown


@app.cell(column=1)
def _(mo):
    mo.md("""
    # CLIF ICU Quality Report
    """)
    return


@app.cell
def _():
    return


@app.cell
def _(date_range, mo, unit_dropdown):
    # Top section: Three columns with key metrics
    mo.hstack([
        # Column 1: Unit selection and admissions
        mo.vstack([
            unit_dropdown,
            mo.md("**Total Admissions**"),
            mo.md("## 156"),
            mo.md("**Daily Census** (7 AM snapshot)"),
            mo.md("## 23"),
        ], align="start"),

        # Column 2: Reporting period and discharges
        mo.vstack([
            date_range,
            mo.md("**Total Discharges**"),
            mo.md("## 142"),
            mo.md("""
    - Floor transfers: 98
    - Deaths in ICU: 18
    - Discharges to hospice: 12
    - Discharges to facility: 14
            """),
        ], align="start"),

        # Column 3: Bed strain and SOFA-2
        mo.vstack([
            mo.md("**Bed Strain**"),
            mo.md("## 87.3%"),
            mo.md("*(% of max beds occupied)*"),
            mo.md("**SOFA-2 Score**"),
            mo.md("## 6 (IQR: 4-9)"),
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
def _(mo):
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
            mo.md("""
    | Metric | Value |
    |--------|-------|
    | **LPV adherence (%)** | **78.5%** |
    | Median VT (mL/kg PBW) | 6.8 |
            """),
            mo.md("*Derived from CLIF Respiratory Support table (hourly resolution).*"),
        ], align="start"),

        # Column 2: Spontaneous Awakening Trials
        mo.vstack([
            mo.md("### Spontaneous Awakening Trials (SAT)"),
            mo.md("""
    For all patients receiving invasive mechanical ventilation at 7 AM, the daily rate of SAT are:
            """),
            mo.md("""
    | Outcome | Rate |
    |---------|------|
    | Complete cessation of all analgesia and sedation | **34.2%** |
    | Cessation of sedation (stop propofol and benzodiazepine drips) | **52.8%** |
    | Dose reduction of sedation | **23.5%** |
            """),
        ], align="start"),

        # Column 3: Spontaneous Breathing Trials
        mo.vstack([
            mo.md("### Spontaneous Breathing Trials (SBT)"),
            mo.md("""
    For all patients receiving a controlled mode of invasive mechanical ventilation at 7 AM, the daily rate of SBT and extubation:
            """),
            mo.md("""
    | Outcome | Rate |
    |---------|------|
    | Any switch to pressure support < 10 cmH2O | **41.3%** |
    | Successful extubation (remains extubated at 7 PM) | **28.7%** |
            """),
        ], align="start"),
    ], gap=2, widths=[1, 1, 1], justify="space-between")
    return


if __name__ == "__main__":
    app.run()
