# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the CLIF ICU Quality Dashboard project (CLIF-IQR), designed to create a unit-level ICU quality report using the Common Longitudinal ICU data Format (CLIF) version 2.1. The dashboard tracks key ICU care metrics including adherence to low-tidal volume ventilation, spontaneous awakening trials (SAT), and spontaneous breathing trials (SBT).

## Environment Setup

- This project uses `uv` for virtual environment management. Use `uv` when adding or changing dependencies.
- Virtual environment is at `.venv/` (Python 3.9)
- Key dependencies: `clifpy` (v0.3.1), `marimo`, `duckdb`, `pandas`

## Common Longitudinal ICU data Format (CLIF)

This project uses CLIF, a data standard for ICU data, and the `clifpy` package, a Python package designed for working with CLIF-formatted EHR data. clifpy handles table loading, datetime conversion, timezone localization, and schema validation.

## CLIF Schema and Skills

This project uses the **clif-icu** skill plugin to access official CLIF schemas and documentation. Use this skill to understand the CLIF schema. It is essential that you understand the table structure and the minimum Common ICU data Elements (mCIDE). Use `*_category` variables in code — when categorical, they have a limited set of permissible values.

## Do not read source CLIF Tables

Do not attempt to access or read CLIF tables directly. Use clifpy to load them.

## Site Configuration

Site-specific settings are in `config/config.json`:
- `site_name`: Institution name
- `tables_path`: Path to CLIF v2.1 parquet files
- `file_type`: Data format (parquet)
- `timezone`: Site timezone (e.g., America/Chicago)

A template is at `config/config_template.json`.

## Key Quality Metrics

### Lung-Protective Ventilation (LPV)
- Tidal volume ≤ 8 mL/kg PBW during invasive mechanical ventilation
- Only in controlled modes: `Assist Control-Volume Control`, `Pressure Control`, `Pressure-Regulated Volume Control`
- Calculated from respiratory_support table (hourly resolution)
- IBW formula: Female = 45.5 + 0.9 * (height_cm - 152), Male = 50 + 0.9 * (height_cm - 152)

### Spontaneous Awakening Trials (SAT)
- For patients on mechanical ventilation at 7 AM
- Tracks: complete cessation of sedation, partial cessation, or dose reduction
- SQL logic in `code/sat.sql`

### Spontaneous Breathing Trials (SBT)
- For patients in controlled mode at 7 AM
- Tracks: switch to pressure support < 10 cmH2O, successful extubation (still extubated at 7 PM)
- SQL logic in `code/sbt.sql`

## Project Structure

```
code/
  app.py          # Main Marimo dashboard (UI + overall summary computation)
  backend.py      # Backend Marimo notebook (SAT/SBT/LPV pipelines via DuckDB)
  sat.sql         # SAT SQL logic executed by backend.py
  sbt.sql         # SBT SQL logic executed by backend.py
config/
  config.json     # Site-specific configuration (gitignored)
  config_template.json
specs/
  UI_specs.md     # Dashboard UI layout specification
  backend_outputs/  # Reference CSVs defining expected output schemas
    overall_summary.csv
    lpv_metrics.csv
    sat_metrics.csv
    sbt_metrics.csv
output/
  intermediate/   # Intermediate data (e.g., processed respiratory, SAT/SBT events)
  final/          # Final results
  logs/           # Validation and error logs
```

## Dashboard Implementation (code/app.py)

The main dashboard is a Marimo app with an interactive interface.

### Current State
- **Implemented**: ICU location dropdown, date range picker, overall summary (admissions, census, discharges by disposition)
- **Placeholders (N/A)**: LPV adherence, SAT rates, SBT rates, SOFA-2 scores, bed strain
- **Not yet connected**: Backend computations from `backend.py` are not wired into the dashboard

### Day Definition

**Important**: A "day" is defined as 7 AM to 7 AM the following day (not midnight to midnight). This aligns with typical ICU shift patterns.

- Day start: 7:00 AM on the selected date
- Day end: 7:00 AM the next day (exclusive)
- Census snapshots: 7 AM and 7 PM on the selected date

### Data Flow in app.py

1. **Load Configuration**: Read `config/config.json` for database paths
2. **Load Tables**: Load ADT and Hospitalization tables via clifpy (handles datetime/timezone)
3. **Get ICU Locations**: Filter ADT for `location_category='icu'` and populate dropdown
4. **Calculate Summary**: For each day in selected range (7 AM to 7 AM):
   - **total_admissions**: Count of `in_dttm` to ICU within the day window
   - **census_7AM/7PM**: Count of patients present in ICU at 7 AM/7 PM
   - **total_discharges**: Count of `out_dttm` from ICU within the day window
   - **floor_transfers**: ICU discharges where next ADT location is ward/stepdown
   - **deaths_in_icu**: ICU discharges where `discharge_category='Expired'`
   - **discharges_to_hospice**: ICU discharges where `discharge_category='Hospice'`
   - **discharges_to_facility**: ICU discharges to SNF/LTACH/Rehab/Assisted Living

## Backend Pipeline (code/backend.py)

A separate Marimo notebook that computes quality metrics using DuckDB SQL:

1. Loads processed respiratory support data, ADT, hospitalization, medications, vitals, RASS assessments
2. Runs `sbt.sql` and `sat.sql` to produce event-level and day-level SAT/SBT results
3. Computes LPV metrics (IBW calculation, tidal volume per kg, controlled mode filtering)
4. Saves outputs to `output/intermediate/`

### Running the Apps

```bash
# Dashboard in watch mode (auto-reloads on file save)
marimo run code/app.py --watch --port 2718

# Dashboard in edit mode
marimo edit code/app.py

# Backend notebook
marimo edit code/backend.py
```

## Expected Deliverable

A Marimo dashboard (`code/app.py`) running locally, displaying:
- Unit selection dropdown (dynamically populated from ICU locations)
- Reporting period selector (constrained to available data dates)
- Overall summary: admissions, daily census, discharges by disposition
- Bed strain and SOFA-2 scores
- Three-column quality metrics: LPV adherence, SAT rates, SBT rates

## Backend Output Schemas

Reference CSVs in `specs/backend_outputs/` define the expected unit-day level output format. All backend computations should produce data matching these schemas, keyed by `location_name` and `day`.
