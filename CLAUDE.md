# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the CLIF ICU Quality Dashboard project (CLIF-IQR), designed to create a unit-level ICU quality report using the Common Longitudinal ICU data Format (CLIF) version 2.1. The dashboard tracks key ICU care metrics including adherence to low-tidal volume ventilation, spontaneous awakening trials (SAT), and spontaneous breathing trials (SBT).

## Environment Setup

This project uses `uv` for virtual environment setup, when changing requirements use uv

## Common Longitudinal ICU data Format (CLIF)

This project uses CLIF, a data standard for ICU data, and the `clifpy` package, a python package designed for working with 

## CLIF Schema and Skills

This project uses the **clif-icu** skill plugin to access official CLIF schemas and documentation. Use this skill to understand the CLIF schema. it is essential that you understand the table structure and the minimum Common ICU data Elements (mCIDE). Use `*_category` variables in code, when categorical they have a limited set of permissible values

## Do not read source CLIF Tables

Do not attempt to access or read CLIF tables

## Key Quality Metrics (from specs.md)

### Lung-Protective Ventilation (LPV)
- Tidal volume ≤ 8 mL/kg PBW during invasive mechanical ventilation
- Only in controlled modes: `Assist Control-Volume Control`, `Pressure Control`, `Pressure-Regulated Volume Control`
- Calculated from respiratory_support table (hourly resolution)

### Spontaneous Awakening Trials (SAT)
- For patients on mechanical ventilation at 7 AM
- Tracks: complete cessation of sedation, partial cessation, or dose reduction

### Spontaneous Breathing Trials (SBT)
- For patients in controlled mode at 7 AM
- Tracks: switch to pressure support < 10 cmH2O, successful extubation (still extubated at 7 PM)

## Output Structure

- `output/intermediate/`: Intermediate cleaned data files
- `output/final/`: Final analysis results, figures, and tables
- Results are organized by data type (labs, vitals, etc.)

## Dashboard Implementation (code/app.py)

The main dashboard is implemented as a Marimo app (`code/app.py`) that provides an interactive interface.

### Key Features

1. **Dynamic ICU Location Selection**: Queries ADT table to get all ICU locations (where `location_category='icu'`)
2. **Date Range Picker**: Automatically sets min/max dates based on available data in ADT table
3. **Overall Summary Calculation**: Generates day-level metrics for selected location and date range

### Day Definition

**Important**: A "day" is defined as 7 AM to 7 AM the following day (not midnight to midnight). This aligns with typical ICU shift patterns and clinical workflows.

- Day start: 7:00 AM on the selected date
- Day end: 7:00 AM the next day (exclusive)
- Census snapshots: 7 AM and 7 PM on the selected date

### Data Flow in app.py

1. **Load Configuration**: Read `config/config.json` for database paths
2. **Load Tables**: Load ADT and Hospitalization tables with datetime conversion
3. **Handle Timezones**: Detect timezone from ADT data and localize comparison timestamps to avoid timezone comparison errors
4. **Get ICU Locations**: Filter ADT for `location_category='icu'` and populate dropdown
5. **Calculate Summary**: For each day in selected range (7 AM to 7 AM):
   - **total_admissions**: Count of `in_dttm` to ICU within the day window
   - **census_7AM/7PM**: Count of patients present in ICU at 7 AM/7 PM
   - **total_discharges**: Count of `out_dttm` from ICU within the day window
   - **floor_transfers**: ICU discharges where next ADT location is ward/stepdown
   - **deaths_in_icu**: ICU discharges where `discharge_category='Expired'`
   - **discharges_to_hospice**: ICU discharges where `discharge_category='Hospice'`
   - **discharges_to_facility**: ICU discharges to SNF/LTACH/Rehab/Assisted Living

### Running the Dashboard

```bash
cd code
marimo edit app.py
```

The app will open in your browser with interactive controls.

## Expected Deliverable

A Marimo app (interactive Python notebook) that runs locally in the browser, displaying:
- Unit selection dropdown (dynamically populated from ICU locations)
- Reporting period selector (constrained to available data dates)
- Day-level overall summary dataframe
- Total admissions, daily census, discharges by disposition
- Bed strain and SOFA-2 scores (to be implemented)
- Three-column layout for LPV, SAT, and SBT metrics (to be implemented)
