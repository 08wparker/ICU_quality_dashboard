# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the CLIF ICU Quality Dashboard project (CLIF-IQR), designed to create a unit-level ICU quality report using the Common Longitudinal ICU data Format (CLIF) version 2.1. The dashboard tracks key ICU care metrics including adherence to low-tidal volume ventilation, spontaneous awakening trials (SAT), and spontaneous breathing trials (SBT).

## Environment Setup

### Python (Preferred Method)
```bash
uv init clif-icu-quality
cd clif-icu-quality
```

Note: `uv` automatically creates virtual environments and manages dependencies. It generates required files like `uv.lock` for reproducible builds.

### R Environment
This project uses `renv` for R package management. The `renv.lock` file specifies all R dependencies. To restore the R environment:
```r
renv::restore()
```

## CLIF Schema and Skills

This project uses the **clif-icu** skill plugin to access official CLIF schemas and documentation.

### Accessing CLIF Schemas

Use the clif-icu skill to get table schemas:

```python
from clif_icu import schema

# Get table schema
adt_schema = schema.get_table_schema('adt')
hosp_schema = schema.get_table_schema('hospitalization')
```

### Key CLIF Tables for This Project

**ADT Table** (Admission/Discharge/Transfer):
- Composite keys: `hospitalization_id` + `in_dttm`
- Key columns: `location_name`, `location_category` (icu/ward/stepdown/etc.), `location_type`, `in_dttm`, `out_dttm`
- Use for: ICU census, admissions, transfers, length of stay

**Hospitalization Table**:
- Primary key: `hospitalization_id`
- Key columns: `admission_dttm`, `discharge_dttm`, `discharge_category`, `age_at_admission`
- Discharge categories: `Expired`, `Hospice`, `Skilled Nursing Facility (SNF)`, `Long Term Care Hospital (LTACH)`, `Acute Inpatient Rehab Facility`, `Home`, etc.
- Use for: Hospital discharge disposition, overall hospitalization metrics

### Important: CLIF Discharge Information

Discharge disposition is **NOT** in the ADT table. To get discharge information:
1. Merge ADT data with hospitalization table on `hospitalization_id`
2. Use `discharge_category` from hospitalization table
3. Check `discharge_dttm` to match timing

## Configuration

Before running any code:

1. Copy the configuration template:
   - Navigate to `config/` directory
   - Rename `config_template.json` to `config.json`

2. Update `config.json` with site-specific settings:
   - `site_name`: Your site/institution name
   - `tables_path`: Absolute path to your CLIF tables
   - `file_type`: Format of your data files (`csv`, `parquet`, or `fst`)

**Important**: `config.json` is git-ignored and will not be committed to the repository.

## Code Execution Workflow

The project follows a structured 4-step pipeline:

### 1. Cohort Identification (`01_cohort_identification_template.py`)
- Applies inclusion/exclusion criteria
- Filters for adults (age >= 18) admitted during specified date range
- Outputs:
  - `cohort_ids`: List of unique `hospitalization_id` values
  - `cohort_data`: Filtered study cohort data
  - `cohort_summary`: Summary table describing the cohort

### 2. Quality Control Checks (`02_project_quality_checks_template.py`)
- Performs project-specific quality control on cohort data
- Input: `cohort_data`
- Output: Quality-checked data ready for outlier handling

### 3. Outlier Handling (`03_outlier_handling_template.py`)
- Replaces outliers with NA based on predefined thresholds in `outlier-thresholds/` directory
- Available threshold files:
  - `outlier_thresholds_adults_vitals.csv`
  - `outlier_thresholds_labs.csv`
  - `outlier_thresholds_respiratory_support.csv`
- Input: Quality-checked data
- Output: `cleaned_cohort_data`

### 4. Analysis (`04_project_analysis_template.py`)
- Main analysis code for generating quality metrics
- Input: `cleaned_cohort_data`
- Output: Statistical results, figures, and tables in `output/final/`

## Project Architecture

### Dual-Language Support
The project supports both R and Python implementations:
- **Python templates**: Located in `code/templates/Python/`
- **R templates**: Referenced in `code/README.md` (templates directory currently removed)
- **Utility modules**: Both `utils/config.py` and `utils/config.R` load the same JSON configuration

### Configuration Loading
- Python: `from utils import config` (uses `utils/config.py`)
- R: `source("utils/config.R")` (uses jsonlite package)

Both load from `config/config.json` and provide the same parameters.

### Data I/O Functions

**R** (`utils/outlier_handler.R`):
- `read_data(filepath, filetype)`: Reads csv/parquet/fst files using data.table/arrow
- `write_data(data, filepath, filetype)`: Writes data in specified format
- `replace_outliers_with_na_long(df, df_outlier_thresholds, category_variable, numeric_variable)`: Replaces values outside thresholds with NA
- `generate_summary_stats(data, category_variable, numeric_variable)`: Generates N, Min, Max, Mean, Median, and quartiles

**Python** (`code/templates/Python/01_cohort_identification_template.py`):
- `read_data(filepath, filetype)`: Reads csv/parquet files and reports load time and memory usage

## Required CLIF Tables

The dashboard requires these CLIF tables with specific fields:

1. **patient**: `patient_id`, `race_category`, `ethnicity_category`, `sex_category`
2. **hospitalization**: `patient_id`, `hospitalization_id`, `admission_dttm`, `discharge_dttm`, `age_at_admission`
3. **vitals**: `hospitalization_id`, `recorded_dttm`, `vital_category`, `vital_value`
   - Required categories: `heart_rate`, `resp_rate`, `sbp`, `dbp`, `map`, `spo2`
4. **labs**: `hospitalization_id`, `lab_result_dttm`, `lab_category`, `lab_value`
   - Required category: `lactate`
5. **medication_admin_continuous**: `hospitalization_id`, `admin_dttm`, `med_name`, `med_category`, `med_dose`, `med_dose_unit`
   - Required categories: vasopressors (norepinephrine, epinephrine, etc.), sedatives, paralytics
6. **respiratory_support**: `hospitalization_id`, `recorded_dttm`, `device_category`, `mode_category`, `tracheostomy`, `fio2_set`, `lpm_set`, `resp_rate_set`, `peep_set`, `resp_rate_obs`

See the [CLIF data dictionary](https://clif-consortium.github.io/website/data-dictionary.html) for complete specifications.

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
