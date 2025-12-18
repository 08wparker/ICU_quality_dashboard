# *CLIF ICU Quality Dashboard*

## CLIF VERSION 

2.1

## Objective

Create a unit-level (physical location) ICU quality report of key ICU care metrics, such as adherence to low-tidal volume ventilation, spontaneous awakening trials, and spontaneous breathing trials

## Required CLIF tables and fields

Please refer to the online [CLIF data dictionary](https://clif-consortium.github.io/website/data-dictionary.html), [ETL tools](https://github.com/clif-consortium/CLIF/tree/main/etl-to-clif-resources), and [specific table contacts](https://github.com/clif-consortium/CLIF?tab=readme-ov-file#relational-clif) for more information on constructing the required tables and fields. 

Example:
The following tables are required:
1. **patient**: `patient_id`, `race_category`, `ethnicity_category`, `sex_category`
2. **hospitalization**: `patient_id`, `hospitalization_id`, `admission_dttm`, `discharge_dttm`, `age_at_admission`
3. **vitals**: `hospitalization_id`, `recorded_dttm`, `vital_category`, `vital_value`
   - `vital_category` = 'heart_rate', 'resp_rate', 'sbp', 'dbp', 'map', 'resp_rate', 'spo2'
4. **labs**: `hospitalization_id`, `lab_result_dttm`, `lab_category`, `lab_value`
   - `lab_category` = 'lactate'
5. **medication_admin_continuous**: `hospitalization_id`, `admin_dttm`, `med_name`, `med_category`, `med_dose`, `med_dose_unit`
   - `med_category` = "norepinephrine", "epinephrine", "phenylephrine", "vasopressin", "dopamine", "angiotensin", "nicardipine", "nitroprusside", "clevidipine", "cisatracurium"
6. **respiratory_support**: `hospitalization_id`, `recorded_dttm`, `device_category`, `mode_category`, `tracheostomy`, `fio2_set`, `lpm_set`, `resp_rate_set`, `peep_set`, `resp_rate_obs`

## Cohort identification
All patients admitted to the ICU defined by `location_name` in the dashboard will be included

## Expected Results

A marimo app that runs locally in your browser

## Detailed Instructions for running the project

## 1. Update `config/config.json` to include the path to your CLIF tables
Follow instructions in the [config/README.md](config/README.md) file for detailed configuration steps.

## 2. Set up the project environment

```
cd path_to_file/ICU_quality_dashboard
uv sync
```
Note: uv automatically creates virtual environments and manages dependencies. It generates required files like `uv.lock` for reproducible builds. For more details, see the [CLIF uv guide by Zewei Whiskey Liao](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-data-huddles/blob/main/notes/uv-and-conv-commits.md).

## 3. Run code

Detailed instructions on the code workflow are provided in the [code directory](code/README.md)

---


