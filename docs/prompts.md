You are a clinical informatics expert with programming expertise. use your clif-icu skill to review the entirety of following jupyter notebook and summarize how SAT is defined in the notebook. Keep in mind that your next task would be to write a duckdb SQL script similar to @docs/sbt.sql that might borrow implementation details from the notebook, but you do not need to worry about that for now until I review your summary of implmentation details. Note that the notebook is hosted in a different directory which you should NOT fully navigate -- only review parts referenced by the notebook that might be useful.

/Users/wliao0504/code/clif/CLIF_rule_based_SAT_SBT_signature/code/01_SAT_standard.ipynb

7am as cutoff for each day.

cool now create a marimo notebook that start to fill in the blanks and generate the output df from the sat and sbt sql script. note that you can reference @output/intermediate/mimic_resp_processed_bf.parquet as the resp_p in sbt.sql. this is the waterfall-processed version that you should be using instead of the original respiratory support table data file. for all other tables you can refer to the path in @config/config.json   

let's rename code/sat_sbt_analysis.py to code/backend.py. in there add cells to calculate the following proportion as a percentage:
- denominator: total number of patient ventilator hours that are on a controlled mode, i.e. total number of patient hours where device_category is 'imv' and mode_category is in ('Assist Control-Volume Control', 'Pressure Control', 'Pressure-Regulated Volume Control').
- numerator: within the denominator, number of total patient hours that are considered low tidal volume, defined by tidal_volume_set < 8 cc/kg. since the current tidal_volume_set is in ml (tv_in_ml), you need to convert it to cc/kg (tv_in_cc_per_kg) using the following formula: 
- tv_in_cc_per_kg = tv_in_ml divided by IBW (ideal body weight in kg) where 
- for sex_category = 'Female', IBW = 45.5 + 0.9 × (height_cm - 152)
- for sex_category = 'Male', IBW = 50 + 0.9 × (height_cm - 152)

-----

You are a clinical informatics expert with programming expertise. use your clif-icu skill to update code/backend.py; code/sat.sql; code/sbt.sql scripts with the goal of better defining the cohort and revising the metrics calculations and expected output schema. In the marimo notebook, only add new cells when necessary; otherwise prioritize modifying existing cells -- in either case, make sure revision or addition of cells are performed under the correct relevant section of the marimo notebook and NOT misplaced in wrong location. Same applies to the sql scripts where existing CTEs can be modified or deleted if deemed irrelevant.

Recall that we want to calculate 3 metrics as an output of the backend.py which will be served to the front-end dashboard: 1. LPV, 2. SAT, 3. SBT. You can delete any irrelevant and now-outdated flags.

## User-defined cohort

Users are allowed to choose start_date, end_date, location (adt.location_name) as parameters in the UI. Thus our backend should be able to take in these 3 parameters to filter the data to a much smaller cohort on which any subsequent metrics will be calculated. That said, make sure when the backend first load the data, we directly apply filtering during import to only load the subset of data defined by time and location.

## SAT

Cohort: All patients receiving invasive mechanical ventilation (respiratory_support.device_category = 'imv') in an ICU (adt.location_category = 'icu') at 7 AM per day. Note that this is effectively the new eligibility criteria and only patients satisfying this criteria on that patient-day will be kept in the dataset and their metrics will be calculated.

Metrics: Note that all of the finalized metrics below are slightly modified versions of the original flags with the additional universal requirement that the flag must occur within the 7 AM to 7 PM window on that patient-day.
- sat_complete_cessation_N_7AM_7PM: Complete cessation of all analgesia and sedation = equivalent to the original `SAT_EHR_delivery` flag 
- sat_sedation_cessation_N_7AM_7PM: Cessation of sedation (stop propofol and benzodiazepine drips) = similar to the original `SAT_modified_delivery` flag
- sat_dose_reduction_N_7AM_7PM: Dose reduction of sedation = similar to the original `SAT_med_halved_rass_pos` flag but without the rass condition.

Output schema to STRICTLY FOLLOW: 
| Output path | Reference output schema |
|--------|-------|
| output/intermediate/sat_metrics.csv | specs/backend_outputs/sat_metrics_intermediate.csv |
| output/final/sat_metrics.csv | specs/backend_outputs/sat_metrics_final.csv |

### SBT

Cohort: All patients receiving a controlled mode (respiratory_support.mode_category IN ('Assist Control-Volume Control', 'Pressure Control', 'Pressure-Regulated Volume Control')) of invasive mechanical ventilation (respiratory_support.device_category = 'imv') in an ICU (adt.location_category = 'icu') at 7 AM per day. Note that this is effectively the new eligibility criteria and only patients satisfying this criteria on that patient-day will be kept in the dataset and their metrics will be calculated.

Metrics: Note that all of the finalized metrics below are slightly modified versions of the original flags with the additional universal requirement that the flag must occur within the 7 AM to 7 PM window on that patient-day.
- sbt_pressure_support_7AM_7PM: identical to the original `sbt_done` flag.
- any_extubation_7AM_7PM: similar to the original `_extub_1st` flag but do not need it to be the first extubation -- can be any extubation within the 7 AM to 7 PM window during that patient-day.
- sbt_successful_extubation_7PM: similar to the original `_success_extub` flag but with the additional condition that the patient remains extubated at 7 PM.

Output schema to STRICTLY FOLLOW: 
| Output path | Reference output schema |
|--------|-------|
| output/intermediate/sbt_metrics.csv | specs/backend_outputs/sbt_metrics_intermediate.csv |
| output/final/sbt_metrics.csv | specs/backend_outputs/sbt_metrics_final.csv |

### LPV

Cohort: All patients receiving a controlled mode (respiratory_support.mode_category IN ('Assist Control-Volume Control', 'Pressure Control', 'Pressure-Regulated Volume Control')) of invasive mechanical ventilation (respiratory_support.device_category = 'imv') in an ICU (adt.location_category = 'icu') at 7 AM per day.

Metrics: 
- total_controlled_IMV_hours: the original denominator
- ltv_hours: the original numerator

Output schema to STRICTLY FOLLOW: 
| Output path | Reference output schema |
|--------|-------|
| output/intermediate/lpv_metrics.csv | specs/backend_outputs/lpv_metrics_intermediate.csv |
| output/final/lpv_metrics.csv | specs/backend_outputs/lpv_metrics_final.csv |
