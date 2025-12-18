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