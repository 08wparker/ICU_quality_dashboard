You are a clinical informatics expert with programming expertise. use your clif-icu skill to review the entirety of following jupyter notebook and summarize how SAT is defined in the notebook. Keep in mind that your next task would be to write a duckdb SQL script similar to @docs/sbt.sql that might borrow implementation details from the notebook, but you do not need to worry about that for now until I review your summary of implmentation details. Note that the notebook is hosted in a different directory which you should NOT fully navigate -- only review parts referenced by the notebook that might be useful.

/Users/wliao0504/code/clif/CLIF_rule_based_SAT_SBT_signature/code/01_SAT_standard.ipynb

7am as cutoff for each day.

cool now create a marimo notebook that start to fill in the blanks and generate the output df from the sat and sbt sql script. note that you can reference @output/intermediate/mimic_resp_processed_bf.parquet as the resp_p in sbt.sql. this is the waterfall-processed version that you should be using instead of the original respiratory support table data file. for all other tables you can refer to the path in @config/config.json   