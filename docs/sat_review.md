# SAT (Spontaneous Awakening Trial) Definition Summary

## Overview

SAT is a clinical protocol for mechanically ventilated ICU patients receiving continuous sedation. The notebook defines multiple ways to detect SAT delivery from EHR data.

---

## Step 1: Eligibility Criteria (SAT-Eligible Days)

A patient-day is eligible for SAT when **ALL** of the following conditions are met for **≥4 consecutive hours** during the overnight window (**10 PM to 6 AM**):

1. **Device**: `device_category` = 'imv' (invasive mechanical ventilation)

2. **Sedation**: Active sedation with dose > 0 (any of: fentanyl, propofol, lorazepam, midazolam, hydromorphone, morphine)

3. **Location**: `location_category` = 'icu'

4. **No Paralytics**: `max_paralytics` ≤ 0 (no cisatracurium, vecuronium, rocuronium)

Key implementation detail: Uses gaps-and-islands technique to find contiguous blocks where all conditions are met, then checks if any block spans ≥4 hours.

---

## Step 2: SAT Delivery Detection (6 Methods)

Once a day is deemed eligible, the notebook evaluates SAT delivery using **6 different flag definitions**. All require the patient to remain on IMV and in ICU during the evaluation window.

### Flag 1: `SAT_EHR_delivery`

**Definition**: Complete cessation of ALL sedation medications for ≥30 minutes

- All 6 sedation meds (fentanyl, propofol, lorazepam, midazolam, hydromorphone, morphine) are either 0 or NaN for 30 minutes forward window

- Patient remains on IMV and in ICU throughout

### Flag 2: `SAT_modified_delivery`

**Definition**: Cessation of non-opioid sedatives for ≥30 minutes

- Only checks 3 meds: propofol, lorazepam, midazolam (allows opioids to continue)

- Patient remains on IMV and in ICU throughout

### Flag 3: `SAT_rass_nonneg_30`

**Definition**: RASS ≥ 0 throughout 30-minute forward window

- All RASS measurements in the next 30 minutes are non-negative (≥0)

- Patient remains on IMV and in ICU throughout

### Flag 4: `SAT_med_halved_rass_pos`

**Definition**: Sedation reduced by ≥50% AND patient awakens (RASS ≥0)

- Compare med doses in 30-min forward window to max doses in 30-min prior window

- All active medications must be ≤50% of their prior maximum

- Last RASS measurement within 45 minutes is ≥0

- Patient remains on IMV and in ICU throughout

### Flag 5: `SAT_no_meds_rass_pos_45`

**Definition**: No sedation meds AND positive awakening response within 45 minutes

- All sedation meds are 0 or NaN for 30 minutes

- Last RASS within 45 minutes is ≥0

- Patient remains on IMV and in ICU throughout

### Flag 6: `SAT_rass_first_neg_30_last45_nonneg`

**Definition**: RASS transitions from negative to non-negative (awakening observed)

- First RASS in prior 30 minutes is < 0 (sedated)

- Last RASS in next 45 minutes is ≥ 0 (awakened)

- Patient remains on IMV and in ICU throughout

---

## Key Data Elements Required

### From CLIF Tables:

| Table | Columns Needed |
|-------|----------------|
| respiratory_support | device_category, device_name, mode_category, recorded_dttm |
| adt | location_category |
| medication_admin_continuous | fentanyl, propofol, lorazepam, midazolam, hydromorphone, morphine, cisatracurium, vecuronium, rocuronium |
| patient_assessments | rass |
| hospitalization | hospitalization_id, admission_dttm, discharge_dttm |

### Derived Columns:

- `min_sedation_dose`: Minimum across all 6 sedation meds

- `min_sedation_dose_2`: Minimum excluding zeros (actual active dose)

- `min_sedation_dose_non_ops`: Minimum of non-opioid sedatives only

- `max_paralytics`: Maximum across paralytic agents

- `hosp_id_day_key`: Composite key for hospitalization + calendar day

---

## Time Windows Summary

| Window | Duration | Direction | Used For |
|--------|----------|-----------|----------|
| Eligibility window | 10 PM - 6 AM | overnight | Checking 4-hour continuous eligibility |
| Forward 30 min | 30 minutes | future | Checking sustained med cessation, RASS stability |
| Forward 45 min | 45 minutes | future | Checking RASS awakening response |
| Prior 30 min | 30 minutes | past | Baseline sedation levels for dose reduction calc |

---

## Comparison to SBT (from sbt.sql)

| Aspect | SAT | SBT |
|--------|-----|-----|
| Target | Sedation cessation/reduction | Ventilator weaning readiness |
| Primary criteria | Sedation meds + RASS | Vent mode (PS/CPAP) + settings (PEEP ≤8, PS ≤8) |
| Duration threshold | 4 hours eligibility, 30-45 min delivery | 30 minutes for SBT trial |
| Key outcome | RASS ≥ 0 (awakening) | Extubation success/failure |

---

## Implementation Notes for DuckDB SQL

1. **Forward-fill logic**: Medications and location are forward-filled by hospitalization_id

2. **Gaps-and-islands**: Use window functions with LAG and cumulative SUM to identify contiguous blocks

3. **Time windows**: Use INTERVAL arithmetic for 30/45-minute windows

4. **ASOF joins**: May be useful for finding most recent RASS relative to sedation events

5. **Event triggering**: SAT flags are evaluated when `rank_sedation` is not null (i.e., when sedation dose becomes 0)
