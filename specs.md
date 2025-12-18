# CLIF ICU Quality Report Dashboard Set up

This markdown file describes the UI of the CLIF ICU Quality Report (CLIF-IQR)

## TITLE: CLIF ICU quality report

## (horizontal box across entire page- no title)
three columns within the box
column 1:
- unit (drop down menu where user selects `location_name`
- **total admissions**
- **daily census** (snapshot of 7 AM census)
column 2:
- **Reporting Period:** (selection interface where user selects calendar date to define date range, defaults to week)
- **total discharges** (broken down by floor transfers, deaths in ICU, discharges to hospices, discharges to facility)

column 3:
- **bed strain** (calculate percentage of max beds occupied every hour)
- **SOFA-2 score** (calculate max daily SOFA for each patient, display median and IQR) 

## (three columns for specific quality metrics)

### Lung-Protective Ventilation (LPV)

**Definition:**  
- Tidal volume ≤ 8 mL/kg PBW  
- Applied during invasive mechanical ventilation in **controlled** modes only, i.e. `mode_category` of `Assist Control-Volume Control`, `Pressure Control`, or `Pressure-Regulated Volume Control`

| Metric | Value |
|------|-------|
| LPV adherence (%) | **XX%** |
| Median VT (mL/kg PBW) | YY |

> *Derived from CLIF Respiratory Support table (hourly resolution).*

---

### Spontaneous awakening trials

For all patients receiving invasive mechanical ventilation at 7 AM, the daily rate of SAT are:

| Outcome | Rate |
|-------|------|
| Complete cessation of all analgesia and sedation | XX% |
| Cessation of sedation (stop propofol and benzodiazepine drips) | XX% |
| Dose reduction of sedation | YY% |

---

### Spontaneous breathing trials

For all patients receiving a controlled mode of invasive mechanical ventilation at 7 AM, the daily rate of SBT and extubation

| Outcome | Rate |
|-------|------|
| Any switch to pressure support < 10 cmH20| XX% |
| Successful extubation (remains extubated at 7 PM) | XX% |


