-- SAT (Spontaneous Awakening Trial) Detection from CLIF Tables
-- Simplified version with 3 flags, evaluated within 7 AM - 7 PM window
--
-- Required input tables:
--   - resp_df: respiratory_support table with device_category, recorded_dttm
--   - meds_df: medication_admin_continuous pivoted wide with sedation columns
--   - adt_df: ADT table with location_category, location_name
--   - sat_cohort_df: Pre-defined cohort (patients on IMV + ICU at 7 AM each day)
--
-- Output: Patient-day level SAT flags (intermediate results)
--
-- Flags:
--   1. sat_complete_cessation_N_7AM_7PM: All sedation+analgesia stopped for 30+ min
--   2. sat_sedation_cessation_N_7AM_7PM: Propofol+benzos stopped for 30+ min (opioids ok)
--   3. sat_dose_reduction_N_7AM_7PM: Sedation reduced by >=50% vs prior 30 min

-- Step 1: Build unified timeline with forward-filled values
-- Only include events within 7 AM - 7 PM window
WITH base_times AS (
    SELECT hospitalization_id, recorded_dttm AS event_dttm FROM resp_df
    WHERE EXTRACT(HOUR FROM recorded_dttm) BETWEEN 7 AND 18
    UNION
    SELECT hospitalization_id, recorded_dttm AS event_dttm FROM meds_df
    WHERE EXTRACT(HOUR FROM recorded_dttm) BETWEEN 7 AND 18
    UNION
    SELECT hospitalization_id, in_dttm AS event_dttm FROM adt_df
    WHERE EXTRACT(HOUR FROM in_dttm) BETWEEN 7 AND 18
)

, t1 AS (
    SELECT
        bt.hospitalization_id
        , bt.event_dttm
        , bt.event_dttm::DATE AS day

        -- Respiratory support (forward-filled)
        , r.device_category

        -- Sedation medications (forward-filled, NULL treated as 0)
        , COALESCE(m.fentanyl, 0) AS fentanyl
        , COALESCE(m.propofol, 0) AS propofol
        , COALESCE(m.lorazepam, 0) AS lorazepam
        , COALESCE(m.midazolam, 0) AS midazolam
        , COALESCE(m.hydromorphone, 0) AS hydromorphone
        , COALESCE(m.morphine, 0) AS morphine

        -- Location (forward-filled)
        , a.location_category
        , a.location_name

    FROM base_times bt
    ASOF LEFT JOIN resp_df r
        ON r.hospitalization_id = bt.hospitalization_id
        AND r.recorded_dttm <= bt.event_dttm
    ASOF LEFT JOIN meds_df m
        ON m.hospitalization_id = bt.hospitalization_id
        AND m.recorded_dttm <= bt.event_dttm
    ASOF LEFT JOIN adt_df a
        ON a.hospitalization_id = bt.hospitalization_id
        AND a.in_dttm <= bt.event_dttm
)

-- Step 2: Filter to cohort and compute sedation metrics
, t2 AS (
    SELECT t1.*
        -- Check if ALL sedation+analgesia is active (any dose > 0)
        , CASE WHEN fentanyl > 0 OR propofol > 0 OR lorazepam > 0
               OR midazolam > 0 OR hydromorphone > 0 OR morphine > 0
          THEN 1 ELSE 0 END AS has_any_sedation

        -- Check if non-opioid sedatives are active (propofol, lorazepam, midazolam)
        , CASE WHEN propofol > 0 OR lorazepam > 0 OR midazolam > 0
          THEN 1 ELSE 0 END AS has_non_opioid_sedation

        -- Sum of all sedation doses (for dose reduction calculation)
        , (COALESCE(fentanyl, 0) + COALESCE(propofol, 0) + COALESCE(lorazepam, 0)
           + COALESCE(midazolam, 0) + COALESCE(hydromorphone, 0) + COALESCE(morphine, 0)
          ) AS total_sedation_dose

    FROM t1
    -- Filter to cohort: must be in sat_cohort_df for this patient-day
    WHERE EXISTS (
        SELECT 1 FROM sat_cohort_df c
        WHERE c.hospitalization_id = t1.hospitalization_id
          AND c.day = t1.day
    )
    -- Must remain on IMV and in ICU
    AND LOWER(t1.device_category) = 'imv'
    AND LOWER(t1.location_category) = 'icu'
)

-- Step 3: Detect cessation events and calculate dose reductions
, t3 AS (
    SELECT t2.*
        , LAG(has_any_sedation) OVER w AS prev_has_any_sedation
        , LAG(has_non_opioid_sedation) OVER w AS prev_has_non_opioid_sedation

        -- Complete cessation event: transition from any sedation to none
        , CASE
            WHEN LAG(has_any_sedation) OVER w = 1 AND has_any_sedation = 0
            THEN 1 ELSE 0
          END AS _complete_cessation_event

        -- Sedation cessation event: transition from non-opioid to none
        , CASE
            WHEN LAG(has_non_opioid_sedation) OVER w = 1 AND has_non_opioid_sedation = 0
            THEN 1 ELSE 0
          END AS _sedation_cessation_event

        -- Max dose in prior 30 minutes (for dose reduction baseline)
        , (
            SELECT MAX(t_pr.total_sedation_dose)
            FROM t2 t_pr
            WHERE t_pr.hospitalization_id = t2.hospitalization_id
              AND t_pr.event_dttm >= t2.event_dttm - INTERVAL 30 MINUTE
              AND t_pr.event_dttm < t2.event_dttm
          ) AS prior_30min_max_dose

    FROM t2
    WINDOW w AS (PARTITION BY hospitalization_id ORDER BY event_dttm)
)

-- Step 4: Calculate SAT flags at each event
, t_events AS (
    SELECT
        t3.hospitalization_id
        , t3.event_dttm
        , t3.day
        , t3.location_name
        , t3.has_any_sedation
        , t3.has_non_opioid_sedation
        , t3.total_sedation_dose
        , t3.prior_30min_max_dose
        , t3._complete_cessation_event
        , t3._sedation_cessation_event

        -- Flag 1: sat_complete_cessation_N_7AM_7PM
        -- All sedation+analgesia stopped for 30+ min within window
        , CASE
            WHEN t3._complete_cessation_event = 1
                AND NOT EXISTS (
                    SELECT 1 FROM t3 t_fw
                    WHERE t_fw.hospitalization_id = t3.hospitalization_id
                      AND t_fw.event_dttm > t3.event_dttm
                      AND t_fw.event_dttm <= t3.event_dttm + INTERVAL 30 MINUTE
                      AND t_fw.has_any_sedation = 1
                )
            THEN 1 ELSE 0
          END AS sat_complete_cessation_N_7AM_7PM

        -- Flag 2: sat_sedation_cessation_N_7AM_7PM
        -- Propofol+benzos stopped for 30+ min (opioids allowed)
        , CASE
            WHEN t3._sedation_cessation_event = 1
                AND NOT EXISTS (
                    SELECT 1 FROM t3 t_fw
                    WHERE t_fw.hospitalization_id = t3.hospitalization_id
                      AND t_fw.event_dttm > t3.event_dttm
                      AND t_fw.event_dttm <= t3.event_dttm + INTERVAL 30 MINUTE
                      AND t_fw.has_non_opioid_sedation = 1
                )
            THEN 1 ELSE 0
          END AS sat_sedation_cessation_N_7AM_7PM

        -- Flag 3: sat_dose_reduction_N_7AM_7PM
        -- Sedation reduced by >= 50% vs prior 30 min max
        , CASE
            WHEN t3.prior_30min_max_dose > 0
                AND t3.total_sedation_dose <= 0.5 * t3.prior_30min_max_dose
            THEN 1 ELSE 0
          END AS sat_dose_reduction_N_7AM_7PM

    FROM t3
)

-- Step 5: Aggregate to patient-day level (intermediate output)
, sat_intermediate AS (
    SELECT
        h.patient_id
        , e.hospitalization_id
        , e.location_name
        , e.day
        , MAX(sat_complete_cessation_N_7AM_7PM) AS sat_complete_cessation_N_7AM_7PM
        , MAX(sat_sedation_cessation_N_7AM_7PM) AS sat_sedation_cessation_N_7AM_7PM
        , MAX(sat_dose_reduction_N_7AM_7PM) AS sat_dose_reduction_N_7AM_7PM
        -- First event time for each flag (for audit/analysis)
        , MIN(CASE WHEN sat_complete_cessation_N_7AM_7PM = 1 THEN event_dttm END) AS sat_complete_cessation_first_dttm
        , MIN(CASE WHEN sat_sedation_cessation_N_7AM_7PM = 1 THEN event_dttm END) AS sat_sedation_cessation_first_dttm
        , MIN(CASE WHEN sat_dose_reduction_N_7AM_7PM = 1 THEN event_dttm END) AS sat_dose_reduction_first_dttm
    FROM t_events e
    LEFT JOIN hosp_df h ON e.hospitalization_id = h.hospitalization_id
    GROUP BY h.patient_id, e.hospitalization_id, e.location_name, e.day
)

-- Final output: intermediate patient-day level results
FROM sat_intermediate
ORDER BY hospitalization_id, day;
