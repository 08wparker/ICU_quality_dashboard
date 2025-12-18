-- SAT (Spontaneous Awakening Trial) Detection from CLIF Tables
-- This script implements 6 SAT delivery detection flags based on EHR data
--
-- Required input tables (raw CLIF tables):
--   - resp_df: respiratory_support table with device_category, recorded_dttm
--   - meds_df: medication_admin_continuous pivoted wide with sedation/paralytic columns
--   - rass_df: patient_assessments filtered to RASS
--   - adt_df: ADT table with location_category
--   - hosp_df: hospitalization table
--
-- Output: Event-level (t_events) and Day-level (t_days) SAT flags

-- Step 0: Create base timeline by combining all event timestamps
WITH base_times AS (
    SELECT hospitalization_id, recorded_dttm AS event_dttm FROM resp_df
    UNION
    SELECT hospitalization_id, recorded_dttm AS event_dttm FROM meds_df
    UNION
    SELECT hospitalization_id, recorded_dttm AS event_dttm FROM rass_df
    UNION
    SELECT hospitalization_id, in_dttm AS event_dttm FROM adt_df
)

-- Step 1: Build unified timeline with forward-filled values
, t1 AS (
    SELECT
        bt.hospitalization_id
        , bt.event_dttm
        , bt.event_dttm::DATE AS event_date
        , CONCAT(bt.hospitalization_id, '_', bt.event_dttm::DATE) AS hosp_id_day_key

        -- Respiratory support (forward-filled)
        , r.device_category

        -- Sedation medications (forward-filled, NULL treated as 0 for comparison)
        , COALESCE(m.fentanyl, 0) AS fentanyl
        , COALESCE(m.propofol, 0) AS propofol
        , COALESCE(m.lorazepam, 0) AS lorazepam
        , COALESCE(m.midazolam, 0) AS midazolam
        , COALESCE(m.hydromorphone, 0) AS hydromorphone
        , COALESCE(m.morphine, 0) AS morphine

        -- Paralytic medications (forward-filled)
        , COALESCE(m.cisatracurium, 0) AS cisatracurium
        , COALESCE(m.vecuronium, 0) AS vecuronium
        , COALESCE(m.rocuronium, 0) AS rocuronium

        -- RASS score
        , ra.rass

        -- Location (forward-filled)
        , a.location_category

    FROM base_times bt
    ASOF LEFT JOIN resp_df r
        ON r.hospitalization_id = bt.hospitalization_id
        AND r.recorded_dttm <= bt.event_dttm
    ASOF LEFT JOIN meds_df m
        ON m.hospitalization_id = bt.hospitalization_id
        AND m.recorded_dttm <= bt.event_dttm
    ASOF LEFT JOIN rass_df ra
        ON ra.hospitalization_id = bt.hospitalization_id
        AND ra.recorded_dttm <= bt.event_dttm
    ASOF LEFT JOIN adt_df a
        ON a.hospitalization_id = bt.hospitalization_id
        AND a.in_dttm <= bt.event_dttm
)

-- Step 2: Compute derived sedation/paralytic metrics
, t2 AS (
    SELECT *
        -- Minimum sedation dose (any active sedation)
        , LEAST(
            NULLIF(fentanyl, 0),
            NULLIF(propofol, 0),
            NULLIF(lorazepam, 0),
            NULLIF(midazolam, 0),
            NULLIF(hydromorphone, 0),
            NULLIF(morphine, 0)
        ) AS min_sedation_dose_active

        -- Check if ANY sedation is active (dose > 0)
        , CASE WHEN fentanyl > 0 OR propofol > 0 OR lorazepam > 0
               OR midazolam > 0 OR hydromorphone > 0 OR morphine > 0
          THEN 1 ELSE 0 END AS has_active_sedation

        -- Non-opioid sedatives only (propofol, lorazepam, midazolam)
        , CASE WHEN propofol > 0 OR lorazepam > 0 OR midazolam > 0
          THEN 1 ELSE 0 END AS has_active_non_opioid_sedation

        -- Max paralytic dose
        , GREATEST(
            COALESCE(cisatracurium, 0),
            COALESCE(vecuronium, 0),
            COALESCE(rocuronium, 0)
        ) AS max_paralytics

        -- All sedation meds are zero/null
        , CASE WHEN fentanyl <= 0 AND propofol <= 0 AND lorazepam <= 0
               AND midazolam <= 0 AND hydromorphone <= 0 AND morphine <= 0
          THEN 1 ELSE 0 END AS all_sedation_zero

        -- Non-opioid sedatives are zero/null
        , CASE WHEN propofol <= 0 AND lorazepam <= 0 AND midazolam <= 0
          THEN 1 ELSE 0 END AS non_opioid_sedation_zero

    FROM t1
)

-- Step 3: Identify SAT eligibility condition at each timestamp
-- Eligible when: IMV + ICU + active sedation + no paralytics
, t3 AS (
    SELECT *
        , CASE
            WHEN LOWER(device_category) = 'imv'
                AND LOWER(location_category) = 'icu'
                AND has_active_sedation = 1
                AND max_paralytics <= 0
            THEN 1 ELSE 0
          END AS _eligibility_condition
    FROM t2
)

-- Step 4: Gaps-and-islands to find contiguous eligibility blocks
, t4 AS (
    SELECT *
        , CASE
            WHEN _eligibility_condition IS DISTINCT FROM LAG(_eligibility_condition) OVER w
            THEN 1 ELSE 0
          END AS _eligibility_change
    FROM t3
    WINDOW w AS (PARTITION BY hospitalization_id ORDER BY event_dttm)
)

, t5 AS (
    SELECT *
        , SUM(_eligibility_change) OVER w AS _eligibility_block_id
    FROM t4
    WINDOW w AS (PARTITION BY hospitalization_id ORDER BY event_dttm)
)

-- Step 5: Calculate duration of each eligibility block
, eligibility_blocks AS (
    SELECT
        hospitalization_id
        , _eligibility_block_id
        , _eligibility_condition
        , MIN(event_dttm) AS block_start_dttm
        , MAX(event_dttm) AS block_end_dttm
    FROM t5
    WHERE _eligibility_condition = 1
    GROUP BY hospitalization_id, _eligibility_block_id, _eligibility_condition
)

, eligibility_blocks_with_duration AS (
    SELECT *
        , LEAD(block_start_dttm) OVER w AS next_block_start
        , COALESCE(next_block_start, block_end_dttm) AS effective_end_dttm
        , DATE_DIFF('minute', block_start_dttm, effective_end_dttm) AS block_duration_mins
    FROM eligibility_blocks
    WINDOW w AS (PARTITION BY hospitalization_id ORDER BY _eligibility_block_id)
)

-- Step 6: Check 4-hour eligibility in overnight window (10 PM - 6 AM)
-- A day is eligible if there's a 4+ hour block overlapping the overnight window
, overnight_eligibility AS (
    SELECT DISTINCT
        e.hospitalization_id
        , e.block_start_dttm::DATE + INTERVAL 1 DAY AS eligible_date  -- The "next day" that this overnight qualifies
        , CONCAT(e.hospitalization_id, '_', (e.block_start_dttm::DATE + INTERVAL 1 DAY)) AS hosp_id_day_key
        , 1 AS sat_eligible
    FROM eligibility_blocks_with_duration e
    WHERE e.block_duration_mins >= 240  -- 4 hours
      AND (
          -- Block overlaps with 10 PM - 6 AM window
          -- 10 PM of previous day to 6 AM of current day
          (EXTRACT(HOUR FROM e.block_start_dttm) >= 22 OR EXTRACT(HOUR FROM e.block_start_dttm) < 6)
          OR (EXTRACT(HOUR FROM e.effective_end_dttm) >= 22 OR EXTRACT(HOUR FROM e.effective_end_dttm) < 6)
          OR e.block_duration_mins >= 480  -- 8+ hours spans overnight anyway
      )
)

-- Step 7: Detect sedation cessation events (when sedation goes to zero)
, t6 AS (
    SELECT t5.*
        , oe.sat_eligible
        , LAG(has_active_sedation) OVER w AS prev_has_sedation
        , LAG(has_active_non_opioid_sedation) OVER w AS prev_has_non_opioid_sedation
        -- Sedation cessation event: transition from active to zero
        , CASE
            WHEN LAG(has_active_sedation) OVER w = 1 AND has_active_sedation = 0
            THEN 1 ELSE 0
          END AS _sedation_cessation_event
        -- Non-opioid cessation event
        , CASE
            WHEN LAG(has_active_non_opioid_sedation) OVER w = 1 AND has_active_non_opioid_sedation = 0
            THEN 1 ELSE 0
          END AS _non_opioid_cessation_event
    FROM t5
    LEFT JOIN overnight_eligibility oe
        ON t5.hosp_id_day_key = oe.hosp_id_day_key
    WINDOW w AS (PARTITION BY t5.hospitalization_id ORDER BY event_dttm)
)

-- Step 8: Compute SAT flags using window functions for time-based lookups
-- For each potential SAT event, check conditions in forward/backward windows
, t_events AS (
    SELECT
        t6.hospitalization_id
        , t6.event_dttm
        , t6.event_date
        , t6.hosp_id_day_key
        , t6.device_category
        , t6.location_category
        , t6.fentanyl, t6.propofol, t6.lorazepam, t6.midazolam, t6.hydromorphone, t6.morphine
        , t6.rass
        , t6.has_active_sedation
        , t6.all_sedation_zero
        , t6.non_opioid_sedation_zero
        , t6.max_paralytics
        , t6.sat_eligible
        , t6._sedation_cessation_event
        , t6._non_opioid_cessation_event

        -- Flag 1: SAT_EHR_delivery
        -- All sedation meds = 0 for 30 min forward window, patient on IMV+ICU
        , CASE
            WHEN t6.sat_eligible = 1
                AND t6._sedation_cessation_event = 1
                AND LOWER(t6.device_category) = 'imv'
                AND LOWER(t6.location_category) = 'icu'
                AND NOT EXISTS (
                    SELECT 1 FROM t6 t_fw
                    WHERE t_fw.hospitalization_id = t6.hospitalization_id
                      AND t_fw.event_dttm > t6.event_dttm
                      AND t_fw.event_dttm <= t6.event_dttm + INTERVAL 30 MINUTE
                      AND (t_fw.has_active_sedation = 1
                           OR LOWER(t_fw.device_category) != 'imv'
                           OR LOWER(t_fw.location_category) != 'icu')
                )
            THEN 1 ELSE 0
          END AS SAT_EHR_delivery

        -- Flag 2: SAT_modified_delivery
        -- Non-opioid sedatives (propofol, lorazepam, midazolam) = 0 for 30 min, patient on IMV+ICU
        , CASE
            WHEN t6.sat_eligible = 1
                AND t6._non_opioid_cessation_event = 1
                AND LOWER(t6.device_category) = 'imv'
                AND LOWER(t6.location_category) = 'icu'
                AND NOT EXISTS (
                    SELECT 1 FROM t6 t_fw
                    WHERE t_fw.hospitalization_id = t6.hospitalization_id
                      AND t_fw.event_dttm > t6.event_dttm
                      AND t_fw.event_dttm <= t6.event_dttm + INTERVAL 30 MINUTE
                      AND (t_fw.has_active_non_opioid_sedation = 1
                           OR LOWER(t_fw.device_category) != 'imv'
                           OR LOWER(t_fw.location_category) != 'icu')
                )
            THEN 1 ELSE 0
          END AS SAT_modified_delivery

        -- Flag 3: SAT_rass_nonneg_30
        -- All RASS measurements in next 30 min are >= 0
        , CASE
            WHEN t6.sat_eligible = 1
                AND t6._sedation_cessation_event = 1
                AND LOWER(t6.device_category) = 'imv'
                AND LOWER(t6.location_category) = 'icu'
                AND EXISTS (
                    SELECT 1 FROM t6 t_fw
                    WHERE t_fw.hospitalization_id = t6.hospitalization_id
                      AND t_fw.event_dttm >= t6.event_dttm
                      AND t_fw.event_dttm <= t6.event_dttm + INTERVAL 30 MINUTE
                      AND t_fw.rass IS NOT NULL
                )
                AND NOT EXISTS (
                    SELECT 1 FROM t6 t_fw
                    WHERE t_fw.hospitalization_id = t6.hospitalization_id
                      AND t_fw.event_dttm >= t6.event_dttm
                      AND t_fw.event_dttm <= t6.event_dttm + INTERVAL 30 MINUTE
                      AND t_fw.rass IS NOT NULL
                      AND t_fw.rass < 0
                )
            THEN 1 ELSE 0
          END AS SAT_rass_nonneg_30

        -- Flag 4: SAT_med_halved_rass_pos
        -- Sedation reduced by >= 50% from prior 30 min AND last RASS in 45 min >= 0
        , CASE
            WHEN t6.sat_eligible = 1
                AND t6._sedation_cessation_event = 1
                AND LOWER(t6.device_category) = 'imv'
                AND LOWER(t6.location_category) = 'icu'
                -- Check if last RASS in 45 min is >= 0
                AND (
                    SELECT t_fw.rass
                    FROM t6 t_fw
                    WHERE t_fw.hospitalization_id = t6.hospitalization_id
                      AND t_fw.event_dttm >= t6.event_dttm
                      AND t_fw.event_dttm <= t6.event_dttm + INTERVAL 45 MINUTE
                      AND t_fw.rass IS NOT NULL
                    ORDER BY t_fw.event_dttm DESC
                    LIMIT 1
                ) >= 0
                -- Check if meds are halved (forward max <= 50% of prior max)
                AND (
                    SELECT GREATEST(
                        COALESCE(MAX(t_fw.fentanyl), 0),
                        COALESCE(MAX(t_fw.propofol), 0),
                        COALESCE(MAX(t_fw.lorazepam), 0),
                        COALESCE(MAX(t_fw.midazolam), 0),
                        COALESCE(MAX(t_fw.hydromorphone), 0),
                        COALESCE(MAX(t_fw.morphine), 0)
                    )
                    FROM t6 t_fw
                    WHERE t_fw.hospitalization_id = t6.hospitalization_id
                      AND t_fw.event_dttm > t6.event_dttm
                      AND t_fw.event_dttm <= t6.event_dttm + INTERVAL 30 MINUTE
                ) <= 0.5 * (
                    SELECT GREATEST(
                        COALESCE(MAX(t_pr.fentanyl), 0),
                        COALESCE(MAX(t_pr.propofol), 0),
                        COALESCE(MAX(t_pr.lorazepam), 0),
                        COALESCE(MAX(t_pr.midazolam), 0),
                        COALESCE(MAX(t_pr.hydromorphone), 0),
                        COALESCE(MAX(t_pr.morphine), 0)
                    )
                    FROM t6 t_pr
                    WHERE t_pr.hospitalization_id = t6.hospitalization_id
                      AND t_pr.event_dttm >= t6.event_dttm - INTERVAL 30 MINUTE
                      AND t_pr.event_dttm < t6.event_dttm
                )
            THEN 1 ELSE 0
          END AS SAT_med_halved_rass_pos

        -- Flag 5: SAT_no_meds_rass_pos_45
        -- No sedation meds for 30 min AND last RASS in 45 min >= 0
        , CASE
            WHEN t6.sat_eligible = 1
                AND t6._sedation_cessation_event = 1
                AND LOWER(t6.device_category) = 'imv'
                AND LOWER(t6.location_category) = 'icu'
                -- No meds for 30 min (same as SAT_EHR_delivery condition)
                AND NOT EXISTS (
                    SELECT 1 FROM t6 t_fw
                    WHERE t_fw.hospitalization_id = t6.hospitalization_id
                      AND t_fw.event_dttm > t6.event_dttm
                      AND t_fw.event_dttm <= t6.event_dttm + INTERVAL 30 MINUTE
                      AND t_fw.has_active_sedation = 1
                )
                -- Last RASS in 45 min >= 0
                AND (
                    SELECT t_fw.rass
                    FROM t6 t_fw
                    WHERE t_fw.hospitalization_id = t6.hospitalization_id
                      AND t_fw.event_dttm >= t6.event_dttm
                      AND t_fw.event_dttm <= t6.event_dttm + INTERVAL 45 MINUTE
                      AND t_fw.rass IS NOT NULL
                    ORDER BY t_fw.event_dttm DESC
                    LIMIT 1
                ) >= 0
            THEN 1 ELSE 0
          END AS SAT_no_meds_rass_pos_45

        -- Flag 6: SAT_rass_first_neg_30_last45_nonneg
        -- First RASS in prior 30 min < 0 AND last RASS in next 45 min >= 0
        , CASE
            WHEN t6.sat_eligible = 1
                AND t6._sedation_cessation_event = 1
                AND LOWER(t6.device_category) = 'imv'
                AND LOWER(t6.location_category) = 'icu'
                -- First RASS in prior 30 min < 0
                AND (
                    SELECT t_pr.rass
                    FROM t6 t_pr
                    WHERE t_pr.hospitalization_id = t6.hospitalization_id
                      AND t_pr.event_dttm >= t6.event_dttm - INTERVAL 30 MINUTE
                      AND t_pr.event_dttm < t6.event_dttm
                      AND t_pr.rass IS NOT NULL
                    ORDER BY t_pr.event_dttm ASC
                    LIMIT 1
                ) < 0
                -- Last RASS in next 45 min >= 0
                AND (
                    SELECT t_fw.rass
                    FROM t6 t_fw
                    WHERE t_fw.hospitalization_id = t6.hospitalization_id
                      AND t_fw.event_dttm >= t6.event_dttm
                      AND t_fw.event_dttm <= t6.event_dttm + INTERVAL 45 MINUTE
                      AND t_fw.rass IS NOT NULL
                    ORDER BY t_fw.event_dttm DESC
                    LIMIT 1
                ) >= 0
            THEN 1 ELSE 0
          END AS SAT_rass_first_neg_30_last45_nonneg

    FROM t6
)

-- Step 9: Aggregate to day level
, t_days AS (
    SELECT
        hospitalization_id
        , event_date
        , hosp_id_day_key
        , MAX(sat_eligible) AS sat_eligible
        , MAX(SAT_EHR_delivery) AS SAT_EHR_delivery
        , MAX(SAT_modified_delivery) AS SAT_modified_delivery
        , MAX(SAT_rass_nonneg_30) AS SAT_rass_nonneg_30
        , MAX(SAT_med_halved_rass_pos) AS SAT_med_halved_rass_pos
        , MAX(SAT_no_meds_rass_pos_45) AS SAT_no_meds_rass_pos_45
        , MAX(SAT_rass_first_neg_30_last45_nonneg) AS SAT_rass_first_neg_30_last45_nonneg
        -- First event time for each flag (for timing analysis)
        , MIN(CASE WHEN SAT_EHR_delivery = 1 THEN event_dttm END) AS SAT_EHR_delivery_first_dttm
        , MIN(CASE WHEN SAT_modified_delivery = 1 THEN event_dttm END) AS SAT_modified_delivery_first_dttm
    FROM t_events
    GROUP BY hospitalization_id, event_date, hosp_id_day_key
)

-- Final output: Select either t_events or t_days based on your needs
-- For event-level analysis:
-- FROM t_events ORDER BY hospitalization_id, event_dttm;

-- For day-level analysis:
FROM t_days
WHERE sat_eligible = 1
ORDER BY hospitalization_id, event_date;
