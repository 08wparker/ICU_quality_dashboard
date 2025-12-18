-- SBT (Spontaneous Breathing Trial) Detection from CLIF Tables
-- Simplified version with 3 flags, evaluated within 7 AM - 7 PM window
--
-- Required input tables:
--   - resp_df: respiratory_support table with device_category, mode_category, etc.
--   - adt_df: ADT table with location_category, location_name
--   - hosp_df: hospitalization table with patient_id
--   - sbt_cohort_df: Pre-defined cohort (patients on controlled-mode IMV + ICU at 7 AM each day)
--
-- Output: Patient-day level SBT flags (intermediate results)
--
-- Flags:
--   1. sbt_pressure_support_7AM_7PM: PS/CPAP mode with PEEP <= 8 and PS <= 8 for 30+ min
--   2. any_extubation_7AM_7PM: Any extubation event during the window
--   3. sbt_successful_extubation_7PM: Extubated and still not on IMV at 7 PM

-- Step 1: Build timeline from respiratory data within 7 AM - 7 PM window
WITH t1 AS (
    SELECT
        r.hospitalization_id
        , r.recorded_dttm AS event_dttm
        , r.recorded_dttm::DATE AS day
        , r.device_category
        , r.device_name
        , r.mode_category
        , r.mode_name
        , COALESCE(r.peep_set, 0) AS peep_set
        , COALESCE(r.pressure_support_set, 0) AS pressure_support_set
        , COALESCE(r.tracheostomy, 0) AS tracheostomy

        -- Location (forward-filled via ASOF join)
        , a.location_category
        , a.location_name

        -- SBT state: PS/CPAP mode with low settings OR T-piece
        , CASE
            WHEN (LOWER(r.mode_category) = 'pressure support/cpap'
                  AND COALESCE(r.peep_set, 0) <= 8
                  AND COALESCE(r.pressure_support_set, 0) <= 8)
                OR REGEXP_MATCHES(LOWER(r.device_name), 't1[\s_-]?piece')
            THEN 1 ELSE 0
          END AS _sbt_state

        -- Detect intubation event (transition TO imv)
        , CASE
            WHEN LAG(r.device_category) OVER w IS DISTINCT FROM 'imv'
                AND LOWER(r.device_category) = 'imv'
            THEN 1 ELSE 0
          END AS _intub

        -- Detect extubation event (transition FROM imv)
        , CASE
            WHEN LOWER(LAG(r.device_category) OVER w) = 'imv'
                AND LOWER(r.device_category) IS DISTINCT FROM 'imv'
            THEN 1 ELSE 0
          END AS _extub

    FROM resp_df r
    ASOF LEFT JOIN adt_df a
        ON a.hospitalization_id = r.hospitalization_id
        AND a.in_dttm <= r.recorded_dttm
    WHERE EXTRACT(HOUR FROM r.recorded_dttm) BETWEEN 7 AND 18
    WINDOW w AS (PARTITION BY r.hospitalization_id ORDER BY r.recorded_dttm)
)

-- Step 2: Filter to cohort (controlled-mode IMV + ICU at 7 AM)
, t2 AS (
    SELECT t1.*
        -- Track SBT state changes for gaps-and-islands
        , CASE
            WHEN _sbt_state IS DISTINCT FROM LAG(_sbt_state) OVER w
            THEN 1 ELSE 0
          END AS _chg_sbt_state
    FROM t1
    WHERE EXISTS (
        SELECT 1 FROM sbt_cohort_df c
        WHERE c.hospitalization_id = t1.hospitalization_id
          AND c.day = t1.day
    )
    AND LOWER(t1.location_category) = 'icu'
    WINDOW w AS (PARTITION BY hospitalization_id ORDER BY event_dttm)
)

-- Step 3: Assign block IDs for SBT state periods
, t3 AS (
    SELECT t2.*
        , SUM(_chg_sbt_state) OVER w AS _block_id
    FROM t2
    WINDOW w AS (PARTITION BY hospitalization_id ORDER BY event_dttm)
)

-- Step 4: Calculate duration for each SBT block
, sbt_blocks AS (
    SELECT
        hospitalization_id
        , day
        , _block_id
        , _sbt_state
        , MIN(event_dttm) AS block_start_dttm
        , MAX(event_dttm) AS block_end_dttm
    FROM t3
    WHERE _sbt_state = 1  -- Only analyze actual SBT blocks
    GROUP BY hospitalization_id, day, _block_id, _sbt_state
)

, sbt_blocks_with_duration AS (
    SELECT b.*
        , LEAD(block_start_dttm) OVER w AS next_block_start
        , COALESCE(next_block_start, block_end_dttm) AS effective_end_dttm
        , DATE_DIFF('minute', block_start_dttm, effective_end_dttm) AS block_duration_mins
    FROM sbt_blocks b
    WINDOW w AS (PARTITION BY hospitalization_id ORDER BY _block_id)
)

-- Step 5: Check device status at 7 PM for extubation success
, status_at_7pm AS (
    SELECT DISTINCT
        r.hospitalization_id
        , r.recorded_dttm::DATE AS day
        , FIRST_VALUE(r.device_category) OVER w AS device_at_7pm
    FROM resp_df r
    WHERE EXTRACT(HOUR FROM r.recorded_dttm) <= 19  -- Up to 7 PM
    WINDOW w AS (
        PARTITION BY r.hospitalization_id, r.recorded_dttm::DATE
        ORDER BY ABS(DATE_DIFF('minute', r.recorded_dttm,
            (r.recorded_dttm::DATE + INTERVAL 19 HOUR)))
    )
)

-- Step 6: Calculate SBT flags at event level
, t_events AS (
    SELECT
        t3.hospitalization_id
        , t3.event_dttm
        , t3.day
        , t3.location_name
        , t3.device_category
        , t3.mode_category
        , t3._sbt_state
        , t3._extub
        , t3._intub
        , b.block_duration_mins

        -- Flag 1: sbt_pressure_support_7AM_7PM
        -- SBT state for 30+ minutes
        , CASE
            WHEN t3._sbt_state = 1
                AND COALESCE(b.block_duration_mins, 0) >= 30
            THEN 1 ELSE 0
          END AS sbt_pressure_support_7AM_7PM

        -- Flag 2: any_extubation_7AM_7PM
        -- Any extubation event (not just first)
        , t3._extub AS any_extubation_7AM_7PM

    FROM t3
    LEFT JOIN sbt_blocks_with_duration b
        ON t3.hospitalization_id = b.hospitalization_id
        AND t3._block_id = b._block_id
        AND t3._sbt_state = b._sbt_state
)

-- Step 7: Aggregate to patient-day level and join 7 PM status
, sbt_intermediate AS (
    SELECT
        h.patient_id
        , e.hospitalization_id
        , e.location_name
        , e.day
        , MAX(sbt_pressure_support_7AM_7PM) AS sbt_pressure_support_7AM_7PM
        , MAX(any_extubation_7AM_7PM) AS any_extubation_7AM_7PM

        -- Flag 3: sbt_successful_extubation_7PM
        -- Had extubation AND device at 7 PM is NOT imv
        , CASE
            WHEN MAX(any_extubation_7AM_7PM) = 1
                AND LOWER(s7pm.device_at_7pm) IS DISTINCT FROM 'imv'
            THEN 1 ELSE 0
          END AS sbt_successful_extubation_7PM

        -- Audit columns
        , MIN(CASE WHEN sbt_pressure_support_7AM_7PM = 1 THEN event_dttm END) AS sbt_first_dttm
        , MIN(CASE WHEN any_extubation_7AM_7PM = 1 THEN event_dttm END) AS extubation_first_dttm
        , s7pm.device_at_7pm

    FROM t_events e
    LEFT JOIN hosp_df h ON e.hospitalization_id = h.hospitalization_id
    LEFT JOIN status_at_7pm s7pm
        ON e.hospitalization_id = s7pm.hospitalization_id
        AND e.day = s7pm.day
    GROUP BY h.patient_id, e.hospitalization_id, e.location_name, e.day, s7pm.device_at_7pm
)

-- Final output: intermediate patient-day level results
FROM sbt_intermediate
ORDER BY hospitalization_id, day;
