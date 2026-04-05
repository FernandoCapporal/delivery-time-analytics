-- ─────────────────────────────────
-- Config de sesión
-- ─────────────────────────────────
BEGIN;
SET search_path TO {{params.schema}}, public;

-- ─────────────────────────────────────────────────────────────
-- Step 2: Crear tabla histórica si no existe
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.country }}_delivery_trips_historic_info (
    -- Identificador temporal
    batch_date              DATE            NOT NULL,   -- Fecha de ejecución del DAG ({{ ds }})
    week_start              DATE            NOT NULL,   -- Inicio de la semana procesada
    week_end                DATE            NOT NULL,   -- Fin de la semana procesada

    territory                               VARCHAR(100),
    country_name                            VARCHAR(100),
    workflow_uuid                           UUID,
    driver_uuid                             UUID,
    delivery_trip_uuid                      UUID            NOT NULL,
    courier_flow                            VARCHAR(50),
    restaurant_offered_timestamp_utc        TIMESTAMP WITH TIME ZONE,
    order_final_state_timestamp_local       TIMESTAMP WITHOUT TIME ZONE,
    eater_request_timestamp_local           TIMESTAMP WITHOUT TIME ZONE,
    geo_archetype                           VARCHAR(100),
    merchant_surface                        VARCHAR(50),
    pickup_distance_km                      NUMERIC(10, 3),
    dropoff_distance_km                     NUMERIC(10, 3),
    atd                                     NUMERIC(10, 2),

    inserted_at             TIMESTAMP       DEFAULT NOW(),
    updated_at              TIMESTAMP       DEFAULT NOW(),

    CONSTRAINT uq_trip_batch UNIQUE (delivery_trip_uuid, batch_date)
);

-- ─────────────────────────────────────────────────────────────
-- Step 3: Merge — INSERT nuevos, UPDATE existentes
-- ─────────────────────────────────────────────────────────────
INSERT INTO {{ params.schema }}.{{ params.country }}_delivery_trips_historic_info (
    batch_date,
    week_start,
    week_end,
    territory,
    country_name,
    workflow_uuid,
    driver_uuid,
    delivery_trip_uuid,
    courier_flow,
    restaurant_offered_timestamp_utc,
    order_final_state_timestamp_local,
    eater_request_timestamp_local,
    geo_archetype,
    merchant_surface,
    pickup_distance_km,
    dropoff_distance_km,
    atd,
    inserted_at,
    updated_at
)
SELECT
    '{{ ds }}'::DATE                                     AS batch_date,
    '{{ ds }}'::DATE - INTERVAL '7 days'                 AS week_start,
    '{{ ds }}'::DATE                                     AS week_end,
    dti.territory,
    dti.country_name,
    dti.workflow_uuid,
    dti.driver_uuid,
    dti.delivery_trip_uuid,
    dti.courier_flow,
    dti.restaurant_offered_timestamp_utc,
    dti.order_final_state_timestamp_local,
    dti.eater_request_timestamp_local,
    dti.geo_archetype,
    dti.merchant_surface,
    dti.pickup_distance_km,
    dti.dropoff_distance,
    dti.atd,
    NOW()                                                        AS inserted_at,
    NOW()                                                        AS updated_at
FROM {{ params.schema }}.{{params.country}}_delivery_trips_info dti

-- MERGE: si el trip ya existe en el mismo batch, actualiza
ON CONFLICT (delivery_trip_uuid, batch_date)
DO UPDATE SET
    territory                           = EXCLUDED.territory,
    courier_flow                        = EXCLUDED.courier_flow,
    order_final_state_timestamp_local   = EXCLUDED.order_final_state_timestamp_local,
    geo_archetype                       = EXCLUDED.geo_archetype,
    merchant_surface                    = EXCLUDED.merchant_surface,
    pickup_distance_km                  = EXCLUDED.pickup_distance_km,
    dropoff_distance_km                 = EXCLUDED.dropoff_distance_km,
    atd                                 = EXCLUDED.atd,
    updated_at                          = NOW();

COMMIT;

