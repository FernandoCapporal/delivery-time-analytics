CREATE TABLE dwh.dim_city (
    city_id         SERIAL          PRIMARY KEY,
    city_name       VARCHAR(100)    NOT NULL,
    country_name    VARCHAR(100)    NOT NULL,
    mega_region     VARCHAR(50)     NOT NULL,
    currency_code   CHAR(3)         NOT NULL,
    population_size BIGINT,
    average_income  NUMERIC(12, 2)
);

CREATE TABLE kirby_external_data.cities_strategy_region (
    city_id         SERIAL          PRIMARY KEY,
    region          VARCHAR(100)     NOT NULL,
    territory       VARCHAR(50)    NOT NULL,
    population_size BIGINT,
    market_demand   NUMERIC(15, 2)
);

CREATE TABLE delivery_matching.eats_dispatch_metrics_job_message (
    job_uuid                UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    city_id                 INTEGER         NOT NULL,
    date_str                TIMESTAMP       NOT NULL,
    pickup_distance_km      NUMERIC(10, 3)  NOT NULL GENERATED ALWAYS AS (pickup_distance / 1000.0) STORED,
    pickup_distance       NUMERIC(10, 1)  NOT NULL,
    travel_distance_km      NUMERIC(10, 3)  NOT NULL GENERATED ALWAYS AS (travel_distance / 1000.0) STORED,
    travel_distance       NUMERIC(10, 1)  NOT NULL,
    is_final_plan           BOOLEAN         NOT NULL DEFAULT FALSE,
    estimated_delivery_time INTERVAL,
    delivery_status         VARCHAR(20)     NOT NULL DEFAULT 'pending',

    CONSTRAINT chk_delivery_status CHECK (delivery_status IN ('completed', 'pending')),
    CONSTRAINT chk_pickup_distance CHECK (pickup_distance >= 0),
    CONSTRAINT chk_travel_distance CHECK (travel_distance >= 0)
);


CREATE INDEX idx_delivery_jobs_city_id  ON AA_tables.delivery_jobs(city_id);
CREATE INDEX idx_delivery_jobs_date_str ON AA_tables.delivery_jobs(date_str);
CREATE INDEX idx_delivery_jobs_status   ON AA_tables.delivery_jobs(delivery_status);


CREATE TABLE tmp.lea_trips_scope_atd_consolidation_v2 (
    delivery_trip_uuid                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_uuid                       UUID        NOT NULL,
    driver_uuid                         UUID        NOT NULL,
    courier_flow                        VARCHAR(50),

    restaurant_offered_timestamp_utc    TIMESTAMP WITH TIME ZONE,
    order_final_state_timestamp_local   TIMESTAMP WITHOUT TIME ZONE,
    eater_request_timestamp_local       TIMESTAMP WITHOUT TIME ZONE,

    geo_archetype                       VARCHAR(100),
    merchant_surface                    VARCHAR(50),
    total_distance_travelled         NUMERIC(10, 3),

    CONSTRAINT chk_total_distance CHECK (total_distance_travelled >= 0)
);

CREATE INDEX idx_delivery_trips_workflow_uuid ON AA_tables.delivery_trips(workflow_uuid);
CREATE INDEX idx_delivery_trips_driver_uuid   ON AA_tables.delivery_trips(driver_uuid);
CREATE INDEX idx_delivery_trips_restaurant_ts ON AA_tables.delivery_trips(restaurant_offered_timestamp_utc);
CREATE INDEX idx_delivery_trips_eater_ts      ON AA_tables.delivery_trips(eater_request_timestamp_local);