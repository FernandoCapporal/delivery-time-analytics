CREATE TABLE IF NOT EXISTS uber_prod.mx_delivery_trips_info AS (
	select csr.territory, dc.country_name, ltsac.workflow_uuid, ltsac.driver_uuid, ltsac.delivery_trip_uuid,
		ltsac.courier_flow, ltsac.restaurant_offered_timestamp_utc, ltsac.order_final_state_timestamp_local,
		ltsac.eater_request_timestamp_local, ltsac.geo_archetype, ltsac.merchant_surface,
		edmjm.pickup_distance/1000 as pickup_distance_km, pickup_distance_km + (edmjm.travel_distance/1000) as dropoff_distance,
		EXTRACT(EPOCH FROM (ltsac.order_final_state_timestamp_local::TIMESTAMP AT TIME ZONE 'UTC' - ltsac.restaurant_offered_timestamp_utc)) / 60.0 AS atd
	from tmp.lea_trips_scope_atd_consolidation_v2 ltsac
	left join delivery_matching.eats_dispatch_metrics_job_message edmjm
		on ltsac.delivery_trip_uuid = edmjm.job_uuid
	left join kirby_external_data.cities_strategy_region csr
		on edmjm.city_id = csr.city_id
	left join dwh.dim_city dc
		on edmjm.city_id = dc.city_id
	where dc.country_name = 'Mexico'
	and edmjm.date_str::date between {{ds}} - interval '7 days' and {{ds}}
);