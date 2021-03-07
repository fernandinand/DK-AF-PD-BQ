-- 1 Check/Validate data
select count(*) from `carto-ds.carto.taxi_data`;
--> 36383558

create or replace view `carto-ds.carto.v_taxi_data`
as
    select
        row_number() over(PARTITION BY VendorID) as id,
        VendorID,
        safe_cast(trip_distance AS float64) as trip_distance,
        pickup_longitude,
        pickup_latitude,
        dropoff_longitude,
        dropoff_latitude,
        safe_cast(fare_amount AS float64) as fare_amount,
        safe_cast(tip_amount AS float64) as tip_amount,
        safe_cast(total_amount AS float64) as total_amount
    from `carto-ds.carto.taxi_data`
    where
        (dropoff_latitude between -90 and 90) and
        (pickup_latitude between -90 and 90) and
        (dropoff_longitude between -180 and 180) and
        (pickup_longitude between -180 and 180)
;

create table `carto-ds.carto.taxi_data_processed` as
    select * from `carto-ds.carto.v_taxi_data`
;

select count(*) from `carto-ds.carto.taxi_data_processed`;
--> 36383542

select 36383542*100/36383558;
--> 99.9% (-16)

create table `carto-ds.carto.taxi_data_geom` as
    select id,
        ST_GEOGPOINT(pickup_longitude, pickup_latitude) as geom_pickup,
        ST_GEOGPOINT(dropoff_longitude, dropoff_latitude) as geom_dropoff
    from `carto-ds.carto.taxi_data_processed`
;

select count(*) from `carto-ds.carto.taxi_data_geom`;
--> 36383542

-- check points in AOI
select count(a.id) from `carto-ds.carto.taxi_data_geom` a
join `carto-ds.carto.taxi_zones` b on
        st_intersects(a.geom_pickup, st_geogfromtext(b.geometry));
--> 35747917 (~98%)


-- 2 Derive some data
-- a. What is the average fare per mile?
select avg(fare_amount/trip_distance) as avg_fare_p_mile
from `carto-ds.carto.taxi_data_processed`
where fare_amount > 0 and trip_distance > 0;
--> 6.408386148579897

-- b. Which are the 10 pickup taxi zones with the highest average tip?
select zone, avg(tip_amount) as avg_tip from (
    select distinct a.zone, b.id, c.tip_amount
    from `carto-ds.carto.taxi_zones` a
    join `carto-ds.carto.taxi_data_geom` b on st_intersects(b.geom_pickup, st_geogfromtext(a.geometry))
    join `carto-ds.carto.taxi_data_processed` c on b.id = c.id
    where c.tip_amount > 0 and c.tip_amount is not null
)
group by 1
order by 2 desc
limit 10;
-- zone	avg_tip
-- Eltingville/Annadale/Prince's Bay	12.260000000000002
-- Newark Airport	9.975653862753562
-- Arrochar/Fort Wadsworth	9.724374999999998
-- Sunset Park East	9.21633891213389
-- Dyker Heights	9.139071729957811
-- Pelham Bay Park	8.971282051282051
-- Rossville/Woodrow	8.068571428571428
-- Charleston/Tottenville	7.75
-- Bloomfield/Emerson Hill	7.669499999999998
-- Country Club	7.532105263157896
