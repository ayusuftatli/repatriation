-- creating tables 
CREATE TABLE ordered (
    dataflow VARCHAR(50),
    last_update TIMESTAMP,
    freq CHAR(1),
    citizen VARCHAR(10),
    age VARCHAR(10),
    sex VARCHAR(5),
    unit VARCHAR(10),
    geo VARCHAR(10),
    time_period INTEGER,
    obs_value INTEGER
);

\COPY ordered (dataflow, last_update, freq, citizen, age, sex, unit, geo, time_period, obs_value) 
FROM '/Users/yusuftatli/Downloads/migr_eiord_linear.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',');

CREATE TABLE returned (
    dataflow VARCHAR(50),
    last_update TIMESTAMP,
    freq CHAR(1),
    citizen VARCHAR(10),
		c_dest VARCHAR(20),
    age VARCHAR(10),
    sex VARCHAR(5),
    unit VARCHAR(10),
    geo VARCHAR(10),
    time_period INTEGER,
    obs_value INTEGER,
		obs_flag VARCHAR(50)
);

\COPY returned (dataflow, last_update, freq, citizen, c_dest, age, sex, unit, geo, time_period, obs_value, obs_flag)  FROM '/Users/yusuftatli/Downloads/migr_eirtn_linear.csv'  WITH (FORMAT csv, HEADER true, DELIMITER ',');


\COPY africa (code, sub_region)  FROM '/Users/yusuftatli/Downloads/african_countries.csv'  WITH (FORMAT csv, HEADER true, DELIMITER ',');
---
select *
from ordered o 
limit 5

select *
from returned
limit 5

select *
from africa a
limit 5

---data exploration

select distinct citizen, sex
from ordered

select distinct age, geo
from ordered

select distinct citizen, sex
from returned

select distinct age, geo
from returned

select distinct c_dest
from returned r 


select distinct *
from ordered o 
where obs_value is null
and age = 'TOTAL'
and sex = 'T'

select distinct *
from returned 
where obs_value is null
and age = 'TOTAL'
and sex = 'T'

--- only null values exist where the citizen is either Croatia or UK, which is not relevant
select distinct *
from returned 
where obs_value is null
and c_dest = 'THRD'
and age = 'TOTAL'
and sex = 'T'
--- same in the returned table





---the main query

drop view if exists v_ordered_returned;
create view v_ordered_returned as (
	with returned_data as (
		select citizen, time_period, sum(obs_value) as returned
		from returned 
		where c_dest = 'THRD' --only those who returned to non-EU countries
		and length(citizen) < 3 -- excluding total values 
		and geo not in ('UK', 'EU27_2020') -- UK and EU aggregation is not necessary
		and sex = 'T' -- a breakdown by sex is not necessary
		and age = 'TOTAL'
		group by citizen, time_period 
	),
	ordered_data as (
		select citizen, time_period, sum(obs_value) as ordered
		from ordered
		where length(citizen) < 3
		and geo not in ('UK', 'EU27_2020')
		and sex = 'T'
		and age = 'TOTAL'
		group by citizen, time_period 
	),
	africa_data as (
		select code, sub_region
		from africa
	)
	
	select a.sub_region, r.time_period as year, r.citizen, sum(r.returned) as returned, sum(o.ordered) as ordered_to_leave
	from returned_data r
	left join ordered_data o on r.citizen = o.citizen and r.time_period = o.time_period
	right join africa_data a on r.citizen = a.code
	group by r.citizen, a.sub_region, r.time_period
	having sum(o.ordered) > 0
	order by 1, 2, 3 desc
)

---
select *
from v_ordered_returned vor 

select sub_region, sum(returned) as returned, sum(ordered_to_leave) as ordered_to_leave
from v_ordered_returned vor 
where year between 2013 and 2022
group by sub_region 
order by 2 desc, 1 desc

select sub_region, year, (sum(returned) * 100.0) / sum(ordered_to_leave) as return_rate
from v_ordered_returned 
group by sub_region, year

select sub_region, (sum(returned) * 100.0) / sum(ordered_to_leave) as return_rate
from v_ordered_returned vor 
where year = 2022
group by sub_region

----
drop view if exists results;
create view results as(
select v.returned, v.ordered_to_leave, v.sub_region, v.year, v.citizen, vt.return_rate
from v_ordered_returned v
left join (
	select sub_region, year, (sum(returned) * 100.0) / sum(ordered_to_leave) as return_rate
	from v_ordered_returned 
	group by sub_region, year
) vt on v.sub_region = vt.sub_region and v.year = vt.year
)



