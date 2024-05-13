

USE [Reporting]
GO
/****** Object:  StoredProcedure [reporting].[dbo].[child_aggregate_tables]    Script Date: 5/13/2024 07:27:07 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



/*=========================================================================================================================
----Create Date:	2024-05-13
----Created By:		Urgent Tsuro
----Description:	Creates an aggregate table for ages < 18 years
-----------------------------------------------------------------------------------------
----Modifications: 
-----------------------------------------------------------------------------------------
Updates: 				
============================================================================================================================ */
--exec reporting.[dbo].[sp_stage_emergency_centre_dashboard] 
create PROCEDURE [reporting].[dbo].[child_aggregate_tables]
AS 
BEGIN
--Logging the Store Procedure
EXECUTE logging.track @@PROCID, @@spid, 'start'

------Declare local variables
DECLARE @CurrentDate datetime= getdate()
	,   @Time datetime
	,	@Rowcount int = null
	,	@StagingID uniqueidentifier= NEWID()
	,	@object_id int = object_id('reporting.stg.sp_cascade_child_demographics')
	,	@ObjectName varchar(50)= 'reporting.stg.sp_cascade_child_demographics'
	,	@Message varchar(200)

SET @Message = 'Started running the procedure to generate report'
----Start Logging the procedure
EXEC [Staging].[logging].[procedure_stats] 0,@CurrentDate,@StagingID,@Object_id,null,null,'reporting.cscd.child_aggregate',null,null,@Message,null,null,@objectName
EXEC [Staging].[logging].[procedure_stats] 0,@CurrentDate,@StagingID,@Object_id,null,null,'reporting.cscd.child_aggregate_by_dominant',null,null,@Message,null,null,@objectName
EXEC [Staging].[logging].[procedure_progress] @StagingID, @Message, @Rowcount, @Time

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists #pmi_table
;declare @date date = getdate()

;with cte1 as (
select  
		 p.pmi_id
		,p.sex
		,p.date_of_birth
		,coalesce(p.date_of_death, m.date_of_death) as date_of_death
		,DATEDIFF(year, p.date_of_birth, @date) - 
			CASE WHEN DATEADD(year, DATEDIFF(year, p.date_of_birth, @date), p.date_of_birth) > @date
				THEN 1
				ELSE 0
			END as age_years
from patient.dbo.pmi p	
left join Patient.dbo.mortality m
on p.pmi_id = m.pmi_id
), cte2 as (
select 
		pmi_id
		,sex
		,age_years
from cte1
where age_years between 0 and 18
and date_of_death is null
)
select 
		 pmi_id
		,case when [sex] = 'I' or sex = '' or sex = 'U' then 'Unknown' else sex end as sex
		,age_years
	into #pmi_table										--
from cte2
order by age_years
--===============================================================================================================================================================================================
drop table if exists #aggregate_data
;with cte4 as (
select 
		p.*
		,AddressLine1
  		,AddressLine2
  		,AddressLine3
  		,AddressLine4
  		,coalesce(d.postal_code, k.postal_code) as postal_code
		,suburb_id
		,suburb
		,district
		,subdistrict
from #pmi_table p												--
-----------including the first table
left join
(
select * 
  from (
  select pmi_id
  		,AddressLine1
  		,AddressLine2
  		,AddressLine3
  		,AddressLine4
  		,postal_code
  		,is_active
  		,date_added_to_table
  		,row_number() over(partition by pmi_id order by date_added_to_table desc) as r
  	from patient.dbo.pmi_address
  		where is_active = 1
		and postal_code is not null) a 
		where r = 1
		) d
	on p.pmi_id = d.pmi_id												--
-----------including the second table
left join 
(
select *
from
(
select
	 pmi_id
	,postal_code
	,suburb_id
	,suburb
	,district
	,subdistrict
	,row_number() over(partition by pmi_id order by date_added_to_table desc) as rn
	,date_added_to_table
from [Patient].[dbo].[geocoded_addresses]
where district is not null or postal_code is not null --------------------------------------------
) ga
where rn = 1
) k
on p.pmi_id = k.pmi_id
), cte5 as (
select 
		 ag.pmi_id
		,ag.sex
		,ag.age_years
		,ag.AddressLine1
  		,ag.AddressLine2
  		,ag.AddressLine3
  		,ag.AddressLine4
		,ag.postal_code
		,suburb_id
		,mp2.suburb
		,mp2.district
		,mp2.subdistrict
from cte4 ag 
left join [Facility].[map].[western_cape_subdistrict_postal_code] mp2
on ag.postal_code = mp2.postal_code
where ag.pmi_id 
in 
(select ag.pmi_id from cte4 where ag.district is null)
)
select * into #aggregate_data from cte4									--
union 
select * from cte5
--===============================================================================================================================================================================================
--working on the districts
update #aggregate_data
set district = 'Cape Town' where district in ('Cape Town Metro', 'City of Cape Town Metropolitan Municipality')
update #aggregate_data
set district = replace(district, 'District Municipality', '')
update #aggregate_data
set district = 'Garden Route' where district = 'Eden'
--working on the subdistricts
update #aggregate_data
set subdistrict = replace(subdistrict, 'cape town ', '')
update #aggregate_data
set subdistrict = replace(subdistrict, 'Health sub-District', '')
update #aggregate_data
set subdistrict = replace(subdistrict, 'Local Municipality', '')
update #aggregate_data
set subdistrict = replace(subdistrict, 'Municipality', '')
update #aggregate_data
set subdistrict = 'Mitchells Plain' where subdistrict = 'Browns Farm, Cross Roads, Philippi, Nyanga and Guguletu'
--====================================================================================================================================================================================================================================================
--Making a district_subdistrict mapping table
drop table if exists #sub_district_mapping
create table #sub_district_mapping (row_id int, district_map varchar(50), subdistrict_map varchar(50))
insert into #sub_district_mapping
values
(1, 'Cape Town', 'Eastern'),
(2, 'Cape Town', 'Khayelitsha'),
(3, 'Cape Town', 'Klipfontein'),
(4, 'Cape Town', 'Mitchells Plain'),
(5, 'Cape Town', 'Northern'),
(6, 'Cape Town', 'Southern'),
(7, 'Cape Town', 'Tygerberg'),
(8, 'Cape Town', 'Western'),
(9, 'Cape Winelands', 'Breede Valley'),
(10, 'Cape Winelands', 'Drakenstein'),
(11, 'Cape Winelands', 'Langeberg'),
(12, 'Cape Winelands', 'Stellenbosch'),
(13, 'Cape Winelands', 'Witzenberg'),
(14, 'Central Karoo', 'Beaufort West'),
(15, 'Central Karoo', 'Laingsburg'),
(16, 'Central Karoo', 'Prince Albert'),
(17, 'Garden Route', 'Bitou'),
(18, 'Garden Route', 'George'),
(19, 'Garden Route', 'Hessequa'),
(20, 'Garden Route', 'Kannaland'),
(21, 'Garden Route', 'Knysna'),
(22, 'Garden Route', 'Mossel Bay'),
(23, 'Garden Route', 'Oudtshoorn'),
(24, 'Overberg', 'Cape Agulhas'),
(25, 'Overberg', 'Overstrand'),
(26, 'Overberg', 'Swellendam'),
(27, 'Overberg', 'Theewaterskloof'),
(28, 'West Coast', 'Bergrivier'),
(29, 'West Coast', 'Cederberg'),
(30, 'West Coast', 'Matzikama'),
(31, 'West Coast', 'Saldanha Bay'),
(32, 'West Coast', 'Swartland')
--Mapping the subdistricts to the districts
drop table if exists #aggregate_data_mapped
select pmi_id
,sex
,age_years
,AddressLine1
,AddressLine2
,AddressLine3
,AddressLine4
,postal_code
,suburb_id
,suburb
,district_map as district
,subdistrict_map as subdistrict 
into #aggregate_data_mapped
from #aggregate_data a
left join #sub_district_mapping m
on a.subdistrict = m.subdistrict_map
--====================================================================================================================================================================================================================================================
--creating a deduplicated dataset
drop table if exists #aggregate_data_deduplicated
select *
into #aggregate_data_deduplicated
from (
select *
		,ROW_NUMBER() over(partition by pmi_id order by district desc) as r 
from #aggregate_data_mapped) new 
where r = 1							--
--===============================================================================================================================================================================================
--extracting individuals with subdistrict and district
--there are 3099146 rows affected
drop table if exists #aggregate_data_with_district
select * 
into #aggregate_data_with_district
from #aggregate_data_deduplicated 
where district is not null and subdistrict is not null	--	
--===============================================================================================================================================================================================
--extracting individuals without subdistrict and district
--there are 173005 rows affected
drop table if exists #aggregate_data_without_district
select * 
into #aggregate_data_without_district
from #aggregate_data_deduplicated 
where pmi_id 
not in 
(
select pmi_id from #aggregate_data_with_district
)
--===============================================================================================================================================================================================
--extracting individuals without addresses
--there are 159239 rows affected
drop table if exists #aggregate_data_no_address
select * 
into #aggregate_data_no_address
from #aggregate_data_without_district
where 
(AddressLine1 is null
and AddressLine2 is null
and AddressLine3 is null
and AddressLine4 is null
and subdistrict is null
and district is null
and postal_code is null)
or 
(
addressline1 like '%xx%'
or addressline2 like '%xx%'
or addressline3 like '%xx%'
or addressline4 like '%xx%')
--updating the district and subdistrict for the no addresses individuals
update #aggregate_data_no_address
set subdistrict = 'Unknown Subdistrict'
update #aggregate_data_no_address
set district = 'Unknown District'
--===============================================================================================================================================================================================
--checking individuals without subdistrict and district but with addresses
--there are 13766 rows affected
drop table if exists #aggregate_data_without_district_but_with_address
select *
into #aggregate_data_without_district_but_with_address
from #aggregate_data_without_district
where pmi_id 
not in 
(
select pmi_id from #aggregate_data_no_address
)
--updating the #aggregate_data_without_district_but_with_address table
update #aggregate_data_without_district_but_with_address
set district = 'Cape Town', subdistrict = 'Klipfontein'
where AddressLine3 like '%(kli)%' and district is null

update #aggregate_data_without_district_but_with_address
set district = 'Cape Town', subdistrict = 'Mitchells Plain'
where AddressLine3 like '%phillipi (sou)%' and district is null

update #aggregate_data_without_district_but_with_address
set district = 'Cape Town', subdistrict = 'Southern'
where AddressLine3 like '%cape town (sou)%' and district is null

update #aggregate_data_without_district_but_with_address
set district = 'Cape Town', subdistrict = 'Mitchells Plain'
where AddressLine3 like '%PHILLIPI (MIT)%'

update #aggregate_data_without_district_but_with_address
set district = 'Cape Town', subdistrict = 'Khayelitsha'
where (AddressLine3 like '%Khayelitsha%' or addressline3 like '%(kha)%') and district is null

update #aggregate_data_without_district_but_with_address
set district = 'Cape Town', subdistrict = 'Western'
where AddressLine3 like '%GREENPOINT%' and district is null

update #aggregate_data_without_district_but_with_address
set district = 'West Coast', subdistrict = 'Cederberg'
where (AddressLine3 like '%CLANWILLIAM%' or AddressLine4 like '%CLANWILLIAM%') and district is null

update #aggregate_data_without_district_but_with_address
set district = 'Cape Town', subdistrict = 'Mitchells Plain'
where (AddressLine3 like '%philippi%' or AddressLine2 like '%philippi%' or AddressLine1 like '%philippi%')
and district is null

update #aggregate_data_without_district_but_with_address
set district = 'Cape Town', subdistrict = 'Tygerberg'
where addressline3 like '%(tyg)%' 
and district is null

update #aggregate_data_without_district_but_with_address
set district = 'Cape Town', subdistrict = 'Northern'
where addressline3 like '%PARLIAMENT%' 
and district is null

update #aggregate_data_without_district_but_with_address
set district = 'Cape Town', subdistrict = 'Eastern'
where addressline3 like '%CAMPS BAY%' 
and district is null

update #aggregate_data_without_district_but_with_address
set district = 'Cape Town', subdistrict = 'Northern'
where addressline3 like '%PAROW (NOR)%' 
and district is null

update #aggregate_data_without_district_but_with_address
set district = 'Cape Town', subdistrict = 'Western'
where addressline3 like '%(wes)%' 
and district is null
--===============================================================================================================================================================================================
--checking individuals from other provinces
--there are 12357 rows affected
drop table if exists #aggregate_data_other_provinces
select * 
into #aggregate_data_other_provinces
from #aggregate_data_without_district_but_with_address
where district is null

update #aggregate_data_other_provinces
set subdistrict = 'Outside Province'
update #aggregate_data_other_provinces
set district = 'Outside Province'
--===============================================================================================================================================================================================
--checking individuals from capetown
--there are 1409 rows affected
drop table if exists #aggregate_data_cape
select * 
into #aggregate_data_cape
from #aggregate_data_without_district_but_with_address
where district is not null
--===============================================================================================================================================================================================
--combine all the tables
drop table if exists #aggregate_data_final
select *
into #aggregate_data_final
from (
select * from #aggregate_data_with_district
union all
select * from #aggregate_data_no_address
union all
select * from #aggregate_data_other_provinces
union all
select * from #aggregate_data_cape
) a 
--===============================================================================================================================================================================================
--creating the aggregate table
--
/*
drop table if exists [reporting].[cscd].[child_aggregate]
create table [reporting].[cscd].[child_aggregate] (
													row_id int identity(1,1)
													, district varchar(100)
													, subdistrict varchar(100)
													, sex varchar(10)
													, age_years varchar(50)
													, [count] int)
*/

truncate table [reporting].[cscd].[child_aggregate]

insert into [reporting].[cscd].[child_aggregate] (district
		,subdistrict
		,sex
		,age_years
		,[count])

select district, subdistrict, sex, age_years, count from (
select *
		,case when district = 'Province Total' then 1
		 when district = 'Outside Province' then 2
		 when district = 'Outside Province  Total' then 3
		 when district = 'Grand Total' then 4
		else 0
		end as sort
		from (
select 
		 district
		,subdistrict
		,sex
		,cast(age_years as varchar) as age_years
		, count(age_years) as 'count'
from #aggregate_data_final
group by district, subdistrict, age_years, sex

union all

select 'Province Total', 'Province Total', 'M', '0 - 18', count(*) 
from #aggregate_data_final
where district <> 'Outside Province'
and sex = 'M'
union all
select 'Province Total', 'Province Total', 'F', '0 - 18', count(*) 
from #aggregate_data_final
where district <> 'Outside Province'
and sex = 'F'
union all
select 'Province Total', 'Province Total', 'Unknown', '0 - 18', count(*) 
from #aggregate_data_final
where district <> 'Outside Province'
and sex = 'Unknown'

union all

select 'Outside Province  Total', 'Outside Province  Total', 'M', '0 - 18', count(*) 
from #aggregate_data_final
where district = 'Outside Province'
and sex = 'M'
union all
select 'Outside Province  Total', 'Outside Province  Total', 'F', '0 - 18', count(*) 
from #aggregate_data_final
where district = 'Outside Province'
and sex = 'F'
union all
select 'Outside Province  Total', 'Outside Province  Total', 'Unknown', '0 - 18', count(*) 
from #aggregate_data_final
where district = 'Outside Province'
and sex = 'Unknown'

union all
select 'Grand Total', 'Grand Total', 'M', '0 - 18', count(*) 
from #aggregate_data_final
where sex = 'M'
union all
select 'Grand Total', 'Grand Total', 'F', '0 - 18', count(*) 
from #aggregate_data_final
where sex = 'F'
union all
select 'Grand Total', 'Grand Total', 'Unknown', '0 - 18', count(*) 
from #aggregate_data_final
where sex = 'Unknown'
) b) c
order by sort, district, subdistrict, age_years, sex
--===============================================================================================================================================================================================
--Aggregate table by dominant
drop table if exists #pmi_dominant
select pmi_dominant_id,
				pmi_id,
				sex
	into #pmi_dominant
	from 
(
select distinct 
				p.pmi_id, 
				pd.pmi_dominant_id,
				sex,
				p.[date_of_birth], 
				p.date_of_death, 
				row_number() over(partition by pd.pmi_dominant_id order by pd.pmi_dominant_id) as rn
				
from [Patient].[dbo].[pmi] p
left join  patient.map.pmi pd
on p.pmi_id = pd.pmi_id
where date_of_death is null
) g
 where rn = 1;
--===================================================================================================================================================================================================================================================
drop table if exists #aggregate_data_by_dominant
select d.pmi_dominant_id, a.sex, a.age_years, a.district, a.subdistrict 
into #aggregate_data_by_dominant
from #pmi_dominant d
left join #aggregate_data_final a
on d.pmi_id = a.pmi_id
where d.pmi_dominant_id is not null
--===================================================================================================================================================================================================================================================
/*
drop table if exists [reporting].[cscd].[child_aggregate_by_dominant]
create table [reporting].[cscd].[child_aggregate_by_dominant] (
													row_id int identity(1,1)
													, district varchar(100)
													, subdistrict varchar(100)
													, sex varchar(10)
													, age_years varchar(50)
													, [count] int)
*/
truncate table [reporting].[cscd].[child_aggregate_by_dominant]
insert into [reporting].[cscd].[child_aggregate_by_dominant] (district
		,subdistrict
		,sex
		,age_years
		,[count])

select district, subdistrict, sex, age_years, count from (
select *
		,case when district = 'Province Total' then 1
		 when district = 'Outside Province' then 2
		 when district = 'Outside Province  Total' then 3
		 when district = 'Grand Total' then 4
		else 0
		end as sort
		from (
select 
		 district
		,subdistrict
		,sex
		,cast(age_years as varchar) as age_years
		, count(age_years) as 'count'
from #aggregate_data_by_dominant
group by district, subdistrict, age_years, sex

union all

select 'Province Total', 'Province Total', 'M', '0 - 18', count(*) 
from #aggregate_data_by_dominant
where district <> 'Outside Province'
and sex = 'M'
union all
select 'Province Total', 'Province Total', 'F', '0 - 18', count(*) 
from #aggregate_data_by_dominant
where district <> 'Outside Province'
and sex = 'F'
union all
select 'Province Total', 'Province Total', 'Unknown', '0 - 18', count(*) 
from #aggregate_data_by_dominant
where district <> 'Outside Province'
and sex = 'Unknown'

union all

select 'Outside Province  Total', 'Outside Province  Total', 'M', '0 - 18', count(*) 
from #aggregate_data_by_dominant
where district = 'Outside Province'
and sex = 'M'
union all
select 'Outside Province  Total', 'Outside Province  Total', 'F', '0 - 18', count(*) 
from #aggregate_data_by_dominant
where district = 'Outside Province'
and sex = 'F'
union all
select 'Outside Province  Total', 'Outside Province  Total', 'Unknown', '0 - 18', count(*) 
from #aggregate_data_by_dominant
where district = 'Outside Province'
and sex = 'Unknown'

union all
select 'Grand Total', 'Grand Total', 'M', '0 - 18', count(*) 
from #aggregate_data_by_dominant
where sex = 'M'
union all
select 'Grand Total', 'Grand Total', 'F', '0 - 18', count(*) 
from #aggregate_data_by_dominant
where sex = 'F'
union all
select 'Grand Total', 'Grand Total', 'Unknown', '0 - 18', count(*) 
from #aggregate_data_by_dominant
where sex = 'Unknown'
) b) c
where district is not null
order by sort, district, subdistrict, age_years, sex
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

---------Completed peads update: loggong
SET @rowcount = @@rowcount
SET @Time = getdate() ;
SET @Message = @Message+'|'+'Completed running the procedure' ;
EXEC [Staging].[logging].[procedure_progress] @StagingID, @Message, @Rowcount, @Time ;
EXEC [Staging].[logging].[procedure_stats] 1,@CurrentDate,@StagingID,@Object_id,null,null,'reporting.cscd.child_aggregate',@Rowcount,@Time,@Message,null,null,@objectname
EXEC [Staging].[logging].[procedure_stats] 1,@CurrentDate,@StagingID,@Object_id,null,null,'reporting.cscd.child_aggregate_by_dominant',@Rowcount,@Time,@Message,null,null,@objectname

execute logging.track @@PROCID, @@spid, 'end'
end