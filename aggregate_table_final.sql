DROP TABLE

IF EXISTS #pmi_table;
	DECLARE @date DATE = getdate();

WITH cte1
AS (
	SELECT p.pmi_id
		,p.sex
		,p.date_of_birth
		,coalesce(p.date_of_death, m.date_of_death) AS date_of_death
		,DATEDIFF(year, p.date_of_birth, @date) - CASE 
			WHEN DATEADD(year, DATEDIFF(year, p.date_of_birth, @date), p.date_of_birth) > @date
				THEN 1
			ELSE 0
			END AS age_years
	FROM patient.dbo.pmi p
	LEFT JOIN Patient.dbo.mortality m ON p.pmi_id = m.pmi_id
	)
	,cte2
AS (
	SELECT pmi_id
		,sex
		,age_years
	FROM cte1
	WHERE age_years BETWEEN 0
			AND 18
		AND date_of_death IS NULL
	)
SELECT pmi_id
	,CASE 
		WHEN [sex] = 'I'
			OR sex = ''
			OR sex = 'U'
			THEN 'Unknown'
		ELSE sex
		END AS sex
	,age_years
INTO #pmi_table --
FROM cte2
ORDER BY age_years

--===============================================================================================================================================================================================
DROP TABLE

IF EXISTS #aggregate_data;
	WITH cte4
	AS (
		SELECT p.*
			,AddressLine1
			,AddressLine2
			,AddressLine3
			,AddressLine4
			,coalesce(d.postal_code, k.postal_code) AS postal_code
			,suburb_id
			,suburb
			,district
			,subdistrict
		FROM #pmi_table p --
			-----------including the first table
		LEFT JOIN (
			SELECT *
			FROM (
				SELECT pmi_id
					,AddressLine1
					,AddressLine2
					,AddressLine3
					,AddressLine4
					,postal_code
					,is_active
					,date_added_to_table
					,row_number() OVER (
						PARTITION BY pmi_id ORDER BY date_added_to_table DESC
						) AS r
				FROM patient.dbo.pmi_address
				WHERE is_active = 1
					AND postal_code IS NOT NULL
				) a
			WHERE r = 1
			) d ON p.pmi_id = d.pmi_id --
			-----------including the second table
		LEFT JOIN (
			SELECT *
			FROM (
				SELECT pmi_id
					,postal_code
					,suburb_id
					,suburb
					,district
					,subdistrict
					,row_number() OVER (
						PARTITION BY pmi_id ORDER BY date_added_to_table DESC
						) AS rn
					,date_added_to_table
				FROM [Patient].[dbo].[geocoded_addresses]
				WHERE district IS NOT NULL
					OR postal_code IS NOT NULL --------------------------------------------
				) ga
			WHERE rn = 1
			) k ON p.pmi_id = k.pmi_id
		)
		,cte5
	AS (
		SELECT ag.pmi_id
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
		FROM cte4 ag
		LEFT JOIN [Facility].[map].[western_cape_subdistrict_postal_code] mp2 ON ag.postal_code = mp2.postal_code
		WHERE ag.pmi_id IN (
				SELECT ag.pmi_id
				FROM cte4
				WHERE ag.district IS NULL
				)
		)
	SELECT *
	INTO #aggregate_data
	FROM cte4 --
	
	UNION
	
	SELECT *
	FROM cte5

--===============================================================================================================================================================================================
--working on the districts
UPDATE #aggregate_data
SET district = 'Cape Town'
WHERE district IN (
		'Cape Town Metro'
		,'City of Cape Town Metropolitan Municipality'
		)

UPDATE #aggregate_data
SET district = replace(district, 'District Municipality', '')

UPDATE #aggregate_data
SET district = 'Garden Route'
WHERE district = 'Eden'

--working on the subdistricts
UPDATE #aggregate_data
SET subdistrict = replace(subdistrict, 'cape town ', '')

UPDATE #aggregate_data
SET subdistrict = replace(subdistrict, 'Health sub-District', '')

UPDATE #aggregate_data
SET subdistrict = replace(subdistrict, 'Local Municipality', '')

UPDATE #aggregate_data
SET subdistrict = replace(subdistrict, 'Municipality', '')

UPDATE #aggregate_data
SET subdistrict = 'Mitchells Plain'
WHERE subdistrict = 'Browns Farm, Cross Roads, Philippi, Nyanga and Guguletu'

--====================================================================================================================================================================================================================================================
--Making a district_subdistrict mapping table
DROP TABLE

IF EXISTS #sub_district_mapping
	CREATE TABLE #sub_district_mapping (
		row_id INT
		,district_map VARCHAR(50)
		,subdistrict_map VARCHAR(50)
		)

INSERT INTO #sub_district_mapping
VALUES  
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
DROP TABLE

IF EXISTS #aggregate_data_mapped
	SELECT pmi_id
		,sex
		,age_years
		,AddressLine1
		,AddressLine2
		,AddressLine3
		,AddressLine4
		,postal_code
		,suburb_id
		,suburb
		,district_map AS district
		,subdistrict_map AS subdistrict
	INTO #aggregate_data_mapped
	FROM #aggregate_data a
	LEFT JOIN #sub_district_mapping m ON a.subdistrict = m.subdistrict_map

--====================================================================================================================================================================================================================================================
--creating a deduplicated dataset
DROP TABLE

IF EXISTS #aggregate_data_deduplicated
	SELECT *
	INTO #aggregate_data_deduplicated
	FROM (
		SELECT *
			,ROW_NUMBER() OVER (
				PARTITION BY pmi_id ORDER BY district DESC
				) AS r
		FROM #aggregate_data_mapped
		) new
	WHERE r = 1 --
		--===============================================================================================================================================================================================
		--extracting individuals with subdistrict and district
		--there are 3099146 rows affected

DROP TABLE

IF EXISTS #aggregate_data_with_district
	SELECT *
	INTO #aggregate_data_with_district
	FROM #aggregate_data_deduplicated
	WHERE district IS NOT NULL
		AND subdistrict IS NOT NULL --	
		--===============================================================================================================================================================================================
		--extracting individuals without subdistrict and district
		--there are 173005 rows affected

DROP TABLE

IF EXISTS #aggregate_data_without_district
	SELECT *
	INTO #aggregate_data_without_district
	FROM #aggregate_data_deduplicated
	WHERE pmi_id NOT IN (
			SELECT pmi_id
			FROM #aggregate_data_with_district
			)

--===============================================================================================================================================================================================
--extracting individuals without addresses
--there are 159239 rows affected
DROP TABLE

IF EXISTS #aggregate_data_no_address
	SELECT *
	INTO #aggregate_data_no_address
	FROM #aggregate_data_without_district
	WHERE (
			AddressLine1 IS NULL
			AND AddressLine2 IS NULL
			AND AddressLine3 IS NULL
			AND AddressLine4 IS NULL
			AND subdistrict IS NULL
			AND district IS NULL
			AND postal_code IS NULL
			)
		OR (
			addressline1 LIKE '%xx%'
			OR addressline2 LIKE '%xx%'
			OR addressline3 LIKE '%xx%'
			OR addressline4 LIKE '%xx%'
			)

--updating the district and subdistrict for the no addresses individuals
UPDATE #aggregate_data_no_address
SET subdistrict = 'Unknown Subdistrict'

UPDATE #aggregate_data_no_address
SET district = 'Unknown District'

--===============================================================================================================================================================================================
--checking individuals without subdistrict and district but with addresses
--there are 13766 rows affected
DROP TABLE

IF EXISTS #aggregate_data_without_district_but_with_address
	SELECT *
	INTO #aggregate_data_without_district_but_with_address
	FROM #aggregate_data_without_district
	WHERE pmi_id NOT IN (
			SELECT pmi_id
			FROM #aggregate_data_no_address
			)

--updating the #aggregate_data_without_district_but_with_address table
UPDATE #aggregate_data_without_district_but_with_address
SET district = 'Cape Town'
	,subdistrict = 'Klipfontein'
WHERE AddressLine3 LIKE '%(kli)%'
	AND district IS NULL

UPDATE #aggregate_data_without_district_but_with_address
SET district = 'Cape Town'
	,subdistrict = 'Mitchells Plain'
WHERE AddressLine3 LIKE '%phillipi (sou)%'
	AND district IS NULL

UPDATE #aggregate_data_without_district_but_with_address
SET district = 'Cape Town'
	,subdistrict = 'Southern'
WHERE AddressLine3 LIKE '%cape town (sou)%'
	AND district IS NULL

UPDATE #aggregate_data_without_district_but_with_address
SET district = 'Cape Town'
	,subdistrict = 'Mitchells Plain'
WHERE AddressLine3 LIKE '%PHILLIPI (MIT)%'

UPDATE #aggregate_data_without_district_but_with_address
SET district = 'Cape Town'
	,subdistrict = 'Khayelitsha'
WHERE (
		AddressLine3 LIKE '%Khayelitsha%'
		OR addressline3 LIKE '%(kha)%'
		)
	AND district IS NULL

UPDATE #aggregate_data_without_district_but_with_address
SET district = 'Cape Town'
	,subdistrict = 'Western'
WHERE AddressLine3 LIKE '%GREENPOINT%'
	AND district IS NULL

UPDATE #aggregate_data_without_district_but_with_address
SET district = 'West Coast'
	,subdistrict = 'Cederberg'
WHERE (
		AddressLine3 LIKE '%CLANWILLIAM%'
		OR AddressLine4 LIKE '%CLANWILLIAM%'
		)
	AND district IS NULL

UPDATE #aggregate_data_without_district_but_with_address
SET district = 'Cape Town'
	,subdistrict = 'Mitchells Plain'
WHERE (
		AddressLine3 LIKE '%philippi%'
		OR AddressLine2 LIKE '%philippi%'
		OR AddressLine1 LIKE '%philippi%'
		)
	AND district IS NULL

UPDATE #aggregate_data_without_district_but_with_address
SET district = 'Cape Town'
	,subdistrict = 'Tygerberg'
WHERE addressline3 LIKE '%(tyg)%'
	AND district IS NULL

UPDATE #aggregate_data_without_district_but_with_address
SET district = 'Cape Town'
	,subdistrict = 'Northern'
WHERE addressline3 LIKE '%PARLIAMENT%'
	AND district IS NULL

UPDATE #aggregate_data_without_district_but_with_address
SET district = 'Cape Town'
	,subdistrict = 'Eastern'
WHERE addressline3 LIKE '%CAMPS BAY%'
	AND district IS NULL

UPDATE #aggregate_data_without_district_but_with_address
SET district = 'Cape Town'
	,subdistrict = 'Northern'
WHERE addressline3 LIKE '%PAROW (NOR)%'
	AND district IS NULL

UPDATE #aggregate_data_without_district_but_with_address
SET district = 'Cape Town'
	,subdistrict = 'Western'
WHERE addressline3 LIKE '%(wes)%'
	AND district IS NULL

--===============================================================================================================================================================================================
--checking individuals from other provinces
--there are 12357 rows affected
DROP TABLE

IF EXISTS #aggregate_data_other_provinces
	SELECT *
	INTO #aggregate_data_other_provinces
	FROM #aggregate_data_without_district_but_with_address
	WHERE district IS NULL

UPDATE #aggregate_data_other_provinces
SET subdistrict = 'Outside Province'

UPDATE #aggregate_data_other_provinces
SET district = 'Outside Province'

--===============================================================================================================================================================================================
--checking individuals from capetown
--there are 1409 rows affected
DROP TABLE

IF EXISTS #aggregate_data_cape
	SELECT *
	INTO #aggregate_data_cape
	FROM #aggregate_data_without_district_but_with_address
	WHERE district IS NOT NULL

--===============================================================================================================================================================================================
--combine all the tables
DROP TABLE

IF EXISTS #aggregate_data_final
	SELECT *
	INTO #aggregate_data_final
	FROM (
		SELECT *
		FROM #aggregate_data_with_district
		
		UNION ALL
		
		SELECT *
		FROM #aggregate_data_no_address
		
		UNION ALL
		
		SELECT *
		FROM #aggregate_data_other_provinces
		
		UNION ALL
		
		SELECT *
		FROM #aggregate_data_cape
		) a
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
TRUNCATE TABLE [reporting].[cscd].[child_aggregate]

INSERT INTO [reporting].[cscd].[child_aggregate] (
	district
	,subdistrict
	,sex
	,age_years
	,[count]
	)
SELECT district
	,subdistrict
	,sex
	,age_years
	,count
FROM (
	SELECT *
		,CASE 
			WHEN district = 'Province Total'
				THEN 1
			WHEN district = 'Outside Province'
				THEN 2
			WHEN district = 'Outside Province  Total'
				THEN 3
			WHEN district = 'Grand Total'
				THEN 4
			ELSE 0
			END AS sort
	FROM (
		SELECT district
			,subdistrict
			,sex
			,cast(age_years AS VARCHAR) AS age_years
			,count(age_years) AS 'count'
		FROM #aggregate_data_final
		GROUP BY district
			,subdistrict
			,age_years
			,sex
		
		UNION ALL
		
		SELECT 'Province Total'
			,'Province Total'
			,'M'
			,'0 - 18'
			,count(*)
		FROM #aggregate_data_final
		WHERE district <> 'Outside Province'
			AND sex = 'M'
		
		UNION ALL
		
		SELECT 'Province Total'
			,'Province Total'
			,'F'
			,'0 - 18'
			,count(*)
		FROM #aggregate_data_final
		WHERE district <> 'Outside Province'
			AND sex = 'F'
		
		UNION ALL
		
		SELECT 'Province Total'
			,'Province Total'
			,'Unknown'
			,'0 - 18'
			,count(*)
		FROM #aggregate_data_final
		WHERE district <> 'Outside Province'
			AND sex = 'Unknown'
		
		UNION ALL
		
		SELECT 'Outside Province  Total'
			,'Outside Province  Total'
			,'M'
			,'0 - 18'
			,count(*)
		FROM #aggregate_data_final
		WHERE district = 'Outside Province'
			AND sex = 'M'
		
		UNION ALL
		
		SELECT 'Outside Province  Total'
			,'Outside Province  Total'
			,'F'
			,'0 - 18'
			,count(*)
		FROM #aggregate_data_final
		WHERE district = 'Outside Province'
			AND sex = 'F'
		
		UNION ALL
		
		SELECT 'Outside Province  Total'
			,'Outside Province  Total'
			,'Unknown'
			,'0 - 18'
			,count(*)
		FROM #aggregate_data_final
		WHERE district = 'Outside Province'
			AND sex = 'Unknown'
		
		UNION ALL
		
		SELECT 'Grand Total'
			,'Grand Total'
			,'M'
			,'0 - 18'
			,count(*)
		FROM #aggregate_data_final
		WHERE sex = 'M'
		
		UNION ALL
		
		SELECT 'Grand Total'
			,'Grand Total'
			,'F'
			,'0 - 18'
			,count(*)
		FROM #aggregate_data_final
		WHERE sex = 'F'
		
		UNION ALL
		
		SELECT 'Grand Total'
			,'Grand Total'
			,'Unknown'
			,'0 - 18'
			,count(*)
		FROM #aggregate_data_final
		WHERE sex = 'Unknown'
		) b
	) c
ORDER BY sort
	,district
	,subdistrict
	,age_years
	,sex
