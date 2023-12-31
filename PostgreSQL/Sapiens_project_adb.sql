-- Creating fact tables 

DROP TABLE std4_63.traffic;
DROP TABLE std4_63.bills_head;
DROP TABLE std4_63.bills_item;

CREATE TABLE std4_63.traffic (
	plant bpchar(4) NULL,
	date date NULL,
	time bpchar(6) NULL,
	frame_id bpchar(10) NULL,
	quantity int4 NULL
)
WITH (appendonly=true, orientation=column, compresslevel=1, compresstype=zstd) 
DISTRIBUTED BY ("date");

CREATE TABLE std4_63.bills_head (
	billnum int8 NULL,
	plant bpchar(4) NULL,
	calday date NULL
)
WITH (appendonly=true, orientation=column, compresslevel=1, compresstype=zstd) 
DISTRIBUTED BY (billnum);

CREATE TABLE std4_63.bills_item (
	billnum int8 NULL,
	billitem int8 NULL,
	material int8 NULL,
	qty int8 NULL,
	netval numeric(17, 2) NULL,
	tax numeric(17, 2) NULL,
	rpa_sat numeric(17, 2) NULL,
	calday date NULL
)
WITH (appendonly=true, orientation=column, compresslevel=1, compresstype=zstd) 
DISTRIBUTED BY (billnum);

-- Creating lookup tables

DROP TABLE std4_63.stores;
DROP TABLE std4_63.coupons;
DROP TABLE std4_63.promos;
DROP TABLE std4_63.promo_types;

CREATE TABLE std4_63.stores (
  plant bpchar(10) NULL,
  plant_txt bpchar(30) NULL
) DISTRIBUTED REPLICATED;

CREATE TABLE std4_63.coupons (
  plant bpchar(10) NULL,
  day bpchar(20) NULL,
  coupon_num bpchar(30) NULL,
  coupon_promo bpchar(50) NULL,
  material int8 NULL,
  bill int8 NULL
) DISTRIBUTED REPLICATED;

CREATE TABLE std4_63.promos (
  coupon_promo bpchar(40) NULL,
  promo_name bpchar(30) NULL,
  promo_type int4 NULL,
  material int8 NULL,
  discount int4 NULL
) DISTRIBUTED REPLICATED;

CREATE TABLE std4_63.promo_types (
  promo_type int4 NULL,
  promo_txt bpchar(50) NULL
) DISTRIBUTED REPLICATED;

-- Creating external tables using PXF

DROP EXTERNAL TABLE std4_63.traffic_ext;

CREATE EXTERNAL TABLE std4_63.traffic_ext
( 	plant bpchar(10),
	"date" bpchar(10),
	"time" bpchar(6),
	frame_id bpchar(10),
	quantity int4 
)
LOCATION ( 'pxf://gp.traffic?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

CREATE EXTERNAL TABLE std4_63.bills_head_ext
( 	billnum int8,
	plant bpchar(10),
	calday date
)
LOCATION ( 'pxf://gp.bills_head?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

CREATE EXTERNAL TABLE std4_63.bills_item_ext
( 	billnum int8,
	billitem int8,
	material int8,
	qty int8,
	netval numeric(17, 2),
	tax numeric(17, 2),
	rpa_sat numeric(17, 2),
	calday date
)
LOCATION ( 'pxf://gp.bills_item?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

SELECT * FROM std4_63.traffic_ext limit 10;
SELECT * FROM std4_63.bills_head_ext limit 10;
SELECT * FROM std4_63.bills_item_ext limit 10;

-- Creating external tables using GPFDIST

DROP EXTERNAL TABLE std4_63.coupons_gpf_ext;

CREATE EXTERNAL TABLE std4_63.stores_gpf_ext
( 
  plant bpchar(10),
  plant_txt bpchar(30)
)
LOCATION ( 'gpfdist://172.16.128.26:8081/stores.csv'
)
FORMAT 'CSV' (DELIMITER ';' NULL '' ESCAPE '"' QUOTE '"')
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 10 ROWS;

SELECT count(*) FROM std4_63.stores_gpf_ext;

CREATE EXTERNAL TABLE std4_63.coupons_gpf_ext
( 
  plant bpchar(10),
  day bpchar(20),
  coupon_num bpchar(30),
  coupon_promo bpchar(50),
  good int8,
  bill int8
)
LOCATION ( 'gpfdist://172.16.128.26:8081/coupons.csv'
)
FORMAT 'CSV' (DELIMITER ';' NULL '' ESCAPE '"' QUOTE '"')
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 10 ROWS;

SELECT count(*) FROM std4_63.coupons_gpf_ext;

CREATE EXTERNAL TABLE std4_63.promos_gpf_ext
( 
  coupon_promo bpchar(40),
  promo_name bpchar(30),
  promo_type int4,
  good int8,
  discount int4
)
LOCATION ( 'gpfdist://172.16.128.26:8081/promos.csv'
)
FORMAT 'CSV' (DELIMITER ';' NULL '' ESCAPE '"' QUOTE '"')
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 10 ROWS;

SELECT count(*) FROM std4_63.promos_gpf_ext;

CREATE EXTERNAL TABLE std4_63.promo_types_gpf_ext
( 
  promo_type int4,
  promo_txt bpchar(50)
)
LOCATION ( 'gpfdist://172.16.128.26:8081/promo_types.csv'
)
FORMAT 'CSV' (DELIMITER ';' NULL '' ESCAPE '"' QUOTE '"')
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 10 ROWS;

SELECT count(*) FROM std4_63.promo_types_gpf_ext;

-- UDF for lookup tables

CREATE OR REPLACE FUNCTION std4_63.f_load_full(p_table text, p_file_name text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$

DECLARE
	v_ext_table_name text;
	v_sql text;
	v_gpfdist text;
	v_result int;

BEGIN
	 
 	v_ext_table_name := p_table||'_ext';
 
	EXECUTE 'TRUNCATE TABLE ' || p_table;
 
	EXECUTE 'DROP EXTERNAL TABLE IF EXISTS ' || v_ext_table_name;
  
	v_gpfdist := 'GPFDIST://172.16.128.26:8081/' || p_file_name ||'.CSV';
  
	v_sql := 'CREATE EXTERNAL TABLE ' || v_ext_table_name || '(LIKE ' || p_table || ') 
			LOCATION (''' || v_gpfdist || ''') 
			ON ALL 
			FORMAT ''CSV'' ( HEADER DELIMITER '';'' NULL '''' ESCAPE ''"'' QUOTE ''"'') 
			ENCODING ''UTF8''';
  
	RAISE NOTICE 'EXTERNAL TABLE IS: %', v_sql;
 
	EXECUTE v_sql;
  
	EXECUTE 'INSERT INTO ' || p_table || ' SELECT * FROM ' || v_ext_table_name;
  
	EXECUTE 'SELECT COUNT(1) FROM ' || p_table INTO v_result;
  
	RETURN v_result;
 
END;

$$
EXECUTE ON ANY;

SELECT std4_63.f_load_full('std4_63.stores', 'stores');
SELECT std4_63.f_load_full('std4_63.coupons', 'coupons');
SELECT std4_63.f_load_full('std4_63.promos', 'promos');
SELECT std4_63.f_load_full('std4_63.promo_types', 'promo_types');

SELECT * FROM std4_63.stores;
SELECT * FROM std4_63.coupons;
SELECT * FROM std4_63.promos;
SELECT * FROM std4_63.promo_types;

-- UDF for fact tables

CREATE OR REPLACE FUNCTION std4_63.f_load_pxf(
    p_table text,
    p_pxf_table text,
    p_date_column text,
    p_start_date text,
    p_end_date text,
    p_user_id text,
    p_pass text
)
RETURNS int4
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_ext_table text;
    v_sql text;
    v_pxf text;
    v_result int;
    v_cnt int8;
BEGIN
    -- Construct names for the external table
    v_ext_table := p_table || '_ext';

    -- Define the location of the PXF external table with date filters
    v_pxf := 'pxf://' || p_pxf_table || '?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=' || p_user_id || '&PASS=' || p_pass;

    -- Drop existing external table if exists
    EXECUTE 'DROP EXTERNAL TABLE IF EXISTS ' || v_ext_table;

    -- Create an external table that mirrors the structure of the target table
    v_sql := 'CREATE EXTERNAL TABLE ' || v_ext_table || ' (LIKE ' || p_table || ') LOCATION (''' || v_pxf || ''') ON ALL FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'') ENCODING ''UTF8''';
    EXECUTE v_sql;

    -- Insert statement with a date range filter based on the provided dates
    v_sql := 'INSERT INTO ' || p_table || ' SELECT * FROM ' || v_ext_table;
    v_sql := v_sql || ' WHERE ' || p_date_column || ' BETWEEN ''' || p_start_date || '''::date AND ''' || p_end_date || '''::date';

    EXECUTE v_sql;

    -- Retrieve the count of inserted rows
    GET DIAGNOSTICS v_cnt = ROW_COUNT;
    RAISE NOTICE 'INSERTED ROWS: %', v_cnt;

    -- Drop the external table after loading is complete
    EXECUTE 'DROP EXTERNAL TABLE IF EXISTS ' || v_ext_table;

    -- Return the count of inserted rows
    RETURN v_cnt;
END;
$$
EXECUTE ON ANY;


SELECT std4_63.f_load_pxf(
    'std4_63.traffic',         -- Table name
    'gp.traffic',              -- External table/data source (replace with actual source)
    'date',                    -- Date column name in the traffic table
    '20210101',                -- Start date (replace with actual start date)
    '20210228',                -- End date (replace with actual end date)
    'intern',                  -- User ID for external source
    'intern'                   -- Password for external source
);

SELECT * FROM std4_63.traffic;

SELECT gp_segment_id, count(*)
FROM std4_63.traffic
GROUP BY 1;

SELECT (gp_toolkit.gp_skew_coefficient('std4_63.traffic'::regclass)).skccoeff;

-- gp_skew_coefficient 36

SELECT std4_63.f_load_pxf(
    'std4_63.bills_head',         -- Table name
    'gp.bills_head',              -- External table/data source (replace with actual source)
    'calday',                    -- Date column name in the traffic table
    '20210101',                -- Start date (replace with actual start date)
    '20210228',                -- End date (replace with actual end date)
    'intern',                  -- User ID for external source
    'intern'                   -- Password for external source
);

SELECT * FROM std4_63.bills_head;

SELECT gp_segment_id, count(*)
FROM std4_63.bills_head
GROUP BY 1;

SELECT (gp_toolkit.gp_skew_coefficient('std4_63.bills_head'::regclass)).skccoeff;

-- gp_skew_coefficient 4,91

SELECT std4_63.f_load_pxf(
    'std4_63.bills_item',         -- Table name
    'gp.bills_item',              -- External table/data source (replace with actual source)
    'calday',                    -- Date column name in the traffic table
    '20210101',                -- Start date (replace with actual start date)
    '20210228',                -- End date (replace with actual end date)
    'intern',                  -- User ID for external source
    'intern'                   -- Password for external source
);

SELECT * FROM std4_63.bills_item;

SELECT gp_segment_id, count(*)
FROM std4_63.bills_item
GROUP BY 1;

SELECT (gp_toolkit.gp_skew_coefficient('std4_63.bills_item'::regclass)).skccoeff;

-- gp_skew_coefficient 5,74

-- Logs function

DROP SEQUENCE std4_63.log_id_seq;

CREATE TABLE std4_63.logs (
    log_id int8 NOT NULL,
    log_timestamp timestamp NOT NULL DEFAULT now(),
    log_type text NOT NULL,
    log_msg text NOT NULL,
    log_location text NULL,
    is_error bool NULL,
    log_user text NULL DEFAULT "current_user"(),
    CONSTRAINT pk_log_id PRIMARY KEY (log_id)
)
DISTRIBUTED BY (log_id);

CREATE SEQUENCE std4_63.log_id_seq 
		INCREMENT BY 1
		minvalue 1
		maxvalue 9223372036854775807
		start 1;

CREATE OR REPLACE FUNCTION std4_63.f_load_write_log(p_log_type text, p_log_message text, p_location text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$

DECLARE

    v_log_type text;
    v_log_message text;
    v_sql text;
    v_location text;
    v_res text;

BEGIN
	
    --Check message type
    v_log_type = upper(p_log_type);
    v_location = lower(p_location);
    IF v_log_type NOT IN ('ERROR', 'INFO') THEN
        RAISE EXCEPTION 'Illegal log type! Use one of: ERROR, INFO';
    END IF;

  RAISE NOTICE '%: %: <%> Location[%]', clock_timestamp(), v_log_type, p_log_message, v_location;

  v_log_message := replace(p_log_message, '''', '''''');

  v_sql := 'INSERT INTO std4_63.logs(log_id, log_type, log_msg, log_location, is_error, log_timestamp, log_user)
             VALUES (' || nextval('std4_63.log_id_seq')|| ',
       		       ''' || v_log_type || ''',
       		         ' || coalesce('''' || v_log_message || '''', '''empty''')|| ',
       		         ' || coalesce('''' || v_location || '''', 'null')|| ',
      		         ' || CASE WHEN v_log_type = 'ERROR' THEN TRUE ELSE FALSE END || ',
       			          current_timestamp, current_user);';

  RAISE NOTICE 'INSERT SQL IS: %', v_sql;
  v_res := dblink('adb_server', v_sql); 
  END;

$$
EXECUTE ON ANY;

-- Function for calculating report data mart

CREATE OR REPLACE FUNCTION std4_63.f_report_data_mart(p_start_date varchar, p_end_date varchar)
    RETURNS int4
    LANGUAGE plpgsql
    VOLATILE
AS $$
DECLARE
    v_table_name text;
    v_return int;
    v_year_month_start date;
    v_year_month_end date;
BEGIN
    -- Determine the start and end dates based on input
    v_year_month_start := to_date(p_start_date, 'YYYYMMDD');
    v_year_month_end := to_date(p_end_date, 'YYYYMMDD');

    -- Construct the table name based on the start date
    -- Construct the table name based on the full start and end dates
	v_table_name := 'std4_63.report_' || replace(p_start_date, '-', '') || '_to_' || replace(p_end_date, '-', '');

    -- Log the start of the process
    PERFORM std4_63.f_load_write_log(p_log_type := 'INFO',
                                     p_log_message := 'Start f_load_mart',
                                     p_location := 'Sales mart calculation');

    -- Create or truncate the mart table for the given month
    EXECUTE 'DROP TABLE IF EXISTS ' || v_table_name;
    EXECUTE 'CREATE TABLE ' || v_table_name || ' ( 
        plant bpchar(10),
 		plant_txt bpchar(30),
		revenue numeric,
		discount numeric,
		revenue_no_discounts numeric,
		goods_sold int4,
		bills_num int4,
		traffic int4,
		goods_with_promo int4,
		goods_with_discount numeric,
		avg_number_goods numeric,
		conversion numeric,
		avg_bill numeric,
		arpu numeric
    ) DISTRIBUTED BY (plant)';

-- Insert data into the mart table
    EXECUTE 'INSERT INTO ' || v_table_name || ' (plant, plant_txt, revenue, discount, revenue_no_discounts, goods_sold, bills_num, traffic, goods_with_promo, goods_with_discount, avg_number_goods, conversion, avg_bill, arpu)
              SELECT 
                  s.plant,
                  s.plant_txt,
                  SUM(bi.netval + bi.tax) AS revenue,
                  SUM(
                      CASE 
                          WHEN p.promo_type = 1 THEN p.discount 
                          ELSE p.discount / 100 * (bi.netval + bi.tax) 
                      END
                  ) AS discount,
                  SUM(bi.netval + bi.tax) - SUM(
                      CASE 
                          WHEN p.promo_type = 1 THEN p.discount
                          ELSE p.discount / 100 * (bi.netval + bi.tax)
                      END
                  ) AS revenue_no_discounts,
                  SUM(bi.qty) AS goods_sold,
                  COUNT(DISTINCT bi.billnum) AS bills_num,
                  COALESCE(traffic_data.total_traffic, 0) AS traffic,
                  COUNT(DISTINCT cp.coupon_num) AS goods_with_promo,
                  COUNT(DISTINCT cp.coupon_num) / NULLIF(SUM(bi.qty), 0) AS goods_with_discount,
                  SUM(bi.qty) / NULLIF(COUNT(DISTINCT bi.billnum), 0) AS avg_number_goods,
                  COUNT(DISTINCT bi.billnum)::numeric / NULLIF(COALESCE(traffic_data.total_traffic, 0), 0) AS conversion,
                  SUM(bi.netval + bi.tax) / NULLIF(COUNT(DISTINCT bi.billnum), 0) AS avg_bill,
                  SUM(bi.netval + bi.tax) / NULLIF(COALESCE(traffic_data.total_traffic, 0), 0) AS arpu
              FROM 
                  std4_63.stores s
                  JOIN std4_63.bills_head h ON s.plant = h.plant
                  JOIN std4_63.bills_item bi ON h.billnum = bi.billnum
                  LEFT JOIN std4_63.coupons cp ON bi.material = cp.material AND h.billnum = cp.bill
                  LEFT JOIN std4_63.promos p ON cp.coupon_promo = p.coupon_promo
                  LEFT JOIN (
                      SELECT 
                          plant, 
                          SUM(quantity) AS total_traffic
                      FROM 
                          std4_63.traffic
                      GROUP BY 
                          plant
                  ) traffic_data ON s.plant = traffic_data.plant
              WHERE 
                  bi.calday >= ' || quote_literal(v_year_month_start) || ' AND
                  bi.calday <= ' || quote_literal(v_year_month_end) || '
              GROUP BY s.plant, s.plant_txt, traffic_data.total_traffic
              ORDER BY s.plant';

    -- Get the count of rows inserted
    EXECUTE 'SELECT count(*) FROM ' || v_table_name INTO v_return; 
   
    -- Log the completion of the process
    PERFORM std4_63.f_load_write_log(p_log_type := 'INFO',
                                     p_log_message := v_return || ' rows inserted',
                                     p_location := 'Sales mart calculation');

    PERFORM std4_63.f_load_write_log(p_log_type := 'INFO',
                                     p_log_message := 'End f_load_mart',
                                     p_location := 'Sales mart calculation');

    RETURN v_return;
END;
$$
EXECUTE ON ANY;

SELECT std4_63.f_report_data_mart('20210101', '20210228');

SELECT * FROM std4_63.report_20210101_to_20210228 ORDER BY plant;

SELECT * FROM std4_63.logs;
