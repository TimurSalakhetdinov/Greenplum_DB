-- Creating intergrated table in Clickhouse

-- Creating database std4_63 on 206 host
CREATE DATABASE std4_63 ON CLUSTER default_cluster;

-- Drop the existing external table if it exists
DROP TABLE IF EXISTS std4_63.ch_report_ext;

-- Create an external table to access Greenplum data
CREATE TABLE std4_63.ch_report_ext
(
    plant String,
    plant_txt String,
    revenue Float64,
    discount Float64,
    revenue_no_discounts Float64,
    goods_sold Int32,
    bills_num Int32,
    traffic Int32,
    goods_with_promo Int32,
    goods_with_discount Float64,
    avg_number_goods Float64,
    conversion Float64,
    avg_bill Float64,
    arpu Float64
)
ENGINE = PostgreSQL('192.168.214.203:5432', 'adb', 'report_20210101_to_20210228', 'std4_63', 'zZVB3nCnJbqE', 'std4_63');

-- Drop the existing replicated table if it exists
DROP TABLE IF EXISTS std4_63.ch_report_copy ON CLUSTER default_cluster;

-- Drop the existing distributed table if it exists
DROP TABLE IF EXISTS std4_63.ch_report_distr;

-- Create a new table with the MergeTree engine
CREATE TABLE std4_63.ch_report
(
    plant String,
    plant_txt String,
    revenue Float64,
    discount Float64,
    revenue_no_discounts Float64,
    goods_sold Int32,
    bills_num Int32,
    traffic Int32,
    goods_with_promo Int32,
    goods_with_discount Float64,
    avg_number_goods Float64,
    conversion Float64,
    avg_bill Float64,
    arpu Float64
)
ENGINE = MergeTree()
ORDER BY (plant);

-- Insert data into this table from the source
INSERT INTO std4_63.ch_report SELECT * FROM std4_63.ch_report_ext;

-- Verify the data
SELECT * FROM std4_63.ch_report;
SELECT COUNT(*) FROM std4_63.ch_report; 
-- Returns 15 rows