-- Copyright 2026 Snowflake Inc.
-- SPDX-License-Identifier: Apache-2.0
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

/******************************************************************************
  _______           _            ____          _
 |__   __|         | |          |  _ \        | |
    | |  __ _  ___ | |_  _   _  | |_) | _   _ | |_  ___  ___
    | | / _` |/ __|| __|| | | | |  _ < | | | || __|/ _ \/ __|
    | || (_| |\__ \| |_ | |_| | | |_) || |_| || |_|  __/\__ \
    |_| \__,_||___/ \__| \__, | |____/  \__, | \__|\___||___/
                          __/ |          __/ |
                         |___/          |___/
Quickstart:   Tasty Bytes - Zero to Snowflake - Introduction
Version:      v2
Author:       Jacob Kranzler
Copyright(c): 2026 Snowflake Inc. All rights reserved.
*******************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ---------------------------------------
2024-05-23          Jacob Kranzler      Initial Release
2026-01-11          Sho Tanaka          Initial commit with JA
*******************************************************************************/

USE ROLE sysadmin;

/*--
 • データベースとウェアハウスの作成
--*/

-- tb_101 データベースを作成
CREATE OR REPLACE DATABASE tb_101;

-- raw_pos データベースを作成
CREATE OR REPLACE SCHEMA tb_101.raw_pos;

-- raw_customer データベースを作成
CREATE OR REPLACE SCHEMA tb_101.raw_customer;

-- harmonized スキーマの作成
CREATE OR REPLACE SCHEMA tb_101.harmonized;

-- analytics スキーマの作成
CREATE OR REPLACE SCHEMA tb_101.analytics;

-- ウェアハウスの作成
-- 初期データロード用にLargeで設定 - このスクリプトの終了時にXSmallにスケールダウン
CREATE OR REPLACE WAREHOUSE tb_de_wh
    WAREHOUSE_SIZE = 'large'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'data engineering warehouse for tasty bytes';

CREATE OR REPLACE WAREHOUSE tb_dev_wh
WAREHOUSE_SIZE = 'xsmall'
WAREHOUSE_TYPE = 'standard'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE
COMMENT = 'developer warehouse for tasty bytes';

-- ★★★ Stop ★★★

-- ロール作成
USE ROLE securityadmin;

-- 機能ロール
CREATE ROLE IF NOT EXISTS tb_admin
COMMENT = 'admin for tasty bytes';

CREATE ROLE IF NOT EXISTS tb_data_engineer
COMMENT = 'data engineer for tasty bytes';

CREATE ROLE IF NOT EXISTS tb_dev
COMMENT = 'developer for tasty bytes';

-- ロールの階層設定
GRANT ROLE tb_admin TO ROLE sysadmin;
GRANT ROLE tb_data_engineer TO ROLE tb_admin;
GRANT ROLE tb_dev TO ROLE tb_data_engineer;

-- 権限付与
USE ROLE accountadmin;

GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE tb_data_engineer;

GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE tb_admin;

USE ROLE securityadmin;

GRANT USAGE ON DATABASE tb_101 TO ROLE tb_admin;
GRANT USAGE ON DATABASE tb_101 TO ROLE tb_data_engineer;
GRANT USAGE ON DATABASE tb_101 TO ROLE tb_dev;

GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tb_admin;
GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tb_data_engineer;
GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.raw_pos TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.raw_pos TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.raw_pos TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.harmonized TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.harmonized TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.harmonized TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.analytics TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.analytics TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.analytics TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.public TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.public TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.public TO ROLE tb_dev;

-- ウェアハウスの権限付与
GRANT OWNERSHIP ON WAREHOUSE tb_de_wh TO ROLE tb_admin COPY CURRENT GRANTS;
GRANT ALL ON WAREHOUSE tb_de_wh TO ROLE tb_admin;
GRANT ALL ON WAREHOUSE tb_de_wh TO ROLE tb_data_engineer;

GRANT ALL ON WAREHOUSE tb_dev_wh TO ROLE tb_admin;
GRANT ALL ON WAREHOUSE tb_dev_wh TO ROLE tb_data_engineer;
GRANT ALL ON WAREHOUSE tb_dev_wh TO ROLE tb_dev;

-- future grants 将来のオブジェクトへの権限付与設定
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_pos TO ROLE tb_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_pos TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_pos TO ROLE tb_dev;

GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_customer TO ROLE tb_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_customer
    TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_customer TO ROLE tb_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.harmonized TO ROLE tb_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.harmonized TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.harmonized TO ROLE tb_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.analytics TO ROLE tb_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.analytics TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.analytics TO ROLE tb_dev;

-- マスキングポリシー適用のための権限付与
USE ROLE accountadmin;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE tb_admin;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE tb_data_engineer;

-- raw_pos テーブルの構築
USE ROLE sysadmin;
USE WAREHOUSE tb_de_wh;

/*--
 • ファイルフォーマットと外部ステージの作成
--*/

CREATE OR REPLACE FILE FORMAT tb_101.public.csv_ff
TYPE = 'csv';

CREATE OR REPLACE STAGE tb_101.public.s3load
    URL = 's3://sfquickstarts/frostbyte_tastybytes/'
    FILE_FORMAT = tb_101.public.csv_ff
    COMMENT = 'Quickstarts S3 Stage Connection';

-- ★★★ Stop ★★★

/*--
 raw zone テーブルの構築
--*/

-- country テーブルの作成
CREATE OR REPLACE TABLE tb_101.raw_pos.country
(
    country_id NUMBER(18, 0),
    country VARCHAR(16777216),
    iso_currency VARCHAR(3),
    iso_country VARCHAR(2),
    city_id NUMBER(19, 0),
    city VARCHAR(16777216),
    city_population VARCHAR(16777216)
);

-- franchise テーブル作成
CREATE OR REPLACE TABLE tb_101.raw_pos.franchise
(
    franchise_id NUMBER(38, 0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216)
);

-- location テーブル作成
CREATE OR REPLACE TABLE tb_101.raw_pos.location
(
    location_id NUMBER(19, 0),
    placekey VARCHAR(16777216),
    location VARCHAR(16777216),
    city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    country VARCHAR(16777216)
);

-- menu テーブル作成
CREATE OR REPLACE TABLE tb_101.raw_pos.menu
(
    menu_id NUMBER(19, 0),
    menu_type_id NUMBER(38, 0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38, 0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38, 4),
    sale_price_usd NUMBER(38, 4),
    menu_item_health_metrics_obj VARIANT
);

-- truck テーブル作成
CREATE OR REPLACE TABLE tb_101.raw_pos.truck
(
    truck_id NUMBER(38, 0),
    menu_type_id NUMBER(38, 0),
    primary_city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_region VARCHAR(16777216),
    country VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    franchise_flag NUMBER(38, 0),
    year NUMBER(38, 0),
    make VARCHAR(16777216),
    model VARCHAR(16777216),
    ev_flag NUMBER(38, 0),
    franchise_id NUMBER(38, 0),
    truck_opening_date DATE
);

-- order_header テーブル作成
CREATE OR REPLACE TABLE tb_101.raw_pos.order_header
(
    order_id NUMBER(38, 0),
    truck_id NUMBER(38, 0),
    location_id FLOAT,
    customer_id NUMBER(38, 0),
    discount_id VARCHAR(16777216),
    shift_id NUMBER(38, 0),
    shift_start_time TIME(9),
    shift_end_time TIME(9),
    order_channel VARCHAR(16777216),
    order_ts TIMESTAMP_NTZ(9),
    served_ts VARCHAR(16777216),
    order_currency VARCHAR(3),
    order_amount NUMBER(38, 4),
    order_tax_amount VARCHAR(16777216),
    order_discount_amount VARCHAR(16777216),
    order_total NUMBER(38, 4)
);

-- order_detail テーブル作成
CREATE OR REPLACE TABLE tb_101.raw_pos.order_detail
(
    order_detail_id NUMBER(38, 0),
    order_id NUMBER(38, 0),
    menu_item_id NUMBER(38, 0),
    discount_id VARCHAR(16777216),
    line_number NUMBER(38, 0),
    quantity NUMBER(5, 0),
    unit_price NUMBER(38, 4),
    price NUMBER(38, 4),
    order_item_discount_amount VARCHAR(16777216)
);

-- customer_loyalty テーブル作成
CREATE OR REPLACE TABLE tb_101.raw_customer.customer_loyalty
(
    customer_id NUMBER(38, 0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    postal_code VARCHAR(16777216),
    preferred_language VARCHAR(16777216),
    gender VARCHAR(16777216),
    favourite_brand VARCHAR(16777216),
    marital_status VARCHAR(16777216),
    children_count VARCHAR(16777216),
    sign_up_date DATE,
    birthday_date DATE,
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216)
);

/*--
 • harmonized スキーマ上でのビューの作成
--*/

-- orders_v ビュー
CREATE OR REPLACE VIEW tb_101.harmonized.orders_v
    AS
SELECT
    oh.order_id,
    oh.truck_id,
    oh.order_ts,
    od.order_detail_id,
    od.line_number,
    m.truck_brand_name,
    m.menu_type,
    t.primary_city,
    t.region,
    t.country,
    t.franchise_flag,
    t.franchise_id,
    f.first_name AS franchisee_first_name,
    f.last_name AS franchisee_last_name,
    l.location_id,
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.children_count,
    cl.gender,
    cl.marital_status,
    od.menu_item_id,
    m.menu_item_name,
    od.quantity,
    od.unit_price,
    od.price,
    oh.order_amount,
    oh.order_tax_amount,
    oh.order_discount_amount,
    oh.order_total
FROM tb_101.raw_pos.order_detail AS od
INNER JOIN tb_101.raw_pos.order_header AS oh
    ON od.order_id = oh.order_id
INNER JOIN tb_101.raw_pos.truck AS t
    ON oh.truck_id = t.truck_id
INNER JOIN tb_101.raw_pos.menu AS m
    ON od.menu_item_id = m.menu_item_id
INNER JOIN tb_101.raw_pos.franchise AS f
    ON t.franchise_id = f.franchise_id
INNER JOIN tb_101.raw_pos.location AS l
    ON oh.location_id = l.location_id
LEFT JOIN tb_101.raw_customer.customer_loyalty AS cl
    ON oh.customer_id = cl.customer_id;

-- loyalty_metrics_v ビュー作成
CREATE OR REPLACE VIEW tb_101.harmonized.customer_loyalty_metrics_v
    AS
SELECT
    cl.customer_id,
    cl.city,
    cl.country,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    SUM(oh.order_total) AS total_sales,
    ARRAY_AGG(DISTINCT oh.location_id) AS visited_location_ids_array
FROM tb_101.raw_customer.customer_loyalty AS cl
INNER JOIN tb_101.raw_pos.order_header AS oh
ON cl.customer_id = oh.customer_id
GROUP BY cl.customer_id, cl.city, cl.country, cl.first_name,
cl.last_name, cl.phone_number, cl.e_mail;

/*--
 • analytics スキーマ上のビュー作成
--*/

-- orders_v ビュー作成
CREATE OR REPLACE VIEW tb_101.analytics.orders_v
COMMENT = 'Tasty Bytes Order Detail View'
    AS
SELECT
    *,
    DATE(order_ts) AS date
FROM tb_101.harmonized.orders_v;

-- customer_loyalty_metrics_v ビュー作成
CREATE OR REPLACE VIEW tb_101.analytics.customer_loyalty_metrics_v
COMMENT = 'Tasty Bytes Customer Loyalty Member Metrics View'
    AS
SELECT * FROM tb_101.harmonized.customer_loyalty_metrics_v;

-- ★★★ Stop ★★★

/*--
 raw zone へのデータロード
--*/

-- ステージ上のファイルをls で確認
LS @tb_101.public.s3load/raw_pos/;

-- country テーブルへのロード
COPY INTO tb_101.raw_pos.country
FROM @tb_101.public.s3load/raw_pos/country/;

-- franchise テーブルへのロード
COPY INTO tb_101.raw_pos.franchise
FROM @tb_101.public.s3load/raw_pos/franchise/;

-- location テーブルへのロード
COPY INTO tb_101.raw_pos.location
FROM @tb_101.public.s3load/raw_pos/location/;

-- menu テーブルへのロード
COPY INTO tb_101.raw_pos.menu
FROM @tb_101.public.s3load/raw_pos/menu/;

-- truck テーブルへのロード
COPY INTO tb_101.raw_pos.truck
FROM @tb_101.public.s3load/raw_pos/truck/;

-- customer_loyalty テーブルへのロード
COPY INTO tb_101.raw_customer.customer_loyalty
FROM @tb_101.public.s3load/raw_customer/customer_loyalty/;

-- order_header テーブルへのロード
COPY INTO tb_101.raw_pos.order_header
FROM @tb_101.public.s3load/raw_pos/order_header/;

-- order_detail テーブルへのロード
COPY INTO tb_101.raw_pos.order_detail
FROM @tb_101.public.s3load/raw_pos/order_detail/;

-- ★★★ Stop ★★★

-- ウェアハウスサイズのスケールダウン
ALTER WAREHOUSE tb_de_wh SET WAREHOUSE_SIZE = 'XSmall';

-- セットアップ完了
SELECT 'tb_101 setup is now complete' AS note;
