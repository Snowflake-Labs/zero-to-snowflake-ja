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
Quickstart:   Tasty Bytes - Zero to Snowflake - Cost Management
Version:      v2
Author:       Jacob Kranzler
Copyright(c): 2024 Snowflake Inc. All rights reserved.
*******************************************************************************

 各種リソースの確認

*******************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ---------------------------------------
2024-05-23          Jacob Kranzler      Initial Release
2026-01-11          Sho Tanaka          Initial commit with JA
******************************************************************************/


USE ROLE sysadmin;

-- 各種リソースの確認

USE ROLE sysadmin;
SHOW DATABASES LIKE 'tb_101';

SHOW SCHEMAS IN DATABASE tb_101;

SHOW TABLES IN SCHEMA tb_101.raw_pos;

SHOW ROLES LIKE 'tb%';

SHOW WAREHOUSES LIKE 'tb%';

USE ROLE tb_data_engineer;
USE WAREHOUSE tb_de_wh;

SELECT
    m.menu_type_id,
    m.menu_type,
    m.truck_brand_name,
    m.menu_item_name
FROM
  tb_101.raw_pos.menu AS m
WHERE
  m.truck_brand_name = 'Plant Palace';
