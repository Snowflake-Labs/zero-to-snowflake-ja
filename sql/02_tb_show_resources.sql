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
FROM tb_101.raw_pos.menu m
WHERE m.truck_brand_name = 'Plant Palace';