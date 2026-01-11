/***************************************************************************************************
  _______           _            ____          _             
 |__   __|         | |          |  _ \        | |            
    | |  __ _  ___ | |_  _   _  | |_) | _   _ | |_  ___  ___ 
    | | / _` |/ __|| __|| | | | |  _ < | | | || __|/ _ \/ __|
    | || (_| |\__ \| |_ | |_| | | |_) || |_| || |_|  __/\__ \
    |_| \__,_||___/ \__| \__, | |____/  \__, | \__|\___||___/
                          __/ |          __/ |               
                         |___/          |___/            
Quickstart:   Tasty Bytes - Zero to Snowflake - Semi-Structured Data
Version:      v2     
Author:       Jacob Kranzler
Copyright(c): 2024 Snowflake Inc. All rights reserved.
****************************************************************************************************
半構造化データ
    1 - 半構造化データとVariantデータ型
    2 - ドットとブラケット表記 + Flattenを使用した半構造化データのクエリ
    3 - フラット化したデータのビジネスユーザーへの提供
    4 - Snowsightでの処理された非構造化データの分析
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2024-05-23          Jacob Kranzler      Initial Release
***************************************************************************************************/

/*----------------------------------------------------------------------------------
ステップ 1 - 半構造化データとVariantデータ型

 Tasty Bytesのデータエンジニアとして、メニューのデータをプロファイルし、下流のビジネスユーザーに
 食事と成分データを提供するAnalyticsレイヤービューを開発するという任務を受けました。
----------------------------------------------------------------------------------*/

-- まず、ロール、ウェアハウス、およびデータベースのコンテキストを設定する必要があります。
USE ROLE tb_data_engineer;
USE WAREHOUSE tb_de_wh;
USE DATABASE tb_101;


-- セッションにクエリタグを割り当てます。
ALTER SESSION SET query_tag = '{"origin":"sf_sit","name":"tb_zts,"version":{"major":1, "minor":1},"attributes":{"medium":"quickstart", "source":"tastybytes", "vignette": "semi_structured"}}';


-- 食事と成分データがどこに保存されているかを確認するために、POSシステムから受け取っている
-- 生のメニューテーブルのいくつかの列を見てみましょう。
SELECT TOP 10
    truck_brand_name,
    menu_type,
    menu_item_name,
    menu_item_health_metrics_obj
FROM raw_pos.menu;


-- 上記の結果に基づいて、提供する必要のあるデータは「Menu Item Health Metrics Object」列に保存されています。
-- 次に、この列のデータ型を調査するために、SHOW COLUMNSコマンドを使用します。
SHOW COLUMNS IN raw_pos.menu;

    /**
     Variant: Snowflakeは、JSON、Avro、ORC、またはParquet形式のデータをARRAY、OBJECT、およびVARIANTデータの
     階層に変換し、直接VARIANT列に保存することができます。
    **/


/*----------------------------------------------------------------------------------
ステップ 2 - 半構造化データのクエリ

 「Menu Item Health Metrics Object」列内のデータはJSON形式です。
 
 このステップでは、Snowflakeのネイティブ半構造化サポートを活用して、この列をクエリし、
 フラット化して、下流のユーザーにわかりやすい表形式でデータを提供できるように準備します。
----------------------------------------------------------------------------------*/

-- Variant列から第一階層の要素を抽出するためには、Variant列の名前と第一階層の識別子の間にコロン「:」を挿入します。
-- これを使用して、「Menu Item Id」と「Menu Item Health Metrics Object」を抽出してみましょう。
SELECT
    menu_item_health_metrics_obj:menu_item_id AS menu_item_id,
    menu_item_health_metrics_obj:menu_item_health_metrics AS menu_item_health_metrics
FROM raw_pos.menu;


/*--
 半構造化データをリレーショナル表現に変換するには、Flattenを使用できます。
 また、JSONオブジェクト内の要素にアクセスするために、ドット表記またはブラケット表記を使用できます。

 これらの両方を活用して、成分を配列列に抽出してみましょう。
--*/

--> ドット表記とLateral Flatten
SELECT
    m.menu_item_name,
    m.menu_item_health_metrics_obj:menu_item_id AS menu_item_id,
    obj.value:"ingredients"::ARRAY AS ingredients
FROM raw_pos.menu m, 
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj
ORDER BY menu_item_id;


--> ブラケット表記とLateral Flatten
SELECT
    m.menu_item_name,
    m.menu_item_health_metrics_obj['menu_item_id'] AS menu_item_id,
    obj.value['ingredients']::ARRAY AS ingredients
FROM raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj
ORDER BY menu_item_id;

    /**
     Array: SnowflakeのARRAYは、他の多くのプログラミング言語における配列に類似しています。
     ARRAYには0個以上のデータが含まれています。各要素には、その位置を指定してアクセスします。
    **/

    
/*--
 半構造化データ処理を完了するために、成分の配列に加えて、残りの食事関連列を
 ドット表記とブラケット表記の両方を使用して抽出しましょう。
--*/

--> ドット表記とLateral Flatten
SELECT
    m.menu_item_health_metrics_obj:menu_item_id AS menu_item_id,
    m.menu_item_name,
    obj.value:"ingredients"::VARIANT AS ingredients,
    obj.value:"is_healthy_flag"::VARCHAR(1) AS is_healthy_flag,
    obj.value:"is_gluten_free_flag"::VARCHAR(1) AS is_gluten_free_flag,
    obj.value:"is_dairy_free_flag"::VARCHAR(1) AS is_dairy_free_flag,
    obj.value:"is_nut_free_flag"::VARCHAR(1) AS is_nut_free_flag
FROM raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj;

    
--> ブラケット表記とLateral Flatten
SELECT
    m.menu_item_health_metrics_obj['menu_item_id'] AS menu_item_id,
    m.menu_item_name,
    obj.value['ingredients']::VARIANT AS ingredients,
    obj.value['is_healthy_flag']::VARCHAR(1) AS is_healthy_flag,
    obj.value['is_gluten_free_flag']::VARCHAR(1) AS is_gluten_free_flag,
    obj.value['is_dairy_free_flag']::VARCHAR(1) AS is_dairy_free_flag,
    obj.value['is_nut_free_flag']::VARCHAR(1) AS is_nut_free_flag
FROM raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj;

    
/*----------------------------------------------------------------------------------
ステップ 3 - フラット化したデータのビジネスユーザーへの提供

 必要なデータがすべて抽出され、フラット化されて表形式で利用可能になりました。
 このステップでは、フラット化した列を含むメニューテーブルを、HarmonizedおよびAnalyticsレイヤーに
 ビューとして提供します。

 Medallionアーキテクチャに慣れている場合、HarmonizedレイヤーはSilver、AnalyticsレイヤーはGold
 と考えることができます。
----------------------------------------------------------------------------------*/

-- まず、前のドット表記クエリに列を追加し、Harmonizedレイヤーに新しいメニュービューとして活用します。
CREATE OR REPLACE VIEW harmonized.menu_v
COMMENT = 'Menu level metrics including Truck Brands and Menu Item details including Cost, Price, Ingredients and Dietary Restrictions'
    AS
SELECT
    m.menu_id,
    m.menu_type_id,
    m.menu_type,
    m.truck_brand_name,
    m.menu_item_health_metrics_obj:menu_item_id::integer AS menu_item_id,
    m.menu_item_name,
    m.item_category,
    m.item_subcategory,
    m.cost_of_goods_usd,
    m.sale_price_usd,
    obj.value:"ingredients"::VARIANT AS ingredients,
    obj.value:"is_healthy_flag"::VARCHAR(1) AS is_healthy_flag,
    obj.value:"is_gluten_free_flag"::VARCHAR(1) AS is_gluten_free_flag,
    obj.value:"is_dairy_free_flag"::VARCHAR(1) AS is_dairy_free_flag,
    obj.value:"is_nut_free_flag"::VARCHAR(1) AS is_nut_free_flag
FROM raw_pos.menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj;

    
-- Harmonizedビューにフラット化ロジックが組み込まれたので、データを
-- Analyticsスキーマに昇格させ、さまざまなビジネスユーザーがアクセスできるようにします。
CREATE OR REPLACE VIEW analytics.menu_v
COMMENT = 'Menu level metrics including Truck Brands and Menu Item details including Cost, Price, Ingredients and Dietary Restrictions'
    AS
SELECT
    *
    EXCLUDE (menu_type_id)  -- MENU_TYPE_IDを除外する
    RENAME (truck_brand_name AS brand_name) -- TRUCK_BRAND_NAMEをBRAND_NAMEにリネームする
FROM harmonized.menu_v;

    /**
     Exclude: SELECT * 文の結果から除外する列を指定します。
     Rename: SELECT * 文の結果で使用される列のエイリアスを指定します。
    **/

-- 次に進む前に、このビューを使用してBetter Off Breadブランドの結果を確認しましょう。
SELECT 
    brand_name,
    menu_item_name,
    sale_price_usd,
    ingredients,
    is_healthy_flag,
    is_gluten_free_flag,
    is_dairy_free_flag,
    is_nut_free_flag
FROM analytics.menu_v
WHERE brand_name = 'Better Off Bread';


-- 結果は良好ですので、このビューをクエリする権限を開発者に付与します。
GRANT SELECT ON analytics.menu_v TO ROLE tb_dev;


/*----------------------------------------------------------------------------------
ステップ 4 - 配列関数の活用

 Analyticsレイヤーでメニュービューを利用できるようになったので、Tasty Bytesの
 開発者の業務に移りましょう。このステップでは、Tasty Bytesのリーダーシップチームから
 提出されたフードトラックメニューに関連する質問に対応します。
 
 この過程で、Snowflakeが半構造化データに対して追加のコピーや複雑なデータ変換を行わずに
 リレーショナルなクエリエクスペリエンスを提供できることがわかるでしょう。
----------------------------------------------------------------------------------*/

-- このステップを開始するにあたり、開発者ロールに切り替え、開発者ウェアハウスを使用します。
USE ROLE tb_dev;
USE WAREHOUSE tb_dev_wh;


-- 最近のレタスリコールに関するニュースを受けて、成分としてレタスを含むメニュー項目はどれですか？
SELECT
    m.menu_item_id,
    m.menu_item_name,
    m.ingredients
FROM analytics.menu_v m
WHERE ARRAY_CONTAINS('Lettuce'::VARIANT, m.ingredients);

    /**
     Array_contains: 指定された値が配列内に存在する場合、関数はTRUEを返します。
    **/

-- メニュータイプ全体で成分が重複するメニュー項目とそれらの成分はどれですか？
SELECT
    m1.brand_name,
    m1.menu_item_name,
    m2.brand_name AS overlap_brand,
    m2.menu_item_name AS overlap_menu_item_name,
    ARRAY_INTERSECTION(m1.ingredients, m2.ingredients) AS overlapping_ingredients
FROM analytics.menu_v m1
JOIN analytics.menu_v m2
    ON m1.menu_item_id <> m2.menu_item_id -- 同じメニュー項目同士の結合を避ける
    AND m1.menu_type <> m2.menu_type
WHERE 1=1
    AND m1.item_category  <> 'Beverage' -- 飲料を除外
    AND ARRAYS_OVERLAP(m1.ingredients, m2.ingredients) -- 重複する成分を持つもののみを返す
ORDER BY ARRAY_SIZE(overlapping_ingredients) DESC;-- 重複する成分の数が多い順に並べる

    /**
     Array_intersection: 2つの入力配列に含まれる共通の要素を含む配列を返します。
     Arrays_overlap: 2つの配列に少なくとも1つの共通要素があるかどうかを比較します。
     Array_size: 入力配列のサイズを返します。
    **/

-- 合計で何件のメニュー項目があり、そのうちどれが食事制限に対応していますか？
SELECT
    COUNT(DISTINCT menu_item_id) AS total_menu_items,
    SUM(CASE WHEN is_gluten_free_flag = 'Y' THEN 1 ELSE 0 END) AS gluten_free_item_count,
    SUM(CASE WHEN is_dairy_free_flag = 'Y' THEN 1 ELSE 0 END) AS dairy_free_item_count,
    SUM(CASE WHEN is_nut_free_flag = 'Y' THEN 1 ELSE 0 END) AS nut_free_item_count
FROM analytics.menu_v m;


-- 「Plant Palace」、「Peking Truck」、「Better Off Bread」ブランドはどのように比較されますか？
    --> Snowsightチャートタイプ: バー | 方向: 最初のオプション | グルーピング: 最初のオプション
        --> Y軸: BRAND_NAME | バー: GLUTEN_FREE_ITEM_COUNT、DAIRY_FREE_ITEM_COUNT、NUT_FREE_ITEM_COUNT
SELECT
    m.brand_name,
    SUM(CASE WHEN is_gluten_free_flag = 'Y' THEN 1 ELSE 0 END) AS gluten_free_item_count,
    SUM(CASE WHEN is_dairy_free_flag = 'Y' THEN 1 ELSE 0 END) AS dairy_free_item_count,
    SUM(CASE WHEN is_nut_free_flag = 'Y' THEN 1 ELSE 0 END) AS nut_free_item_count
FROM analytics.menu_v m
WHERE m.brand_name IN ('Plant Palace', 'Peking Truck','Revenge of the Curds')
GROUP BY m.brand_name;


/*----------------------------------------------------------------------------------
 リセットスクリプト
 
  このビネットを再実行するために必要な状態にアカウントをリセットするには、
  以下のスクリプトを実行します。
----------------------------------------------------------------------------------*/
USE ROLE accountadmin;

-- drop the Harmonized Menu View
DROP VIEW IF EXISTS tb_101.harmonized.menu_v;

-- drop the Analytics Menu View
DROP VIEW IF EXISTS tb_101.analytics.menu_v;