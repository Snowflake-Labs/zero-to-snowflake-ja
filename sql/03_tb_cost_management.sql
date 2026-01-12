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
Copyright(c): 2025 Snowflake Inc. All rights reserved.
*******************************************************************************
コスト管理
    a) コスト最適化
        1 - 仮想ウェアハウスと設定
    b) コスト管理
        2 - ウェアハウスの再開、一時停止、スケールアップ
        3 - セッションタイムアウトパラメータの設定
        4 - アカウントタイムアウトパラメータの設定
        5 - リソースモニタの設定
    c) コストの可視性
        6 - 支出を属性にタグ付けする
        7 - Snowsight によるコストの調査
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ---------------------------------------
2024-05-23          Jacob Kranzler      Initial Release
2025-01-11          Sho Tanaka          Initial commit with JA
*******************************************************************************

/*-----------------------------------------------------------------------------
Step 1 - 仮想ウェアハウスと設定

 Tasty Bytes Snowflake 管理者として、私たちは
 Snowflake が提供する機能について理解を深め、
 データから価値を引き出す前に、適切なコスト管理を確立する必要があります。

 このステップでは、最初の Snowflake Warehouse を作成します。
 これは、仮想コンピュートとというものです。

 Snowflake では、割り当てられたワークロードに対して可能な限り最小のサイズのウェアハウスから
 始めることを推奨しています。
 そのため、テスト用ウェアハウスは X-Small で作成します。
-----------------------------------------------------------------------------*/

-- 始める前に、ロール、ウェアハウス、データベースのコンテキストを設定
USE ROLE tb_admin;
USE WAREHOUSE tb_de_wh;
USE DATABASE tb_101;


-- クエリタグをセッションに割り当てる 
ALTER SESSION SET query_tag
= '{"origin": "sf_sit",
    "name": "tb_zts,
    "version":{"major": 1, "minor": 1},
    "attributes":{
        "medium": "quickstart",
        "source": "tastybytes",
        "vignette": "cost_management"
    }}';

-- test_whを作成し。
-- 以下のセクションを参照して、各パラメータが処理している内容を理解しましょう。

CREATE OR REPLACE WAREHOUSE tb_test_wh WITH
COMMENT = 'test warehouse for tasty bytes'
    WAREHOUSE_TYPE = 'standard'
    WAREHOUSE_SIZE = 'xsmall'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'STANDARD'
    AUTO_SUSPEND = 60
    AUTO_RESUME = true -- turn on 
    INITIALLY_SUSPENDED = true;

/**
1) ウェアハウスタイプ: 検索やテーブルへのデータのロードを含むすべての DML 操作には、
ウェアハウスが必要です。
Snowflake は標準(最も一般的な)または
Snowpark 最適化のウェアハウスタイプをサポートしています。
Snowpark 最適化のウェアハウスはメモリ集約型のワークロードに適用すべきです。

2) ウェアハウスサイズ: サイズはウェアハウス内のクラスタごとに
利用可能なコンピュートリソースの量を指定します。
Snowflake は、X-Small から 6X-Large までのサイズをサポートしています。

3) 最大クラスタ数:マルチクラスタウェアハウスでは、
Snowflakeは静的または動的に追加のクラスタを割り当て
より大規模なコンピュートリソースプールを利用できるようにします。
マルチクラスタウェアハウスは、以下のプロパティを指定することで定義されます。
- 最小クラスタ数:最小クラスタ数、最大数(10)以下。
- 最大クラスタ数:最大クラスタ数、1(最大10)以上。

4) スケーリング・ポリシー:オートスケール・モードで稼働する
マルチクラスタ・ウェアハウスにおけるクラスタの自動起動および自動停止のポリシーを指定します。

5) 自動サスペンド:デフォルトでは、自動サスペンドは有効になっています。
Snowflakeは、指定した時間(ここでは60秒)にわたって非アクティブな場合、
自動的にウェアハウスを一時停止します。

6) 自動再開:デフォルトでは、自動再開は有効になっています。
ウェアハウスを必要とするステートメントが送信され、そのウェアハウスがセッションの
現在のウェアハウスである場合、Snowflakeは自動的にウェアハウスを再開します。

7) 初期状態の一時停止: ウェアハウスを「一時停止」状態で最初に作成するかどうかを指定します。
**/


/*-----------------------------------------------------------------------------
Step 2 - ウェアハウスの再開、一時停止、スケーリング

 ウェアハウスを作成しました。
 次にビジネスからのいくつかの質問に回答するためにそれを使用してみましょう。
 その際、ウェアハウスの再開、一時停止、柔軟なスケーリングの方法についても学びます。
-----------------------------------------------------------------------------*/

-- TastyByteの管理者のロールとtest_whをコンテキストに設定しましょう
USE ROLE tb_admin;
USE WAREHOUSE tb_test_wh;


-- Plant Palaceブランドのトラックでは、どのようなメニューを提供していますか?
    --> NOTE: Snowflakeは、ウェアハウスを必要とするステートメントが送信されると、
    --> 自動的にそのウェアハウスを再開します。
SELECT
    m.menu_type,
    m.truck_brand_name,
    m.menu_item_id,
    m.menu_item_name
FROM raw_pos.menu AS m
WHERE m.truck_brand_name = 'Plant Palace';


-- Snowflakeの弾力的なスケーラビリティを実証するために、ウェアハウスをスケールアップし、
-- より大規模な集約クエリをいくつか実行してみましょう。

-- 当社のロイヤルティ会員の上位顧客の総注文数と総販売数量は? 
SELECT
    o.customer_id,
    CONCAT(clm.first_name, ' ', clm.last_name) AS name,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.price) AS total_sales
FROM analytics.orders_v AS o
INNER JOIN analytics.customer_loyalty_metrics_v AS clm
    ON o.customer_id = clm.customer_id
GROUP BY o.customer_id, name
ORDER BY order_count DESC;

-- WHサイズをSmallに
ALTER WAREHOUSE tb_test_wh SET WAREHOUSE_SIZE = 'Small';

-- 結果キャッシュの利用をしないように設定
SHOW PARAMETERS LIKE '%USE_CACHED_RESULT%';
ALTER SESSION SET USE_CACHED_RESULT = false;
SHOW PARAMETERS LIKE '%USE_CACHED_RESULT%';

SELECT
    o.customer_id,
    CONCAT(clm.first_name, ' ', clm.last_name) AS name,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.price) AS total_sales
FROM
  analytics.orders_v AS o
INNER JOIN
  analytics.customer_loyalty_metrics_v AS clm
  ON o.customer_id = clm.customer_id
GROUP BY
  o.customer_id, name
ORDER BY
  order_count DESC;

ALTER SESSION SET USE_CACHED_RESULT = true;

-- WHサイズをスケールダウン
ALTER WAREHOUSE tb_test_wh SET WAREHOUSE_SIZE = 'XSmall';


-- 手動で一時停止に
    --> NOTE: 「無効な状態です。ウェアハウスを一時停止できません」というメッセージが表示された場合、
    --> 以前に設定した自動一時停止がすでに発生していることを意味します。
ALTER WAREHOUSE tb_test_wh SUSPEND;


/*-----------------------------------------------------------------------------
スクリプトのリセット

  以下のスクリプトを実行して、
  このセクションを再実行するために必要な状態にアカウントをリセットします
-----------------------------------------------------------------------------*/
USE ROLE accountadmin;

-- drop Test Warehouse
DROP WAREHOUSE IF EXISTS tb_test_wh;

-- drop Cost Center Tag
DROP TAG IF EXISTS cost_center;

-- drop Resource Monitor
DROP RESOURCE MONITOR IF EXISTS tb_test_rm;

-- reset Account Timeout Parameters
ALTER ACCOUNT SET statement_timeout_in_seconds = default;
ALTER ACCOUNT SET statement_queued_timeout_in_seconds = default;

-- unset Query Tag
ALTER SESSION UNSET query_tag;
