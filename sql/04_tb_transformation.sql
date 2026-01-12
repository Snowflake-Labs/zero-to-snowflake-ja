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
Transformation
    1 - ゼロコピークローン
    2 - クエリ結果キャッシュの使用
    3 - テーブルへのカラムの追加と更新
    4 - タイムトラベルの利用
    5 - テーブルのスワップ、ドロップ、アンドロップ
******************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ---------------------------------------
2024-05-23          Jacob Kranzler      Initial Release
2025-01-11          Sho Tanaka          Initial commit with JA
******************************************************************************/


/*----------------------------------------------------------------------------
Step 1 - Zero Copy Cloning

 Tasty Bytes Fleet Analysisの一環として、当社の開発者は、Year、Make、Modelを
 組み合わせた新しいトラックタイプの列を
 Rawレイヤートラックテーブル内に作成し、更新する作業を任されました。
 このステップでは、まずSnowflake Zero Copy Cloningを使用して開発環境を構築し
 開発が完了してテストが完了する前に本番環境に展開します。 -
-----------------------------------------------------------------------------*/

-- コンテキスト設定
USE ROLE tb_dev;
USE DATABASE tb_101;


-- クエリタグをセッションに割り当てる 
ALTER SESSION SET query_tag = '{
    "origin":"sf_sit",
    "name":"tb_zts",
    "version":{"major":1, "minor":1},
    "attributes":{
        "medium":"quickstart",
        "source":"tastybytes",
        "vignette": "transformation"
    }
}';


-- 新しいカラムの開発が本番環境に影響を与えないようにするため
-- まずCloneを使用してTruckテーブルのスナップショットコピーを作成します。

CREATE OR REPLACE TABLE raw_pos.truck_dev CLONE raw_pos.truck;

/**
ゼロコピー・クローン:データベース、スキーマ、テーブルのコピーを作成します。
クローンが作成される際に、ソース・オブジェクトに存在するデータのスナップショットが取得され、クローン・オブジェクトで利用可能になります。
クローン・オブジェクトは書き込み可能であり、クローン元とは独立しています。
つまり、ソース・オブジェクトまたはクローン・オブジェクトのいずれかに加えられた変更は、もう一方には影響しません。
データベースをクローンすると、そのデータベース内のすべてのスキーマとテーブルがクローンされます。
スキーマを複製すると、そのスキーマ内のすべてのテーブルが複製されます。
**/

-- クローンに問い合わせる前にWarehouseコンテキストを設定
    --> NOTE: Clone ステートメントでは、Snowflake のCloud Services経由で処理されるため、ウェアハウスは不要です。
USE WAREHOUSE tb_dev_wh;


-- ゼロコピークローンを作成したので、新しいトラックタイプ列を組み合わせるために必要なものを問い合わせてみましょう。
SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model
FROM raw_pos.truck_dev AS t
ORDER BY t.truck_id;


/*------------------------------------------------------------------------
Step 2 - クエリ結果キャッシュの使用

 ユーザーが過去24時間以内にすでに実行済みのクエリを再度実行し
 テーブルのデータが前回クエリが実行されたときから変更されていない場合、
 クエリの結果は同じになります。
 クエリを再度実行する代わりに、Snowflakeは結果セットキャッシュから
 前回と同じ結果を単に返します。
 このステップでは、この機能をテストします。
------------------------------------------------------------------------*/

-- Snowflakeの結果キャッシュをテストするために、
-- ウェアハウスを一時停止しましょう。
-- NOTE: 「無効な状態です。ウェアハウスを一時停止できません」という
--       メッセージが表示された場合、以前に設定した自動一時停止が
--       すでに発生していることを意味します。
ALTER WAREHOUSE tb_dev_wh SUSPEND;

-- コンピュートを中断した状態で、上記のクエリを再実行してみましょう。
SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model --> SnowflakeはSELECT句のコンマの末尾利用をサポートしています
FROM raw_pos.truck_dev AS t
ORDER BY t.truck_id;


-- クエリを実行した後に確認すべき事項がいくつかあります

    -- ウェアハウスはONになったのか
        --> 右上コンテキストウィンドウで、([ウェアハウス詳細を表示])をクリックして、ウェアハウスのステータスを確認します。

    -- クエリプロファイルには何が表示されますか?
        --> 「結果」の隣にある「クエリの詳細」パネルで、「クエリID」をクリックして「クエリプロファイル」を開きます。

    -- また、Make列の内のFord_からFordへの誤字も修正する必要があります。
        --> 次のステップでこれを行います


/*------------------------------------------------------------------------
Step 3 - テーブルのカラムの追加と更新

このステップでは、以前に作成した開発用トラックテーブルに
トラックタイプのカラムを追加し、更新します。
また、Make列のタイプミスも修正します。
------------------------------------------------------------------------*/

-- まず、Makeの列で気づいた誤字を訂正しましょう。
UPDATE raw_pos.truck_dev
    SET make = 'Ford'
WHERE make = 'Ford_';


-- トラックタイプを構成する列を結合するクエリを作成しましょう。
SELECT
    truck_id,
    year,
    make,
    model,
    CONCAT(YEAR, ' ', make, ' ', REPLACE(model, ' ', '_')) AS truck_type
FROM raw_pos.truck_dev;


-- テーブルにトラックタイプの列を追加しましょう
ALTER TABLE raw_pos.truck_dev
    ADD COLUMN truck_type VARCHAR(100);

-- null であることを確認
SELECT
    truck_id,
    year,
    truck_type
FROM raw_pos.truck_dev
ORDER BY truck_id;

-- 空の列を配置したので、Update文を実行して各行にデータを入力することができます。
UPDATE raw_pos.truck_dev
    SET truck_type = CONCAT(YEAR, make, ' ', REPLACE(model, ' ', '_'));


--450行の更新に成功したので、作業の検証を実施
SELECT
    truck_id,
    year,
    truck_type
FROM raw_pos.truck_dev
ORDER BY truck_id;


/*------------------------------------------------------------------------
Step 4 - テーブル復元のためのタイムトラベル

先ほどUpdateステートメントでミスを犯し、
YearとMakeの間にスペースを挿入するのを忘れてしまいました。
幸いにもTime Travel機能を使用して、
スペルミスを修正した後のテーブルの状態に戻すことができます。
これにより、作業を修正することができます。

Time Travel機能では90日以内であれば、
変更または削除されたデータにアクセスすることができます。
以下のタスクを実行する強力なツールとして役立ちます。

- 不正に変更または削除されたデータ・オブジェクトを復元する
- 過去の重要な時点におけるデータの複製とバックアップ
- 指定した期間におけるデータの使用/操作の分析
------------------------------------------------------------------------*/

-- まず、クエリ履歴機能を使用して、開発テーブルのすべての更新ステートメントを確認します。
SELECT
    query_id,
    query_text,
    user_name,
    query_type,
    start_time
FROM TABLE(information_schema.query_history())
WHERE 1 = 1
    AND query_type = 'UPDATE'
    AND query_text LIKE '%raw_pos.truck_dev%'
ORDER BY start_time DESC;


-- 今後のためにSQL変数を作成し、その中にUpdate文のクエリIDを格納しましょう。
SET query_id
    = (
    SELECT TOP 1 query_id
    FROM TABLE(information_schema.query_history())
    WHERE 1 = 1
        AND query_type = 'UPDATE'
        AND query_text LIKE '%SET truck_type =%'
    ORDER BY start_time DESC
    );

    /**
    タイムトラベルには、以下を含むさまざまなステートメントオプションがあります。
        ・「At」、 「Before」、 「Timestamp」、 「Offset」、 「Statement」

    ここでは Statement を使用します。
    誤ったアップデート文からクエリIDを取得しており、テーブルを実行前の状態に戻したいのでStatementを使用します。
    **/

-- ここでタイムトラベルと変数を利用して、開発テーブルの状態を確認します。
SELECT
    truck_id,
    make,
    truck_type
FROM raw_pos.truck_dev
BEFORE (STATEMENT => $query_id)
ORDER BY truck_id;


-- タイムトラベルとテーブルの作成または置換を使用して、開発テーブルを復元してみましょう。
CREATE OR REPLACE TABLE raw_pos.truck_dev
    AS
SELECT * FROM raw_pos.truck_dev
BEFORE (STATEMENT => $query_id); -- 指定したクエリIDが実行される前の状態に戻す

-- トラックタイプが null になっていることを確認
SELECT
    truck_id,
    year,
    truck_type
FROM raw_pos.truck_dev
ORDER BY truck_id;

--正しいUpdate文を実行してみましょう。
UPDATE raw_pos.truck_dev t
    SET truck_type = CONCAT(
        t.year, ' ', t.make, ' ', REPLACE(t.model, ' ', '_')
    );

-- 正しいテーブルの状態確認
SELECT
    truck_id,
    year,
    truck_type
FROM raw_pos.truck_dev
ORDER BY truck_id;

/*------------------------------------------------------------------------
Step 5 - テーブルのスワップ、ドロップ、アンドロップ

これまでの取り組みを踏まえ私たちは与えられた要件に対応しました。
タスクを完了するには開発を本番環境に移行する必要があります。
このステップでは、開発用トラックテーブルを現在運用中のものと
入れ替え本番環境で現在利用可能なものに置き換えます。
------------------------------------------------------------------------*/

-- アカウント管理者ロールで開発用テーブルが本番用を入れ替えます。
USE ROLE accountadmin;

ALTER TABLE raw_pos.truck_dev
    SWAP WITH raw_pos.truck;


-- 本番のトラックテーブルに新しい列が追加されていることを確認しましょう。
SELECT
    t.truck_id,
    t.truck_type
FROM raw_pos.truck AS t
WHERE t.make = 'Ford';


-- 素晴らしいですね。では、開発テーブルを削除しましょう。
DROP TABLE raw_pos.truck;


-- 誤って本番のテーブルを削除してしまいました
-- タイムトラベル機能に頼らず、素早く別の機能を使って復元しましょう
UNDROP TABLE raw_pos.truck;

-- テーブルが復元されていることを確認
SELECT
    t.truck_id,
    t.truck_type
FROM raw_pos.truck AS t
WHERE t.make = 'Ford';

-- 本番のテーブルが復元されたので、開発用テーブルを正しく削除します
DROP TABLE raw_pos.truck_dev;


/*------------------------------------------------------------------------
スクリプトのリセット

  以下のスクリプトを実行して、このセクションを再実行するために
  必要な状態にアカウントをリセットします

------------------------------------------------------------------------*/
USE ROLE accountadmin;

-- revert Ford to Ford_
UPDATE tb_101.raw_pos.truck SET make = 'Ford_'
WHERE make = 'Ford';

-- remove Truck Type column
ALTER TABLE tb_101.raw_pos.truck DROP COLUMN IF EXISTS truck_type;

-- unset SQL Variable
UNSET query_id;

-- unset Query Tag
ALTER SESSION UNSET query_tag;
