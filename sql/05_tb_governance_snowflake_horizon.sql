/*----------------------------------------------------------------------------
  _______           _            ____          _
 |__   __|         | |          |  _ \        | |
    | |  __ _  ___ | |_  _   _  | |_) | _   _ | |_  ___  ___
    | | / _` |/ __|| __|| | | | |  _ < | | | || __|/ _ \/ __|
    | || (_| |\__ \| |_ | |_| | | |_) || |_| || |_|  __/\__ \
    |_| \__,_||___/ \__| \__, | |____/  \__, | \__|\___||___/
                          __/ |          __/ |
                         |___/          |___/
Quickstart:   Tasty Bytes - Zero to Snowflake
              - Governance with Snowflake Horizon
Version:      v2
Author:       Jacob Kranzler
Copyright(c): 2024 Snowflake Inc. All rights reserved.
------------------------------------------------------------------------------
 Snowflake Horizonによるガバナンス
  データを保護
    1 - システム定義のロールと権限
    2 - ロールベースのアクセス制御
    3 - タグベースマスキング
    4 - 行アクセスポリシー
    5 - 集約ポリシー
    6 - 投影ポリシー

  データを知る
    7 – 機密データ分類
    8 – 機密性の高いカスタム分類
    9 – アクセス履歴(読み取りと書き込み)

 Snowflake Horizonによる検出
    10 - ユニバーサル検索
------------------------------------------------------------------------------
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ----------------------------------------
2024-05-23          Jacob Kranzler      Initial Release
2025-01-11          Sho Tanaka          Initial commit with JA
------------------------------------------------------------------------------*/

/*------------------------------------------------------------------------
作業を開始する前に、Snowflakeアクセス制御フレームワークは
以下のものを基にしています。
  • ロールベースのアクセス制御(RBAC):
    アクセス権限は役割に割り当てられ、順にユーザーに割り当てられます。
  • 任意アクセス制御(DAC):
    各オブジェクトには所有者がおり、その所有者は
    そのオブジェクトへのアクセス権限を付与することができます。

Snowflakeのアクセス制御を理解する上で重要な概念は次のとおりです。
  • セキュアブルオブジェクト:
    アクセス権が付与できるエンティティ。アクセスは拒否されます。
    セキュアブルオブジェクトはロール(ユーザとは対照的)によって
    所有されます
      例:データベース、スキーマ、テーブル、ビュー、ウェアハウス、関数など
  • ロール:
    権限を付与できるエンティティ。ロールは、順番にユーザーに
    割り当てられます。ロールは他のロールにも割り当てることができ、
    ロール階層が作成されることに注意してください。
  • 特権:
    オブジェクトへのアクセス権限の定義されたレベル。
    複数の異なる特権を付与されるアクセスの粒度を制御するために
    使用される場合があります。
  • ユーザー:
    Snowflakeによって認識されるユーザーID。
    プログラムであるかに関わらず認識されます。

まとめ:
  Snowflakeでは、ロールはセキュアなオブジェクトに対する
  特権のコンテナです。
  • 特権はロールに付与できる
  • ロールはユーザに付与できる
  • ロールは他のロールに付与することも可能
    (付与されたロールは付与されたロールの特権を継承する)
  • ユーザーがロールを選択すると、
    そのロール階層内のすべての特権を継承します。
------------------------------------------------------------------------*/

/*------------------------------------------------------------------------
ステップ 1 - システム定義のロールと権限

 Tasty Bytesにロールベースのアクセス制御(RBAC)を導入する前に、
 まず、Snowflakeシステム定義のロールと権限について見てみましょう。
------------------------------------------------------------------------*/

-- まずは、アカウント管理者の役割とSnowflake開発用ウェアハウス(コンピュートと同義)を想定してみましょう。
USE ROLE accountadmin;
USE WAREHOUSE tb_dev_wh;


-- クエリタグをセッションに割り当てる
ALTER SESSION SET query_tag = '{
    "origin":"sf_sit",
    "name":"tb_zts",
    "version":{"major":1, "minor":1},
    "attributes":{
        "medium":"quickstart",
        "source":"tastybytes",
        "vignette": "governance_with_horizon"
    }
}';


-- ベストプラクティスに従うため、私たちはRBAC(ロールベースのアクセス制御)の調査と導入を開始します
-- まず、現在のアカウント内のロールを見てみましょう。
SHOW ROLES;


-- 次のクエリは、最後のSHOWコマンドの出力を変更し、Snowflakeシステムロールをフィルタリングできるようにします。
-- Snowflakeアカウントでデフォルトで提供されているロールが対象
  --> Note: 権限によっては、以下の「Where」節のすべてのロールの結果が表示されない場合があります。
SELECT
    "name",
    "comment"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" IN (
    'ORGADMIN', 'ACCOUNTADMIN', 'SYSADMIN',
    'USERADMIN', 'SECURITYADMIN', 'PUBLIC'
);

    /**
      Snowflake システム定義のロール定義:
       1 - ORGADMIN: 組織レベルでの操作を管理する役割。
       2 - ACCOUNTADMIN: SYSADMINおよびSECURITYADMINのシステム定義ロールを包含するロール。
           これはシステムにおける最上位のロールであり、アカウント内の限られた数のユーザーのみに付与すべきです
           アカウント内の限られた数のユーザーのみに付与すべきです。
       3 - SECURITYADMIN:グローバルにあらゆるオブジェクトの権限付与を管理できるほか、ユーザーおよびロールの作成、監視、
           ユーザーおよびロールの作成、監視、管理が可能です。
       4 - USERADMIN: ユーザーおよびロールの管理のみに特化した役割。
       5 - SYSADMIN:アカウント内にウェアハウスおよびデータベースを作成する権限を持つ役割。
           推奨されているように、最終的にすべてのカスタムロールをSYSADMINロールに割り当てるロール階層を作成する場合、このロールには、
           他のロールに、ウェアハウス、データベース、およびその他のオブジェクトの権限を付与する機能も有しています。
       6 - PUBLIC:アカウント内のすべてのユーザーおよびすべてのロールに自動的に付与される擬似ロールです。
           PUBLICロールは、他のロールと同様に、セキュアなオブジェクトを所有することができます
           他のロールと同様にセキュアなオブジェクトを所有することができます。ただし、このロールが所有するオブジェクトは、
           アカウント内の他のすべてのユーザーおよびロールが利用できます。

             +---------------+
            | ACCOUNTADMIN  |
            +---------------+
              ^    ^     ^
              |    |     |
+-------------+-+  |  +--+----------+
| SECURITYADMIN |  |  |  SYSADMIN   |<------+
+---------------+  |  +-------------+       |
        ^          |   ^      ^             |
        |          |   |      |             |
+-------+-------+  | +-+------+-+ +------+--+-+
|   USERADMIN   |  | | CUSTOM  | | CUSTOM    |
+---------------+  | +---------+ +-----------+
        ^          |      ^         ^    ^
        |          |      |         |    |
        |          |      |         | +--+------+
        |          |      |         | | CUSTOM  |
        |          |      |         | +---------+
        |          |      |         |      ^
        |          |      |         |      |
        +----------+--+---+---------+------+
                      |
                 +----+-----+
                 |  PUBLIC  |
                 +----------+
    **/

/*------------------------------------------------------------------------
Step 2 - ロールの作成、GRANTS、SQL変数

システム定義ロールについて理解したので、それらを活用して
テスト用のロールを作成し、そのロールに初期のSnowflake Horizon
Governance機能が適用されるカスタマー・ロイヤリティ・データへの
アクセス権限を与えてみましょう。
------------------------------------------------------------------------*/

-- USERADMINロールを使用してTestロールを作成してみましょう。
USE ROLE useradmin;

CREATE OR REPLACE ROLE tb_test_role
    COMMENT = 'Test role for Tasty Bytes';


-- SECURITYADMINに切り替えて、特権GRANTSを処理します。
USE ROLE securityadmin;


-- まず、開発用ウェアハウスへのすべての権限をSYSADMINに付与します。
GRANT ALL ON WAREHOUSE tb_dev_wh TO ROLE sysadmin;


-- テストロールには「OPERATE」と「USAGE」の権限のみを付与します。
GRANT OPERATE, USAGE ON WAREHOUSE tb_dev_wh TO ROLE tb_test_role;

/**
Snowflake Warehouse Privilege Grants
1 - MODIFY: ウェアハウスのサイズ変更など、
    あらゆるプロパティの変更を可能にします。
2 - MONITOR: 現在および過去のウェアハウスで実行されたクエリ、
    およびその使用統計の表示を可能にします。
3 - OPERATE: ウェアハウスの状態変更(停止、開始、一時停止、再開)を
    可能にします。また、現在および過去のウェアハウスで実行された
    クエリの表示、および実行中のクエリのキャンセルを可能にします。
4 - USAGE: 仮想ウェアハウスを使用し、その結果として、
    そのウェアハウス上でクエリを実行することを可能にします。
    SQLステートメントが送信された際に自動的に再開するように
    設定されている場合、そのウェアハウスは自動的に再開し、
    ステートメントを実行します。
5 - ALL: そのウェアハウスに対して、
    所有権を除くすべての権限を付与します。
**/


-- データベースとその中のすべてのスキーマに対して使用権を付与します
GRANT USAGE ON DATABASE tb_101 TO ROLE tb_test_role;

GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tb_test_role;

/**
Snowflakeデータベースおよびスキーマの権限
1 - MODIFY:データベースの設定の変更を可能にします。
2 - MONITOR: データベース上でDESCRIBEコマンドの実行を可能にします。
3 - USAGE: データベースの使用を可能にします。SHOW DATABASESコマンドの出力でデータベースの詳細を返すことも含まれます。
データベース内のオブジェクトの表示や操作には、さらに権限が必要です。
4 - ALL: データベース上で、所有権を除くすべての権限を付与します。
**/

-- テストロールとしてデータガバナンス機能をテストするつもりなので、データモデルに対してSELECT文を実行できることを確認しましょう。
GRANT SELECT ON ALL TABLES IN SCHEMA tb_101.raw_customer TO ROLE tb_test_role;

GRANT SELECT ON ALL TABLES IN SCHEMA tb_101.raw_pos TO ROLE tb_test_role;

GRANT SELECT ON ALL VIEWS IN SCHEMA tb_101.analytics TO ROLE tb_test_role;

    /**
     Snowflakeビューおよびテーブル権限付与
      1 - SELECT:テーブル/ビューに対するSELECT文の実行を可能にします。
      2 - INSERT:テーブルへのINSERTコマンドの実行を可能にします。
      3 - UPDATE:テーブルに対する UPDATE コマンドの実行を可能にします。
      4 - TRUNCATE:テーブルで TRUNCATE TABLE コマンドの実行を可能にします。
      5 - DELETE: テーブル上でDELETEコマンドを実行できるようにします。
    **/

-- 変数に CURRENT_USER()の値を設定しましょう
SET my_user_var = CURRENT_USER();


-- 現在ログインしているユーザーにロールを付与できます。
GRANT ROLE tb_test_role TO USER IDENTIFIER($my_user_var);


/*------------------------------------------------------------------------
Step 3 - カラムレベルのセキュリティとタグ付け = タグベースのマスキング

  最初に展開してテストしたいガバナンス機能は、Snowflake タグベースの
  動的データマスキングです。これにより、テストロールの列にある
  PII データをマスキングできますが、より特権の大きいロールからは
  マスキングできません。
------------------------------------------------------------------------*/

-- テストロール、開発用ウェアハウス、データベースを使用できるようになりました。
USE ROLE tb_test_role;
USE WAREHOUSE tb_dev_wh;
USE DATABASE tb_101;


-- まず、Rawレイヤーの「Customer Loyalty」テーブルを見てみましょう。
-- 顧客ロイヤルティプログラムから取り込まれた生のデータが含まれています。
SELECT
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.city,
    cl.country,
    cl.sign_up_date,
    cl.birthday_date
FROM raw_customer.customer_loyalty AS cl
SAMPLE (1000 ROWS);


-- おっと!ユーザーがこのデータにアクセスできるようになる前に、処理しなければならないPIIが大量にあります。
-- 幸いにも、Snowflakeのネイティブタグベースのマスキング機能を使用すれば、まさにこのことが可能になります。

    /**
     タグベースのマスキングポリシーは、オブジェクトのタグ付けとマスキングポリシーの機能を組み合わせたもので、
     、ALTER TAG コマンドを使用してタグにマスクポリシーを設定できるようになります。
     マスクポリシーの署名のデータタイプとカラムのデータタイプが一致する場合、タグ付けされたカラムは
     マスクポリシーの条件によって自動的に保護されます。
    **/

-- まず、タグとガバナンス・スキーマを作成して、整理し、ベストプラクティスに従うようにしましょう。
USE ROLE accountadmin;


-- オブジェクトタグを格納するタグスキーマを作成する
CREATE OR REPLACE SCHEMA tags
    COMMENT = 'Schema containing Object Tags';


-- このテーブルにアクセスできるすべての人にタグを表示できるようにしたい
GRANT USAGE ON SCHEMA tags TO ROLE public;


-- 次に、セキュリティポリシーを格納するガバナンススキーマを作成します。
CREATE OR REPLACE SCHEMA governance
    COMMENT = 'Schema containing Security Policies';

GRANT ALL ON SCHEMA governance TO ROLE sysadmin;


-- 次に、PII用のタグを1つ作成し、これらの値を許可します:NAME、PHONE_NUMBER、EMAIL、BIRTHDAY
-- これにより、自由形式の値を防ぐだけでなく、選択メニューをSnowsightに追加することもできます。
CREATE OR REPLACE TAG tags.tasty_pii
    ALLOWED_VALUES 'NAME', 'PHONE_NUMBER', 'EMAIL', 'BIRTHDAY'
    COMMENT = 'Tag for PII, allowed values are: NAME, PHONE_NUMBER, EMAIL';


-- タグが作成されたので、それらを「顧客ロイヤリティ」テーブルの
-- 該当する列に割り当てましょう。
ALTER TABLE raw_customer.customer_loyalty MODIFY COLUMN
    first_name SET TAG tags.tasty_pii = 'NAME';
ALTER TABLE raw_customer.customer_loyalty MODIFY COLUMN
    last_name SET TAG tags.tasty_pii = 'NAME';
ALTER TABLE raw_customer.customer_loyalty MODIFY COLUMN
    phone_number SET TAG tags.tasty_pii = 'PHONE_NUMBER';
ALTER TABLE raw_customer.customer_loyalty MODIFY COLUMN
    e_mail SET TAG tags.tasty_pii = 'EMAIL';
ALTER TABLE raw_customer.customer_loyalty MODIFY COLUMN
    birthday_date SET TAG tags.tasty_pii = 'BIRTHDAY';


-- TAG_REFERENCE_ALL_COLUMNS関数を使用して、顧客ロイヤルティテーブルに関連付けられたタグを返すことができます。
SELECT
    tag_database,
    tag_schema,
    tag_name,
    column_name,
    tag_value
FROM TABLE(
    information_schema.tag_references_all_columns(
        'tb_101.raw_customer.customer_loyalty', 'table'
    )
);

/**
タグを配置したので、特権ロール以外のすべてのロールの
データをマスクするマスクポリシーを作成することができます。

返されるデータ型が暗黙的にカラムデータ型にキャストされる
可能性があるすべてのデータ型に対して、1つのポリシーを作成する
必要があります。個々のタグには、データ型ごとに
1つのポリシーのみを割り当てることができます。
**/

-- 文字列データ型マスクポリシーを作成する
-- Note: マスキングポリシーは、CASE文などの標準的な条件ロジックで構成
-- noqa: disable=all
CREATE OR REPLACE MASKING POLICY governance.tasty_pii_string_mask
AS(val STRING) RETURNS STRING -> (
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN')
            THEN val
        -- 列が TASTY_PII : PHONE_NUMBER というタグでタグ付けされている場合 
        -- 最初の3桁以外をすべてマスクする  
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('TAGS.TASTY_PII') = 'PHONE_NUMBER'
            THEN CONCAT(LEFT(val, 3), '-***-****')
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('TAGS.TASTY_PII') = 'EMAIL'
            THEN CONCAT('**~MASKED~**', '@', SPLIT_PART(val, '@', -1))
        ELSE '**~MASKED~**'
    END;
-- noqa: enable=all

/**
個人の市町村、電話番号の最初の3桁、誕生日の組み合わせで本人を特定されてしまうため
安全策として、誕生日は5年ごとの区切りで切り捨て、アナリストの使用事例に合うようにする
**/

-- 変更された誕生日を返すための日付マスクポリシーを作成する
-- noqa: disable=all
CREATE OR REPLACE MASKING POLICY governance.tasty_pii_date_mask
AS (val DATE) RETURNS DATE -> (
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN')
            THEN val
        -- 列が TASTY_PII : BIRTHDAY というタグでタグ付けされている場合、  
        -- 5年刻みで切り捨てます。
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('TAGS.TASTY_PII') = 'BIRTHDAY'
            THEN DATE_FROM_PARTS(YEAR(val) - (YEAR(val) % 5),1,1)
        -- 誕生日のタグが付けられていない日付カラムの場合は NULL を返す
        ELSE NULL 
    END
);
-- noqa: enable=all

-- これで、ALTER TAGステートメントを使用して、PIIタグ付き列にマスキングポリシーを設定できるようになりました。
ALTER TAG tags.tasty_pii SET
    MASKING POLICY governance.tasty_pii_string_mask,
    MASKING POLICY governance.tasty_pii_date_mask;


-- タグベースのマスキングをそのまま使用して、テストロールと開発用ウェアハウスを使用して作業をテストしてみましょう。
USE ROLE tb_test_role;
USE WAREHOUSE tb_dev_wh;

SELECT
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    cl.birthday_date,
    cl.city,
    cl.country
FROM raw_customer.customer_loyalty cl
WHERE cl.country IN ('United States','Canada','Brazil');


-- マスキングが機能しています!このテーブルを活用する下流の解析レイヤービューも確認しましょう。
SELECT TOP 10
    clm.customer_id,
    clm.first_name,
    clm.last_name,
    clm.phone_number,
    clm.e_mail,
    SUM(clm.total_sales) AS lifetime_sales_usd
FROM analytics.customer_loyalty_metrics_v clm
WHERE clm.city = 'San Mateo'
GROUP BY clm.customer_id, clm.first_name, clm.last_name, clm.phone_number, clm.e_mail
ORDER BY lifetime_sales_usd;


/*----------------------------------------------------------------------------------
Step 4 - 行アクセスポリシー

 列レベルでのマスク制御を行うタグベースの動的マスクに満足しているため、
 次にテストロールに対して行レベルでのアクセスを制限する方法を見ていきます。

 顧客ロイヤリティテーブル内では、役割は東京在住の顧客のみを表示する必要があります。


 Snowflake Horizonには、スケールアップしてこれを処理できる強力なネイティブガバナンス機能である「行アクセスポリシー」があります。
 今回のユースケースでは、マッピングテーブルアプローチを活用します。
----------------------------------------------------------------------------------*/

-- アカウント管理者は、ロールと都市権限の列を含むマッピングテーブルを作成します。
-- このテーブルを他者から見えないようにしたいので、ガバナンススキーマで作成します。
USE ROLE accountadmin;

CREATE OR REPLACE TABLE governance.row_policy_map
    (role STRING, city_permissions STRING);


-- テーブルを配置したので、関連するロールをCity Permissionsに挿入し、
-- テストユーザーには東京の顧客のみが表示されるようにします。
INSERT INTO governance.row_policy_map
    VALUES ('TB_TEST_ROLE','Tokyo'); 



-- マッピングテーブルが配置されたので、行アクセスポリシーを作成しましょう

/**
Snowflakeは、クエリ結果で返す行を決定するために行アクセスポリシーを使用することで、行レベルのセキュリティをサポートしています。
行アクセスポリシーは、特定のロールに対して行の表示を許可する比較的単純なものでも、
エリ結果で行へのアクセスを決定するためにマッピングテーブルをポリシー定義に含める複雑なものでも可能です。
**/

CREATE OR REPLACE ROW ACCESS POLICY governance.customer_city_row_policy
    AS (city STRING) RETURNS BOOLEAN ->
       CURRENT_ROLE() IN ('ACCOUNTADMIN','SYSADMIN') -- ポリシーの対象外となるロールの一覧
        OR EXISTS -- この節では、行レベルのフィルタリングを処理するために、上記のマッピングテーブルを参照しています。
            (
            SELECT rp.role
                FROM governance.row_policy_map rp
            WHERE 1=1
                AND rp.role = CURRENT_ROLE()
                AND rp.city_permissions = city
            )
COMMENT = 'Policy to limit rows returned based on mapping table of ROLE and CITY: governance.row_policy_map';


 -- それでは、顧客ロイヤリティテーブルのCity列に、行アクセスポリシーを適用してみましょう。
ALTER TABLE raw_customer.customer_loyalty
    ADD ROW ACCESS POLICY governance.customer_city_row_policy ON (city);


-- ポリシーが正しく適用されたので、Test Role を使用してテストしてみましょう。
USE ROLE tb_test_role;

SELECT
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.city,
    cl.marital_status,
    DATEDIFF(year, cl.birthday_date, CURRENT_DATE()) AS age
FROM raw_customer.customer_loyalty cl SAMPLE (10000 ROWS)
GROUP BY cl.customer_id, cl.first_name, cl.last_name, cl.city, cl.marital_status, age;


 -- マスク処理の場合と同様に、行レベルのセキュリティが下流の分析ビューに適用されていることを再度確認しましょう。
SELECT
    clm.city,
    SUM(clm.total_sales) AS total_sales_usd
FROM analytics.customer_loyalty_metrics_v clm
GROUP BY clm.city;

/*----------------------------------------------------------------------------------
スクリプトのリセット 
 
  以下のスクリプトを実行して、このセクションを再実行するために必要な状態にアカウントをリセットします
----------------------------------------------------------------------------------*/
USE ROLE accountadmin;

-- drop Test Role
DROP ROLE IF EXISTS tb_test_role;

-- unset our Masking Policies
ALTER TAG tags.tasty_pii UNSET 
    MASKING POLICY governance.tasty_pii_string_mask,
    MASKING POLICY governance.tasty_pii_date_mask;

-- drop our Row Access Policy
ALTER TABLE raw_customer.customer_loyalty
DROP ROW ACCESS POLICY governance.customer_city_row_policy;


-- unset the System Tags
--> customer_loyalty
ALTER TABLE raw_customer.customer_loyalty MODIFY
    COLUMN first_name UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN last_name UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN e_mail UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN city UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN country UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN gender UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN marital_status UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN birthday_date UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN phone_number UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN postal_code UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category;

--> franchise
ALTER TABLE raw_pos.franchise MODIFY
    COLUMN first_name UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN last_name UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN e_mail UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN phone_number UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN city UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN country UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category;

--> menu
ALTER TABLE raw_pos.menu MODIFY
    COLUMN menu_item_name UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category;

--> location
ALTER TABLE raw_pos.location MODIFY
    COLUMN placekey UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN city UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN iso_country_code UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN country UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category;    

--> truck
ALTER TABLE raw_pos.truck MODIFY
    COLUMN primary_city UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN country UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN iso_country_code UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category;   

--> country
ALTER TABLE raw_pos.country MODIFY
    COLUMN country UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN iso_country UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN city UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category;  

-- drop Tags, Governance and Classifiers Schemas (including everything within)
DROP SCHEMA IF EXISTS tags;
DROP SCHEMA IF EXISTS governance;
DROP SCHEMA IF EXISTS classifiers;

-- remove test Insert records
DELETE FROM raw_customer.customer_loyalty WHERE customer_id IN (000001, 000002, 000003, 000004, 000005, 000006);

-- unset Query Tag
ALTER SESSION UNSET query_tag;