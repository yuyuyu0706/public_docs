# Deltaテーブル基本操作 Lv1

## 概要

**目的**
- Azure Databricks でデータ開発を始める為の最初の1歩となる情報を提供する
- Unity Catalog の基本となるデータ形式（Delta）の基本操作を覚える

**仕様**
- 定数で指定した catalog.schema.table の Delta テーブルを読み込む
- 先頭5行、ヘッダ（列名・スキーマ）をプレビューとして出力する
- 別名テーブルで保管する

**使い方**
1. 定数 を設定する
2. テーブルを2つ読み込む → load_delta()を実行する
3. プレビューを確認する → print()で不要な列を選定する
4. 不要列を削除する → drop()を実行する
5. テーブルを結合する → join()を実行する
6. Deltaテーブルとして保存する → saveAsTable()を実行する
7. 保存結果のプレビューとスキーマを出力する

**前提**
- 必須 : 対象テーブルに SELECT 権限があること
- 必須 : 保存先スキーマへの CREATE/WRITE 権限があること
- 任意 : USE CATALOG / USE SCHEMA できること（本書は完全修飾名で参照する仕様）

**参考**
- [PySpark basics](https://learn.microsoft.com/ja-jp/azure/databricks/pyspark/basics)
- [チュートリアル: Apache Spark DataFrame を使用してデータを読み込んで変換する](https://learn.microsoft.com/ja-jp/azure/databricks/getting-started/dataframes)



## 実装

### インポートと定数

```
from pyspark.sql import DataFrame

# === 入力テーブル1 ===
SRC1_CATALOG = "main"
SRC1_SCHEMA  = "default"
SRC1_TABLE   = "sample_delta_1"

# === 入力テーブル2 ===
SRC2_CATALOG = "main"
SRC2_SCHEMA  = "default"
SRC2_TABLE   = "sample_delta_2"

# === 不要列（確認してから必要に応じて編集） ===
DROP_COLS_1 = []  # 例: ["raw_ts", "ingest_file"]
DROP_COLS_2 = []  # 例: ["note"]

# === 結合設定 ===
JOIN_KEYS  = ["id"]   # 例: 共通キー列名
JOIN_TYPE  = "inner"  # 例: "inner" / "left" / "right" / "full"

# === 出力（保存先 Delta テーブル） ===
DST_CATALOG = "main"
DST_SCHEMA  = "mart"
DST_TABLE   = "joined_sample"
SAVE_MODE   = "overwrite"  # "overwrite" or "append"
```

### 共通モジュール

#### 完全修飾名の組立て関数

```
def _fq_name(cat: str, sch: str, tbl: str) -> str:
    return f"`{cat}`.`{sch}`.`{tbl}`"
```
- [f-string(フォーマット済み文字列リテラル)](https://docs.python.org/ja/3.8/tutorial/inputoutput.html#tut-f-strings)

#### ロード関数

```
def load_delta(cat: str = CATALOG, sch: str = SCHEMA, tbl: str = TABLE):
    """
    完全修飾名（catalog.schema.table）で Delta テーブルを取得し、結果を返す。
    """
    fq = _fq_name(cat, sch, tbl)
    return spark.table(fq)
```

- [pyspark.sql.SparkSession.table](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.table.html)

### ロードする

```
# 定数で指定したテーブルをロード
df1 = load_delta(SRC1_CATALOG, SRC1_SCHEMA, SRC1_TABLE)
df2 = load_delta(SRC2_CATALOG, SRC2_SCHEMA, SRC2_TABLE)
```

### プレビューする

- プレビュー結果を見て、不要な列を選定します

```
print("=== SRC1 Columns ===")
print(df1.columns)
df1.show(5, truncate=False)
display(df1)
df1.printSchema()

print("\n=== SRC2 Columns ===")
print(df2.columns)
df2.show(5, truncate=False)
display(df2)
df2.printSchema()
```
- [pyspark.sql.DataFrame.show](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.show.html)
- [pyspark.sql.DataFrame.printSchema](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.printSchema.html)

### 不要列を削除する

```
# 不要列を削除する
if DROP_COLS_1:
    df1 = df1.drop(*DROP_COLS_1)

if DROP_COLS_2:
    df2 = df2.drop(*DROP_COLS_2)

# 確認する
print("=== After Drop SRC1 Columns ===")
print(df1.columns)
print("\n=== After Drop SRC2 Columns ===")
print(df2.columns)
```

- [pyspark.sql.DataFrame.drop](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.drop.html)



### 結合する

```
# JOIN_KEYS が存在するか確認
missing_1 = [c for c in JOIN_KEYS if c not in df1.columns]
missing_2 = [c for c in JOIN_KEYS if c not in df2.columns]
if missing_1 or missing_2:
    raise ValueError(f"Join key not found. df1 missing={missing_1}, df2 missing={missing_2}")

df_joined = df1.join(df2, on=JOIN_KEYS, how=JOIN_TYPE)

print("=== Joined Columns ===")
print(df_joined.columns)
df_joined.show(10, truncate=False)
```
- [pyspark.sql.DataFrame.join](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)


### Deltaテーブルに保存する

```
dst_fq = _fq_name(DST_CATALOG, DST_SCHEMA, DST_TABLE)

# テーブルとして保存（Unity Catalog配下を想定）
# 既存テーブルに上書きする場合は SAVE_MODE="overwrite"
df_joined.write.format("delta").mode(SAVE_MODE).saveAsTable(dst_fq)

print(f"Saved to {dst_fq} (mode={SAVE_MODE})")

# 保存結果を確認
df_out = spark.table(dst_fq)
print("=== Output Preview ===")
df_out.show(10, truncate=False)

print("\n=== Output Schema ===")
df_out.printSchema()
```

- [pyspark.sql.DataFrame](https://api-docs.databricks.com/python/pyspark/latest/pyspark.sql/api/pyspark.sql.DataFrame.html#pyspark-sql-dataframe)
- [pyspark.sql.DataFrameWriter.saveAsTable](https://api-docs.databricks.com/python/pyspark/latest/pyspark.sql/api/pyspark.sql.DataFrameWriter.saveAsTable.html#pyspark-sql-dataframewriter-saveastable)


## 参考

### 他のノートブックからload_delta()を利用する

```
# 例: /Shared/lib/LoadDelta_Lv1 を共通ライブラリ的に配置した場合
%run /Shared/lib/LoadDelta_Lv1

# デフォルト（定数）で読み込む場合
df1 = load_delta()

# 別テーブルを指定して読み込む場合（引数で上書き）
df2 = load_delta(cat="analytics", sch="mart", tbl="sales_daily")
display(df2.limit(100))
```

### Unity Catalog でカレントを設定する

```
# カレントカタログを設定する
spark.sql(f"USE CATALOG `{CATALOG}`")

# カレントスキーマを設定する
spark.sql(f"USE `{SCHEMA}`")

# 設定を確認する
print(f"Current: catalog={CATALOG}, schema={SCHEMA}, table={TABLE}")
```

### なぜ、spark.* で扱えるのか？
- Databricks ノートブック/REPL では SparkSession が自動生成され、spark という変数に割り当てられています
- つまり、ユーザーが SparkSession.builder.getOrCreate() を書かなくても、最初から spark が使えます

```
type(spark)                # -> <class 'pyspark.sql.session.SparkSession'>
print(spark.version)       # 例: '3.5.1'
df = spark.table("main.default.sample_delta")  # DataFrameを取得
df.show(5)
```