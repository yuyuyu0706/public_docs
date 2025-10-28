# LoadDelta - UC Delta テーブル読込テンプレート

## 仕様説明（LoadDelta）

```
# %md
# LoadDelta — UC Delta テーブル読込テンプレ
# 目的
#   - Unity Catalog 配下の Delta テーブルを Python（PySpark）でロードし、DataFrame として表示・検査する
# 主な機能
#   - カタログ/スキーマ/テーブル名を Widgets で指定（既定値あり）
#   - SELECT 列の絞り込み、WHERE 句、LIMIT を任意で指定
#   - spark.table() を利用した メタストア名指定の読込（catalog.schema.table）
#   - プレビュー表示（display/show）、件数、スキーマ、簡易プロファイル
#   - オプションで キャッシュ（CACHE TABLE または df.cache()）
# 前提
#   - Databricks Runtime（Delta Lake 同梱）
#   - 対象テーブルへの 参照権限（USE CATALOG / USE SCHEMA / SELECT）
# 想定ユース
#   - データ探索／品質確認 下流ノートブックに渡す一時ビュー登録（createOrReplaceTempView）
# 出力
#   - DataFrame（変数名：df）、一時ビュー（tmp_load_delta）
# 追加メモ
#   - パス直指定での Delta 読込にも差し替え可能
#   - （spark.read.format("delta").load(path)）（後述セル参照）
```

- [Databricks ノートブックでコードを開発する](https://docs.databricks.com/aws/ja/notebooks/notebooks-code)
  - %md は、マジックコマンド で Markdown を使うことを意味する

- [Jupyter Notebook マジックコマンド自分的まとめ](https://qiita.com/mgsk_2/items/437656b8ce42c03e41a6)


## 環境・ランタイム確認

### Databricks Runtimeを確認

```
print("Spark version :", spark.version)
print("App Name      :", spark.sparkContext.appName)
print("Executor cores:", spark.sparkContext.defaultParallelism)
print("Delta enabled :", spark.conf.get("spark.databricks.delta.preview.enabled", "n/a"))
```


### 便利オプション

- 任意の設定。必要に応じて実施する

```
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")  # display() とは無関係、toPandas 時の高速化
spark.conf.set("spark.sql.adaptive.enabled", "true")                 # AQE（通常既定で有効）
```

## パラメータ（Widgets）

### Widgets を作成

- 既に存在する場合は再作成されません

```
# テキストボックスを作成する
dbutils.widgets.text("catalog", "main", "catalog")
dbutils.widgets.text("schema",  "default", "schema")
dbutils.widgets.text("table",   "sample_delta", "table")

# 省略時は全列
dbutils.widgets.text("columns_csv", "", "select columns (csv)")  # 例: "id,name,amount"

# WHERE 句（例: "amount > 100 and status = 'ok'"）
dbutils.widgets.text("where_expr", "", "where")

# LIMIT 行数（空なら無制限）
dbutils.widgets.text("limit_n", "", "limit")

# 読み取り後のキャッシュ方法: none/df/table
dbutils.widgets.dropdown("cache_mode", "none", ["none","df","table"], "cache mode")
```

- Azure Learn
  - [Databricks ウィジェット](https://learn.microsoft.com/ja-jp/azure/databricks/notebooks/widgets)
  - [widgets ユーティリティ](https://learn.microsoft.com/ja-jp/azure/databricks/dev-tools/databricks-utils#dbutils-widgets)
    - [dbutils.widgets.text](https://learn.microsoft.com/ja-jp/azure/databricks/dev-tools/databricks-utils#dbutils-widgets-text)
    - [dbutils.widgets.dropdown](https://learn.microsoft.com/ja-jp/azure/databricks/dev-tools/databricks-utils#dbutils-widgets-dropdown)
    - [dbutils.widgets.get](https://learn.microsoft.com/ja-jp/azure/databricks/dev-tools/databricks-utils#dbutils-widgets-get)


### 取得

```
catalog     = dbutils.widgets.get("catalog").strip()
schema      = dbutils.widgets.get("schema").strip()
table       = dbutils.widgets.get("table").strip()
columns_csv = dbutils.widgets.get("columns_csv").strip()
where_expr  = dbutils.widgets.get("where_expr").strip()
limit_n     = dbutils.widgets.get("limit_n").strip()
cache_mode  = dbutils.widgets.get("cache_mode").strip().lower()
```

- Azure Learn
  - [widgets ユーティリティ](https://learn.microsoft.com/ja-jp/azure/databricks/dev-tools/databricks-utils#dbutils-widgets)
    - [dbutils.widgets.get](https://learn.microsoft.com/ja-jp/azure/databricks/dev-tools/databricks-utils#dbutils-widgets-get)
- Python組み込み関数 - [str.strip()](https://docs.python.org/ja/3/library/stdtypes.html#str.strip)
  - 文字を除去します
  - 文字列の先頭および末尾を対象に除去します
  - 引数が省略されるか None の場合、空白文字が除去されます
- note.nkmk.me - [Pythonの文字列の削除処理一覧](https://note.nkmk.me/python-str-remove-strip/)


### 表示

```
print("Target:", f"{catalog}.{schema}.{table}")
print("Select:", columns_csv or "*")
print("Where :", where_expr or "(none)")
print("Limit :", limit_n or "(none)")
print("Cache :", cache_mode)
```


## カタログとスキーマを選択

### カレントを合わせる

- 完全修飾名で読む場合は必須ではないが、権限チェックを早期に反映できる

```
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE {schema}")
```


## ロード関数

- LoadDelta の中核

```
from typing import Optional, List
from pyspark.sql import DataFrame

def load_delta_table(
    fq_table: str,
    columns: Optional[List[str]] = None,
    where: Optional[str] = None,
    limit: Optional[int] = None,
    cache: str = "none",   # "none" | "df" | "table"
) -> DataFrame:
    """
    fq_table: "catalog.schema.table" の完全修飾名
    columns : 選択列のリスト（None なら全列）
    where   : Spark SQL の WHERE 条件文字列
    limit   : 取得行数（None で無制限）
    cache   : "none" / "df" / "table"（table は一時ビューにキャッシュ）
    """
    # 1) ベース DF
    df = spark.table(fq_table)

    # 2) 列投影
    if columns:
        cols = [c.strip() for c in columns if c.strip()]
        if not cols:
            raise ValueError("columns is empty after stripping.")
        df = df.select(*cols)

    # 3) 条件
    if where:
        df = df.where(where)

    # 4) LIMIT
    if isinstance(limit, int):
        df = df.limit(limit)

    # 5) キャッシュ
    if cache == "df":
        df = df.cache()
        _ = df.count()  # Materialize
    elif cache == "table":
        # 一時ビューにしてテーブルキャッシュ（ビュー名は固定 or 生成）
        df.createOrReplaceTempView("tmp_load_delta")
        spark.sql("CACHE LAZY TABLE tmp_load_delta")
        # 再取得して返す（キャッシュ層を経由）
        df = spark.table("tmp_load_delta")

    return df
```

## ロード実行

```
# 入力整形
fq_table = f"{catalog}.{schema}.{table}"
columns  = [c.strip() for c in columns_csv.split(",")] if columns_csv else None
limit_i  = int(limit_n) if limit_n.isdigit() else None

# 読込
df = load_delta_table(
    fq_table=fq_table,
    columns=columns,
    where=where_expr or None,
    limit=limit_i,
    cache=cache_mode,
)

# 基本情報
print("Rows (approx or limited):", df.count())
df.printSchema()

# 先頭行（ログ用）
df.show(20, truncate=False)

# Databricks ネイティブ UI 表示（スクロール・統計などに便利）
display(df)
```


## 簡易プロファイル

- 欠損/ユニーク件数など

```
from pyspark.sql import functions as F

# 数値列の基本統計
num_stats = df.select([
    F.count(F.col(c)).alias(f"{c}__count") for c, t in df.dtypes if t in ("int","bigint","double","float","decimal","long","short")
])
display(num_stats)

# 欠損とユニーク数（任意）
summary = []
for c, _t in df.dtypes:
    summary.append(
        df.agg(
            F.count(F.col(c)).alias("non_null"),
            F.countDistinct(F.col(c)).alias("nunique")
        ).withColumn("column", F.lit(c))
    )
profile_df = None
for s in summary:
    profile_df = s if profile_df is None else profile_df.unionByName(s)
profile_df = profile_df.select("column","non_null","nunique")
display(profile_df.orderBy("column"))
```

## テンポラリビュー化

- 下流ステップに受け渡し

```
# 以降の SQL セルや別ノートブックで参照できるように
df.createOrReplaceTempView("tmp_load_delta")

# 例：SQL でプレビュー
display(spark.sql("SELECT * FROM tmp_load_delta LIMIT 100"))
```

## パス直指定での Delta 読込

```
# Unity Catalog 名称ではなく、物理パスで読みたい場合（外部ロケーション/ボリュームなど）
# 注意：パスは環境に合わせて変更
delta_path = "/Volumes/mycat/myschema/myvol/some_delta"  # 例

df_by_path = spark.read.format("delta").load(delta_path)
print("Rows by path:", df_by_path.count())
display(df_by_path.limit(100))
```

## 後始末

- キャッシュ解除など

```
# DataFrame キャッシュを明示的にクリアしたい場合
try:
    df.unpersist()
except Exception:
    pass

# テーブルキャッシュを使った場合
spark.sql("UNCACHE TABLE tmp_load_delta")
```




