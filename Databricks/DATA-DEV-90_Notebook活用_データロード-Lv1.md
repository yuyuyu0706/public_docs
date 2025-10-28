# LoadDelta - Deltaテーブル読込モジュール Lv1

## 仕様説明

**目的**
- 定数で指定した catalog.schema.table の Delta テーブルを読み込む
- 先頭数行、ヘッダ（列名・スキーマ）を出力する

**使い方**
- 定数 CATALOG, SCHEMA, TABLE を設定する
- load_delta()を実行する
- プレビューを確認する

**前提**
- 必須 : 対象テーブルに SELECT できること
- 任意 : USE CATALOG / USE SCHEMA できること

**出力**
- display(df) のプレビュー
- df.columns
- printSchema()

**備考**
- ウィジェット・条件・LIMIT・キャッシュ等は Lv2 以降で拡張予定

## 実装

### インポートと定数

```
from pyspark.sql import DataFrame

# === 定数（ここだけ編集） ===
CATALOG = "main"
SCHEMA  = "default"
TABLE   = "sample_delta"
```

### 関数モジュール

#### 完全修飾名の作成関数

```
def _fq_name(cat: str, sch: str, tbl: str) -> str:
    return f"`{cat}`.`{sch}`.`{tbl}`"
```

#### ロード関数

```
def load_delta(cat: str = CATALOG, sch: str = SCHEMA, tbl: str = TABLE):
    """
    完全修飾名（catalog.schema.table）で Delta テーブルを取得し、結果を返す。
    """
    fq = _fq_name(cat, sch, tbl)
    return spark.table(fq)
```

### 実行する

```
df = load_delta()  # 定数で指定したテーブルをロード
```

### プレビューする

```
# 先頭5行
df.show(5, truncate=False)

# 表形式プレビュー
display(df)

# ヘッダ（列名）とスキーマ
print("=== Columns ===")
print(df.columns)

print("\n=== Schema ===")
df.printSchema()
```

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
