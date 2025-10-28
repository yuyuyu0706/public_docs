# Deltaテーブル基本操作 Lv2 widgets入力版

## 概要

**目的**
- Lv1の骨格（データロード→加工→保存）を維持する
- dbutils.widgets で入力値を切り替え可能にする
- 手動探索をしやすくしつつ、安全ドロップ・結合キー存在検証を追加
- “壊れにくい”最小限の堅牢性を付与する

**仕様**
- 2つの入力テーブルを catalog/schema/table で指定（ウィジェット）
- 不要列は CSV で指定して削除（存在する列だけ安全に削除）
- 結合キーは CSV、結合タイプは 選択式（inner/left/right/full）
- 保存先テーブルを catalog/schema/table で指定、SAVE_MODE（overwrite/append） を選択
- プレビューは先頭5行とスキーマ、保存後の再読込・確認までを実施

**使い方**
  1. ウィジェットを設定する
  2. セルを上から順に実行する
  3. プレビューとスキーマを確認する

**前提**
- 必須 : SELECT 権限（入力テーブル）
- 必須 : CREATE/WRITE 権限（保存先スキーマ）
- 任意 : USE CATALOG / USE SCHEMA できること（本書は完全修飾名で参照する仕様）

**参考**
- 型揃え：JOIN前に withColumn(... .cast(...)) を使って明示的に合わせる（Lv2では必要時のみ）
- 保存の堅牢化：Lv3/4以降で CREATE OR REPLACE TABLE ... AS SELECT ...（CRTAS）や MERGE INTO に置換
- 自動化：Lv3から Jobs 環境変数駆動、Lv4で設定JSON駆動（GitHub管理＋CI配布）、Lv5で制御TBL駆動＋品質ゲート



## 実装

### インポートと定数

```
from typing import List
```

### ユーティリティ関数

#### CSV→リスト
```
def parse_csv_list(s: str) -> List[str]:
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]
```

#### 完全修飾名の組立て関数
```
def fq(cat: str, sch: str, tbl: str) -> str:
    # バッククォートで常にクォート（予約語/特殊文字に強い）
    return f"`{cat}`.`{sch}`.`{tbl}`"
```
#### 列の安全ドロップ
```
def safe_drop(df, cols: List[str]):
    # 存在する列だけを削除（存在しない列で失敗しない）
    existing = [c for c in cols if c in df.columns]
    return df.drop(*existing) if existing else df
```

### ウィジェット定義

```
# 入力テーブル1
dbutils.widgets.text("SRC1_CATALOG", "main", "SRC1_CATALOG")
dbutils.widgets.text("SRC1_SCHEMA",  "default", "SRC1_SCHEMA")
dbutils.widgets.text("SRC1_TABLE",   "sample_delta_1", "SRC1_TABLE")
dbutils.widgets.text("DROP_COLS_1",  "", "DROP_COLS_1 (csv)")

# 入力テーブル2
dbutils.widgets.text("SRC2_CATALOG", "main", "SRC2_CATALOG")
dbutils.widgets.text("SRC2_SCHEMA",  "default", "SRC2_SCHEMA")
dbutils.widgets.text("SRC2_TABLE",   "sample_delta_2", "SRC2_TABLE")
dbutils.widgets.text("DROP_COLS_2",  "", "DROP_COLS_2 (csv)")

# 結合
dbutils.widgets.text("JOIN_KEYS", "id", "JOIN_KEYS (csv)")
dbutils.widgets.dropdown("JOIN_TYPE", "inner", ["inner","left","right","full"], "JOIN_TYPE")

# 出力テーブル
dbutils.widgets.text("DST_CATALOG", "main", "DST_CATALOG")
dbutils.widgets.text("DST_SCHEMA",  "mart", "DST_SCHEMA")
dbutils.widgets.text("DST_TABLE",   "joined_sample", "DST_TABLE")
dbutils.widgets.dropdown("SAVE_MODE", "overwrite", ["overwrite","append"], "SAVE_MODE")
```

### ウィジェット取得

```
SRC1_CATALOG = dbutils.widgets.get("SRC1_CATALOG").strip()
SRC1_SCHEMA  = dbutils.widgets.get("SRC1_SCHEMA").strip()
SRC1_TABLE   = dbutils.widgets.get("SRC1_TABLE").strip()
DROP_COLS_1  = parse_csv_list(dbutils.widgets.get("DROP_COLS_1"))

SRC2_CATALOG = dbutils.widgets.get("SRC2_CATALOG").strip()
SRC2_SCHEMA  = dbutils.widgets.get("SRC2_SCHEMA").strip()
SRC2_TABLE   = dbutils.widgets.get("SRC2_TABLE").strip()
DROP_COLS_2  = parse_csv_list(dbutils.widgets.get("DROP_COLS_2"))

JOIN_KEYS    = parse_csv_list(dbutils.widgets.get("JOIN_KEYS"))
JOIN_TYPE    = dbutils.widgets.get("JOIN_TYPE").strip()

DST_CATALOG  = dbutils.widgets.get("DST_CATALOG").strip()
DST_SCHEMA   = dbutils.widgets.get("DST_SCHEMA").strip()
DST_TABLE    = dbutils.widgets.get("DST_TABLE").strip()
SAVE_MODE    = dbutils.widgets.get("SAVE_MODE").strip()

print("=== INPUTS ===")
print("SRC1:", SRC1_CATALOG, SRC1_SCHEMA, SRC1_TABLE, "DROP:", DROP_COLS_1)
print("SRC2:", SRC2_CATALOG, SRC2_SCHEMA, SRC2_TABLE, "DROP:", DROP_COLS_2)
print("JOIN:", JOIN_KEYS, JOIN_TYPE)
print("DEST:", DST_CATALOG, DST_SCHEMA, DST_TABLE, "MODE:", SAVE_MODE)
```

### ロード関数

- Lv1 から変更なし

```
def load_delta(cat: str = CATALOG, sch: str = SCHEMA, tbl: str = TABLE):
    """
    完全修飾名（catalog.schema.table）で Delta テーブルを取得し、結果を返す。
    """
    fq = _fq_name(cat, sch, tbl)
    return spark.table(fq)
```


### ロード実行する
- Lv1 から変更なし

```
# 定数で指定したテーブルをロード
df1 = load_delta(SRC1_CATALOG, SRC1_SCHEMA, SRC1_TABLE)
df2 = load_delta(SRC2_CATALOG, SRC2_SCHEMA, SRC2_TABLE)
```

### プレビューする
- プレビュー結果を見て、不要な列を選定します
- Lv1 から変更なし

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


### 不要列を削除する
- 新規ユーティリティ関数 safe_drop() を適用

```
if DROP_COLS_1:
    df1 = safe_drop(df1, DROP_COLS_1)

if DROP_COLS_2:
    df2 = safe_drop(df2, DROP_COLS_2)

print("=== After Drop SRC1 Columns ===")
print(df1.columns)
print("\n=== After Drop SRC2 Columns ===")
print(df2.columns)
```

### 結合する
- キー検証を追加

```
# 結合キーの存在チェック
missing_1 = [c for c in JOIN_KEYS if c not in df1.columns]
missing_2 = [c for c in JOIN_KEYS if c not in df2.columns]
if missing_1 or missing_2:
    raise ValueError(f"Join key not found. df1 missing={missing_1}, df2 missing={missing_2}")

# （任意・簡易）Nullキー検知：必要に応じて厳格化してください
for k in JOIN_KEYS:
    null1 = df1.filter(df1[k].isNull()).count()
    null2 = df2.filter(df2[k].isNull()).count()
    if null1 > 0 or null2 > 0:
        print(f"[WARN] Null key detected on '{k}': df1={null1}, df2={null2}")

df_joined = df1.join(df2, on=JOIN_KEYS, how=JOIN_TYPE)

print("=== Joined Columns ===")
print(df_joined.columns)
df_joined.show(10, truncate=False)
display(df_joined)
```

### Deltaテーブルに保存する

```
dst_fq = fq(DST_CATALOG, DST_SCHEMA, DST_TABLE)

# テーブルとして保存（Unity Catalog配下を想定）
df_joined.write.format("delta").mode(SAVE_MODE).saveAsTable(dst_fq)
print(f"Saved to {dst_fq} (mode={SAVE_MODE})")

# 保存結果を確認
df_out = spark.table(dst_fq)
print("=== Output Preview ===")
df_out.show(10, truncate=False)

print("\n=== Output Schema ===")
df_out.printSchema()
display(df_out)
```

