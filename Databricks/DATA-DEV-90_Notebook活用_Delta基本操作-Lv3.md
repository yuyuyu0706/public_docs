# Deltaテーブル基本操作 Lv3 環境設定／自動実行版

## 概要

**目的**
- Lv1 の骨格（データロード→加工→保存）を維持する
- 環境設定（環境変数 / spark.conf）でパラメータを注入して実行する
- 手動実行から Jobs による自動実行へスムーズに移行するための最小構成を整える

**仕様**
- 2つの入力テーブル（catalog/schema/table）等を環境から取得
- 優先順位：spark.conf > os.environ > 既定値
- 取得パラメータの妥当性検証（必須／選択肢）を実施
- ロード→不要列ドロップ→結合→Delta保存→保存結果の再読込
- プレビューまでを自動実行

**使い方**
  1. Jobs（またはクラスターの Spark 設定／環境変数）でパラメータを設定する
  2. ノートブックを実行（Jobs スケジュールでも可）
  3. ログとプレビューを確認

**前提**
- 必須 : SELECT 権限（入力テーブル）
- 必須 : CREATE/WRITE 権限（保存先スキーマ）
- 任意 : USE CATALOG / USE SCHEMA できること（本書は完全修飾名で参照する仕様）

**参考**
- `os.environ["KEY"]` と `spark.conf.get("key")` のいずれでも取得できるようにしています
- 例：Jobs の “環境変数” で `SRC1_CATALOG=main`、または “Spark 設定” で`config.SRC1_CATALOG=main`

**今後**
- Jobs での自動実行：本ノートは Jobs の環境変数 / Spark 設定で切替可能。
- ログの取り方：必要に応じて print を logging へ置換（Lv5 の品質ゲート化と通知を見据える）。
- 設定の外出し：Lv4 では 設定JSON（UC Volumes/DBFS） を読み、CI/CDで配布。
- 制御TBL：Lv5 では 制御テーブルに置き換え、品質ゲート（閾値違反で失敗）と実行記録を付加。

## 実装

### インポートと定数

```
import os
from typing import List, Optional
```

### ユーティリティ関数

#### CSV→リスト
- Lv2 から変更なし
```
def parse_csv_list(s: str) -> List[str]:
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]
```

#### 完全修飾名の組立て関数
- Lv2 から変更なし
```
def fq(cat: str, sch: str, tbl: str) -> str:
    # バッククォートで常にクォート（予約語/特殊文字に強い）
    return f"`{cat}`.`{sch}`.`{tbl}`"
```
#### 列の安全ドロップ
- Lv2 から変更なし
```
def safe_drop(df, cols: List[str]):
    # 存在する列だけを削除（存在しない列で失敗しない）
    existing = [c for c in cols if c in df.columns]
    return df.drop(*existing) if existing else df
```

#### パラメータ取得
```
# 優先順位: spark.conf > os.environ > default
def get_param(key: str, default: Optional[str] = None, required: bool = False) -> str:
    # spark.conf 側は "config." プレフィックスを付けておくと整理しやすい
    conf_key = f"config.{key}"
    val = None
    try:
        val = spark.conf.get(conf_key)
    except Exception:
        pass
    if val is None:
        val = os.environ.get(key, default)
    if required and (val is None or str(val).strip() == ""):
        raise ValueError(f"Required parameter '{key}' is missing. Provide via spark.conf('{conf_key}', ...) or os.environ['{key}'].")
    return str(val) if val is not None else None
```

#### パラメータ取得 CSV版
```
def get_param_csv(key: str, default: Optional[str] = None) -> List[str]:
    return parse_csv_list(get_param(key, default=default, required=False) or "")
```


### パラメータ取得（環境／spark.conf）＆検証
**セットアップ例**
- Jobs → Spark 設定：config.SRC1_CATALOG=main / config.DST_SCHEMA=mart …
- Jobs → 環境変数：SRC1_TABLE=source_a / JOIN_KEYS=id,dt …

```
# ==== 入力テーブル1 ====
SRC1_CATALOG = get_param("SRC1_CATALOG", default="main", required=True)
SRC1_SCHEMA  = get_param("SRC1_SCHEMA",  default="default", required=True)
SRC1_TABLE   = get_param("SRC1_TABLE",   default="sample_delta_1", required=True)
DROP_COLS_1  = get_param_csv("DROP_COLS_1", default="")  # CSV

# ==== 入力テーブル2 ====
SRC2_CATALOG = get_param("SRC2_CATALOG", default="main", required=True)
SRC2_SCHEMA  = get_param("SRC2_SCHEMA",  default="default", required=True)
SRC2_TABLE   = get_param("SRC2_TABLE",   default="sample_delta_2", required=True)
DROP_COLS_2  = get_param_csv("DROP_COLS_2", default="")  # CSV

# ==== 結合 ====
JOIN_KEYS    = get_param_csv("JOIN_KEYS", default="id")  # CSV
JOIN_TYPE    = get_param("JOIN_TYPE", default="inner", required=False).lower()
if JOIN_TYPE not in ("inner", "left", "right", "full"):
    raise ValueError(f"JOIN_TYPE must be one of inner/left/right/full, got '{JOIN_TYPE}'")

# ==== 出力テーブル ====
DST_CATALOG  = get_param("DST_CATALOG", default="main", required=True)
DST_SCHEMA   = get_param("DST_SCHEMA",  default="mart", required=True)
DST_TABLE    = get_param("DST_TABLE",   default="joined_sample", required=True)
SAVE_MODE    = get_param("SAVE_MODE",   default="overwrite", required=False).lower()
if SAVE_MODE not in ("overwrite", "append"):
    raise ValueError(f"SAVE_MODE must be overwrite or append, got '{SAVE_MODE}'")

print("=== RESOLVED PARAMS ===")
print("SRC1:", SRC1_CATALOG, SRC1_SCHEMA, SRC1_TABLE, "DROP:", DROP_COLS_1)
print("SRC2:", SRC2_CATALOG, SRC2_SCHEMA, SRC2_TABLE, "DROP:", DROP_COLS_2)
print("JOIN:", JOIN_KEYS, JOIN_TYPE)
print("DEST:", DST_CATALOG, DST_SCHEMA, DST_TABLE, "MODE:", SAVE_MODE)
```

### ロード関数

- Lv1/Lv2 から変更なし

```
def load_delta(cat: str = CATALOG, sch: str = SCHEMA, tbl: str = TABLE):
    """
    完全修飾名（catalog.schema.table）で Delta テーブルを取得し、結果を返す。
    """
    fq = _fq_name(cat, sch, tbl)
    return spark.table(fq)
```


### ロード実行する
- Lv1/Lv2 から変更なし

```
# 定数で指定したテーブルをロード
df1 = load_delta(SRC1_CATALOG, SRC1_SCHEMA, SRC1_TABLE)
df2 = load_delta(SRC2_CATALOG, SRC2_SCHEMA, SRC2_TABLE)
```

### プレビューする
- プレビュー結果を見て、不要な列を選定します
- Lv1/Lv2 から変更なし

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
- Lv2 から変更なし

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

# 参考：必要に応じて型合わせ（例）
# from pyspark.sql import functions as F
# df1 = df1.select(*[F.col(k).cast("string").alias(k) if k in JOIN_KEYS else F.col(k) for k in df1.columns])
# df2 = df2.select(*[F.col(k).cast("string").alias(k) if k in JOIN_KEYS else F.col(k) for k in df2.columns])

# Nullキー警告（必要に応じて厳格化）
for k in JOIN_KEYS:
    n1 = df1.filter(df1[k].isNull()).count()
    n2 = df2.filter(df2[k].isNull()).count()
    if n1 or n2:
        print(f"[WARN] Null key detected on '{k}': df1={n1}, df2={n2}")

df_joined = df1.join(df2, on=JOIN_KEYS, how=JOIN_TYPE)

print("=== Joined Columns ===")
print(df_joined.columns)
df_joined.show(10, truncate=False)
display(df_joined)
```

### Deltaテーブルに保存する
- Lv2 から変更なし
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

