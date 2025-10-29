# Deltaテーブル基本操作 Lv5 制御TBL駆動／品質ゲート／実行記録

## 概要

**目的**
- Lv1 の骨格（データロード→加工→保存）を維持する
- 制御用テーブル（Unity Catalog 上の Delta）で実行パラメータを管理する
- 品質ゲート（件数・Null率・重複など）に違反したら ジョブを失敗させる
- ジョブは実行ログをテーブルに記録して運用可能性を高める

**仕様**
- 制御用TBL（例：ctrl.pipelines）
  - pipeline_id や env に合致し、is_enabled=true の 最新行を取得
  - 行の params_json に Lv4 と同構造の設定を格納（src1/src2/join/dest）
  -   品質設定は quality_json に格納（閾値）
- 実行ログTBL（例：ctrl.pipeline_runs）
  - 開始時刻・終了時刻・処理件数・品質結果・ステータス（SUCCESS/FAILED）を追記


**使い方**
  1. 制御用TBL・実行ログTBLを作成（DDL例は備考参照）
  2. 制御用TBLに pipeline_id ごとのレコードを投入
    - params_json / quality_json をセット
  3. Jobs から本ノートを実行
    - 最低限 PIPELINE_ID と 制御TBLの完全修飾名を spark.conf か 環境変数で渡す
  4. 実行後、出力テーブルと 実行ログTBL を確認

**前提**
- 必須 : SELECT 権限（入力テーブル）
- 必須 : CREATE/WRITE 権限（保存先スキーマ）
- 必須 : SELECT/WRITE（制御・ログTBL）
- 任意 : USE CATALOG / USE SCHEMA できること（本書は完全修飾名で参照する仕様）

**参考 - 設定ファイル(JSON)の例**
- params_json
```
{
  "config_version": "2025-10-01",
  "src1": { "catalog": "main", "schema": "default", "table": "sample_delta_1", "drop_cols": ["raw_ts"] },
  "src2": { "catalog": "main", "schema": "default", "table": "sample_delta_2", "drop_cols": [] },
  "join": { "keys": ["id"], "type": "inner" },
  "dest": { "catalog": "main", "schema": "mart", "table": "joined_sample", "mode": "overwrite" }
}
```

- quality_json
```
{
  "min_rows_joined": 1,
  "max_null_rate_on_keys": 0.0,
  "max_duplicate_keys": 0
}
```

**今後**
- GitHub/CI：DDL と Seed（初期行）は Git 管理し、CI で適用する
  - 更新は MERGE で安全に
- 通知：Jobs/Workflows の失敗通知設定を有効化する
  - 必要があればログTBLにメトリクスを追加（処理秒、bytes 等）
- 監査：params_json は Lv4 と同じ構造なので、設定の一元管理・差分レビューが容易。
- 発展：保存方式を CRTAS(※) や MERGE に置換しても同じ枠組みで運用可
  - ※ CREATE OR REPLACE TABLE … AS SELECT …

## 実装

### インポートと定数

```
import os, json, time, uuid
from typing import List, Dict, Any, Optional
```

### ユーティリティ関数

#### 完全修飾名の組立て
- Lv2 から変更なし
```
def fq(cat: str, sch: str, tbl: str) -> str:
    # バッククォートで常にクォート（予約語/特殊文字に強い）
    return f"`{cat}`.`{sch}`.`{tbl}`"
```

#### データロード
```
def load_delta(cat: str, sch: str, tbl: str):
    return spark.table(fq(cat, sch, tbl))
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
# 優先順位: spark.conf > env > default
def get_param(key: str, default: Optional[str] = None, required: bool = False) -> str:
    conf_key = f"config.{key}"
    v = None
    try:
        v = spark.conf.get(conf_key)
    except Exception:
        pass
    if v is None:
        v = os.environ.get(key, default)
    if required and (v is None or str(v).strip() == ""):
        raise ValueError(f"Required parameter '{key}' is missing. Provide via spark.conf('{conf_key}', ...) or env {key}.")
    return str(v) if v is not None else None
```

#### 制御TBLの読み込み

```
def read_active_control_row(ctrl_fq: str, pipeline_id: str, env: Optional[str] = None):
    df_ctrl = spark.table(ctrl_fq)
    q = df_ctrl.filter(df_ctrl.pipeline_id == pipeline_id).filter(df_ctrl.is_enabled == True)
    if env:
        q = q.filter(df_ctrl.env == env)
    # 有効期間やバージョンがあれば、それで降順
    if "effective_from" in q.columns:
        q = q.orderBy(q.effective_from.desc())
    elif "updated_at" in q.columns:
        q = q.orderBy(q.updated_at.desc())
    row = q.limit(1).collect()
    if not row:
        raise ValueError(f"No active control row found for pipeline_id='{pipeline_id}' (env={env}) in {ctrl_fq}")
    return row[0].asDict()
```

#### 実行ログの書き込み

```
def append_run_log(log_fq: str, rec: Dict[str, Any]):
    # 単一行のAppend
    df = spark.createDataFrame([rec])
    df.write.mode("append").format("delta").saveAsTable(log_fq)
```

#### 品質ゲート

```
def quality_check_joined(df_joined, join_keys: List[str], quality: Dict[str, Any]) -> Dict[str, Any]:
    result = {"passed": True, "violations": []}
    # 1) 結果行数
    min_rows = int(quality.get("min_rows_joined", 0))
    rows = df_joined.count()
    if rows < min_rows:
        result["passed"] = False
        result["violations"].append(f"rows({rows}) < min_rows_joined({min_rows})")

    # 2) Null率 on keys
    max_null_rate = float(quality.get("max_null_rate_on_keys", 1.0))
    for k in join_keys:
        nulls = df_joined.filter(df_joined[k].isNull()).count()
        rate = nulls / rows if rows > 0 else 0.0
        if rate > max_null_rate:
            result["passed"] = False
            result["violations"].append(f"null_rate({k})={rate:.6f} > max_null_rate_on_keys({max_null_rate})")

    # 3) 重複キー
    max_dups = int(quality.get("max_duplicate_keys", 10**9))
    if join_keys:
        dup_cnt = (df_joined.groupBy(*join_keys).count().filter("count > 1").count())
        if dup_cnt > max_dups:
            result["passed"] = False
            result["violations"].append(f"duplicate_key_groups({dup_cnt}) > max_duplicate_keys({max_dups})")
    return {"rows": rows, **result}
```


### 設定の読み込み

#### 設定の定義
```
# 必須：制御TBLの完全修飾名、パイプラインID
CTRL_CATALOG = get_param("CTRL_CATALOG", required=True)
CTRL_SCHEMA  = get_param("CTRL_SCHEMA",  required=True)
CTRL_TABLE   = get_param("CTRL_TABLE",   required=True)
PIPELINE_ID  = get_param("PIPELINE_ID",  required=True)
ENV          = get_param("ENV",          default=None)  # 任意
```

#### 設定の読み込み

```
CTRL_FQ = fq(CTRL_CATALOG, CTRL_SCHEMA, CTRL_TABLE)
ctrl = read_active_control_row(CTRL_FQ, PIPELINE_ID, ENV)

# params_json / quality_json は文字列想定
params = json.loads(ctrl["params_json"])
quality = json.loads(ctrl.get("quality_json", "{}"))
```

#### 設定の確認

```
print("=== CONTROL ROW ===")
print("pipeline_id :", ctrl.get("pipeline_id"))
print("env         :", ctrl.get("env"))
print("config_ver  :", params.get("config_version"))
print("quality     :", quality)
```

#### 設定の構造整理
```
# Lv4と同じ構造を想定
src1 = params["src1"]
src2 = params["src2"]
join = params["join"]
dest = params["dest"]
```

### データロードする
- 設定は conf、データは load_delta()から読み込む

```
df1 = load_delta(src1["catalog"], src1["schema"], src1["table"])
df2 = load_delta(src2["catalog"], src2["schema"], src2["table"])
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
df1 = safe_drop(df1, src1.get("drop_cols", []))
df2 = safe_drop(df2, src2.get("drop_cols", []))

print("=== After Drop SRC1 Columns ===")
print(df1.columns)
print("\n=== After Drop SRC2 Columns ===")
print(df2.columns)
```

### 結合する
- キー検証を追加

```
join_keys = join["keys"]
join_type = join["type"]

# キー存在チェック
missing_1 = [c for c in join_keys if c not in df1.columns]
missing_2 = [c for c in join_keys if c not in df2.columns]
if missing_1 or missing_2:
    raise ValueError(f"Join key not found. df1 missing={missing_1}, df2 missing={missing_2}")

# Nullキー警告（任意）
for k in join_keys:
    n1 = df1.filter(df1[k].isNull()).count()
    n2 = df2.filter(df2[k].isNull()).count()
    if n1 or n2:
        print(f"[WARN] Null key detected on '{k}': df1={n1}, df2={n2}")

# 結合
df_joined = df1.join(df2, on=join_keys, how=join_type)
df_joined.show(10, truncate=False)
print("=== Joined Columns ===")
print(df_joined.columns)

# 品質ゲート（Joined）
qc = quality_check_joined(df_joined, join_keys, quality)
print("=== QUALITY (joined) ===")
print(qc)
if not qc["passed"]:
    raise ValueError(f"Quality gate violated: {qc['violations']}")
```

### Deltaテーブルに保存する

```
dest_fq = fq(dest["catalog"], dest["schema"], dest["table"])
mode = dest["mode"]

start_ts = int(time.time() * 1000)
run_id = str(uuid.uuid4())
status = "SUCCESS"
message = ""
rows_out = None

try:
    df_joined.write.format("delta").mode(mode).saveAsTable(dest_fq)
    df_out = spark.table(dest_fq)
    rows_out = df_out.count()

    print(f"Saved to {dest_fq} (mode={mode})")
    print("=== Output Preview ===")
    df_out.show(10, truncate=False)
    print("\n=== Output Schema ===")
    df_out.printSchema()

except Exception as e:
    status = "FAILED"
    message = str(e)
    print("[ERROR] Save failed:", message)
    raise

finally:
    end_ts = int(time.time() * 1000)
    # 実行ログTBLを指定（必須）
    LOG_CATALOG = get_param("LOG_CATALOG", required=True)
    LOG_SCHEMA  = get_param("LOG_SCHEMA",  required=True)
    LOG_TABLE   = get_param("LOG_TABLE",   required=True)
    LOG_FQ = fq(LOG_CATALOG, LOG_SCHEMA, LOG_TABLE)

    log_rec = {
        "run_id": run_id,
        "pipeline_id": ctrl.get("pipeline_id"),
        "env": ctrl.get("env"),
        "config_version": params.get("config_version"),
        "started_at_ms": start_ts,
        "finished_at_ms": end_ts,
        "status": status,
        "dest_table": f"{dest['catalog']}.{dest['schema']}.{dest['table']}",
        "mode": mode,
        "rows_joined": qc.get("rows"),
        "rows_out": rows_out,
        "violations": ",".join(qc.get("violations", [])) if not qc["passed"] else "",
        "message": message[:2000],  # 長過ぎるメッセージを抑制
        "inserted_at_ms": int(time.time() * 1000),
    }
    append_run_log(LOG_FQ, log_rec)
    print(f"[RUN LOG] appended to {LOG_FQ} (run_id={run_id}, status={status})")
```

## 環境構築

### 制御用TBLのDDL

```
CREATE TABLE IF NOT EXISTS `ctrl`.`admin`.`pipelines` (
  pipeline_id STRING,
  env STRING,
  is_enabled BOOLEAN,
  effective_from TIMESTAMP,
  params_json STRING,   -- Lv4と同じ構造（src1/src2/join/dest）
  quality_json STRING,  -- 品質ゲート（min_rows_joined/max_null_rate_on_keys/max_duplicate_keys）
  updated_at TIMESTAMP
) USING DELTA
TBLPROPERTIES ("comment"="Pipeline control table");
```

### 実行ログTBLのDDL

```
CREATE TABLE IF NOT EXISTS `ctrl`.`admin`.`pipeline_runs` (
  run_id STRING,
  pipeline_id STRING,
  env STRING,
  config_version STRING,
  started_at_ms BIGINT,
  finished_at_ms BIGINT,
  status STRING,                 -- SUCCESS / FAILED
  dest_table STRING,
  mode STRING,
  rows_joined BIGINT,
  rows_out BIGINT,
  violations STRING,
  message STRING,
  inserted_at_ms BIGINT
) USING DELTA
TBLPROPERTIES ("comment"="Pipeline run logs");
```

### Jobs 実行時のパラメータ
```
spark.conf もしくは 環境変数 で指定
config.CTRL_CATALOG=ctrl
config.CTRL_SCHEMA=admin
config.CTRL_TABLE=pipelines
config.LOG_CATALOG=ctrl
config.LOG_SCHEMA=admin
config.LOG_TABLE=pipeline_runs
config.PIPELINE_ID=load_join_save_01
config.ENV=prd（任意）
```

## 参考

### 実行ログ は TBL か ファイルか？

- まず System Tables を有効化し、標準のジョブ可視化はそれで賄う。
- パイプライン専用のログは Delta に正規保存（あなたの Lv5 テンプレ通り）。
- 超高頻度な明細は一時ファイル
  - 定期取り込み（Auto Loader/COPY INTO）
  - Delta。取り込み後は OPTIMIZE 等で整備。
- 長期保管が必要な System Tables 相当の情報は、期限前にエクスポート（保持 60 日の注意）
- ログ分析/ダッシュボードは Delta（＋System Tables） を統合参照
- Databricks 公式ブログも構造化ログの集中管理を推奨しています 

> まとめ：“見える化は System Tables、運用の真実は Delta テーブル”。
> ファイルは“受け皿”に留め、最終的には Delta に寄せるのが Databricks 流です。




