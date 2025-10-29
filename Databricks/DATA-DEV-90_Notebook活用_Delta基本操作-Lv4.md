# Deltaテーブル基本操作 Lv4 外部設定ファイル駆動 & CI配布

## 概要

**目的**
- Lv1 の骨格（データロード→加工→保存）を維持する
- 設定JSON からパラメータを読み込み、自動実行に適した形へ
- 設定は GitHubで版管理し、CI で UC Volumes/DBFS に配布。ノートブックは不変
 
**仕様**
- 設定
  - JSONに以下を保持し、ノートはそれを読むだけ（同一処理を環境ごとに切替可能）
  - src1 / src2：catalog / schema / table / drop_cols[]
  - join：keys[] / type（inner/left/right/full）
  - dest：catalog / schema / table / mode（overwrite/append）
- 任意
  - config_version / notes
- JSONのスキーマ検証を実施（必須キー・型）

**使い方**
  1. CONFIG_PATH（設定ファイルのパス）を Jobs の引数 or 環境変数 or spark.conf で渡す
  2. ノートブックを実行（スケジュール実行可）
  3. ログ・プレビュー・保存結果を確認

**前提**
- 必須 : SELECT 権限（入力テーブル）
- 必須 : CREATE/WRITE 権限（保存先スキーマ）
- 任意 : USE CATALOG / USE SCHEMA できること（本書は完全修飾名で参照する仕様）
- 設定ファイル : UC Volumes/DBFS に配置
  - 例：/Volumes/{cat}/{sch}/{vol}/configs/dev/pipeline.json

**参考 - 設定ファイル(JSON)の例**
```
{
  "config_version": "2025-10-01",
  "src1": { "catalog": "main", "schema": "default", "table": "sample_delta_1", "drop_cols": ["raw_ts"] },
  "src2": { "catalog": "main", "schema": "default", "table": "sample_delta_2", "drop_cols": [] },
  "join": { "keys": ["id"], "type": "inner" },
  "dest": { "catalog": "main", "schema": "mart", "table": "joined_sample", "mode": "overwrite" },
  "notes": "dev environment"
}
```

**今後**
- GitHub管理：configs/<env>/pipeline.json をコミット → PR で JSONスキーマ検証
- CI 配布：main/タグで UC Volumes/DBFS へ pipeline.json を配置
  - 例：dbfs:/configs/dev/pipeline.json
- Jobs 実行：Jobs から spark.conf: config.CONFIG_PATH あるいは 環境変数 CONFIG_PATH を渡す
- ノート不変：環境差分は 設定ファイルのみ で吸収（再利用・差分管理が容易）
- エラー時：スキーマ検証で早期失敗 → CI／実行ログで検知。
- 次Lv5で品質ゲート／制御TBLへ発展


## 実装

### インポートと定数

```
import os, json
from typing import List, Dict, Any, Optional
```

### ユーティリティ関数

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
# 優先順位: spark.conf("config.CONFIG_PATH") > os.environ["CONFIG_PATH"]
def get_config_path() -> str:
    try:
        v = spark.conf.get("config.CONFIG_PATH")
        if v and v.strip():
            return v.strip()
    except Exception:
        pass
    v = os.environ.get("CONFIG_PATH", "").strip()
    if not v:
        raise ValueError("CONFIG_PATH is required. Provide via spark.conf('config.CONFIG_PATH', ...) or env CONFIG_PATH.")
    return v
```

#### 設定JSONのロード関数

```
def load_json_from_fs(path: str) -> Dict[str, Any]:
    with dbutils.fs.open(path, "r") as f:
        txt = f.read()
    return json.loads(txt)
```

#### 設定スキーマ検証関数

```
def validate_config(conf: Dict[str, Any]) -> None:
    required_top = ["src1", "src2", "join", "dest"]
    for k in required_top:
        if k not in conf:
            raise ValueError(f"Missing key '{k}' in config")

    def require_fields(name: str, d: Dict[str, Any], fields: List[str]):
        for f in fields:
            if f not in d:
                raise ValueError(f"Missing key '{name}.{f}' in config")

    # src1/src2
    for s in ("src1", "src2"):
        require_fields(s, conf[s], ["catalog", "schema", "table", "drop_cols"])
        if not isinstance(conf[s]["drop_cols"], list):
            raise ValueError(f"'{s}.drop_cols' must be list")

    # join
    require_fields("join", conf["join"], ["keys", "type"])
    if not isinstance(conf["join"]["keys"], list) or not conf["join"]["keys"]:
        raise ValueError("'join.keys' must be a non-empty list")
    if conf["join"]["type"] not in ("inner","left","right","full"):
        raise ValueError("'join.type' must be one of inner/left/right/full")

    # dest
    require_fields("dest", conf["dest"], ["catalog", "schema", "table", "mode"])
    if conf["dest"]["mode"] not in ("overwrite","append"):
        raise ValueError("'dest.mode' must be overwrite or append")
```

### 設定のロード

#### 設定を読み込む

```
CONFIG_PATH = get_config_path()
conf = load_json_from_fs(CONFIG_PATH)
validate_config(conf)
```

#### 設定の確認

```
print("=== CONFIG LOADED ===")
print("config_version:", conf.get("config_version"))
print("CONFIG_PATH   :", CONFIG_PATH)
print("SRC1          :", conf["src1"])
print("SRC2          :", conf["src2"])
print("JOIN          :", conf["join"])
print("DEST          :", conf["dest"])
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
- 設定は conf、データは load_delta()から読み込む

```
src1 = conf["src1"]; src2 = conf["src2"]
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
df1 = safe_drop(df1, conf["src1"]["drop_cols"])
df2 = safe_drop(df2, conf["src2"]["drop_cols"])

print("=== After Drop SRC1 Columns ===")
print(df1.columns)
print("\n=== After Drop SRC2 Columns ===")
print(df2.columns)
```

### 結合する
- キー検証を追加

```
join_keys = conf["join"]["keys"]
join_type = conf["join"]["type"]

# 結合キーの存在チェック
missing_1 = [c for c in JOIN_KEYS if c not in df1.columns]
missing_2 = [c for c in JOIN_KEYS if c not in df2.columns]
if missing_1 or missing_2:
    raise ValueError(f"Join key not found. df1 missing={missing_1}, df2 missing={missing_2}")

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

```
dest = conf["dest"]
dst_fq = fq(dest["catalog"], dest["schema"], dest["table"])

# テーブルとして保存（Unity Catalog配下を想定）
df_joined.write.format("delta").mode(dest["mode"]).saveAsTable(dst_fq)
print(f"Saved to {dst_fq} (mode={dest['mode']})")

# 保存結果を確認
df_out = spark.table(dst_fq)
print("=== Output Preview ===")
df_out.show(10, truncate=False)

print("\n=== Output Schema ===")
df_out.printSchema()
display(df_out)
```

