# Deltaテーブル基本操作

## テーマ / ゴール

### Lv1 最小実装（骨格）
- コード内定数で 2テーブルをロード
- 不要列ドロップ→結合→保存してプレビュー

### Lv2 操作性と堅牢性の底上げ
- ウィジェット入力
- 同じ骨格をパラメータで切替して実行

### Lv3 環境変数ベースの自動実行
- Jobs/環境設定
- 同じ骨格を環境編集（ジョブ／クラスターの環境変数）で駆動

### Lv4 外部設定ファイル駆動
- UC Volumes/DBFSのJSON等
- GitHub/CIで“設定を配布
- 同じ骨格を設定ファイルで一元管理

### Lv5 制御用テーブル駆動
- Unity Catalogの制御TBL
- GitHub/CI/CDで“定義もデプロイ
- 同じ骨格をDeltaの制御レコードで制御
- 品質ゲートと通知を付加


## ユースケース例

### Lv1
- まずは“読むだけ”の疎通確認
- 対象テーブルの列構成を把握
- 後続の加工に備えた目視チェック

### Lv2
- 手動探索でテーブルや結合キーを都度変更

### Lv3
- 夜間ジョブでの定型処理

### Lv4
- 環境差分をファイルで管理・配布

### Lv5
- 本番運用・監査

## ノートブックの主な工程

### Lv1
  1. 設定入力：コード内定数
  2. load_delta()
  3. df1/df2読込
  4. show/printSchema/display
  5. drop()
  6. join()
  7. saveAsTable()
  8. 保存結果のプレビュー

### Lv2
  1. 設定入力：dbutils.widgets（cat/sch/tbl・drop列・joinキー・mode）
  2. 入力取得→検証
  3. Lv1と同じ処理
  4. 結果出力

### Lv3
  1. 設定入力：環境変数／spark.conf（os.environ.get/spark.conf.get）
  2. パラメータ解決とデフォルト
  3. Lv2相当の検証
  4. Lv1同処理
  5. 実行ログ（print）

### Lv4
  1. 設定入力：外部設定ファイル（JSON想定）
  2. dbutils.fs.open→json.loads でロード
  3. スキーマ検証（必須キー）
  4. Lv2相当の検証
  5. Lv1同処理
  6. 結果出力

### Lv5
  1. 設定入力：制御用TBL
    - spark.table("cat.sch.ctrl") をクエリ）
    - pipeline_id/env/effective_from/params_json など
  2. 現行行を選択
  3. params_json を展開
  4. 品質ゲート（件数>0、重複=0、NULL率<閾値 等）
  5. Lv1同処理
  6. 成否を制御TBLに記録

## 主要API / 機能

### Lv1
- spark.table()
- display()
- DataFrame.show()
- （任意）USE CATALOG/USE SCHEMA
- 完全修飾名 `catalog`.`schema`.`table`

### Lv2
- dbutils.widgets.text
- dropdown
- widgets.get
- withColumn(必要時の軽い型合わせ)

### Lv3
- os.environ
- spark.conf.get/set
- Jobsのスケジュール（※ノート側は取得のみ）

### Lv4
- dbutils.fs
- json
- UC Volumes（設定の配置先）

### Lv5
- spark.table
- JSON展開（from_json or Python json）
- 品質チェックはassert相当
- （必要に応じJobsの通知設定）

## 品質・バリデーション

### Lv1
- 件数の目安確認（show で先頭のみ）
- 列名/型の確認（columns/printSchema）
- エラー時に TABLE NOT FOUND などの基本診断

### Lv2
- 安全ドロップ（存在列のみ削除）
- 結合前のキー型一致チェック
- 件数ログ（count）

### Lv3
- 必須パラメータ欠落時に停止
- ログに入力値・行数・出力先を記録

### Lv4
- 設定スキーマ検証（必須キー・型）
- 設定のバージョン（config_version）
- ログ出力

### Lv5
- 閾値違反でジョブ失敗（静かに壊れない）
- 制御TBLへ実行結果ログ（件数・開始/終了時刻）


## パフォーマンス・運用

### Lv1
- 状態依存を避けるため USE なしでも動く完全修飾名方式を採用
- キャッシュ/最適化は未使用（Lv2以降）
- 最小セル数で共有しやすく（%run で再利用可能）

### Lv2
- 再実行の見通し向上（パラメタライズ）
- 必要時のみ df.cache()→unpersist()

### Lv3
- 手動→自動へ
- 環境で切替できるためノートは不変
- 失敗時の再実行が容易

### Lv4
- GitHub 版管理
- CIで設定を配布（dev→stg→prd）
- ノートは不変
- Jobs で自動実行

### Lv5
- 設定＝データなので監査・ロールバック容易
- GitHub/CI/CDで制御TBLのDDL
- Seedデータも管理
- 通知（Workflows）
- スケジュール
- リトライ
- メトリクス整備


## 成果物 / 演習課題

### Lv1
**成果物**
- Lv1ノートブック本体（関数＋実行の2セル）

**演習**
  1. CATALOG/SCHEMA/TABLE を自環境に合わせ実行 
  2. 予約語/ハイフン含むテーブル名をバッククォートで読み込む
  3. display のグリッド統計と printSchema の型が一致しているか確認

### Lv2
**演習**
- 3種類の入力で連続実行→すべて成功
- 欠損キー時に早期エラーを出せること

### Lv3
**演習**
- Jobsで環境を dev/stg 切替実行
- 出力先が切り替わること

### Lv4
**演習**
- 設定JSON( dev / stg ) をブランチで管理
- CIでVolumesへ配布
- Jobsで実行

### Lv5
**演習**
- 制御TBLの行を差し替え
- 次回実行で出力先/モードが切替
- 品質ゲート違反で失敗になること


