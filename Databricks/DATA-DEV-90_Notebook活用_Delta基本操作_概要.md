# Deltaテーブル基本操作

## テーマ / ゴール

### Lv1 最小のデータ操作
- 定数 CATALOG/SCHEMA/TABLE を使って Delta テーブルを読み
- 先頭行・ヘッダ・スキーマを確認できる

### Lv2 パラメータ化ロード & 基本前処理
- ウィジェットでテーブル/列/条件/LIMITを切替える
- 軽い整形（型変換・列追加）まで

### Lv3 ファイル取込→Delta化
- UC Volumes
- 外部ロケーションのファイル（CSV/JSON/Parquet）を読む
- Deltaテーブルに保存

### Lv4 加工・編集＆増分対応
- 正規化/結合/集約
- 差分ファイルでUpsert（MERGE）

### Lv5 本番保存（Gold）＆運用
- 書込みモード/特性を理解する
- 品質ゲート・権限・監視を備えたジョブ化


## ユースケース例

### Lv1
- まずは“読むだけ”の疎通確認
- 対象テーブルの列構成を把握
- 後続の加工に備えた目視チェック

### Lv2
- 同じカタログで複数のテーブルを試す
- サンプル抽出

### Lv3
- 外部提供データの初期ロード

### Lv4
- ログ/トランザクションの日次更新
- 参照マスタとの結合

### Lv5
- 夜間バッチ
- ダッシュボード向けサマリの確定保存

## ノートブックの主な工程

### Lv1
  1. 仕様と定数宣言（CATALOG/SCHEMA/TABLE）
  2. （任意）USE CATALOG/USE SCHEMA
  3. load_delta() 定義（完全修飾名で読込）
  4. 実行：df = load_delta() 
  5. プレビュー：df.show()/display(df) 
  6. 不要列の削除
  7. データ結合
  8. データ保存

### Lv2
  1. 仕様/前提
  2. dbutils.widgetsで catalog/schema/table/columns/where/limit 
  3. （任意）USE CATALOG/SCHEMA
  4. load_delta拡張（select/where/limit）
  5. 軽い加工（withColumn, cast）
  6. プレビュー&ヘッダ

### Lv3
  1. 入出力パラメータ（パス/テーブル名/書込みモード）
  2. spark.readでファイル読込（必要なoption設定）
  3. スキーマ確認
  4. write or saveAsTable（Delta化）
  5. 保存後読み直し＆プレビュー

### Lv4
  1. ソース読込（Bronze）
  2. クレンジング（trim/null処理/重複排除）
  3. 結合・集約（join,groupBy）
  4. キー設計（主キー/ハッシュ）
  5. MERGE INTOでUpsert（更新/挿入/論理削除）
  6. 結果検証

### Lv5
  1. ジョブ引数→ウィジェット受け（再利用）
  2. 加工済DFをDelta保存（mode, partitionBy）
  3. 品質ゲート（閾値未達で失敗）
  4. 権限（UCのGRANT戦略、所有者）
  5. ジョブ/ワークフロー設定（リトライ・通知）
  6. メトリクス記録（処理件数/秒/件）

## 主要API / 機能

### Lv1
- spark.table()
- display()
- DataFrame.show()
- （任意）USE CATALOG/USE SCHEMA
- 完全修飾名 `catalog`.`schema`.`table`

### Lv2
- dbutils.widgets
- spark.table
- select
- where
- limit
- withColumn
- cast
- display

### Lv3

- spark.read.csv
- json
- parquet
- option(header,inferSchema,escape)
- `write.format("delta").mode("append")

### Lv4
- dropDuplicates
- join
- groupBy
- agg
- MERGE INTO
- when
- 監査列（ingest_ts,source_file）

### Lv5
- write.format("delta").mode(...)
- partitionBy
- テーブル特性（tblproperties）
- Unity Catalog権限（USE/SELECT/MODIFY）
- Workflows/Jobs
- Secrets

## 品質・バリデーション

### Lv1
- 件数の目安確認（show で先頭のみ）
- 列名/型の確認（columns/printSchema）
- エラー時に TABLE NOT FOUND などの基本診断

### Lv2
- 行数チェック
- NULL/ユニーク件数の簡易集計（count, countDistinct）

### Lv3
- overwrite").saveAsTable
- UC Volumes

### Lv4
- 期待件数・更新/挿入/削除件数の記録
- 重複キー検知
- 差分の整合性チェック

### Lv5
- 期待件数・NULL率・重複率の閾値
- 違反時はジョブ停止＆通知
- 監査列の保存

## パフォーマンス・運用

### Lv1
- 状態依存を避けるため USE なしでも動く完全修飾名方式を採用
- キャッシュ/最適化は未使用（Lv2以降）
- 最小セル数で共有しやすく（%run で再利用可能）

### Lv2
- df.cache()とunpersist()の基礎
- count()でマテリアライズ

### Lv3
- 必須列の有無・重複行検知
- 読込/書込件数突合
- 欠損率のしきい値チェック

### Lv4
- パーティション設計
- ZORDERの考え方
- OPTIMIZE/VACUUMの運用
- schemaEvolutionの注意

### Lv5
- スケジュール/リトライ/失敗通知
- メタデータ管理（所有者/説明）
- 保存後のOPTIMIZEの自動化

## 成果物 / 演習課題

### Lv1
- 成果物：Lv1ノートブック本体（関数＋実行の2セル）
- 演習
  1. CATALOG/SCHEMA/TABLE を自環境に合わせ実行 
  2. 予約語/ハイフン含むテーブル名をバッククォートで読み込む
  3. display のグリッド統計と printSchema の型が一致しているか確認

### Lv2
- 演習
  - 3つのテーブルを切替
  - 列型を狙いどおりに揃え
  - LIMIT付きでプレビュー

### Lv3
- overwriteSchemaの扱い
- partitionBy入門
- OPTIMIZE/VACUUMの触り

### Lv4
- 演習
  - 日次差分をMERGEで取り込み
  - 更新/挿入/削除件数をログに残す
  - 重複キーを検出して失敗させるオプション

### Lv5
- 演習
  - JobsでLv4の処理を実行する
  - 品質違反で失敗→通知する
  - UCの最小権限付与で動くことを確認


