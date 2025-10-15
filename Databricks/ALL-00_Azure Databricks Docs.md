# 全体サマリ

|カテゴリ|目的|主な読者|作成主体|
|---|---|---|---|
|アーキテクチャ文書|“全体像”の合意（非機能要件・閉域設計方針）|IT企画、セキュリティ、NW、データ責任者|アーキテクト|
|設計文書|実装に必要な具体設計（VNet/PE/NSG/UDR、UC、ID連携、コスト）|構築担当、運用、監査|基盤設計|
|構築手順|再現可能な IaC/手順（検証手順含む）|構築・運用|構築担当|
|運用管理文書|監視・権限・ランタイム・コストの標準運用|運用、SRE、セキュリティ|運用担当|
|利用者ガイド|利用者が安全に自走するためのHow-to|データユーザ／データアナリスト|データエンジニア|


# アーキテクチャ文書（例：ARCH-xx_*.md）

- **ARCH-01_全体アーキテクチャ概要**
    - 目的／範囲／前提（閉域 vNET、インターネット非到達方針）
    - 環境分離（Dev/Stg/Prod、サブスクリプションの切り方）
    - 主要コンポーネント（Azure Databricks、ADLS Gen2、Key Vault、Log Analytics、Azure Firewall/NAT、Private DNS）
        
- **ARCH-02_ネットワーク＆セキュリティ・アーキテクチャ**
    - vNET/VNet Injection、サブネット設計思想（workspace/cluster）
    - Private Link/Private Endpoint 採用方針、No Public IP/Secure Cluster Connectivity、NAT 退出設計
    - NSG/UDR、送信先制限、FQDN/Service Tag の扱い
        
- **ARCH-03_アイデンティティ＆アクセス基盤**
    - Entra ID（SSO、SCIM）、Databricks グループ方針、RBAC/ACL 境界
    - 秘密管理（Key Vault、Managed Identity、PAT 最小化）
        
- **ARCH-04_データ／カタログ＆ガバナンス（Unity Catalog）**
    - メタストア配置、カタログ/スキーマ命名、外部ロケーション、データラインジ／監査
        
- **ARCH-05_統合アーキ（周辺サービス連携）**
    - Event Hubs／Synapse／Power BI／Functions 等の接続境界
        
- **ARCH-06_可用性・回復性・BCP**
    - DR レベル、バックアップ対象（ノートブック、UC メタデータ、ポリシー）
        
- **ARCH-07_観測性＆コンプライアンス**
    - ログ設計、アラート責任分界、証跡・保持ポリシー
        
- **ARCH-08_コストとスケーリング戦略**
    - ワークロード別（ジョブ／インタラクティブ／SQL）最適化原則、ポリシー適用



# 設計文書（例：DESIGN-xx_*.md）
- **DESIGN-01_ワークスペース設計**
    - リージョン、SKU、機能選択（SQL/サーバーレス有無、ポリシー）、制限事項
        
- **DESIGN-02_vNET/サブネット/アドレス計画**
    - アドレス設計、サブネット割当（workspace/cluster/PE）、将来拡張余白
        
- **DESIGN-03_Private Link/Private Endpoint 設計**
    - 必要 PE 一覧（Workspace/Front-End/Back-End、Storage、KV、Log Analytics など）
    - Private DNS ゾーン／レコード、名前解決フロー
        
- **DESIGN-04_NSG/UDR/NAT/Azure Firewall 設計**
    - 方向別ルール、既知制御プレーン宛の例外、Egress 先一覧、監査方法
        
- **DESIGN-05_ID連携／権限設計**
    - SSO/SCIM、グループ設計、Databricks ACL/RBAC、管理者ロール分離
        
- **DESIGN-06_Unity Catalog 設計**
    - メタストア、外部ロケーション、カタログ構造、 Grants 標準、データ分類＆タグ
        
- **DESIGN-07_ストレージ設計（ADLS Gen2）**
    - コンテナ構造、ACL/RBAC、暗号化キー、ライフサイクル管理
        
- **DESIGN-08_監視・ログ・アラート設計**
    - Diagnostic Settings、Log Analytics テーブル、アラートルール、可観測ダッシュボード
- **DESIGN-09_CI/CD＆環境分離**
    - IaC（Bicep/Terraform）標準、Databricks Repos、リリースフロー、ポリシー適用
- **DESIGN-10_コスト管理設計**
    - インスタンスポリシー、ジョブクラスター推奨、Auto Termination、タグ設計

# 構築手順（例：BUILD-xx_*.md ／ IaC 同梱）

- **BUILD-00_前提条件と権限セットアップ**（サブスクリプション RBAC、プロバイダ登録）
- **BUILD-10_ネットワーク基盤**（vNET/サブネット/NSG/UDR/NAT/Azure Firewall）
- **BUILD-20_Private DNS／Private Endpoint**（ゾーン作成、リンク、レコード検証）
- **BUILD-30_共通リソース**（Key Vault、ADLS Gen2、Log Analytics、ストレージ暗号鍵）
- **BUILD-40_Databricks ワークスペース**（VNet Injection／No Public IP／SCC）
- **BUILD-50_Unity Catalog 初期化**（メタストア、外部ロケーション、初期 Grants）
- **BUILD-60_ID/SSO/SCIM 連携**（グループ同期、管理者委任）
- **BUILD-70_監視・アラート有効化**（Diag 設定、アラートルール配備）
- **BUILD-80_CI/CD 配備**（IaC パイプライン、Repos 接続、ポリシー配布）
- **BUILD-90_検証・受入**（接続テスト、E2E 動作確認、セキュリティスキャン）
- **BUILD-99_引き継ぎ資料**（構成図／アカウント台帳／秘密保管手順）

> 付帯物：作業者チェックリスト、ロールバック手順、Smoke Test 台本、スクリーンショット集

# 運用管理文書（例：OPS-xx_*.md）

- **OPS-01_運用総則＆RACI**（窓口、責任分界、変更管理）
- **OPS-02_監視・アラート運用**（しきい値、当番、一次切り分け手順、KQL 例）
- **OPS-03_権限／アカウント運用**（定期権限見直し、入退社、SCIM 同期、監査ログ）
- **OPS-04_セキュリティ運用**（秘密鍵／シークレットローテーション、脆弱性対応方針）
- **OPS-05_ランタイム＆ポリシー運用**（DBR バージョン方針、クラスタポリシー、既知不具合管理）
- **OPS-06_バックアップ／BCP**（ノートブック／UC メタデータのバックアップ法、復旧手順）
- **OPS-07_コスト運用**（ダッシュボード、予算／アラート、タグ運用、レポート例）
- **OPS-08_インシデント対応集**（NW到達不可、PE/DNS 異常、クォータ枯渇、スロットリング等）
- **OPS-09_定期点検**（月次健康診断、設定ドリフト検出、IaC ドライラン）


# 利用者ガイド（例：USER-xx_*.md）

- **USER-01_はじめに（初回サインイン＆ワークスペース概要）**
    - SSO、UI 基本、ノートブック／Repos の使い方、ジョブ／ワークフローの概念
        
- **USER-02_クラスタ利用とベストプラクティス**
    - クラスタの選び方、インスタンスポリシー、ライブラリ管理、コスト節約のコツ
        
- **USER-03_データアクセス（Unity Catalog）**
    - カタログ／スキーマ／テーブル、外部ロケーションの読み書き、権限申請フロー
        
- **USER-04_SQL／Notebook／ジョブ実行ガイド**
    - SQL エディタ、Delta Lake の基本、スケジュール実行、エラーの見方
        
- **USER-05_MLflow／実験管理（任意）**
    - 実験の作り方、モデル登録、簡易 MLOps パターン
        
- **USER-06_データの品質・セキュリティ心得**
    - 個人情報の扱い、監査対象操作、ログの可視化
        
- **USER-07_トラブルシューティング FAQ**
    - 典型事象（権限不足、接続不可、依存ライブラリエラー等）の自己解決手順

# 付録：共通テンプレ（各ドキュメントの先頭に）

- タイトル／版数／作成日／担当
- 対象環境（Dev/Stg/Prod）
- 前提・非機能要件（セキュリティ、可用性、運用性、性能、コスト）
- 用語集・略語
- 参照文書（相互リンク：ARCH ⇄ DESIGN ⇄ BUILD ⇄ OPS ⇄ USER）
- 変更履歴（誰が／何を／いつ）


# 推奨リポジトリ構成（例）


docs/
  architecture/   (ARCH-*.md, 図: drawio/png)
  design/         (DESIGN-*.md, IaC 変数表)
  build/          (BUILD-*.md, IaC: terraform/ or bicep/)
  operations/     (OPS-*.md, Runbook, KQL)
  user/           (USER-*.md, スクショ)
  images/         (共通図版)
  templates/      (md テンプレ、チェックリスト、検証台本)