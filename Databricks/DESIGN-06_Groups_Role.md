# 机上検証レポート：付与専用グループ（ent.*）＋高秘匿専用（confidential.*）設計

作成日: 2025-10-13

## プロンプト
改めてグループ・ロール設計を次の通りにしたいと思います。
下記にて、権限管理が運用可能か机上検証をお願いします。
検証結果は、mdファイルでダウンロード可能か形で提供ください

- 付与専用グループ `ent.*` と高秘匿専用グループ `confidential.*` の2種類を想定する
- 付与専用グループ `ent.*` は入れ子を組まずフラットに並べる
 - `ent.<domain>.<env>.<role>.<com><org>.<employee>`　でグループを構成する
   - `<domain>` は、「BtoC」「BtoB」「BtoG」を想定する
 - `<env>`は、「prod(本番)」「dev(検証)」を想定する
 - `<role>`は、「データエンジニア」「データユーザ」「データサイエンティスト」を想定する
 - `<com>`は、「AAA(戦略・営業企業)」「ZZZ(開発・運用企業)」を想定する
 - `<org>`は、「戦略グループ」「開発グループ」「営業グループ」「運用グループ」を想定する
 - `<org>`は、G100100 から、100の位を繰り上げる形でグループ名を設定する
 - `<employee>`は、proper (正社員) と、partner (パートナー) とする
 - 付与専用グループ `ent.*` 全てに対して、ABACタグの高秘匿マスキング処理を適用する
 - 高秘匿専用グループ `confidential.*` は、ABACタグの制御対象外とする
 - 高秘匿専用グループ `confidential.*` に2次所属したメンバーは高秘匿データが閲覧可能に設計する

## 0. 結論（この構成で運用可能か）

- **運用可能**です。ポイントは次の3つ：  
  - **グループは“交差の完成形”として付与先に使う（入れ子にしない）**  
  - **データ側の細則（マスク／行フィルタ）は ABAC / 動的ビュー で制御**（グループは粗い入場券）  
  - `confidential.*` は「高秘匿の例外キー」**として扱う
    - **ABAC（または動的ビュー）の条件で優先解除**できるようにする
- リスクは「confidential.* の誤配属」「ent.* の爆発」と「ABACの例外条件の不備」。
  - 対策は本書末尾の運用ガードレール参照。



## 1. 前提と命名（ユーザー要件の写し）

### 1.1 グループの種類
- **UC付与専用グループ（ent.*）**
  - フラットに並べ、子グループは持たない（= 直接ユーザ所属）  
  - ABAC制御の対象として、全ユーザにデータマスキングを適用する
  - UC のカタログ利用権限を設定する
  - 形式：`ent.<domain>.<env>.<role>.<com>.<org>.<employee>`
- **高秘匿専用グループ（confidential.*）**
  - ABAC制御の対象外
  - **2次所属**（= ent.* と併用）で、高秘匿データの閲覧を可能にする。
- **計算資源専用グループ（compute.*）**
  - 申請に基づいて、SQLウェアハウス・汎用コンピュート-クラシックを提供する
  - **2次所属**（= ent.* と併用）で、専用の計算資源利用を可能にする


### 各軸の候補

#### UC付与専用グループ

- `<domain>`: `BtoC`, `BtoB`, `BtoG`
- `<env>`: `prod`, `dev`
- `<role>`: `データエンジニア`, `データユーザ`, `データサイエンティスト`
- `<com>`: `AAA`, `ZZZ`
- `<org>`: `戦略グループ`, `開発グループ`, `営業グループ`, `運用グループ`  
  - ※名称コード: G100100 から **100刻み**（例: G100100, G100200, ...）を付与名に含めてもよい
- `<employee>`: `proper`, `partner`

> **例**：`ent.BtoC.prod.データユーザ.AAA.営業グループ.G100300.proper`  

#### 高秘匿専用グループ

- `<abac>`: `pii` (個人情報)
- `<crod>`: `read` (読み取り)

> **例**：`confidential.pii.read`

#### 計算資源専用グループ

- `<type>`: `classic` (クラシック), `sqlwh`(SQLウェアハウス)

> **例**：`compute.classic.prod`


## 2. 権限カテゴリと割り当て原則

- **エンタイトルメント**（Workspace access / Databricks SQL access / Allow cluster create）  
  → ent.* に付与（必要最小）。confidential.* には基本付与しない（入場権は ent.* 側）。

- **Unity Catalog（UC）**  
  - **ent.***：`USE CATALOG/SCHEMA` + 対象に応じた `SELECT/CREATE/MODIFY` を **catalog / schema** で付与（継承活用）。  
  - **confidential.***：**高秘匿領域（後述）**に対して `SELECT` 等を直接付与。

- **SQL Warehouse**  
  - **ent.***：分析用WHに `CAN USE`。  
  - **confidential.***：高秘匿専用WHがあるなら `CAN USE` を別途付与（無ければ不要）。

- **Compute（クラスタ/プール）**  
  - **ent.***：`CAN ATTACH TO` / `CAN MANAGE` を必要範囲で付与。  
  - **confidential.***：基本不要（高秘匿操作用に明示のジョブ/クエリ運用が適切）。

- **Workspaceオブジェクト（フォルダACL）**  
  - **ent.***：プロジェクトフォルダに `CAN READ/EDIT/MANAGE` を継承付与。  
  - **confidential.***：高秘匿ダッシュボード等がある場合のみ最小付与。



## 3. ABAC と confidential.* の関係（“二段ロック”の設計）

### 3.1 方針
- **ABAC**：**ent.* 全員**に対して **高秘匿マスキング**を適用（列マスク/行フィルタ）。  
- **confidential.***：**ABACの例外キー**として扱う（= 2次所属者はマスク解除/フィルタ緩和）。

### 3.2 実装の選択肢
- **A. ABACポリシー側で例外条件を記述**  
  - 例）「`classification='restricted'` ならマスク。ただし **confidential.* のメンバーは除外**」
  - 実現方法は環境により異なるため、**“グループ所属の判定”**がポリシーで利用可能かを確認。

- **B. 動的ビュー（ダイナミックビュー）で条件分岐**（実務で堅実）  
  - **列マスク**：`CASE WHEN is_member('confidential.sales_core_readers') THEN email ELSE regexp_replace(email, '(^.).+(@.*$)', '\\1***\\2') END AS email_masked`  
  - **行フィルタ**：`WHERE is_member('confidential.sales_core_readers') OR org = current_user_org()`  
  - これを**スキーマ単位**のビューとして公開（基テーブルは非公開）。

> *注*: ビルトイン関数名はワークスペースのバージョン差異により `is_member` / `is_account_group_member` 等が存在します。実環境の関数を確認して記述してください。


## 4. “高秘匿領域”の切り出し（推奨）

- **物理分離**：`catalog core_sensitive` / `schema pii` などを用意。  
- **ABACの適用対象**：通常領域は ABAC でマスク、**高秘匿領域は「ABAC対象外」**（= ポリシー未適用 or 緩和）。  
- `confidential.*` のみ GRANT**：高秘匿領域の `SELECT` は `confidential.*` グループだけに付与。

これにより、**ent.* だけのユーザ**は**高秘匿領域そのものに入場できず**、**confidential.* 所属者**だけが**素データ**にアクセス可能。ABACの例外条件と二重化でき、誤設定耐性が上がります。


## 5. 机上テスト（シナリオ別の可否判定）

| # | 前提 | 操作 | 期待結果 | 判定 |
|---|---|---|---|---|
| 1 | ユーザUは `ent.BtoC.prod.データユーザ.AAA.営業.G100300.proper` のみ所属 | 通常カタログで顧客一覧をクエリ | **閲覧可**。ABACでメール/電話が**マスク**される | ✅ |
| 2 | ユーザUは 1 に加えて `confidential.sales_core_readers` に所属 | 同じクエリ | **閲覧可**。ABAC例外 or 動的ビュー条件で**マスク解除** | ✅ |
| 3 | ユーザUは ent.* 所属のみ | 高秘匿カタログ `core_sensitive.pii.customers` をクエリ | **権限なしエラー**（confidential にのみGRANTのため） | ✅ |
| 4 | ユーザVは `ent.BtoB.dev.データエンジニア.ZZZ.開発.G100200.partner` | 開発スキーマで DDL/DML | **CREATE/MODIFY/SELECT 可**（ent.* に dev権限） | ✅ |
| 5 | 管理者が誤って ent.* に高秘匿スキーマの `SELECT` を付与 | なし | **NG**。運用ルール違反。検知は棚卸とガードレール（後述）で | ⚠️ |
| 6 | confidential.* への誤配属 | なし | **即時に素データ閲覧可能**になるリスク | ⚠️（入会フローを厳格化） |


## 6. 代表的な付与テンプレ（抜粋）

> **原則**：**ent.* は通常領域に最小権限**、**confidential.* は高秘匿領域のみに権限**。

### 6.1 UC（通常領域）
```sql
-- 入場：catalog / schema
GRANT USE CATALOG ON CATALOG marketing TO `ent.BtoC.prod.データユーザ.AAA.営業.G100300.proper`;
GRANT USE SCHEMA ON SCHEMA marketing.public TO `ent.BtoC.prod.データユーザ.AAA.営業.G100300.proper`;

-- 参照
GRANT SELECT ON SCHEMA marketing.public TO `ent.BtoC.prod.データユーザ.AAA.営業.G100300.proper`;
```

### 6.2 UC（高秘匿領域 / ABAC対象外）
```sql
-- 高秘匿は confidential のみに付与
GRANT USE CATALOG ON CATALOG core_sensitive TO `confidential.sales_core_readers`;
GRANT USE SCHEMA ON SCHEMA core_sensitive.pii TO `confidential.sales_core_readers`;
GRANT SELECT ON SCHEMA core_sensitive.pii TO `confidential.sales_core_readers`;
```

### 6.3 SQL Warehouse / Compute
```text
WH:  ent.* に BI用WH の CAN USE（confidential は必要時のみ）
CMP: ent.* に開発クラスタ CAN MANAGE / 参照系クラスタ CAN ATTACH TO
```

### 6.4 動的ビュー例（マスク＆例外）
```sql
CREATE OR REPLACE VIEW marketing.public.customers_v AS
SELECT
  CASE
    WHEN is_member('confidential.sales_core_readers') THEN email
    ELSE regexp_replace(email, '(^.).+(@.*$)', '\\1***\\2')
  END AS email,
  CASE
    WHEN is_member('confidential.sales_core_readers') THEN phone
    ELSE '***-****-****'
  END AS phone,
  *
FROM marketing.public.customers_base;
```

## 7. ガードレール（必須の運用ルール）

1) **高秘匿領域の GRANT は confidential.* 限定**（ent.* への付与禁止）  
2) **ABAC/ビューの例外条件に使うグループ名は「confidential.*」に統一**（実装ミスを減らす）  
3) **confidential.* への入会はチケット必須・二重承認**、退会は即時（人事連動）  
4) **四半期棚卸**：  
   - a. `SHOW GRANTS` / Permissions API のエクスポート  
   - b. confidential.* のメンバーリスト  
   - c. ABAC/ビューの定義差分（Git管理）  
5) **監査クエリ**：  
   - 「ent.* が高秘匿に直接GRANTされていないか」  
   - 「confidential.* 以外が高秘匿に入場していないか」

## 8. 規模見積りの目安（サマリ）

- ent.* の総数は `|domain| × |env| × |role| × |com| × |org| × |employee|` に比例。  
- 高秘匿は **領域を分けて最少の confidential.* に集約**し、増やさない。  
- ABACを上にかぶせることで **ent.* のバリエーションを増やさずに細かな見せ分け**が可能。


## 9. まとめ

- **YES：運用可能**。  
- **鍵は二段ロック**（**グループ＝入場**, **ABAC/ビュー＝見せ方**）と、**高秘匿の物理分離＋confidential.* 限定GRANT**。  
- 実装は **小さく開始（1カタログ/1スキーマ）→検証→全体展開** が安全です。

