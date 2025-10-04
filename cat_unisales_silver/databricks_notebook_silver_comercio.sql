-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Camada Silver — Comércio
-- MAGIC
-- MAGIC Objetivo: tipar, higienizar e deduplicar dados da Bronze
-- MAGIC (`cat_unisales_bronze.db_comercio`) e disponibilizar dimensões
-- MAGIC e fatos prontos para consumo analítico.
-- MAGIC
-- MAGIC Como usar:
-- MAGIC 1) Selecione seu SQL Warehouse.
-- MAGIC 2) Execute as células na ordem.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS cat_unisales_silver;
CREATE SCHEMA  IF NOT EXISTS cat_unisales_silver.db_comercio;

-- COMMAND ----------

CREATE OR REPLACE TABLE cat_unisales_silver.db_comercio.dim_calendario AS
WITH limites AS (
  SELECT to_date(min(dt_venda)) AS dt_min, to_date(max(dt_venda)) AS dt_max
  FROM cat_unisales_bronze.db_comercio.tb_vendas
),
cal AS (
  SELECT sequence((SELECT dt_min FROM limites), (SELECT dt_max FROM limites), INTERVAL 1 DAY) AS ds
)
SELECT
  dte AS dt,
  year(dte) AS ano,
  month(dte) AS mes,
  weekofyear(dte) AS semana,
  day(dte) AS dia_mes,
  date_format(dte, 'E') AS dia_semana,
  (1.0 + 0.15 * sin(2*pi() * (dayofweek(dte)-1)/7)) AS fator_sazonal
FROM (SELECT explode(ds) AS dte FROM cal);

-- COMMAND ----------

CREATE OR REPLACE TABLE cat_unisales_silver.db_comercio.dim_loja AS
SELECT
  CAST(id_loja AS STRING) AS id_loja,
  CAST(nome AS STRING) AS nm_loja,
  CAST(cidade AS STRING) AS cidade,
  CAST(uf AS STRING) AS uf,
  current_timestamp() AS ts_silver
FROM cat_unisales_bronze.db_comercio.tb_lojas;

-- COMMAND ----------

CREATE OR REPLACE TABLE cat_unisales_silver.db_comercio.dim_produto AS
SELECT
  CAST(id_produto AS STRING) AS id_produto,
  CAST(sku AS STRING) AS sku,
  CAST(nome AS STRING) AS nm_produto,
  CAST(categoria AS STRING) AS categoria,
  CAST(marca AS STRING) AS marca,
  CAST(preco_lista AS DECIMAL(18,2)) AS preco_lista,
  CAST(rank_prod AS INT) AS rank_pop,
  CAST(weight_zipf AS DOUBLE) AS weight_zipf,
  current_timestamp() AS ts_silver
FROM cat_unisales_bronze.db_comercio.tb_produtos;

-- COMMAND ----------

CREATE OR REPLACE TABLE cat_unisales_silver.db_comercio.vendas AS
SELECT
  TRY_CAST(dt_venda AS DATE) AS dt_venda,
  TRY_CAST(id_pedido AS STRING) AS id_pedido,
  TRY_CAST(id_loja AS STRING) AS id_loja,
  TRY_CAST(id_produto AS STRING) AS id_produto,
  TRY_CAST(qtd AS INT) AS qtd,
  TRY_CAST(receita AS DECIMAL(18,2)) AS receita,
  TRY_CAST(desconto AS DECIMAL(18,2)) AS desconto,
  COALESCE(ts_bronze, current_timestamp()) AS ts_bronze,
  current_timestamp() AS ts_silver
FROM cat_unisales_bronze.db_comercio.tb_vendas
WHERE dt_venda IS NOT NULL;

-- COMMAND ----------

CREATE OR REPLACE TABLE cat_unisales_silver.db_comercio.vendas_dedup AS
WITH r AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id_pedido, id_loja, id_produto ORDER BY ts_bronze DESC, ts_silver DESC) AS rn
  FROM cat_unisales_silver.db_comercio.vendas
)
SELECT * EXCEPT(rn) FROM r WHERE rn = 1;

-- COMMAND ----------

CREATE OR REPLACE TABLE cat_unisales_silver.db_comercio.vendas_checked AS
SELECT
  v.*,
  CASE
    WHEN dt_venda IS NULL OR dt_venda > current_date() THEN 'ERRO_DATA'
    WHEN id_pedido IS NULL OR id_loja IS NULL OR id_produto IS NULL THEN 'ERRO_CHAVE'
    WHEN qtd IS NULL OR qtd <= 0 THEN 'ERRO_QTD'
    WHEN receita IS NULL OR receita < 0 OR desconto IS NULL OR desconto < 0 THEN 'ERRO_VALOR'
    ELSE 'OK'
  END AS qa_status
FROM cat_unisales_silver.db_comercio.vendas_dedup v;

-- COMMAND ----------

CREATE OR REPLACE TABLE cat_unisales_silver.db_comercio.fct_vendas AS
SELECT
  v.dt_venda,
  v.id_pedido,
  v.id_loja,
  l.nm_loja,
  l.cidade,
  l.uf,
  v.id_produto,
  p.nm_produto,
  p.categoria,
  p.marca,
  v.qtd,
  v.receita,
  v.desconto,
  (v.receita + v.desconto) AS valor_bruto,
  v.ts_bronze,
  v.ts_silver
FROM cat_unisales_silver.db_comercio.vendas_checked v
LEFT JOIN cat_unisales_silver.db_comercio.dim_loja l USING (id_loja)
LEFT JOIN cat_unisales_silver.db_comercio.dim_produto p USING (id_produto)
WHERE qa_status = 'OK';

-- COMMAND ----------

CREATE OR REPLACE TABLE cat_unisales_silver.db_comercio.estoque AS
SELECT
  TRY_CAST(dt AS DATE) AS dt,
  TRY_CAST(id_produto AS STRING) AS id_produto,
  TRY_CAST(id_loja AS STRING) AS id_loja,
  TRY_CAST(qtd_estoque AS INT) AS qtd_estoque,
  current_timestamp() AS ts_silver
FROM cat_unisales_bronze.db_comercio.tb_estoque;

-- COMMAND ----------

OPTIMIZE cat_unisales_silver.db_comercio.fct_vendas ZORDER BY (dt_venda, id_loja, id_produto);
OPTIMIZE cat_unisales_silver.db_comercio.estoque ZORDER BY (dt, id_loja, id_produto);

-- COMMAND ----------

SELECT 'dim_calendario' AS tabela, COUNT(*) AS linhas FROM cat_unisales_silver.db_comercio.dim_calendario
UNION ALL SELECT 'dim_loja', COUNT(*) FROM cat_unisales_silver.db_comercio.dim_loja
UNION ALL SELECT 'dim_produto', COUNT(*) FROM cat_unisales_silver.db_comercio.dim_produto
UNION ALL SELECT 'vendas', COUNT(*) FROM cat_unisales_silver.db_comercio.vendas
UNION ALL SELECT 'vendas_dedup', COUNT(*) FROM cat_unisales_silver.db_comercio.vendas_dedup
UNION ALL SELECT 'vendas_checked', COUNT(*) FROM cat_unisales_silver.db_comercio.vendas_checked
UNION ALL SELECT 'fct_vendas', COUNT(*) FROM cat_unisales_silver.db_comercio.fct_vendas
UNION ALL SELECT 'estoque', COUNT(*) FROM cat_unisales_silver.db_comercio.estoque;
