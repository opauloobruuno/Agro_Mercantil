-- Queries de análise para o projeto de scraping da Conab/AgroMercantil
-- Propósito: Realizar análises como variação mensal, top commodities, detecção de anomalias e preparação de dados para dashboard.
-- Executar no schema 'agromercado'. Útil para relatórios e BI (ex.: Power BI ou Tableau).

-- Define o schema para consultas
SET search_path TO agromercado, public;

-- 1) Análise Mensal de Preços Médios
-- Calcula média mensal por commodity e região, filtrando meses com poucos dados.
SELECT
    c.nome AS commodity,
    r.nome AS regiao,
    TO_CHAR(p.data_preco, 'YYYY-MM') AS mes_ano,
    ROUND(AVG(p.valor_preco)::NUMERIC, 2) AS preco_medio,
    COUNT(p.id) AS qtd_registros
FROM precos p
JOIN commodities c ON p.commodity_id = c.id
JOIN regioes r ON p.regiao_id = r.id
GROUP BY c.nome, r.nome, mes_ano
HAVING COUNT(p.id) >= 2
ORDER BY commodity, regiao, mes_ano;

-- 2) Variação Percentual Semanal (usando LAG)
-- Calcula variação % em relação à semana anterior (para a mesma commodity e região).
-- Observação: aqui assumimos 'data_preco' como granularidade semanal (se for diária, ajuste a lógica).
SELECT
    c.nome AS commodity,
    r.nome AS regiao,
    p.data_preco,
    p.valor_preco,
    LAG(p.valor_preco) OVER (PARTITION BY c.id, r.id ORDER BY p.data_preco) AS preco_anterior,
    ROUND(
        (
            (p.valor_preco - LAG(p.valor_preco) OVER (PARTITION BY c.id, r.id ORDER BY p.data_preco))
            / NULLIF(LAG(p.valor_preco) OVER (PARTITION BY c.id, r.id ORDER BY p.data_preco), 0)
        ) * 100
    , 2) AS variacao_percentual
FROM precos p
JOIN commodities c ON p.commodity_id = c.id
JOIN regioes r ON p.regiao_id = r.id
ORDER BY commodity, regiao, data_preco;

-- 3) Top 5 Commodities Mais Negociadas (por volume de registros)
SELECT
    c.nome AS commodity,
    COUNT(p.id) AS total_registros,
    ROUND(AVG(p.valor_preco)::NUMERIC, 2) AS preco_medio_geral
FROM precos p
JOIN commodities c ON p.commodity_id = c.id
GROUP BY c.id, c.nome
ORDER BY total_registros DESC
LIMIT 5;

-- 4) Detecção de Anomalias (preços > 20% acima/abaixo de média móvel aproximada)
-- Flag para outliers, útil para validação de dados.
WITH media_movel AS (
    SELECT
        p.id,
        p.commodity_id,
        p.regiao_id,
        p.data_preco,
        p.valor_preco,
        AVG(p.valor_preco) OVER (
            PARTITION BY p.commodity_id, p.regiao_id
            ORDER BY p.data_preco
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW  -- janela aproximada: 4 registros (se semanal)
        ) AS media_movel_4registros
    FROM precos p
)
SELECT
    c.nome AS commodity,
    r.nome AS regiao,
    mm.data_preco,
    mm.valor_preco,
    mm.media_movel_4registros,
    CASE
        WHEN mm.valor_preco > (mm.media_movel_4registros * 1.20) THEN 'ANOMALIA_ALTA'
        WHEN mm.valor_preco < (mm.media_movel_4registros * 0.80) THEN 'ANOMALIA_BAIXA'
        ELSE 'NORMAL'
    END AS flag_anomalia
FROM media_movel mm
JOIN commodities c ON mm.commodity_id = c.id
JOIN regioes r ON mm.regiao_id = r.id
WHERE flag_anomalia != 'NORMAL'
ORDER BY commodity, regiao, data_preco;

-- 5) Estatísticas Descritivas por Commodity e Região
SELECT
    c.nome AS commodity,
    r.nome AS regiao,
    COUNT(p.id) AS total_registros,
    ROUND(AVG(p.valor_preco)::NUMERIC, 2) AS media,
    MIN(p.valor_preco) AS minimo,
    MAX(p.valor_preco) AS maximo,
    ROUND(STDDEV(p.valor_preco)::NUMERIC, 2) AS desvio_padrao
FROM precos p
JOIN commodities c ON p.commodity_id = c.id
JOIN regioes r ON p.regiao_id = r.id
GROUP BY c.nome, r.nome
ORDER BY commodity, regiao;

-- 6) Tabela Curated para Dashboard (Visão Materializada)
-- Pré-agrega dados para BI, incluindo variação mensal e ranking.
-- Para atualizar após novas cargas ETL:
--   REFRESH MATERIALIZED VIEW agromercado.vw_dashboard_precos;
CREATE MATERIALIZED VIEW IF NOT EXISTS vw_dashboard_precos AS
SELECT
    c.nome AS commodity,
    c.unidade_medida,
    r.nome AS regiao,
    DATE_TRUNC('month', p.data_preco) AS mes_referencia,
    AVG(p.valor_preco) AS preco_medio_mensal,
    COUNT(p.id) AS volume_registros,
    -- Variação mensal (comparado ao mês anterior para a mesma commodity/região)
    LAG(AVG(p.valor_preco)) OVER (
        PARTITION BY c.id, r.id
        ORDER BY DATE_TRUNC('month', p.data_preco)
    ) AS preco_medio_anterior,
    CASE
        WHEN LAG(AVG(p.valor_preco)) OVER (
            PARTITION BY c.id, r.id
            ORDER BY DATE_TRUNC('month', p.data_preco)
        ) IS NOT NULL THEN
            ROUND(
                (
                    (AVG(p.valor_preco) - LAG(AVG(p.valor_preco)) OVER (
                        PARTITION BY c.id, r.id
                        ORDER BY DATE_TRUNC('month', p.data_preco)
                    ))
                    / NULLIF(LAG(AVG(p.valor_preco)) OVER (
                        PARTITION BY c.id, r.id
                        ORDER BY DATE_TRUNC('month', p.data_preco)
                    ), 0)
                ) * 100
            , 2)
        ELSE 0
    END AS variacao_mensal_percentual,
    ROW_NUMBER() OVER (
        PARTITION BY DATE_TRUNC('month', p.data_preco)
        ORDER BY COUNT(p.id) DESC
    ) AS ranking_volume
FROM precos p
JOIN commodities c ON p.commodity_id = c.id
JOIN regioes r ON p.regiao_id = r.id
GROUP BY
    c.id, c.nome, c.unidade_medida,
    r.id, r.nome,
    DATE_TRUNC('month', p.data_preco)
ORDER BY mes_referencia DESC, ranking_volume;

-- Exemplo de uso da visão: Top 10 por mês recente (>= 3 meses)
SELECT *
FROM vw_dashboard_precos
WHERE mes_referencia >= CURRENT_DATE - INTERVAL '3 months'
ORDER BY mes_referencia DESC, ranking_volume
LIMIT 10;

-- --------------------------------------------------------------------
-- Consulta (a): Preço Médio Mensal por Commodity com Variação Mês a Mês
-- --------------------------------------------------------------------
-- Análise de preço médio mensal por commodity com variação (usando LAG)
WITH precos_mensais AS (
    SELECT
        c.nome AS commodity,
        DATE_TRUNC('month', p.data_preco) AS mes_referencia,
        ROUND(AVG(p.valor_preco)::NUMERIC, 2) AS preco_medio_mensal
    FROM precos p
    JOIN commodities c ON p.commodity_id = c.id
    GROUP BY c.nome, mes_referencia
)
SELECT
    commodity,
    TO_CHAR(mes_referencia, 'YYYY-MM') AS mes_ano,
    preco_medio_mensal,
    LAG(preco_medio_mensal) OVER (PARTITION BY commodity ORDER BY mes_referencia) AS preco_anterior,
    CASE
        WHEN LAG(preco_medio_mensal) OVER (PARTITION BY commodity ORDER BY mes_referencia) IS NOT NULL THEN
            ROUND(
                (
                    (preco_medio_mensal - LAG(preco_medio_mensal) OVER (PARTITION BY commodity ORDER BY mes_referencia))
                    / LAG(preco_medio_mensal) OVER (PARTITION BY commodity ORDER BY mes_referencia)
                ) * 100, 2
            )
        ELSE NULL
    END AS variacao_percentual
FROM precos_mensais
ORDER BY commodity, mes_referencia;

-- --------------------------------------------------------------------
-- Consulta (b): Top 5 Produtos Mais Negociados no Último Ano
-- --------------------------------------------------------------------
-- Top 5 commodities mais negociadas no último ano (filtro por data, agrupado por volume de registros)
SELECT
    c.nome AS commodity,
    COUNT(p.id) AS volume_negociado,  -- Métrica: número de registros (frequência de coletas/preços)
    ROUND(AVG(p.valor_preco)::NUMERIC, 2) AS preco_medio_recente
FROM precos p
JOIN commodities c ON p.commodity_id = c.id
WHERE p.data_preco >= CURRENT_DATE - INTERVAL '1 year'  -- Último ano a partir de 23/03/2026
GROUP BY c.id, c.nome
ORDER BY volume_negociado DESC
LIMIT 5;

-- --------------------------------------------------------------------
-- Consulta (c): Registros Anômalos
-- --------------------------------------------------------------------
-- Detecção de anomalias: preços negativos, fora de faixa e inconsistentes
WITH anomalias AS (
    SELECT
        p.id,
        c.nome AS commodity,
        r.nome AS regiao,
        p.data_preco,
        p.valor_preco,
        p.url_fonte,
        -- Regra 1: Preços negativos
        CASE WHEN p.valor_preco < 0 THEN 'ANOMALIA_NEGATIVO' ELSE NULL END AS flag_negativo,
        -- Regra 2: Fora de faixa (ex.: >3x desvio padrão da média por commodity/região)
        CASE
            WHEN ABS(p.valor_preco - AVG(p.valor_preco) OVER (PARTITION BY c.id, r.id)) >
                 (3 * STDDEV(p.valor_preco) OVER (PARTITION BY c.id, r.id))
            THEN 'ANOMALIA_FORA_FAIXA'
            ELSE NULL
        END AS flag_faixa,
        -- Regra 3: Inconsistentes (ex.: data futura ou duplicata em data/região/commodity)
        CASE
            WHEN p.data_preco > CURRENT_DATE THEN 'ANOMALIA_DATA_FUTURA'
            WHEN p.valor_preco IS NULL OR p.valor_preco = 0 THEN 'ANOMALIA_VALOR_NULO_ZERADO'
            ELSE NULL
        END AS flag_inconsistente
    FROM precos p
    JOIN commodities c ON p.commodity_id = c.id
    JOIN regioes r ON p.regiao_id = r.id
)
SELECT
    commodity,
    regiao,
    data_preco,
    valor_preco,
    url_fonte,
    COALESCE(flag_negativo, flag_faixa, flag_inconsistente) AS tipo_anomalia
FROM anomalias
WHERE flag_negativo IS NOT NULL OR flag_faixa IS NOT NULL OR flag_inconsistente IS NOT NULL
ORDER BY data_preco DESC;

