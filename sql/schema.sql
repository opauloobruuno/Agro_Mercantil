-- Criação do Schema para o projeto de scraping da Conab/AgroMercantil
-- Propósito: Organizar tabelas para armazenamento normalizado de preços de commodities agrícolas.
-- Executar em PostgreSQL.
-- Observação: este script assume um banco/ambiente inicial; se você rodar em um banco já criado,
-- pode ser necessário ajustar migrações/ALTERs para aplicar novas constraints.

-- Cria o schema se não existir
CREATE SCHEMA IF NOT EXISTS agromercado;

-- Define o schema como padrão
SET search_path TO agromercado, public;

-- Tabela de commodities (normalização para evitar duplicatas)
CREATE TABLE IF NOT EXISTS commodities (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(100) UNIQUE NOT NULL, -- Ex: 'Soja', 'Milho'
    unidade_medida VARCHAR(50) DEFAULT 'saca de 60kg', -- Unidade de medida comum
    categoria VARCHAR(50) DEFAULT 'agrícola', -- Para expansão futura
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de regiões (normalização geográfica)
CREATE TABLE IF NOT EXISTS regioes (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(100) UNIQUE NOT NULL, -- Ex: 'Centro-Oeste', 'Sul'
    uf VARCHAR(2), -- Opcional: sigla do estado para granularidade
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de cargas ETL (rastreamento de importações)
CREATE TABLE IF NOT EXISTS cargas_dados (
    id SERIAL PRIMARY KEY,
    timestamp_carga TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    arquivo_fonte VARCHAR(255) UNIQUE, -- Nome do CSV/JSON bruto
    registros_processados INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'PENDENTE', -- 'SUCESSO', 'ERRO', 'PARCIAL'
    mensagem_erro TEXT
);

-- Tabela principal de preços (fato central do data warehouse)
CREATE TABLE IF NOT EXISTS precos (
    id SERIAL PRIMARY KEY,
    commodity_id INTEGER NOT NULL REFERENCES commodities(id) ON DELETE CASCADE,
    regiao_id INTEGER NOT NULL REFERENCES regioes(id) ON DELETE CASCADE,
    data_preco DATE NOT NULL,
    valor_preco NUMERIC(10, 2) NOT NULL CHECK (valor_preco >= 0),
    moeda VARCHAR(3) DEFAULT 'BRL',
    url_fonte TEXT,
    carga_id INTEGER REFERENCES cargas_dados(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (commodity_id, regiao_id, data_preco) -- Evita duplicatas diárias
);

-- Índices para performance em consultas analíticas comuns
CREATE INDEX IF NOT EXISTS idx_precos_data ON precos (data_preco DESC);
CREATE INDEX IF NOT EXISTS idx_precos_commodity ON precos (commodity_id);
CREATE INDEX IF NOT EXISTS idx_precos_regiao ON precos (regiao_id);
CREATE INDEX IF NOT EXISTS idx_precos_commodity_regiao_data ON precos (commodity_id, regiao_id, data_preco);
CREATE INDEX IF NOT EXISTS idx_cargas_timestamp ON cargas_dados (timestamp_carga DESC);

-- Dados de exemplo fictícios (coerentes com commodities agrícolas)

-- Inserir commodities de exemplo
INSERT INTO commodities (nome, unidade_medida) VALUES
('Soja', 'saca de 60kg'),
('Milho', 'saca de 60kg'),
('Café Arábica', 'saca de 60kg'),
('Algodão', 'arroba'),
('Trigo', 'tonelada')
ON CONFLICT (nome) DO NOTHING;

-- Inserir regiões de exemplo
INSERT INTO regioes (nome, uf) VALUES
('Centro-Oeste', NULL),
('Sudeste', NULL),
('Sul', NULL),
('Nordeste', NULL)
ON CONFLICT (nome) DO NOTHING;

-- Inserir carga de exemplo (se a carga já existir, não duplica)
INSERT INTO cargas_dados (arquivo_fonte, registros_processados, status) VALUES
('conab_raw_202603.csv', 50, 'SUCESSO')
ON CONFLICT (arquivo_fonte) DO NOTHING;

-- Inserir preços de exemplo (simulando dados semanais de março/2026)
-- Usamos um SELECT para associar o carga_id correto (caso o ID não seja 1).
INSERT INTO precos (commodity_id, regiao_id, data_preco, valor_preco, url_fonte, carga_id) VALUES
-- Soja no Centro-Oeste
((SELECT id FROM commodities WHERE nome = 'Soja'), (SELECT id FROM regioes WHERE nome = 'Centro-Oeste'), '2026-03-01', 150.00, 'https://conab.gov.br/soja', (SELECT id FROM cargas_dados WHERE arquivo_fonte = 'conab_raw_202603.csv' ORDER BY id DESC LIMIT 1)),
((SELECT id FROM commodities WHERE nome = 'Soja'), (SELECT id FROM regioes WHERE nome = 'Centro-Oeste'), '2026-03-08', 152.50, 'https://conab.gov.br/soja', (SELECT id FROM cargas_dados WHERE arquivo_fonte = 'conab_raw_202603.csv' ORDER BY id DESC LIMIT 1)),
((SELECT id FROM commodities WHERE nome = 'Soja'), (SELECT id FROM regioes WHERE nome = 'Centro-Oeste'), '2026-03-15', 155.00, 'https://conab.gov.br/soja', (SELECT id FROM cargas_dados WHERE arquivo_fonte = 'conab_raw_202603.csv' ORDER BY id DESC LIMIT 1)),
((SELECT id FROM commodities WHERE nome = 'Soja'), (SELECT id FROM regioes WHERE nome = 'Centro-Oeste'), '2026-03-22', 158.00, 'https://conab.gov.br/soja', (SELECT id FROM cargas_dados WHERE arquivo_fonte = 'conab_raw_202603.csv' ORDER BY id DESC LIMIT 1)),
-- Milho no Sul
((SELECT id FROM commodities WHERE nome = 'Milho'), (SELECT id FROM regioes WHERE nome = 'Sul'), '2026-03-01', 80.50, 'https://conab.gov.br/milho', (SELECT id FROM cargas_dados WHERE arquivo_fonte = 'conab_raw_202603.csv' ORDER BY id DESC LIMIT 1)),
((SELECT id FROM commodities WHERE nome = 'Milho'), (SELECT id FROM regioes WHERE nome = 'Sul'), '2026-03-08', 81.20, 'https://conab.gov.br/milho', (SELECT id FROM cargas_dados WHERE arquivo_fonte = 'conab_raw_202603.csv' ORDER BY id DESC LIMIT 1)),
((SELECT id FROM commodities WHERE nome = 'Milho'), (SELECT id FROM regioes WHERE nome = 'Sul'), '2026-03-15', 82.00, 'https://conab.gov.br/milho', (SELECT id FROM cargas_dados WHERE arquivo_fonte = 'conab_raw_202603.csv' ORDER BY id DESC LIMIT 1)),
((SELECT id FROM commodities WHERE nome = 'Milho'), (SELECT id FROM regioes WHERE nome = 'Sul'), '2026-03-22', 83.50, 'https://conab.gov.br/milho', (SELECT id FROM cargas_dados WHERE arquivo_fonte = 'conab_raw_202603.csv' ORDER BY id DESC LIMIT 1)),
-- Café no Sudeste
((SELECT id FROM commodities WHERE nome = 'Café Arábica'), (SELECT id FROM regioes WHERE nome = 'Sudeste'), '2026-03-01', 950.00, 'https://conab.gov.br/cafe', (SELECT id FROM cargas_dados WHERE arquivo_fonte = 'conab_raw_202603.csv' ORDER BY id DESC LIMIT 1)),
((SELECT id FROM commodities WHERE nome = 'Café Arábica'), (SELECT id FROM regioes WHERE nome = 'Sudeste'), '2026-03-08', 965.00, 'https://conab.gov.br/cafe', (SELECT id FROM cargas_dados WHERE arquivo_fonte = 'conab_raw_202603.csv' ORDER BY id DESC LIMIT 1)),
((SELECT id FROM commodities WHERE nome = 'Café Arábica'), (SELECT id FROM regioes WHERE nome = 'Sudeste'), '2026-03-15', 970.00, 'https://conab.gov.br/cafe', (SELECT id FROM cargas_dados WHERE arquivo_fonte = 'conab_raw_202603.csv' ORDER BY id DESC LIMIT 1)),
((SELECT id FROM commodities WHERE nome = 'Café Arábica'), (SELECT id FROM regioes WHERE nome = 'Sudeste'), '2026-03-22', 980.00, 'https://conab.gov.br/cafe', (SELECT id FROM cargas_dados WHERE arquivo_fonte = 'conab_raw_202603.csv' ORDER BY id DESC LIMIT 1)),
-- Outras commodities para diversidade
((SELECT id FROM commodities WHERE nome = 'Algodão'), (SELECT id FROM regioes WHERE nome = 'Nordeste'), '2026-03-15', 120.00, 'https://conab.gov.br/algodao', (SELECT id FROM cargas_dados WHERE arquivo_fonte = 'conab_raw_202603.csv' ORDER BY id DESC LIMIT 1)),
((SELECT id FROM commodities WHERE nome = 'Trigo'), (SELECT id FROM regioes WHERE nome = 'Sul'), '2026-03-22', 1500.00, 'https://conab.gov.br/trigo', (SELECT id FROM cargas_dados WHERE arquivo_fonte = 'conab_raw_202603.csv' ORDER BY id DESC LIMIT 1))
ON CONFLICT (commodity_id, regiao_id, data_preco) DO NOTHING;

-- Verificação final: contagem de registros inseridos
SELECT 'Commodities inseridas' AS tipo, COUNT(*) AS total FROM commodities
UNION ALL
SELECT 'Regiões inseridas' AS tipo, COUNT(*) AS total FROM regioes
UNION ALL
SELECT 'Preços inseridos' AS tipo, COUNT(*) AS total FROM precos;

