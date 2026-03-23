from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "database": "agromercado_db",
    "user": "postgres",
    "password": "sua_senha",
    "port": 5432,
}

OUTPUT_DIR = Path("data") / "curated" / "eda"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def conectar_db():
    return psycopg2.connect(**DB_CONFIG)


def carregar_dados() -> pd.DataFrame:
    query = """
    SELECT
        c.nome AS commodity,
        r.nome AS regiao,
        p.data_preco,
        p.valor_preco
    FROM precos p
    JOIN commodities c ON p.commodity_id = c.id
    JOIN regioes r ON p.regiao_id = r.id
    WHERE p.valor_preco IS NOT NULL;
    """
    with conectar_db() as conn:
        df = pd.read_sql_query(query, conn)
    df["data_preco"] = pd.to_datetime(df["data_preco"])
    return df


def salvar_estatisticas(df: pd.DataFrame):
    stats = df.groupby("commodity")["valor_preco"].agg(
        media="mean",
        mediana="median",
        desvio_padrao="std",
        minimo="min",
        maximo="max",
        observacoes="count",
    ).reset_index()
    stats = stats.sort_values("media", ascending=False)
    stats.to_csv(OUTPUT_DIR / "estatisticas_descritivas.csv", index=False, encoding="utf-8-sig")


def detectar_outliers_iqr(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for commodity, group in df.groupby("commodity"):
        q1 = group["valor_preco"].quantile(0.25)
        q3 = group["valor_preco"].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        outliers = group[(group["valor_preco"] < lower) | (group["valor_preco"] > upper)].copy()
        outliers["limite_inferior"] = lower
        outliers["limite_superior"] = upper
        rows.append(outliers)
    result = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    result.to_csv(OUTPUT_DIR / "outliers_iqr.csv", index=False, encoding="utf-8-sig")
    return result


def gerar_graficos(df: pd.DataFrame):
    plt.figure(figsize=(10, 5))
    df.boxplot(column="valor_preco", by="commodity", rot=45)
    plt.title("Boxplot de Preços por Commodity")
    plt.suptitle("")
    plt.xlabel("Commodity")
    plt.ylabel("Preço")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "boxplot_precos_por_commodity.png", dpi=150)
    plt.close()

    plt.figure(figsize=(9, 5))
    plt.hist(df["valor_preco"], bins=30)
    plt.title("Histograma de Preços")
    plt.xlabel("Preço")
    plt.ylabel("Frequência")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "histograma_precos.png", dpi=150)
    plt.close()

    plt.figure(figsize=(10, 5))
    plt.scatter(df["data_preco"], df["valor_preco"], s=12, alpha=0.5)
    plt.title("Scatter Data x Preço")
    plt.xlabel("Data")
    plt.ylabel("Preço")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "scatter_data_preco.png", dpi=150)
    plt.close()


def main():
    df = carregar_dados()
    salvar_estatisticas(df)
    detectar_outliers_iqr(df)
    gerar_graficos(df)
    print(f"EDA concluída. Arquivos em: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
