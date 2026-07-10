# src/extract/bacen_api.py

import requests
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
import logging

logger = logging.getLogger(__name__)

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"

SERIES = {
    "selic_diaria":    11,    # Taxa Selic diária
    "ipca_mensal":     433,   # IPCA mensal
    "cambio_usd_brl":  1,     # Taxa de câmbio USD/BRL
}


def extrair_serie_bacen(codigo: int, data_inicio: str, data_fim: str) -> pd.DataFrame:
    """
    Extrai série temporal do Banco Central via API REST pública.
    Retorna DataFrame pandas com colunas: data, valor.
    """
    url = BASE_URL.format(codigo=codigo)
    params = {
        "formato":     "json",
        "dataInicial": data_inicio,  # formato: DD/MM/YYYY
        "dataFinal":   data_fim,
    }

    logger.info(f"Extraindo série {codigo} de {data_inicio} a {data_fim}")
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    dados = response.json()
    df = pd.DataFrame(dados)
    df.columns = ["data", "valor"]
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df["codigo_serie"] = codigo

    logger.info(f"Série {codigo}: {len(df)} registros extraídos")
    return df


def extrair_todas_series(
    spark: SparkSession,
    data_inicio: str,
    data_fim: str
) -> dict[str, DataFrame]:
    """
    Extrai todas as séries configuradas e retorna
    um dicionário de DataFrames Spark.
    data_inicio e data_fim no formato DD/MM/YYYY.
    """
    resultados = {}

    for nome, codigo in SERIES.items():
        try:
            pdf = extrair_serie_bacen(codigo, data_inicio, data_fim)
            sdf = spark.createDataFrame(pdf)
            from pyspark.sql.functions import lit
            sdf = sdf.withColumn("nome_serie", lit(nome))
            resultados[nome] = sdf
            logger.info(f"Série {nome}: convertida para Spark DataFrame")
        except Exception as e:
            logger.error(f"Erro ao extrair série {nome} (código {codigo}): {e}")
            raise

    return resultados


if __name__ == "__main__":
    # Teste rápido de extração
    df = extrair_serie_bacen(11, "01/01/2026", "30/06/2026")
    print(df.head(10))
    print(f"Total: {len(df)} registros")