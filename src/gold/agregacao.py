# src/gold/agregacao.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min, max, stddev,
    year, month, round as spark_round
)
from src.quality.validacoes import gate_qualidade
import logging

logger = logging.getLogger(__name__)


def processar_gold_indicadores(
    spark: SparkSession,
    caminho_silver_base: str,
    caminho_gold: str
) -> int:
    """
    Consolida todas as séries do Silver num único DataFrame Gold
    com estatísticas mensais por série.
    Idempotência via overwrite completo.
    Gate de qualidade antes de gravar.
    """
    logger.info("Gold: consolidando indicadores financeiros")

    from src.extract.bacen_api import SERIES
    from functools import reduce
    from pyspark.sql import DataFrame

    dfs = []
    for nome in SERIES.keys():
        caminho = f"{caminho_silver_base}/{nome}/"
        df = spark.read.format("delta").load(caminho)
        dfs.append(df)

    unificado_df = reduce(DataFrame.union, dfs)

    gold_df = (
        unificado_df
        .withColumn("ano", year(col("data")))
        .withColumn("mes", month(col("data")))
        .groupBy("nome_serie", "ano", "mes")
        .agg(
            spark_round(avg("valor"),    4).alias("media_mensal"),
            spark_round(min("valor"),    4).alias("minimo_mensal"),
            spark_round(max("valor"),    4).alias("maximo_mensal"),
            spark_round(stddev("valor"), 4).alias("desvio_padrao"),
        )
        .orderBy("nome_serie", "ano", "mes")
    )

    # ── Gate de qualidade antes de gravar ─────────────────────
    gate_qualidade(gold_df, "gold_indicadores_mensais")

    gold_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(caminho_gold)

    contagem = gold_df.count()
    logger.info(f"Gold: {contagem} registros consolidados")
    return contagem