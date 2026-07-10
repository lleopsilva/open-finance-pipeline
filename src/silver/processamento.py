# src/silver/processamento.py
from pyspark.sql import SparkSession  # manter SparkSession, remover só DataFrame
from pyspark.sql.functions import col, to_date, trim, upper, when
from delta.tables import DeltaTable
import logging

logger = logging.getLogger(__name__)


def processar_silver_serie(
    spark: SparkSession,
    caminho_bronze: str,
    caminho_silver: str,
    nome_serie: str,
    data_ref: str
) -> int:
    """
    Lê do Bronze, limpa e tipa a série temporal.
    MERGE por (data, codigo_serie) garante idempotência.
    """
    logger.info(f"Silver: processando {nome_serie}")

    bronze_df = spark.read.format("delta").load(caminho_bronze) \
        .filter(col("data_ref") == data_ref) \
        .drop("ingestion_timestamp", "source_system", "data_ref")

    silver_df = (
        bronze_df
        .withColumn("data",  to_date(col("data"), "dd/MM/yyyy"))
        .withColumn("valor", spark_round(col("valor").cast("double"), 4))
        .filter(col("data").isNotNull() & col("valor").isNotNull())
        .dropDuplicates(["data", "codigo_serie"])
    )

    if DeltaTable.isDeltaTable(spark, caminho_silver):
        delta = DeltaTable.forPath(spark, caminho_silver)
        delta.alias("alvo").merge(
            silver_df.alias("fonte"),
            "alvo.data = fonte.data AND alvo.codigo_serie = fonte.codigo_serie"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        silver_df.write.format("delta").save(caminho_silver)

    contagem = silver_df.count()
    logger.info(f"Silver: {contagem} registros em {nome_serie}")
    return contagem


def processar_todas_series(
    spark: SparkSession,
    caminho_bronze_base: str,
    caminho_silver_base: str,
    data_ref: str
) -> dict[str, int]:
    from src.extract.bacen_api import SERIES

    contagens = {}
    for nome in SERIES.keys():
        contagem = processar_silver_serie(
            spark,
            f"{caminho_bronze_base}/{nome}/",
            f"{caminho_silver_base}/{nome}/",
            nome,
            data_ref
        )
        contagens[nome] = contagem

    return contagens