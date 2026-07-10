# src/bronze/ingestao.py

from pyspark.sql.functions import current_timestamp, lit
from src.quality.validacoes import gate_qualidade
import logging

logger = logging.getLogger(__name__)


def ingerir_bronze(
    spark: SparkSession,
    dataframes: dict,
    caminho_base: str,
    data_ref: str
) -> dict:
    """
    Grava todas as séries na camada Bronze.
    Cada série vira uma tabela Delta separada.
    Idempotência via replaceWhere por data_ref.
    Gate de qualidade antes de gravar.
    """
    contagens = {}

    for nome, df in dataframes.items():
        caminho = f"{caminho_base}/{nome}/"
        logger.info(f"Bronze: gravando {nome} em {caminho}")

        bronze_df = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_system", lit("bacen_api")) \
            .withColumn("data_ref", lit(data_ref))

        # ── Gate de qualidade antes de gravar ─────────────────
        gate_qualidade(bronze_df, f"bronze_{nome}")

        bronze_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"data_ref = '{data_ref}'") \
            .save(caminho)

        contagem = bronze_df.count()
        contagens[nome] = contagem
        logger.info(f"Bronze: {contagem} registros gravados em {nome}")

    return contagens