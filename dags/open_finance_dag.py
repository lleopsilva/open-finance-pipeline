# dags/open_finance_dag.py

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

CAMINHO_BASE = "s3://open-finance-pipeline"
# local para desenvolvimento:
# CAMINHO_BASE = "/tmp/open-finance-pipeline"


def alertar_falha(context):
    logger.error(
        f"FALHA | {context['dag'].dag_id} "
        f"| {context['task'].task_id} "
        f"| {context['execution_date']}"
    )


default_args = {
    "owner": "engenharia_dados",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "on_failure_callback": alertar_falha,
}


@dag(
    dag_id="open_finance_pipeline",
    default_args=default_args,
    schedule="0 6 * * 1",          # toda segunda-feira às 06:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["open-finance", "bacen", "medallion"],
    doc_md="""
    ## Open Finance Pipeline

    Pipeline de indicadores financeiros públicos do Banco Central do Brasil.

    ### Séries extraídas
    - **Selic diária** (código 11)
    - **IPCA mensal** (código 433)
    - **Câmbio USD/BRL** (código 1)

    ### Arquitetura
    API BACEN → Bronze (Delta) → Silver (Delta) → Gold (Delta)

    ### Schedule
    Semanal às segundas 06:00 UTC

    ### Fonte dos dados
    https://api.bcb.gov.br
    """
)
def open_finance_pipeline():

    inicio = EmptyOperator(task_id="inicio")
    fim = EmptyOperator(task_id="fim")

    # ── Extração ──────────────────────────────────────────────
    with TaskGroup("extracao") as extracao_group:

        @task(task_id="extrair_series_bacen")
        def extrair_series(data_ref: str) -> dict:
            from src.extract.bacen_api import extrair_todas_series, SERIES
            from pyspark.sql import SparkSession

            # Converter data_ref para formato BACEN (DD/MM/YYYY)
            from datetime import datetime, timedelta
            dt = datetime.strptime(data_ref, "%Y-%m-%d")
            dt_inicio = dt - timedelta(days=7)
            data_inicio_bacen = dt_inicio.strftime("%d/%m/%Y")
            data_fim_bacen    = dt.strftime("%d/%m/%Y")

            spark = SparkSession.builder.appName("open-finance-extract").getOrCreate()
            dfs = extrair_todas_series(spark, data_inicio_bacen, data_fim_bacen)

            contagens = {nome: df.count() for nome, df in dfs.items()}
            logger.info(f"Extração concluída: {contagens}")
            return {"data_ref": data_ref, "contagens_extracao": contagens}

        resultado_extracao = extrair_series(data_ref="{{ ds }}")

    # ── Bronze ────────────────────────────────────────────────
    with TaskGroup("camada_bronze") as bronze_group:

        @task(task_id="ingerir_bronze")
        def ingerir_bronze(resultado: dict) -> dict:
            from src.bronze.ingestao import ingerir_bronze as _ingerir
            from src.extract.bacen_api import extrair_todas_series
            from pyspark.sql import SparkSession
            from datetime import datetime, timedelta

            data_ref = resultado["data_ref"]
            dt = datetime.strptime(data_ref, "%Y-%m-%d")
            dt_inicio = dt - timedelta(days=7)

            spark = SparkSession.builder.appName("open-finance-bronze").getOrCreate()
            dfs = extrair_todas_series(
                spark,
                dt_inicio.strftime("%d/%m/%Y"),
                dt.strftime("%d/%m/%Y")
            )
            contagens = _ingerir(spark, dfs, f"{CAMINHO_BASE}/bronze", data_ref)
            return {**resultado, "contagens_bronze": contagens}

        bronze_resultado = ingerir_bronze(resultado_extracao)

    # ── Silver ────────────────────────────────────────────────
    with TaskGroup("camada_silver") as silver_group:

        @task(task_id="processar_silver")
        def processar_silver(resultado: dict) -> dict:
            from src.silver.processamento import processar_todas_series
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.appName("open-finance-silver").getOrCreate()
            contagens = processar_todas_series(
                spark,
                f"{CAMINHO_BASE}/bronze",
                f"{CAMINHO_BASE}/silver",
                resultado["data_ref"]
            )
            return {**resultado, "contagens_silver": contagens}

        silver_resultado = processar_silver(bronze_resultado)

    # ── Gold ──────────────────────────────────────────────────
    with TaskGroup("camada_gold") as gold_group:

        @task(task_id="processar_gold")
        def processar_gold(resultado: dict) -> dict:
            from src.gold.agregacao import processar_gold_indicadores
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.appName("open-finance-gold").getOrCreate()
            contagem = processar_gold_indicadores(
                spark,
                f"{CAMINHO_BASE}/silver",
                f"{CAMINHO_BASE}/gold/indicadores_mensais"
            )
            return {**resultado, "registros_gold": contagem}

        gold_resultado = processar_gold(silver_resultado)

    # ── Conclusão ─────────────────────────────────────────────
    @task(task_id="notificar_conclusao")
    def notificar(resultado: dict):
        logger.info("=" * 50)
        logger.info("OPEN FINANCE PIPELINE — CONCLUÍDO")
        logger.info("=" * 50)
        for chave, valor in resultado.items():
            logger.info(f"{chave}: {valor}")
        logger.info("=" * 50)

    # ── Dependências ──────────────────────────────────────────
    (
        inicio
        >> extracao_group
        >> bronze_group
        >> silver_group
        >> gold_group
        >> notificar(gold_resultado)
        >> fim
    )


dag_instance = open_finance_pipeline()