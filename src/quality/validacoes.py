# src/quality/validacoes.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import logging

logger = logging.getLogger(__name__)


def validar_dataframe(df: DataFrame, nome: str) -> dict:
    """
    Executa validações de qualidade no DataFrame.
    Retorna relatório com pass/fail por check.
    """
    total = df.count()
    resultados = {}

    # ── Check 1: volume mínimo ────────────────────────────────
    resultados["volume_minimo"] = {
        "passou": total > 0,
        "detalhe": f"{total} registros (esperado: > 0)"
    }

    # ── Check 2: nulos em colunas obrigatórias ────────────────
    for coluna in ["data", "valor"]:
        if coluna in df.columns:
            qtd_nulos = df.filter(col(coluna).isNull()).count()
            resultados[f"nulos_{coluna}"] = {
                "passou": qtd_nulos == 0,
                "detalhe": f"{qtd_nulos} nulos em {coluna}"
            }

    # ── Check 3: valores negativos ────────────────────────────
    if "valor" in df.columns:
        qtd_negativos = df.filter(col("valor") < 0).count()
        resultados["valor_positivo"] = {
            "passou": qtd_negativos == 0,
            "detalhe": f"{qtd_negativos} registros com valor negativo"
        }

    # ── Check 4: duplicatas por chave natural ─────────────────
    if "data" in df.columns and "codigo_serie" in df.columns:
        total_dedup = df.dropDuplicates(["data", "codigo_serie"]).count()
        qtd_duplicados = total - total_dedup
        resultados["sem_duplicatas"] = {
            "passou": qtd_duplicados == 0,
            "detalhe": f"{qtd_duplicados} duplicatas por (data, codigo_serie)"
        }

    # ── Relatório ─────────────────────────────────────────────
    falhas = [k for k, v in resultados.items() if not v["passou"]]
    passou_tudo = len(falhas) == 0

    print(f"\n{'='*50}")
    print(f"QUALIDADE — {nome}")
    print(f"{'='*50}")
    for check, resultado in resultados.items():
        status = "✅ PASS" if resultado["passou"] else "❌ FAIL"
        print(f"{status} | {check:<25} | {resultado['detalhe']}")
    print(f"{'='*50}")
    print(f"Resultado: {'APROVADO' if passou_tudo else f'REPROVADO ({len(falhas)} falhas)'}")
    print(f"{'='*50}\n")

    return {"passou": passou_tudo, "falhas": falhas, "resultados": resultados}


def gate_qualidade(df: DataFrame, nome: str, threshold_falhas: int = 0):
    """
    Gate de qualidade — interrompe o pipeline se falhas acima do threshold.
    """
    relatorio = validar_dataframe(df, nome)

    if len(relatorio["falhas"]) > threshold_falhas:
        raise ValueError(
            f"Qualidade insuficiente em {nome}: "
            f"{relatorio['falhas']} — pipeline interrompido"
        )

    logger.info(f"Gate de qualidade aprovado: {nome}")
    return df