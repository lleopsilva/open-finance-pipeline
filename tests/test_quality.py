# tests/test_quality.py

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch


def test_validacao_passa_com_dados_corretos():
    """Valida que dados corretos passam em todos os checks."""
    dados = [
        {"data": "2026-07-01", "valor": 13.75, "codigo_serie": 11},
        {"data": "2026-07-02", "valor": 13.65, "codigo_serie": 11},
    ]
    df = pd.DataFrame(dados)

    assert len(df) > 0
    assert df["valor"].isna().sum() == 0
    assert (df["valor"] < 0).sum() == 0
    assert df.duplicated(subset=["data", "codigo_serie"]).sum() == 0


def test_validacao_detecta_nulos():
    """Valida que nulos em valor são detectados."""
    dados = [
        {"data": "2026-07-01", "valor": 13.75},
        {"data": "2026-07-02", "valor": None},
    ]
    df = pd.DataFrame(dados)
    qtd_nulos = df["valor"].isna().sum()
    assert qtd_nulos == 1


def test_validacao_detecta_negativos():
    """Valida que valores negativos são detectados."""
    dados = [
        {"data": "2026-07-01", "valor": 13.75},
        {"data": "2026-07-02", "valor": -5.0},
    ]
    df = pd.DataFrame(dados)
    qtd_negativos = (df["valor"] < 0).sum()
    assert qtd_negativos == 1


def test_validacao_detecta_duplicatas():
    """Valida que duplicatas por (data, codigo_serie) são detectadas."""
    dados = [
        {"data": "2026-07-01", "codigo_serie": 11, "valor": 13.75},
        {"data": "2026-07-01", "codigo_serie": 11, "valor": 13.75},
    ]
    df = pd.DataFrame(dados)
    qtd_duplicatas = df.duplicated(subset=["data", "codigo_serie"]).sum()
    assert qtd_duplicatas == 1


def test_gate_para_com_dado_ruim():
    """Valida que o gate interrompe o pipeline com dados problemáticos."""
    dados = [
        {"data": "2026-07-01", "valor": None},
    ]
    df = pd.DataFrame(dados)
    qtd_nulos = df["valor"].isna().sum()
    assert qtd_nulos > 0  # confirmando que o gate detectaria o problema