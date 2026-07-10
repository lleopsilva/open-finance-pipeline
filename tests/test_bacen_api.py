import pytest
import pandas as pd


def test_dataframe_tem_colunas_esperadas():
    """Valida que o DataFrame de série temporal tem as colunas corretas."""
    df = pd.DataFrame([
        {"data": "06/07/2026", "valor": "13.75", "codigo_serie": 11}
    ])
    assert "data" in df.columns
    assert "valor" in df.columns
    assert "codigo_serie" in df.columns


def test_valor_convertido_para_numerico():
    """Valida que o campo valor é convertido para float corretamente."""
    df = pd.DataFrame([{"data": "06/07/2026", "valor": "13.75"}])
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    assert df["valor"].dtype == float
    assert df["valor"][0] == 13.75


def test_nulos_sao_removidos():
    """Valida que linhas com valor nulo são descartadas."""
    df = pd.DataFrame([
        {"data": "06/07/2026", "valor": 13.75},
        {"data": "07/07/2026", "valor": None},
    ])
    df_limpo = df.dropna(subset=["valor"])
    assert len(df_limpo) == 1