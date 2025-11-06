"""
Testes unitários para o módulo analyzer.py

Execute com: python -m pytest tests/test_analyzer.py
Ou: python tests/test_analyzer.py
"""

import sys
import os
from pathlib import Path

# Adiciona diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Agora pode importar módulos do projeto
from src.python.analyzer import analyze_options, validate_schema
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


# ==============================================================================
# FIXTURES - Dados de Teste
# ==============================================================================

@pytest.fixture
def sample_parquet(tmp_path):
    """Cria um arquivo Parquet de teste."""

    df = pd.DataFrame({
        'trade_date': pd.to_datetime(['2024-01-10'] * 10).date,
        'symbol': ['PETRK250', 'VALEF240', 'BBASK245', 'ITUBK230', 'MGLUV235',
                   'B3SAK240', 'WDOK235', 'SUZBL230', 'VIAJ245', 'RADLM240'],
        'underlying': ['PETR4', 'VALE3', 'BBAS3', 'ITUB4', 'MGLU3',
                       'B3SA3', 'WDOH', 'SUZB3', 'VIIA3', 'RADL3'],
        'option_type': ['CALL', 'CALL', 'CALL', 'CALL', 'PUT',
                        'CALL', 'PUT', 'CALL', 'PUT', 'CALL'],
        'qtdneg': [3, 4, 2, 5, 3, 1, 4, 2, 5, 6],
        'quatot': [1000, 2000, 1500, 3000, 1200, 800, 1600, 900, 2500, 3500],
        'voltot': [500000.0, 450000.0, 400000.0, 350000.0, 300000.0,
                   250000.0, 200000.0, 150000.0, 125000.0, 100000.0],
        'preult': [1.52, 2.35, 3.15, 1.85, 0.92,
                   2.10, 1.30, 1.70, 0.55, 0.30],
        'premed': [1.50, 2.30, 3.10, 1.80, 0.90,
                   2.05, 1.25, 1.65, 0.52, 0.29]
    })

    parquet_file = tmp_path / "test_data.parquet"
    df.to_parquet(parquet_file)

    return parquet_file


@pytest.fixture
def filters_default():
    """Filtros padrão para testes."""
    return {
        'max_operations': 5,
        'min_financial_volume': 100000,
        'top_n': 20
    }


# ==============================================================================
# TESTES - Validação de Schema
# ==============================================================================

def test_validate_schema_valid(sample_parquet):
    """Testa validação de schema com dados válidos."""
    df = pq.read_table(sample_parquet).to_pandas()
    # Não deve levantar exceção
    validate_schema(df)


def test_validate_schema_missing_columns():
    """Testa validação de schema com colunas faltando."""
    df = pd.DataFrame({
        'symbol': ['PETRK250'],
        'underlying': ['PETR4']
        # Faltam: option_type, qtdneg, voltot
    })

    with pytest.raises(ValueError, match="Colunas obrigatórias ausentes"):
        validate_schema(df)


# ==============================================================================
# TESTES - Análise e Filtros
# ==============================================================================

def test_analyze_options_basic(sample_parquet, filters_default):
    """Testa análise básica com dados válidos."""

    df_top, stats = analyze_options(sample_parquet, filters_default)

    # Verifica retorno
    assert isinstance(df_top, pd.DataFrame)
    assert isinstance(stats, dict)

    # Verifica estatísticas
    assert 'total_options' in stats
    assert 'after_filters' in stats
    assert 'total_volume' in stats

    assert stats['total_options'] == 10
    assert stats['total_volume'] == pytest.approx(2825000.0, rel=1e-2)


def test_analyze_options_filters_applied(sample_parquet):
    """Testa se filtros são aplicados corretamente."""

    filters = {
        'max_operations': 3,        # Apenas ops com <= 3 negócios
        'min_financial_volume': 200000,  # Volume mínimo 200k
        'top_n': 5
    }

    df_top, stats = analyze_options(sample_parquet, filters)

    # Verifica filtros
    assert (df_top['qtdneg'] <= 3).all(), "Deve filtrar qtdneg <= 3"
    assert (df_top['voltot'] >= 200000).all(), "Deve filtrar voltot >= 200k"
    assert len(df_top) <= 5, "Deve retornar no máximo top_n"


def test_analyze_options_ordering(sample_parquet, filters_default):
    """Testa se resultados estão ordenados por ticket médio decrescente."""

    df_top, _ = analyze_options(sample_parquet, filters_default)

    if len(df_top) > 1:
        # Verifica ordenação descendente (permite empates)
        for i in range(len(df_top) - 1):
            assert df_top.iloc[i]['ticket_medio'] >= df_top.iloc[i+1]['ticket_medio'], \
                "Deve estar ordenado por ticket_medio decrescente"


def test_analyze_options_ticket_medio_calculation(sample_parquet, filters_default):
    """Testa se ticket médio é calculado corretamente."""

    df_top, _ = analyze_options(sample_parquet, filters_default)

    for _, row in df_top.iterrows():
        expected_ticket = row['voltot'] / row['qtdneg']
        assert abs(row['ticket_medio'] - expected_ticket) < 0.01, \
            f"Ticket médio deve ser voltot/qtdneg: {expected_ticket:.2f}"


def test_analyze_options_pct_do_dia(sample_parquet, filters_default):
    """Testa se % do dia é calculada corretamente."""

    df_top, stats = analyze_options(sample_parquet, filters_default)

    total_volume = stats['total_volume']

    for _, row in df_top.iterrows():
        expected_pct = (row['voltot'] / total_volume) * 100
        assert abs(row['pct_do_dia'] - expected_pct) < 0.01, \
            f"% do dia deve ser (voltot/total)*100: {expected_pct:.2f}%"


def test_analyze_options_empty_result(sample_parquet):
    """Testa comportamento quando nenhuma operação passa pelos filtros."""

    filters = {
        'max_operations': 1,
        'min_financial_volume': 10000000,  # 10 milhões (nenhuma passa)
        'top_n': 20
    }

    df_top, stats = analyze_options(sample_parquet, filters)

    assert len(df_top) == 0, "Deve retornar DataFrame vazio"
    assert stats['after_filters'] == 0


def test_analyze_options_premed_fallback(tmp_path):
    """Testa fallback de premed para preult quando premed ausente."""

    # Cria Parquet sem coluna premed
    df = pd.DataFrame({
        'trade_date': pd.to_datetime(['2024-01-10'] * 3).date,
        'symbol': ['PETRK250', 'VALEF240', 'BBASK245'],
        'underlying': ['PETR4', 'VALE3', 'BBAS3'],
        'option_type': ['CALL', 'CALL', 'CALL'],
        'qtdneg': [3, 4, 2],
        'quatot': [1000, 2000, 1500],
        'voltot': [300000.0, 250000.0, 200000.0],
        'preult': [1.52, 2.35, 3.15],
        # SEM premed!
    })

    parquet_file = tmp_path / "test_no_premed.parquet"
    df.to_parquet(parquet_file)

    filters = {'max_operations': 5, 'min_financial_volume': 100000, 'top_n': 10}

    df_top, _ = analyze_options(parquet_file, filters)

    # Deve ter usado preult como fallback
    assert 'premed' in df_top.columns
    # premed deve ser igual a preult (fallback)
    assert (df_top['premed'] == df_top['preult']).all()


# ==============================================================================
# TESTES - Edge Cases
# ==============================================================================

def test_analyze_options_zero_qtdneg(tmp_path):
    """Testa comportamento com qtdneg = 0 (deve ser removido)."""

    df = pd.DataFrame({
        'trade_date': pd.to_datetime(['2024-01-10'] * 2).date,
        'symbol': ['PETRK250', 'VALEF240'],
        'underlying': ['PETR4', 'VALE3'],
        'option_type': ['CALL', 'CALL'],
        'qtdneg': [0, 3],  # Primeiro tem 0 (deve ser removido)
        'quatot': [1000, 2000],
        'voltot': [300000.0, 250000.0],
        'preult': [1.52, 2.35],
        'premed': [1.50, 2.30]
    })

    parquet_file = tmp_path / "test_zero_qtdneg.parquet"
    df.to_parquet(parquet_file)

    filters = {'max_operations': 5, 'min_financial_volume': 100000, 'top_n': 10}

    df_top, stats = analyze_options(parquet_file, filters)

    # Deve ter apenas 1 linha (a com qtdneg=3)
    assert len(df_top) == 1
    assert df_top.iloc[0]['symbol'] == 'VALEF240'


# ==============================================================================
# MAIN (permite executar como script)
# ==============================================================================

if __name__ == '__main__':
    # Executa testes se pytest disponível
    try:
        import pytest
        pytest.main([__file__, '-v'])
    except ImportError:
        print("pytest não instalado. Instale com: pip install pytest")
        print("\nExecutando testes manualmente (básico)...")

        # Testes manuais simples
        import tempfile

        print("\n[TEST] Criando dados de teste...")
        tmp = tempfile.mkdtemp()
        tmp_path = Path(tmp)

        # Cria Parquet de teste
        df = pd.DataFrame({
            'trade_date': pd.to_datetime(['2024-01-10'] * 5).date,
            'symbol': ['PETRK250', 'VALEF240', 'BBASK245', 'ITUBK230', 'MGLUV235'],
            'underlying': ['PETR4', 'VALE3', 'BBAS3', 'ITUB4', 'MGLU3'],
            'option_type': ['CALL', 'CALL', 'CALL', 'CALL', 'PUT'],
            'qtdneg': [3, 4, 2, 5, 3],
            'quatot': [1000, 2000, 1500, 3000, 1200],
            'voltot': [300000.0, 250000.0, 200000.0, 150000.0, 100000.0],
            'preult': [1.52, 2.35, 3.15, 1.85, 0.92],
            'premed': [1.50, 2.30, 3.10, 1.80, 0.90]
        })

        parquet_file = tmp_path / "manual_test.parquet"
        df.to_parquet(parquet_file)

        print(f"✓ Parquet criado: {parquet_file}")

        print("\n[TEST] Testando análise...")
        filters = {'max_operations': 5, 'min_financial_volume': 100000, 'top_n': 3}
        df_top, stats = analyze_options(parquet_file, filters)

        print(f"✓ Análise concluída")
        print(f"  Total opções: {stats['total_options']}")
        print(f"  Após filtros: {stats['after_filters']}")
        print(f"  Top retornados: {len(df_top)}")

        assert stats['total_options'] == 5
        assert len(df_top) <= 3

        print("\n✅ TESTES MANUAIS PASSARAM!")

        # Cleanup
        import shutil
        shutil.rmtree(tmp)
