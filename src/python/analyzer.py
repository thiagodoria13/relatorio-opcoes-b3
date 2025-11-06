"""
Analise de opcoes com validacao de schema e tratamento robusto.

Este modulo:
- Carrega dados do Parquet gerado pelo R
- Valida schema e presenca de colunas obrigatorias
- Aplica filtros configuraveis
- Calcula metricas (ticket medio, % do dia)
- Retorna top N operacoes com maior ticket medio
"""

import pandas as pd
import pyarrow.parquet as pq
import numpy as np
import logging
from pathlib import Path
from typing import Tuple, Dict

logger = logging.getLogger(__name__)


# ==============================================================================
# SCHEMA ESPERADO DO PARQUET
# ==============================================================================

EXPECTED_SCHEMA = {
    'trade_date': ['date32', 'date64'],  # Aceita ambos
    'symbol': ['string', 'large_string'],
    'underlying': ['string', 'large_string'],
    'option_type': ['string', 'large_string'],
    'maturity_date': ['date32', 'date64'],
    'strike_price': ['double', 'float'],
    'qtdneg': ['int32', 'int64'],
    'quatot': ['int64'],
    'voltot': ['double', 'float'],
    'preult': ['double', 'float'],
    'premed': ['double', 'float']  # Pode estar ausente em alguns casos
}

REQUIRED_COLUMNS = ['symbol', 'underlying', 'option_type', 'qtdneg', 'voltot']


def validate_schema(df: pd.DataFrame) -> None:
    """
    Valida presenca de colunas chave e tipos basicos.

    Args:
        df: DataFrame carregado do Parquet

    Raises:
        ValueError: Se colunas obrigatorias estiverem ausentes

    Notes:
        - Valida apenas colunas REQUIRED_COLUMNS (nao falha por colunas opcionais)
        - Loga warning se tipos nao esperados
    """
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]

    if missing:
        raise ValueError(
            f"Colunas obrigatorias ausentes no Parquet: {missing}\n"
            f"Colunas disponiveis: {list(df.columns)}"
        )

    logger.info(f" Schema validado: {len(df.columns)} colunas presentes")

    # Log das colunas disponiveis
    logger.debug(f"Colunas: {', '.join(df.columns)}")

    # Valida tipos basicos (apenas warning, nao falha)
    for col in REQUIRED_COLUMNS:
        dtype = str(df[col].dtype)
        logger.debug(f"  {col}: {dtype}")

        # Aviso se tipo inesperado
        if col == 'qtdneg' and not dtype.startswith('int'):
            logger.warning(f"Tipo inesperado para {col}: {dtype} (esperado int)")
        elif col == 'voltot' and not dtype.startswith('float'):
            logger.warning(f"Tipo inesperado para {col}: {dtype} (esperado float)")


def analyze_options(parquet_path: Path, filters: Dict) -> Tuple[pd.DataFrame, Dict]:
    """
    Aplica filtros e retorna top N operacoes + estatisticas.

    Args:
        parquet_path: Caminho do arquivo Parquet
        filters: Dicionario com filtros:
            - max_operations: int (numero maximo de operacoes)
            - min_financial_volume: float (volume minimo em R$)
            - top_n: int (quantas operacoes retornar)

    Returns:
        Tupla (DataFrame com top N, dict com estatisticas)

    DataFrame retornado contem colunas:
        - symbol: Codigo da opcao
        - underlying: Ativo-objeto
        - option_type: "CALL" ou "PUT"
        - maturity_date: Data de vencimento da opcao
        - premed: Preco medio (ou preult se premed ausente)
        - preult: Preco de fechamento
        - voltot: Volume total negociado (R$)
        - qtdneg: Numero de operacoes
        - quatot: Quantidade total de contratos negociados
        - strike_price: Strike (preco de exercicio)
        - ticket_medio: Volume medio por operacao (R$)
        - pct_do_dia: % do volume total do dia

    Estatisticas retornadas:
        - total_options: Total de opcoes no arquivo
        - total_volume: Volume total do dia (todas as opcoes)
        - after_filters: Numero de opcoes apos filtros
        - top_n_volume: Volume total das top N

    Raises:
        FileNotFoundError: Se Parquet nao existir
        ValueError: Se schema invalido
    """
    logger.info(f"=" * 70)
    logger.info(f"Carregando dados: {parquet_path}")
    logger.info(f"=" * 70)

    # Verifica existencia
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Arquivo Parquet nao encontrado: {parquet_path}\n"
            f"Execute o script R primeiro para fazer o download dos dados."
        )

    # Le Parquet
    try:
        df = pq.read_table(parquet_path).to_pandas()
    except Exception as e:
        raise ValueError(f"Erro ao ler Parquet: {e}") from e

    logger.info(f"Dados carregados: {len(df):,} linhas, {len(df.columns)} colunas")

    # Valida schema
    validate_schema(df)

    # Estatisticas iniciais
    stats = {
        'total_options': len(df),
        'total_volume': float(df['voltot'].sum())  # Garante que e float, nao numpy
    }

    # Normaliza maturidade (se disponivel)
    if 'maturity_date' in df.columns:
        df['maturity_date'] = pd.to_datetime(df['maturity_date']).dt.date
    if 'strike_price' in df.columns:
        df['strike_price'] = pd.to_numeric(df['strike_price'], errors='coerce')

    logger.info(f"Volume total do dia: R$ {stats['total_volume']:,.2f}")

    # ==============================================================================
    # FALLBACK: Se premed ausente ou vazio, usa preult
    # ==============================================================================

    if 'premed' not in df.columns:
        logger.warning("Coluna 'premed' ausente. Usando 'preult' como fallback.")
        df['premed'] = df['preult'].copy()
    else:
        # Conta quantos NaN em premed
        premed_na_count = df['premed'].isna().sum()
        if premed_na_count > 0:
            logger.warning(
                f"premed contem {premed_na_count} valores NaN "
                f"({premed_na_count/len(df)*100:.1f}%). Preenchendo com preult."
            )
            df['premed'] = df['premed'].fillna(df['preult'])

        # Se TODOS forem NaN, copia preult
        if df['premed'].isna().all():
            logger.warning("TODOS os valores de premed sao NaN. Usando preult.")
            df['premed'] = df['preult'].copy()

    # ==============================================================================
    # LIMPEZA: Remove linhas com qtdneg <= 0 (evita divisao por zero)
    # ==============================================================================

    before_clean = len(df)
    df = df[df['qtdneg'] > 0].copy()
    after_clean = len(df)

    if before_clean > after_clean:
        removed = before_clean - after_clean
        logger.warning(
            f"Removidas {removed} linhas com qtdneg <= 0 "
            f"({removed/before_clean*100:.1f}%)"
        )

    if len(df) == 0:
        logger.error("Nenhum dado restante apos limpeza!")
        return pd.DataFrame(), {**stats, 'after_filters': 0, 'top_n_volume': 0}

    # ==============================================================================
    # CALCULO: Ticket medio por operacao
    # ==============================================================================

    df['ticket_medio'] = df['voltot'] / df['qtdneg']

    # Trata inf/NaN gerados (nao deveria acontecer apos filtro qtdneg > 0, mas...)
    inf_count = np.isinf(df['ticket_medio']).sum()
    if inf_count > 0:
        logger.warning(f"Encontrados {inf_count} valores infinitos em ticket_medio. Removendo...")
        df = df.replace([np.inf, -np.inf], np.nan)

    nan_count = df['ticket_medio'].isna().sum()
    if nan_count > 0:
        logger.warning(f"Encontrados {nan_count} valores NaN em ticket_medio. Removendo...")
        df = df.dropna(subset=['ticket_medio'])

    if len(df) == 0:
        logger.error("Nenhum dado restante apos tratar inf/NaN!")
        return pd.DataFrame(), {**stats, 'after_filters': 0, 'top_n_volume': 0}

    # ==============================================================================
    # APLICACAO DOS FILTROS
    # ==============================================================================

    logger.info(f"\nAplicando filtros:")
    logger.info(f"   Maximo de operacoes: {filters['max_operations']}")
    logger.info(f"   Volume minimo: R$ {filters['min_financial_volume']:,.2f}")

    df_filtered = df[
        (df['qtdneg'] <= filters['max_operations']) &
        (df['voltot'] >= filters['min_financial_volume'])
    ].copy()

    logger.info(
        f"\nResultado dos filtros: {len(df_filtered):,} operacoes "
        f"({len(df_filtered)/len(df)*100:.1f}% do total)"
    )

    if len(df_filtered) == 0:
        logger.warning("Nenhuma operacao passou pelos filtros!")
        logger.warning("Sugestoes:")
        logger.warning(f"   - Reduzir min_financial_volume (atual: R$ {filters['min_financial_volume']:,.2f})")
        logger.warning(f"   - Aumentar max_operations (atual: {filters['max_operations']})")

        return pd.DataFrame(), {**stats, 'after_filters': 0, 'top_n_volume': 0}

    # ==============================================================================
    # CALCULO: % do dia (participacao no volume total)
    # ==============================================================================

    total_voltot = stats['total_volume']
    df_filtered['pct_do_dia'] = (df_filtered['voltot'] / total_voltot) * 100

    # ==============================================================================
    # ORDENACAO: Por ticket medio (decrescente) com stable sort
    # ==============================================================================

    df_sorted = df_filtered.sort_values(
        'ticket_medio',
        ascending=False,
        kind='stable'  # Mantem ordem relativa em caso de empate
    )

    # ==============================================================================
    # SELECAO: Top N
    # ==============================================================================

    top_n = filters['top_n']
    df_top = df_sorted.head(top_n).copy()

    logger.info(f"\nTop {len(df_top)} selecionados (de {len(df_filtered)} candidatos)")

    # ==============================================================================
    # ESTATISTICAS FINAIS
    # ==============================================================================

    stats['after_filters'] = len(df_filtered)
    stats['top_n_volume'] = float(df_top['voltot'].sum())
    stats['top_n_pct'] = (stats['top_n_volume'] / stats['total_volume']) * 100

    logger.info(f"\nEstatisticas finais:")
    logger.info(f"   Volume das top {len(df_top)}: R$ {stats['top_n_volume']:,.2f}")
    logger.info(f"   % do volume total: {stats['top_n_pct']:.2f}%")

    # Estatistica adicional: distribuicao por tipo
    if len(df_top) > 0:
        call_count = (df_top['option_type'] == 'CALL').sum()
        put_count = (df_top['option_type'] == 'PUT').sum()
        logger.info(f"   Calls: {call_count} ({call_count/len(df_top)*100:.1f}%)")
        logger.info(f"   Puts: {put_count} ({put_count/len(df_top)*100:.1f}%)")

        # Top 3 ativos-objeto
        top_underlyings = df_top['underlying'].value_counts().head(3)
        logger.info(f"   Top 3 ativos-objeto:")
        for underlying, count in top_underlyings.items():
            logger.info(f"    - {underlying}: {count} operacoes")

    # ==============================================================================
    # RETORNO: Colunas relevantes
    # ==============================================================================

    result_cols = [
        'symbol', 'underlying', 'option_type',
        'premed', 'preult',
        'voltot', 'qtdneg', 'quatot',
        'strike_price',
        'maturity_date',
        'ticket_medio', 'pct_do_dia'
    ]

    # Garante que todas as colunas existem (preult pode nao existir em casos raros)
    available_cols = [col for col in result_cols if col in df_top.columns]

    if len(available_cols) < len(result_cols):
        missing = set(result_cols) - set(available_cols)
        logger.warning(f"Colunas ausentes no retorno: {missing}")

    df_result = df_top[available_cols].reset_index(drop=True)

    logger.info(f"=" * 70)

    return df_result, stats


# ==============================================================================
# TESTES RAPIDOS (executar com: python -m src.python.analyzer)
# ==============================================================================

if __name__ == '__main__':
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )

    print("=" * 70)
    print("TESTES - Modulo Analyzer")
    print("=" * 70)

    # Verifica se ha arquivo de exemplo
    sample_files = list(Path('tests/sample_data').glob('*.parquet'))

    if not sample_files:
        print("\n Nenhum arquivo Parquet de exemplo encontrado em tests/sample_data/")
        print("   Crie um arquivo de teste ou execute o script R primeiro.")
        sys.exit(1)

    # Usa primeiro arquivo encontrado
    sample_parquet = sample_files[0]
    print(f"\nUsando arquivo de teste: {sample_parquet}")

    # Testa analise
    filters = {
        'max_operations': 5,
        'min_financial_volume': 100000,
        'top_n': 20
    }

    try:
        df_top, stats = analyze_options(sample_parquet, filters)

        print(f"\n Analise concluida com sucesso!")
        print(f"\nEstatisticas:")
        for key, value in stats.items():
            if isinstance(value, float):
                print(f"  {key}: {value:,.2f}")
            else:
                print(f"  {key}: {value:,}")

        print(f"\nPrimeiras 5 linhas do resultado:")
        print(df_top.head().to_string())

    except Exception as e:
        logger.exception(f" Erro no teste: {e}")
        sys.exit(1)

    print("\n" + "=" * 70)
