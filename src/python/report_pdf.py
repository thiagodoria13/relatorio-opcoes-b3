"""
Geracao de PDF com matplotlib (backend Agg) e formatacao pt-BR.

Este modulo gera relatorios PDF profissionais contendo:
- Capa com informacoes do pregao
- Tabela com top N operacoes
- Graficos de analise (barras, pizza, dispersao)
- Formatacao monetaria brasileira (R$ 1.234,56)
"""

import matplotlib
matplotlib.use('Agg')  # Backend headless (sem display) - DEVE ser antes de import pyplot

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import pandas as pd
from datetime import date
import logging
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)


# ==============================================================================
# FORMATACAO PT-BR
# ==============================================================================

def fmt_brl(value: float, decimals: int = 2) -> str:
    """
    Formata valor em R$ com padrao pt-BR.

    Milhares: . (ponto)
    Decimais: , (virgula)

    Args:
        value: Valor numerico
        decimals: Numero de casas decimais (default: 2)

    Returns:
        String formatada (ex: "R$ 1.234.567,89")

    Examples:
        >>> fmt_brl(1234567.89)
        'R$ 1.234.567,89'
        >>> fmt_brl(1000, 0)
        'R$ 1.000'
    """
    try:
        # Tenta usar Babel se disponivel (melhor suporte i18n)
        from babel.numbers import format_currency
        return format_currency(value, 'BRL', locale='pt_BR')
    except ImportError:
        # Fallback manual se Babel nao instalado
        if pd.isna(value):
            return "R$ -"

        # Formata com separadores en-US primeiro
        if decimals == 0:
            formatted = f"{value:,.0f}"
        else:
            formatted = f"{value:,.{decimals}f}"

        # Troca separadores: , para . (milhares) e . para , (decimais)
        formatted = formatted.replace(',', 'TEMP').replace('.', ',').replace('TEMP', '.')

        return f"R$ {formatted}"


def fmt_pct(value: float, decimals: int = 2) -> str:
    """
    Formata percentual pt-BR.

    Args:
        value: Valor percentual (ex: 12.34 para 12.34%)
        decimals: Casas decimais

    Returns:
        String formatada (ex: "12,34%")
    """
    if pd.isna(value):
        return "-"

    formatted = f"{value:.{decimals}f}%"
    # Troca . por ,
    formatted = formatted.replace('.', ',')

    return formatted


def fmt_int(value: int) -> str:
    """
    Formata inteiro com separador de milhares pt-BR.

    Args:
        value: Numero inteiro

    Returns:
        String formatada (ex: "1.234")
    """
    if pd.isna(value):
        return "-"

    return f"{int(value):,}".replace(',', '.')


# ==============================================================================
# GERACAO DO PDF
# ==============================================================================

def generate_pdf(df: pd.DataFrame, trade_date: date, config: Dict, stats: Dict) -> str:
    """
    Gera PDF profissional com tabela e graficos.

    Args:
        df: DataFrame com top N operacoes (colunas: symbol, underlying, option_type,
            premed, preult, voltot, qtdneg, ticket_medio, pct_do_dia)
        trade_date: Data do pregao
        config: Configuracao completa (dict do settings.yaml)
        stats: Estatisticas da analise (dict do analyzer.py)

    Returns:
        Caminho do PDF gerado (str)

    Raises:
        Exception: Se erro na geracao do PDF
    """
    # Garante que diretorio de output existe
    Path('output/reports').mkdir(parents=True, exist_ok=True)

    pdf_path = f"output/reports/relatorio_{trade_date.strftime('%Y-%m-%d')}.pdf"

    logger.info(f"{'='*70}")
    logger.info(f"Gerando PDF: {pdf_path}")
    logger.info(f"{'='*70}")

    # Formatacao de datas pt-BR
    trade_date_pt = trade_date.strftime('%d/%m/%Y')
    now_pt = pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')

    # Configuracao de fontes (melhor legibilidade)
    plt.rcParams['font.family'] = 'DejaVu Sans'
    plt.rcParams['font.size'] = 10

    with PdfPages(pdf_path) as pdf:

        # ======================================================================
        # PAGINA 1: CAPA
        # ======================================================================

        logger.info("Gerando pagina 1: Capa")

        fig = plt.figure(figsize=(8.27, 11.69))  # A4 portrait
        fig.patch.set_facecolor('white')

        # Titulo principal
        fig.text(0.5, 0.65, 'Relatorio Diario',
                 ha='center', size=28, weight='bold', color='#1f77b4')
        fig.text(0.5, 0.58, 'Grandes Operacoes de Opcoes',
                 ha='center', size=20, weight='bold', color='#333333')
        fig.text(0.5, 0.53, 'B3 - Brasil, Bolsa, Balcao',
                 ha='center', size=14, style='italic', color='#666666')

        # Data do pregao
        fig.text(0.5, 0.45, f'Pregao: {trade_date_pt}',
                 ha='center', size=16, weight='bold', color='#000000')
        fig.text(0.5, 0.41, f'Gerado em: {now_pt}',
                 ha='center', size=10, style='italic', color='#888888')

        # Box com sumario executivo
        summary_y = 0.30
        fig.text(0.5, summary_y, 'Sumario Executivo',
                 ha='center', size=14, weight='bold', color='#1f77b4')

        fig.text(0.5, summary_y - 0.05,
                 f"Total de opcoes processadas: {fmt_int(stats['total_options'])}",
                 ha='center', size=11)
        fig.text(0.5, summary_y - 0.09,
                 f"Operacoes apos filtros: {fmt_int(stats['after_filters'])}",
                 ha='center', size=11)
        fig.text(0.5, summary_y - 0.13,
                 f"Volume total dia: {fmt_brl(stats['total_volume'], 0)}",
                 ha='center', size=11)

        if len(df) > 0 and 'top_n_volume' in stats:
            fig.text(0.5, summary_y - 0.17,
                     f"Volume top {len(df)}: {fmt_brl(stats['top_n_volume'], 0)} ({fmt_pct(stats.get('top_n_pct', 0))})",
                     ha='center', size=11)

        # Box com filtros aplicados
        filters_y = 0.10
        fig.text(0.5, filters_y, 'Filtros Aplicados',
                 ha='center', size=12, weight='bold', color='#ff7f0e')

        fig.text(0.5, filters_y - 0.04,
                 f" Maximo de operacoes: {config['filters']['max_operations']}",
                 ha='center', size=10)
        fig.text(0.5, filters_y - 0.07,
                 f" Volume minimo: {fmt_brl(config['filters']['min_financial_volume'], 0)}",
                 ha='center', size=10)
        fig.text(0.5, filters_y - 0.10,
                 f" Top exibidos: {config['filters']['top_n']}",
                 ha='center', size=10)

        plt.axis('off')
        pdf.savefig(fig, bbox_inches='tight')
        plt.close()

        # ======================================================================
        # PAGINA 2: TABELA TOP N
        # ======================================================================

        if len(df) > 0:
            logger.info(f"Gerando pagina 2: Tabela Top {len(df)}")

            fig, ax = plt.subplots(figsize=(11.69, 8.27))  # A4 landscape
            ax.axis('tight')
            ax.axis('off')

            # Formata dados para tabela (pt-BR)
            table_data = df.copy()
            table_data['premed'] = table_data['premed'].apply(lambda x: fmt_brl(x, 2))
            table_data['voltot'] = table_data['voltot'].apply(lambda x: fmt_brl(x, 0))
            table_data['qtdneg'] = table_data['qtdneg'].apply(fmt_int)
            if 'quatot' in table_data.columns:
                table_data['quatot'] = table_data['quatot'].apply(fmt_int)
            else:
                table_data['quatot'] = "-"
            if 'strike_price' in table_data.columns:
                table_data['strike_price'] = table_data['strike_price'].apply(lambda x: fmt_brl(x, 2))
            else:
                table_data['strike_price'] = "-"
            if 'maturity_date' in table_data.columns:
                table_data['maturity_date'] = table_data['maturity_date'].apply(
                    lambda d: d.strftime('%d/%m/%Y') if pd.notna(d) else '-'
                )
            else:
                table_data['maturity_date'] = "-"
            table_data['ticket_medio'] = table_data['ticket_medio'].apply(lambda x: fmt_brl(x, 0))
            table_data['pct_do_dia'] = table_data['pct_do_dia'].apply(fmt_pct)

            # Cria tabela
            table = ax.table(
                cellText=table_data[['symbol', 'underlying', 'option_type', 'maturity_date',
                                     'strike_price', 'premed', 'voltot', 'quatot', 'qtdneg',
                                     'ticket_medio', 'pct_do_dia']].values,
                colLabels=['Opcao', 'Ativo', 'Tipo', 'Vencimento', 'Strike',
                           'Preco Medio', 'Volume Total', 'Qtde (contratos)', 'No Ops',
                           'Ticket Medio', '% do Dia'],
                cellLoc='center',
                loc='center',
                colWidths=[0.10, 0.09, 0.07, 0.11, 0.10, 0.10, 0.11, 0.11, 0.08, 0.11, 0.11]
            )

            # Estilizacao
            table.auto_set_font_size(False)
            table.set_fontsize(8)
            table.scale(1, 2.0)

            # Cabecalho com cor
            for i in range(11):
                cell = table[(0, i)]
                cell.set_facecolor('#1f77b4')
                cell.set_text_props(weight='bold', color='white')

            # Linhas alternadas (zebra striping)
            for i in range(1, len(table_data) + 1):
                for j in range(11):
                    cell = table[(i, j)]
                    if i % 2 == 0:
                        cell.set_facecolor('#f0f0f0')

            ax.set_title(f'Top {len(df)} Maiores Operacoes - Pregao {trade_date_pt}',
                         size=16, weight='bold', pad=20)

            pdf.savefig(fig, bbox_inches='tight')
            plt.close()

            # ==================================================================
            # PAGINA 3: GRAFICO DE BARRAS - Ticket Medio
            # ==================================================================

            logger.info("Gerando pagina 3: Grafico de barras (Ticket Medio)")

            fig, ax = plt.subplots(figsize=(11.69, 8.27))

            df_plot = df.head(20)
            colors = plt.cm.viridis(range(len(df_plot)))

            bars = ax.barh(df_plot['symbol'], df_plot['ticket_medio'], color=colors, edgecolor='black')

            ax.set_xlabel('Ticket Medio por Operacao', size=12, weight='bold')
            ax.set_ylabel('Opcao', size=12, weight='bold')
            ax.set_title(f'Top {len(df_plot)} - Ticket Medio por Operacao',
                         size=16, weight='bold', pad=15)
            ax.invert_yaxis()

            # Formatacao eixo X em R$ pt-BR
            ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: fmt_brl(x, 0)))

            ax.grid(axis='x', alpha=0.3, linestyle='--')
            plt.tight_layout()
            pdf.savefig(fig)
            plt.close()

            # ==================================================================
            # PAGINA 4: GRAFICO DE PIZZA - Distribuicao por Ativo
            # ==================================================================

            logger.info("Gerando pagina 4: Grafico de pizza (Distribuicao por Ativo)")

            fig, ax = plt.subplots(figsize=(10, 10))

            underlying_vol = df.groupby('underlying')['voltot'].sum().nlargest(10)

            colors_pie = plt.cm.Set3(range(len(underlying_vol)))
            wedges, texts, autotexts = ax.pie(
                underlying_vol,
                labels=underlying_vol.index,
                autopct='%1.1f%%',
                startangle=90,
                colors=colors_pie,
                textprops={'size': 10, 'weight': 'bold'}
            )

            # Destaca percentuais
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_weight('bold')

            ax.set_title('Distribuicao de Volume por Ativo-Objeto (Top 10)',
                         size=16, weight='bold', pad=20)

            pdf.savefig(fig, bbox_inches='tight')
            plt.close()

            # ==================================================================
            # PAGINA 5: DISPERSAO - Volume Total vs No Operacoes
            # ==================================================================

            logger.info("Gerando pagina 5: Grafico de dispersao")

            fig, ax = plt.subplots(figsize=(11, 8))

            scatter = ax.scatter(
                df['qtdneg'],
                df['voltot'],
                c=df['ticket_medio'],
                cmap='plasma',
                s=150,
                alpha=0.7,
                edgecolors='black',
                linewidths=1.5
            )

            ax.set_xlabel('Numero de Operacoes', size=13, weight='bold')
            ax.set_ylabel('Volume Total', size=13, weight='bold')
            ax.set_title('Relacao: Volume Total  Numero de Operacoes',
                         size=16, weight='bold', pad=15)

            # Formatacao eixo Y em R$ pt-BR
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: fmt_brl(x, 0)))

            # Colorbar
            cbar = plt.colorbar(scatter, ax=ax)
            cbar.set_label('Ticket Medio', size=11, weight='bold')
            cbar.ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: fmt_brl(x, 0)))

            ax.grid(alpha=0.3, linestyle='--')
            plt.tight_layout()
            pdf.savefig(fig)
            plt.close()

        else:
            # Caso nao haja dados
            logger.warning("Nenhum dado para graficos. Gerando pagina de aviso.")

            fig = plt.figure(figsize=(8.27, 11.69))
            fig.text(0.5, 0.5, ' Nenhuma operacao encontrada para os filtros aplicados',
                     ha='center', va='center', size=16, color='#ff7f0e')
            fig.text(0.5, 0.45, 'Tente ajustar os filtros em config/settings.yaml',
                     ha='center', va='center', size=12, color='#666666')
            plt.axis('off')
            pdf.savefig(fig)
            plt.close()

        # ======================================================================
        # METADADOS DO PDF
        # ======================================================================

        d = pdf.infodict()
        d['Title'] = f'Relatorio Grandes Operacoes B3 - {trade_date_pt}'
        d['Author'] = 'Sistema Automatizado de Analise'
        d['Subject'] = f'Analise pregao {trade_date_pt}'
        d['Keywords'] = 'B3, Opcoes, Boletas Grandes, Analise Quantitativa'
        d['Creator'] = 'Python matplotlib + report_pdf.py'

    logger.info(f" PDF gerado com sucesso: {pdf_path}")

    # Valida tamanho do arquivo
    pdf_size = Path(pdf_path).stat().st_size
    pdf_size_kb = pdf_size / 1024

    logger.info(f"  Tamanho: {pdf_size_kb:.1f} KB")

    if pdf_size_kb < 10:
        logger.warning(f" PDF muito pequeno ({pdf_size_kb:.1f} KB). Pode haver erro na geracao.")

    logger.info(f"{'='*70}")

    return pdf_path


# ==============================================================================
# TESTES RAPIDOS
# ==============================================================================

if __name__ == '__main__':
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )

    print("=" * 70)
    print("TESTES - Modulo Report PDF")
    print("=" * 70)

    # Dados de teste
    df_test = pd.DataFrame({
        'symbol': ['PETRK250', 'VALEF240', 'BBASK245', 'ITUBK230', 'MGLUV235'],
        'underlying': ['PETR4', 'VALE3', 'BBAS3', 'ITUB4', 'MGLU3'],
        'option_type': ['CALL', 'CALL', 'CALL', 'CALL', 'PUT'],
        'premed': [1.50, 2.30, 3.10, 1.80, 0.90],
        'preult': [1.52, 2.35, 3.15, 1.85, 0.92],
        'voltot': [500000, 450000, 400000, 350000, 300000],
        'qtdneg': [3, 4, 2, 5, 3],
        'ticket_medio': [166666.67, 112500.00, 200000.00, 70000.00, 100000.00],
        'pct_do_dia': [0.5, 0.45, 0.4, 0.35, 0.3]
    })

    config_test = {
        'filters': {
            'max_operations': 5,
            'min_financial_volume': 100000,
            'top_n': 20
        }
    }

    stats_test = {
        'total_options': 1000,
        'after_filters': 50,
        'total_volume': 100000000,
        'top_n_volume': 2000000,
        'top_n_pct': 2.0
    }

    try:
        pdf_path = generate_pdf(df_test, date(2024, 1, 10), config_test, stats_test)
        print(f"\n PDF de teste gerado com sucesso!")
        print(f"  Caminho: {pdf_path}")

        # Verifica tamanho
        size_kb = Path(pdf_path).stat().st_size / 1024
        print(f"  Tamanho: {size_kb:.1f} KB")

        if size_kb > 10:
            print(f"\n TESTE PASSOU: PDF tem tamanho adequado")
        else:
            print(f"\n TESTE FALHOU: PDF muito pequeno")
            sys.exit(1)

    except Exception as e:
        logger.exception(f" Erro no teste: {e}")
        sys.exit(1)

    print("\n" + "=" * 70)
