"""
Envio de e-mail via SMTP Gmail com retry, SSL e formatacao pt-BR.

Este modulo:
- Envia e-mails com anexo PDF via SMTP do Gmail
- Usa SSL/TLS para conexao segura
- Implementa retry exponencial em caso de falha
- Formata corpo do e-mail em HTML com tabela top 5
- Envia alertas simples em caso de falha critica do sistema
"""

import smtplib
import ssl
import time
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)


# ==============================================================================
# FORMATACAO PT-BR (simplificada para e-mail)
# ==============================================================================

def fmt_brl_simple(value: float, decimals: int = 2) -> str:
    """
    Formatacao simples pt-BR para e-mail (sem dependencia Babel).

    Args:
        value: Valor numerico
        decimals: Casas decimais

    Returns:
        String formatada (ex: "R$ 1.234,56")
    """
    if pd.isna(value):
        return "R$ -"

    # Formata com separadores en-US
    if decimals == 0:
        formatted = f"{value:,.0f}"
    else:
        formatted = f"{value:,.{decimals}f}"

    # Troca separadores: , para . (milhares) e . para , (decimais)
    formatted = formatted.replace(',', 'TEMP').replace('.', ',').replace('TEMP', '.')

    return f"R$ {formatted}"


def fmt_int_simple(value: int) -> str:
    """Formata inteiro com separador pt-BR."""
    if pd.isna(value):
        return "-"
    return f"{int(value):,}".replace(',', '.')


# ==============================================================================
# ENVIO DE E-MAIL COM RETRY
# ==============================================================================

def send_email_with_retry(sender: str, password: str, recipients: list,
                          msg: MIMEMultipart, smtp_host: str, smtp_port: int,
                          max_retries: int = 3) -> None:
    """
    Envia e-mail com retry exponencial e SSL.

    Args:
        sender: E-mail remetente
        password: Senha/App Password
        recipients: Lista de destinatarios
        msg: Mensagem MIME completa
        smtp_host: Servidor SMTP
        smtp_port: Porta SMTP
        max_retries: Maximo de tentativas (default: 3)

    Raises:
        RuntimeError: Se falhar apos todas as tentativas

    Notes:
        - Usa TLS com contexto SSL seguro
        - Backoff exponencial: 5s, 10s, 20s
        - Timeout de 30 segundos por tentativa
    """

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Tentativa {attempt}/{max_retries}: Conectando ao servidor SMTP...")

            # Contexto SSL seguro
            context = ssl.create_default_context()

            # Conecta ao servidor SMTP
            with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:

                # Habilita modo debug se log level for DEBUG
                if logger.isEnabledFor(logging.DEBUG):
                    server.set_debuglevel(1)

                # Inicia TLS
                server.starttls(context=context)
                logger.info("TLS estabelecido")

                # Autentica
                server.login(sender, password)
                logger.info("Autenticacao bem-sucedida")

                # Envia e-mail
                server.sendmail(sender, recipients, msg.as_string())
                logger.info(f"E-mail enviado para: {', '.join(recipients)}")

                return  # Sucesso!

        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"Erro de autenticacao: {e}")
            logger.error("  Verifique GMAIL_USER e GMAIL_APP_PASSWORD no .env")
            logger.error("  App Password: https://myaccount.google.com/apppasswords")
            raise  # Nao faz sentido retry em erro de autenticacao

        except smtplib.SMTPException as e:
            logger.error(f"Erro SMTP (tentativa {attempt}): {e}")

            if attempt < max_retries:
                wait = 5 * (2 ** (attempt - 1))  # 5s, 10s, 20s
                logger.warning(f"Aguardando {wait}s antes de retry...")
                time.sleep(wait)
            else:
                raise RuntimeError(
                    f"Falha no envio de e-mail apos {max_retries} tentativas"
                ) from e

        except Exception as e:
            logger.error(f"Erro inesperado (tentativa {attempt}): {e}")

            if attempt < max_retries:
                wait = 5 * (2 ** (attempt - 1))
                logger.warning(f"Aguardando {wait}s antes de retry...")
                time.sleep(wait)
            else:
                raise RuntimeError(
                    f"Falha no envio de e-mail apos {max_retries} tentativas"
                ) from e


# ==============================================================================
# ENVIO DO RELATORIO PRINCIPAL
# ==============================================================================

def send_email(pdf_path: str, trade_date, df_summary: pd.DataFrame,
               email_config: Dict, stats: Dict) -> None:
    """
    Envia e-mail com PDF anexo via SMTP Gmail.

    Args:
        pdf_path: Caminho do PDF gerado
        trade_date: Data do pregao (datetime.date)
        df_summary: DataFrame com top N (para tabela no e-mail)
        email_config: Configuracao de e-mail (dict do settings.yaml)
        stats: Estatisticas da analise (dict do analyzer.py)

    Raises:
        ValueError: Se credenciais ausentes ou PDF invalido
        RuntimeError: Se falhar apos retries
    """

    logger.info(f"{'='*70}")
    logger.info(f"Preparando envio de e-mail")
    logger.info(f"{'='*70}")

    # ===== Validacao de Credenciais =====

    sender = email_config['smtp_user']
    password = os.getenv('GMAIL_APP_PASSWORD') or email_config.get('smtp_pass')

    if not sender:
        raise ValueError("smtp_user nao configurado em settings.yaml")

    if not password:
        raise ValueError(
            "GMAIL_APP_PASSWORD nao encontrado.\n"
            "Configure a variavel de ambiente ou smtp_pass no settings.yaml\n"
            "Para criar App Password: https://myaccount.google.com/apppasswords"
        )

    recipients = email_config['recipients']

    if not recipients:
        raise ValueError("Nenhum destinatario configurado em settings.yaml")

    smtp_host = email_config['smtp_host']
    smtp_port = email_config['smtp_port']

    logger.info(f"Remetente: {sender}")
    logger.info(f"Destinatarios: {', '.join(recipients)}")

    # ===== Validacao do Anexo PDF =====

    pdf_path_obj = Path(pdf_path)

    if not pdf_path_obj.exists():
        raise ValueError(f"PDF nao encontrado: {pdf_path}")

    pdf_size = pdf_path_obj.stat().st_size
    pdf_size_mb = pdf_size / 1024 / 1024

    if pdf_size == 0:
        raise ValueError(f"PDF vazio: {pdf_path}")

    logger.info(f"Anexo: {pdf_path} ({pdf_size_mb:.2f} MB)")

    # Gmail tem limite de 25MB por e-mail
    if pdf_size_mb > 20:
        logger.warning(f"PDF grande ({pdf_size_mb:.2f} MB). Gmail limita a 25MB.")

    # ===== Montagem da Mensagem =====

    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    msg['Subject'] = email_config['subject_template'].format(
        trade_date_pt=trade_date.strftime('%d/%m/%Y')
    )

    logger.info(f"Assunto: {msg['Subject']}")

    # ===== Corpo HTML com Resumo e Tabela Top 5 =====

    trade_date_pt = trade_date.strftime('%d/%m/%Y')

    # Top 5 para o e-mail (ou menos se df_summary tiver menos linhas)
    top5 = df_summary.head(5)

    # Gera linhas da tabela HTML
    html_rows = ""
    for idx, row in top5.iterrows():
        maturity = row.get('maturity_date')
        maturity_str = maturity.strftime('%d/%m/%Y') if pd.notna(maturity) else '-'
        strike = row.get('strike_price')
        strike_str = fmt_brl_simple(strike, 2) if pd.notna(strike) else '-'
        quatot_val = row.get('quatot')
        quatot_str = fmt_int_simple(quatot_val) if pd.notna(quatot_val) else '-'

        html_rows += f"""
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;">{row['symbol']}</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{row['underlying']}</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{row['option_type']}</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{maturity_str}</td>
            <td style="padding: 8px; border: 1px solid #ddd; text-align: right;">{strike_str}</td>
            <td style="padding: 8px; border: 1px solid #ddd; text-align: right;">{fmt_brl_simple(row['voltot'], 0)}</td>
            <td style="padding: 8px; border: 1px solid #ddd; text-align: center;">{fmt_int_simple(row['qtdneg'])}</td>
            <td style="padding: 8px; border: 1px solid #ddd; text-align: center;">{quatot_str}</td>
            <td style="padding: 8px; border: 1px solid #ddd; text-align: right;"><strong>{fmt_brl_simple(row['ticket_medio'], 0)}</strong></td>
        </tr>
        """

    body_html = f"""
    <html>
      <head>
        <style>
          body {{ font-family: Arial, sans-serif; color: #333; }}
          table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
          th {{ background-color: #1f77b4; color: white; padding: 10px; text-align: left; border: 1px solid #ddd; }}
          td {{ border: 1px solid #ddd; }}
          .summary {{ background-color: #f0f0f0; padding: 15px; border-radius: 5px; margin: 15px 0; }}
          .footer {{ font-size: 11px; color: #888; margin-top: 40px; padding-top: 20px; border-top: 1px solid #ccc; }}
        </style>
      </head>
      <body>
        <h2 style="color: #1f77b4;">Relatorio Diario - Grandes Operacoes de Opcoes B3</h2>

        <div class="summary">
          <p><strong> Pregao:</strong> {trade_date_pt}</p>
          <p><strong> Total de opcoes analisadas:</strong> {fmt_int_simple(stats['total_options'])}</p>
          <p><strong> Operacoes apos filtros:</strong> {fmt_int_simple(stats['after_filters'])}</p>
          <p><strong> Volume total do dia:</strong> {fmt_brl_simple(stats['total_volume'], 0)}</p>
          {f"<p><strong> Volume top {len(df_summary)}:</strong> {fmt_brl_simple(stats.get('top_n_volume', 0), 0)} ({stats.get('top_n_pct', 0):.2f}% do total)</p>" if 'top_n_volume' in stats else ""}
        </div>

        <h3 style="color: #1f77b4;"> Top {len(top5)} Maiores Tickets Medios:</h3>

        <table>
          <thead>
            <tr>
              <th>Opcao</th>
              <th>Ativo</th>
              <th>Tipo</th>
              <th>Vencimento</th>
              <th>Strike</th>
              <th>Volume Total</th>
              <th>No Ops</th>
              <th>Qtde</th>
              <th>Ticket Medio</th>
            </tr>
          </thead>
          <tbody>
            {html_rows}
          </tbody>
        </table>

        <p style="margin-top: 30px;">
           <strong>O relatorio completo em PDF com graficos e analises detalhadas esta anexo.</strong>
        </p>

        <div class="footer">
          <p><strong>Fonte:</strong> B3 (COTAHIST) - Dados historicos de mercado</p>
          <p><strong>Gerado por:</strong> Sistema Automatizado de Analise de Opcoes</p>
          <p><strong>Data/Hora:</strong> {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
        </div>
      </body>
    </html>
    """

    msg.attach(MIMEText(body_html, 'html', 'utf-8'))

    # ===== Anexa PDF =====

    logger.info("Anexando PDF...")

    with open(pdf_path, 'rb') as f:
        part = MIMEBase('application', 'pdf')
        part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header(
            'Content-Disposition',
            f'attachment; filename="{pdf_path_obj.name}"'
        )
        msg.attach(part)

    logger.info("PDF anexado")

    # ===== Envia com Retry =====

    send_email_with_retry(sender, password, recipients, msg, smtp_host, smtp_port)

    logger.info(f"{'='*70}")


# ==============================================================================
# ENVIO DE ALERTA DE FALHA
# ==============================================================================

def send_failure_alert(error_message: str, email_config: Dict) -> None:
    """
    Envia e-mail de alerta simples em caso de falha critica do sistema.

    Args:
        error_message: Mensagem de erro a ser reportada
        email_config: Configuracao de e-mail

    Notes:
        - Usa menos retries (2) para nao atrasar muito
        - Nao anexa PDF (apenas texto)
        - Formatacao simples para garantir envio mesmo em condicoes adversas
    """

    logger.info("Enviando e-mail de alerta de falha...")

    try:
        sender = email_config['smtp_user']
        password = os.getenv('GMAIL_APP_PASSWORD') or email_config.get('smtp_pass')
        recipients = email_config['recipients']

        if not sender or not password or not recipients:
            logger.error("Credenciais incompletas. Nao foi possivel enviar alerta.")
            return

        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = f" ERRO - Relatorio B3 Opcoes - {pd.Timestamp.now().strftime('%d/%m/%Y')}"

        body_html = f"""
        <html>
          <body style="font-family: Arial, sans-serif; color: #333;">
            <h2 style="color: #d9534f;"> Falha na Execucao do Relatorio</h2>

            <p><strong>Data/Hora:</strong> {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')}</p>

            <h3>Erro:</h3>
            <pre style="background-color: #f0f0f0; padding: 15px; border-radius: 5px; border-left: 4px solid #d9534f; overflow-x: auto;">{error_message}</pre>

            <h3>Proximos Passos:</h3>
            <ol>
              <li>Verifique os logs em <code>logs/execution.log</code> para detalhes completos</li>
              <li>Verifique conectividade com a internet e disponibilidade dos dados da B3</li>
              <li>Certifique-se de que as dependencias (R e Python) estao instaladas</li>
              <li>Se o erro persistir, execute manualmente:
                <pre style="background-color: #f9f9f9; padding: 10px; border-radius: 3px;">python src/python/orchestrator.py --date=YYYY-MM-DD</pre>
              </li>
            </ol>

            <hr style="margin-top: 30px; border: none; border-top: 1px solid #ccc;">
            <p style="font-size: 11px; color: #888;">
              <strong>Sistema Automatizado de Analise de Opcoes B3</strong><br>
              Este e um e-mail automatico de alerta.
            </p>
          </body>
        </html>
        """

        msg.attach(MIMEText(body_html, 'html', 'utf-8'))

        # Envia com menos retries (2) para nao atrasar muito
        send_email_with_retry(
            sender, password, recipients, msg,
            email_config['smtp_host'], email_config['smtp_port'],
            max_retries=2
        )

        logger.info("E-mail de alerta enviado com sucesso")

    except Exception as e:
        logger.error(f"Falha ao enviar e-mail de alerta: {e}")
        # Nao re-raise para nao mascarar erro original


# ==============================================================================
# TESTES RAPIDOS
# ==============================================================================

if __name__ == '__main__':
    import sys
    from datetime import date

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )

    print("=" * 70)
    print("TESTES - Modulo Mailer")
    print("=" * 70)

    # Carrega .env
    from dotenv import load_dotenv
    load_dotenv()

    # Verifica credenciais
    if not os.getenv('GMAIL_USER') or not os.getenv('GMAIL_APP_PASSWORD'):
        print("\n Credenciais nao configuradas no .env")
        print("  Configure GMAIL_USER e GMAIL_APP_PASSWORD para testar")
        sys.exit(1)

    print("\n Credenciais encontradas no .env")
    print(f"  GMAIL_USER: {os.getenv('GMAIL_USER')}")

    # Verifica se ha PDF de teste
    test_pdfs = list(Path('output/reports').glob('*.pdf'))

    if not test_pdfs:
        print("\n Nenhum PDF de teste encontrado em output/reports/")
        print("  Execute primeiro o modulo report_pdf.py para gerar um PDF de teste")
        sys.exit(1)

    print(f"\n PDF de teste encontrado: {test_pdfs[0]}")

    # Dados de teste
    df_test = pd.DataFrame({
        'symbol': ['PETRK250', 'VALEF240'],
        'underlying': ['PETR4', 'VALE3'],
        'option_type': ['CALL', 'CALL'],
        'voltot': [500000, 450000],
        'qtdneg': [3, 4],
        'ticket_medio': [166666.67, 112500.00]
    })

    email_config_test = {
        'smtp_user': os.getenv('GMAIL_USER'),
        'smtp_host': 'smtp.gmail.com',
        'smtp_port': 587,
        'recipients': [os.getenv('GMAIL_USER')],  # Envia para si mesmo
        'subject_template': 'TESTE - Relatorio B3 - {trade_date_pt}'
    }

    stats_test = {
        'total_options': 1000,
        'after_filters': 50,
        'total_volume': 100000000,
        'top_n_volume': 950000,
        'top_n_pct': 0.95
    }

    # Pergunta antes de enviar
    print("\n" + "=" * 70)
    print("ATENCAO: O teste ira enviar um e-mail REAL!")
    print(f"Destinatario: {os.getenv('GMAIL_USER')}")
    response = input("Deseja continuar? (s/n): ")

    if response.lower() != 's':
        print("Teste cancelado.")
        sys.exit(0)

    try:
        send_email(
            str(test_pdfs[0]),
            date.today(),
            df_test,
            email_config_test,
            stats_test
        )

        print("\n TESTE PASSOU: E-mail enviado com sucesso!")
        print(f"  Verifique a caixa de entrada de {os.getenv('GMAIL_USER')}")

    except Exception as e:
        logger.exception(f" TESTE FALHOU: {e}")
        sys.exit(1)

    print("\n" + "=" * 70)
