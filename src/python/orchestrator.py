"""
Orquestrador principal do sistema de relatorios B3.

Este modulo coordena:
- Calculo de dia util
- Download de dados (R script)
- Analise e filtros (Python)
- Geracao de PDF
- Envio de e-mail
- Logging e observabilidade
- Retry com backoff exponencial
- Lock por data (idempotencia)
- CLI com argparse
"""

import subprocess
import logging
from logging.handlers import TimedRotatingFileHandler
import time
import os
import re
import argparse
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
import yaml
import sys

# ==============================================================================
# SETUP INICIAL (ANTES DE TUDO!)
# ==============================================================================

# CRITICO: Criar diretorios ANTES de configurar logging
Path('logs').mkdir(parents=True, exist_ok=True)
Path('output/reports').mkdir(parents=True, exist_ok=True)
Path('data/processed').mkdir(parents=True, exist_ok=True)
Path('data/raw').mkdir(parents=True, exist_ok=True)

# Carregar variaveis de ambiente do .env
load_dotenv()

# ==============================================================================
# LOGGING
# ==============================================================================

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Handler rotativo (30 dias, rotacao a meia-noite)
file_handler = TimedRotatingFileHandler(
    'logs/execution.log',
    when='midnight',
    interval=1,
    backupCount=30,
    encoding='utf-8'
)
file_handler.setFormatter(
    logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
)

# Handler console (para ver output em tempo real)
console_handler = logging.StreamHandler()
console_handler.setFormatter(
    logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# ==============================================================================
# IMPORTS DE MODULOS LOCAIS (apos setup de logging)
# ==============================================================================

from business_days import get_last_business_day
from analyzer import analyze_options
from report_pdf import generate_pdf
from mailer import send_email, send_failure_alert

# ==============================================================================
# CONFIGURACAO COM EXPANSAO DE ENV VARS
# ==============================================================================

def expand_env_vars(value):
    """
    Expande ${VAR} ou $VAR em strings usando variaveis de ambiente.

    Args:
        value: String com variaveis ou outro tipo

    Returns:
        String expandida ou valor original se nao for string

    Examples:
        >>> os.environ['TEST'] = 'valor'
        >>> expand_env_vars('${TEST}')
        'valor'
        >>> expand_env_vars('$TEST')
        'valor'
        >>> expand_env_vars(123)
        123
    """
    if not isinstance(value, str):
        return value

    # Padrao ${VAR} ou $VAR
    pattern = r'\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)'

    def replacer(match):
        var_name = match.group(1) or match.group(2)
        env_value = os.getenv(var_name)

        if env_value is None:
            logger.warning(f"Variavel de ambiente nao encontrada: {var_name}")
            return match.group(0)  # Mantem original

        return env_value

    return re.sub(pattern, replacer, value)


def expand_dict(d):
    """
    Recursivamente expande env vars em dicionario/lista.

    Args:
        d: Dicionario, lista ou valor

    Returns:
        Estrutura expandida
    """
    if isinstance(d, dict):
        return {k: expand_dict(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [expand_dict(item) for item in d]
    elif isinstance(d, str):
        return expand_env_vars(d)
    else:
        return d


def load_config():
    """
    Carrega settings.yaml com expansao de variaveis de ambiente.

    Returns:
        Dict com configuracao expandida

    Raises:
        FileNotFoundError: Se settings.yaml nao existir
        yaml.YAMLError: Se YAML invalido
    """
    config_file = Path('config/settings.yaml')

    if not config_file.exists():
        raise FileNotFoundError(
            f"Arquivo de configuracao nao encontrado: {config_file}\n"
            f"Execute o setup primeiro ou copie settings.yaml.example"
        )

    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # Expande variaveis de ambiente
    config = expand_dict(config)

    return config


# ==============================================================================
# FILE LOCK (Idempotencia)
# ==============================================================================

class DateLock:
    """
    Lock baseado em arquivo para evitar execucoes concorrentes da mesma data.

    Usa file lock para garantir que apenas uma execucao processe cada data por vez.
    Se um lock existir por mais de TTL horas, e considerado stale e pode ser sobrescrito.
    """

    def __init__(self, trade_date: str, ttl_hours: int = 24):
        """
        Args:
            trade_date: Data no formato YYYY-MM-DD
            ttl_hours: Tempo de vida do lock em horas (default: 24)
        """
        self.lock_path = Path(f"data/processed/.lock-{trade_date}")
        self.ttl_hours = ttl_hours
        self.lock_file = None

    def acquire(self):
        """
        Tenta adquirir lock. Se lock stale, sobrescreve.

        Raises:
            RuntimeError: Se lock ativo (nao stale) existir
        """
        while True:
            try:
                fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                break
            except FileExistsError:
                if not self.lock_path.exists():
                    continue

                age_seconds = time.time() - self.lock_path.stat().st_mtime
                age_hours = age_seconds / 3600

                if age_hours < self.ttl_hours:
                    raise RuntimeError(
                        f"Lock ativo encontrado: {self.lock_path}\n"
                        f"Idade: {age_hours:.1f}h (TTL: {self.ttl_hours}h)\n"
                        f"Outra execucao pode estar rodando ou falhou recentemente.\n"
                        f"Se tiver certeza de que nao ha execucao ativa, delete: {self.lock_path}"
                    )

                logger.warning(
                    f"Lock stale detectado (idade: {age_hours:.1f}h > TTL: {self.ttl_hours}h). "
                    f"Removendo..."
                )
                try:
                    self.lock_path.unlink()
                except OSError as unlink_error:
                    raise RuntimeError(
                        f"Nao foi possivel remover lock stale: {self.lock_path}"
                    ) from unlink_error

        self.lock_file = os.fdopen(fd, 'w')
        self.lock_file.write(f"{datetime.now().isoformat()}\n")
        self.lock_file.write(f"PID: {os.getpid()}\n")
        self.lock_file.flush()
        logger.info(f"Lock adquirido: {self.lock_path}")

    def release(self):
        """Libera lock removendo arquivo."""
        if self.lock_file:
            self.lock_file.close()

        if self.lock_path.exists():
            self.lock_path.unlink()
            logger.info(f"Lock liberado: {self.lock_path}")

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


# ==============================================================================
# DOWNLOAD R (com retry exponencial)
# ==============================================================================

def run_r_download(trade_date_str: str, config: dict, force: bool = False,
                    fast_retry: bool = False) -> Path:
    """
    Executa script R com retry e backoff exponencial.

    Args:
        trade_date_str: Data no formato YYYY-MM-DD
        config: Dicionario de configuracao
        force: Se True, forca re-download mesmo se Parquet existir

    Returns:
        Path do Parquet gerado

    Raises:
        RuntimeError: Se falhar apos todos os retries
    """
    parquet_path = Path(f"data/processed/cotahist_{trade_date_str}.parquet")

    # ===== Idempotencia: Verifica se ja existe =====

    if not force and parquet_path.exists() and parquet_path.stat().st_size > 0:
        logger.info(f" Parquet ja existe: {parquet_path} ({parquet_path.stat().st_size:,} bytes)")
        logger.info(f"  Pulando download. Use --force para forcar re-download.")
        return parquet_path

    # ===== Configuracoes de Retry =====

    max_retries = config['scheduling']['retries']
    retry_interval = config['scheduling']['retry_interval_minutes'] * 60
    if fast_retry:
        logger.debug("Fast retry habilitado: intervalo de espera reduzido.")
        retry_interval = 1

    # Path do Rscript (configuravel ou usa PATH do sistema)
    configured_rscript = config.get('paths', {}).get('rscript')
    default_rscript = shutil.which('Rscript') or 'Rscript'

    if configured_rscript:
        # Se estivermos rodando em ambiente nao-Windows e o caminho parecer Windows, faz fallback
        if os.name != 'nt' and '\\' in configured_rscript:
            logger.warning(
                "Caminho Rscript configurado parece Windows, mas estamos fora do Windows. "
                "Usando Rscript do PATH."
            )
            rscript_cmd = default_rscript
        else:
            # Se caminho absoluto nao existir, tenta usar Rscript do PATH
            candidate_path = Path(configured_rscript)
            if candidate_path.is_absolute() and not candidate_path.exists():
                fallback = shutil.which('Rscript')
                if fallback:
                    logger.warning(
                        "Caminho Rscript configurado nao encontrado. "
                        "Usando Rscript encontrado no PATH."
                    )
                    rscript_cmd = fallback
                else:
                    rscript_cmd = configured_rscript
            else:
                rscript_cmd = configured_rscript
    else:
        rscript_cmd = default_rscript

    logger.info(f"Comando Rscript: {rscript_cmd}")

    # ===== Loop de Retry =====

    for attempt in range(1, max_retries + 1):
        logger.info(f"")
        logger.info(f"{'='*70}")
        logger.info(f"Tentativa {attempt}/{max_retries}: Download B3 para {trade_date_str}")
        logger.info(f"{'='*70}")

        try:
            result = subprocess.run(
                [rscript_cmd, 'src/r_scripts/download_b3_data.R', f'--date={trade_date_str}'],
                capture_output=True,
                text=True,
                timeout=600  # 10 minutos de timeout
            )

            # Loga stdout do R (sempre, mesmo em sucesso)
            if result.stdout:
                for line in result.stdout.strip().split('\n'):
                    logger.info(f"[R] {line}")

            # Loga stderr do R (warnings/errors)
            if result.stderr:
                for line in result.stderr.strip().split('\n'):
                    if line.strip():  # Ignora linhas vazias
                        logger.warning(f"[R stderr] {line}")

            # Verifica sucesso
            if result.returncode == 0 and parquet_path.exists():
                size = parquet_path.stat().st_size
                size_mb = size / 1024 / 1024
                logger.info(f"")
                logger.info(f" Download bem-sucedido!")
                logger.info(f"  Arquivo: {parquet_path}")
                logger.info(f"  Tamanho: {size_mb:.2f} MB ({size:,} bytes)")
                return parquet_path

            else:
                logger.error(f" R retornou codigo {result.returncode}")

                if not parquet_path.exists():
                    logger.error(f"  Parquet nao foi criado: {parquet_path}")

        except subprocess.TimeoutExpired:
            logger.error(f" Timeout apos 10 minutos")

        except FileNotFoundError:
            logger.error(
                f" Comando Rscript nao encontrado: {rscript_cmd}\n"
                f"  Verifique se R esta instalado e no PATH, ou configure paths.rscript no settings.yaml"
            )
            raise  # Nao faz sentido retry se Rscript nao existe

        except Exception as e:
            logger.exception(f" Erro inesperado ao executar R: {e}")

        # ===== Retry com Backoff Exponencial =====

        if attempt < max_retries:
            if fast_retry:
                logger.warning("Fast retry habilitado: pulando espera antes da proxima tentativa.")
                continue

            wait_seconds = retry_interval * (1.5 ** (attempt - 1))
            wait_minutes = wait_seconds / 60

            logger.warning("")
            logger.warning(f" Aguardando {wait_minutes:.1f} minutos antes do proximo retry...")
            logger.warning(f"   (Backoff exponencial: base {retry_interval}s  1.5^{attempt-1})")

            time.sleep(wait_seconds)

    # ===== Falhou Apos Todos os Retries =====

    raise RuntimeError(
        f"Falha no download B3 apos {max_retries} tentativas.\n"
        f"Possiveis causas:\n"
        f"  - Dados ainda nao disponibilizados pela B3 (tente mais tarde)\n"
        f"  - Data e feriado ou fim de semana (verifique calendario B3)\n"
        f"  - Problema de conectividade (verifique internet)\n"
        f"  - Erro no script R (verifique logs acima)"
    )


# ==============================================================================
# CLI (Command Line Interface)
# ==============================================================================

def parse_args():
    """
    Argumentos de linha de comando.

    Returns:
        argparse.Namespace com argumentos parseados
    """
    parser = argparse.ArgumentParser(
        description='Relatorio diario de grandes operacoes de opcoes B3',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Execucao normal (ultimo dia util)
  python orchestrator.py

  # Data especifica
  python orchestrator.py --date=2024-01-10

  # Forca re-download mesmo se dados existirem
  python orchestrator.py --date=2024-01-10 --force

  # Gera PDF sem enviar e-mail
  python orchestrator.py --no-email

  # Modo debug (mais verboso)
  python orchestrator.py --debug
        """
    )

    parser.add_argument(
        '--date',
        type=str,
        metavar='YYYY-MM-DD',
        help='Data especifica para processar. Se omitido, usa ultimo dia util.'
    )

    parser.add_argument(
        '--no-email',
        action='store_true',
        help='Nao envia e-mail (apenas gera PDF)'
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Forca re-download mesmo se Parquet ja existir'
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='Habilita modo debug (logs mais verbosos)'
    )

    parser.add_argument(
        '--fast-retry',
        action='store_true',
        help='Reduz espera entre retries (usar apenas em testes)'
    )

    return parser.parse_args()


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    """
    Orquestracao principal.

    Returns:
        int: Exit code (0 = sucesso, 1 = erro)
    """

    args = parse_args()
    exit_code = 0
    start_time = time.perf_counter()

    # Ajusta nivel de log se --debug
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Modo DEBUG ativado")

    try:
        # ===== Carrega Configuracao =====

        logger.info("Carregando configuracao...")
        config = load_config()
        logger.info("Configuracao carregada")

        # ===== Determina Data =====

        if args.date:
            try:
                trade_date = datetime.strptime(args.date, '%Y-%m-%d').date()
                logger.info(f"Data especificada via --date: {trade_date}")
            except ValueError as e:
                logger.error(f"Formato de data invalido: {args.date}")
                logger.error(f"Use o formato YYYY-MM-DD (ex: 2024-01-10)")
                return 1
        else:
            trade_date = get_last_business_day()
            logger.info(f"Ultimo dia util calculado: {trade_date}")

        trade_date_str = trade_date.strftime('%Y-%m-%d')

        # ===== Log Inicial =====

        logger.info(f"")
        logger.info(f"{'#'*70}")
        logger.info(f"INICIO DA EXECUCAO")
        logger.info(f"{'#'*70}")
        logger.info(f"Pregao: {trade_date.strftime('%d/%m/%Y (%A)')}")
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info(f"PID: {os.getpid()}")
        logger.info(f"Modo: {'DEBUG' if args.debug else 'INFO'}")
        logger.info(f"Flags: force={args.force}, no-email={args.no_email}")
        logger.info(f"{'#'*70}")

        # ===== Adquire Lock (Idempotencia) =====

        with DateLock(trade_date_str):

            # ===== FASE 1: Download COTAHIST (R) =====

            logger.info(f"")
            logger.info(f"[FASE 1/4] Download COTAHIST via rb3")
            logger.info(f"")

            parquet_path = run_r_download(trade_date_str, config, args.force, args.fast_retry)

            # ===== FASE 2: Analise e Filtros (Python) =====

            logger.info(f"")
            logger.info(f"[FASE 2/4] Analise e aplicacao de filtros")
            logger.info(f"")

            df_top, stats = analyze_options(parquet_path, config['filters'])

            # ===== Log Observabilidade =====

            logger.info(f"")
            logger.info("ESTATISTICAS DA ANALISE:")
            logger.info(f"{'='*70}")
            logger.info(f"   Total de opcoes processadas: {stats['total_options']:,}")
            logger.info(f"   Apos filtros aplicados: {stats['after_filters']:,}")
            logger.info(f"   Top N selecionados: {len(df_top)}")
            logger.info(f"   Volume total do dia: R$ {stats['total_volume']:,.2f}")

            if 'top_n_volume' in stats:
                logger.info(f"   Volume top {len(df_top)}: R$ {stats['top_n_volume']:,.2f}")
                logger.info(f"   % do volume total: {stats.get('top_n_pct', 0):.2f}%")

            # ===== Top 3 Maiores Tickets =====

            if len(df_top) > 0:
                logger.info(f"")
                logger.info("TOP 3 MAIORES TICKETS MEDIOS:")
                logger.info(f"{'='*70}")

                for idx in range(min(3, len(df_top))):
                    row = df_top.iloc[idx]
                    logger.info(
                        f"  {idx+1}. {row['symbol']:12s} ({row['underlying']:6s} - {row['option_type']:4s}): "
                        f"R$ {row['ticket_medio']:>12,.2f}  "
                        f"[{row['qtdneg']} ops, Vol: R$ {row['voltot']:,.0f}]"
                    )

                logger.info(f"{'='*70}")

            # ===== FASE 3: Geracao de PDF =====

            logger.info(f"")
            logger.info(f"[FASE 3/4] Geracao do relatorio PDF")
            logger.info(f"")

            pdf_path = generate_pdf(df_top, trade_date, config, stats)

            pdf_size_kb = Path(pdf_path).stat().st_size / 1024
            logger.info(f"PDF gerado: {pdf_path} ({pdf_size_kb:.1f} KB)")

            # Validacao: PDF deve ter tamanho razoavel
            if pdf_size_kb < 10:
                raise ValueError(
                    f"PDF muito pequeno ({pdf_size_kb:.1f} KB). "
                    f"Possivel erro na geracao. Verifique logs acima."
                )

            # ===== FASE 4: Envio de E-mail =====

            if config['email']['enabled'] and not args.no_email:
                logger.info(f"")
                logger.info(f"[FASE 4/4] Envio de e-mail")
                logger.info(f"")

                send_email(pdf_path, trade_date, df_top, config['email'], stats)
                logger.info("E-mail enviado com sucesso")

            else:
                logger.info(f"")
                logger.info(f"[FASE 4/4] Envio de e-mail DESABILITADO")
                logger.info(f"  Motivo: {'--no-email flag' if args.no_email else 'email.enabled=false no config'}")

        # ===== Sucesso! =====

        logger.info(f"")
        logger.info(f"{'#'*70}")
        logger.info("EXECUCAO CONCLUIDA COM SUCESSO")
        logger.info(f"{'#'*70}")
        timestamp = datetime.now()
        duration = time.perf_counter() - start_time
        logger.info(f"Timestamp: {timestamp.isoformat()}")
        logger.info(f"Duracao: {duration:.1f}s")
        logger.info(f"{'#'*70}")
        logger.info(f"")

    except KeyboardInterrupt:
        logger.warning(f"\n Execucao interrompida pelo usuario (Ctrl+C)")
        exit_code = 130  # Padrao Unix para SIGINT

    except Exception as e:
        exit_code = 1

        logger.error(f"")
        logger.error(f"{'!'*70}")
        logger.error(f" ERRO FATAL")
        logger.error(f"{'!'*70}")
        logger.exception(f"{e}")
        logger.error(f"{'!'*70}")
        logger.error(f"")

        # ===== Tenta Enviar E-mail de Alerta =====

        try:
            config = load_config()

            if config['email'].get('send_failure_alerts', True):
                logger.info(f"Tentando enviar e-mail de alerta...")
                send_failure_alert(str(e), config['email'])
                logger.info("E-mail de alerta enviado")

        except Exception as alert_error:
            logger.error(f"Nao foi possivel enviar e-mail de alerta: {alert_error}")

    return exit_code


# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == '__main__':
    sys.exit(main())
