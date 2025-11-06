"""
Calculo de dias uteis da B3 considerando feriados nacionais e especificos.

Este modulo calcula o ultimo dia util da B3, levando em conta:
- Fins de semana (sabado e domingo)
- Feriados nacionais brasileiros (via holidays.Brazil)
- Feriados especificos da B3 (config/b3_holidays.yaml)
"""

from datetime import datetime, timedelta, date
from holidays import Brazil
import yaml
from pathlib import Path
from typing import Set
import logging

logger = logging.getLogger(__name__)


def load_b3_holidays() -> Set[date]:
    """
    Carrega feriados especificos da B3 do arquivo YAML.

    Returns:
        Set com datas dos feriados especificos da B3

    Notes:
        - Retorna set vazio se arquivo nao existir
        - Ignora datas invalidas com warning
    """
    holidays_file = Path('config/b3_holidays.yaml')

    if not holidays_file.exists():
        logger.warning(f"Arquivo de feriados B3 nao encontrado: {holidays_file}")
        return set()

    try:
        with open(holidays_file, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)

        if not data or 'b3_specific_holidays' not in data:
            logger.warning("Chave 'b3_specific_holidays' nao encontrada no YAML")
            return set()

        b3_holidays = set()

        for date_str in data['b3_specific_holidays']:
            try:
                holiday_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                b3_holidays.add(holiday_date)
            except ValueError as e:
                logger.warning(f"Data invalida ignorada: {date_str} - {e}")

        logger.info(f"Feriados especificos B3 carregados: {len(b3_holidays)}")
        return b3_holidays

    except Exception as e:
        logger.error(f"Erro ao carregar feriados B3: {e}")
        return set()


def is_business_day(check_date: date, br_holidays: Brazil = None,
                    b3_specific: Set[date] = None) -> bool:
    """
    Verifica se uma data e dia util da B3.

    Args:
        check_date: Data para verificar
        br_holidays: Objeto holidays.Brazil (opcional, sera criado se None)
        b3_specific: Set de feriados especificos B3 (opcional)

    Returns:
        True se e dia util, False caso contrario
    """
    # Cria objetos se nao fornecidos
    if br_holidays is None:
        br_holidays = Brazil(state='SP')  # B3 em Sao Paulo

    if b3_specific is None:
        b3_specific = load_b3_holidays()

    # Verifica fim de semana (sabado=5, domingo=6)
    if check_date.weekday() >= 5:
        return False

    # Verifica feriados nacionais
    if check_date in br_holidays:
        return False

    # Verifica feriados especificos B3
    if check_date in b3_specific:
        return False

    return True


def get_last_business_day(reference_date: date = None) -> date:
    """
    Retorna o ultimo dia util da B3 antes de reference_date.

    Considera:
    - Fins de semana (sabado e domingo)
    - Feriados nacionais brasileiros (holidays.Brazil com estado SP)
    - Feriados especificos da B3 (config/b3_holidays.yaml)

    Args:
        reference_date: Data de referencia (default: hoje)

    Returns:
        Data do ultimo dia util

    Examples:
        >>> # Se hoje e segunda-feira util, retorna sexta anterior
        >>> get_last_business_day()
        datetime.date(2024, 1, 5)

        >>> # Pode fornecer data especifica
        >>> get_last_business_day(date(2024, 1, 10))
        datetime.date(2024, 1, 9)
    """
    if reference_date is None:
        reference_date = datetime.now().date()

    logger.info(f"Calculando ultimo dia util antes de {reference_date}")

    # Carrega feriados
    br_holidays = Brazil(state='SP')  # B3 em Sao Paulo
    b3_specific = load_b3_holidays()

    all_holidays = set(br_holidays.keys()) | b3_specific

    # Comeca do dia anterior
    current = reference_date - timedelta(days=1)

    # Retrocede ate achar dia util
    max_iterations = 30  # Seguranca: no maximo 30 dias atras
    iterations = 0

    while not is_business_day(current, br_holidays, b3_specific):
        current -= timedelta(days=1)
        iterations += 1

        if iterations >= max_iterations:
            raise RuntimeError(
                f"Nao foi possivel encontrar dia util apos {max_iterations} "
                f"iteracoes a partir de {reference_date}"
            )

    logger.info(f"Ultimo dia util encontrado: {current} ({current.strftime('%A, %d/%m/%Y')})")

    # Log se teve que retroceder muito
    days_back = (reference_date - current).days
    if days_back > 5:
        logger.warning(
            f"Ultimo dia util esta {days_back} dias atras. "
            f"Pode haver feriado prolongado ou problema no calendario."
        )

    return current


def get_next_business_day(reference_date: date = None) -> date:
    """
    Retorna o proximo dia util da B3 apos reference_date.

    Args:
        reference_date: Data de referencia (default: hoje)

    Returns:
        Data do proximo dia util
    """
    if reference_date is None:
        reference_date = datetime.now().date()

    logger.info(f"Calculando proximo dia util apos {reference_date}")

    br_holidays = Brazil(state='SP')
    b3_specific = load_b3_holidays()

    # Comeca do dia seguinte
    current = reference_date + timedelta(days=1)

    max_iterations = 30
    iterations = 0

    while not is_business_day(current, br_holidays, b3_specific):
        current += timedelta(days=1)
        iterations += 1

        if iterations >= max_iterations:
            raise RuntimeError(
                f"Nao foi possivel encontrar dia util apos {max_iterations} "
                f"iteracoes a partir de {reference_date}"
            )

    logger.info(f"Proximo dia util encontrado: {current} ({current.strftime('%A, %d/%m/%Y')})")

    return current


def count_business_days(start_date: date, end_date: date) -> int:
    """
    Conta o numero de dias uteis entre duas datas (inclusive).

    Args:
        start_date: Data inicial
        end_date: Data final

    Returns:
        Numero de dias uteis no intervalo
    """
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    br_holidays = Brazil(state='SP')
    b3_specific = load_b3_holidays()

    count = 0
    current = start_date

    while current <= end_date:
        if is_business_day(current, br_holidays, b3_specific):
            count += 1
        current += timedelta(days=1)

    return count


# ==============================================================================
# TESTES RAPIDOS (executar com: python -m src.python.business_days)
# ==============================================================================

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )

    print("=" * 70)
    print("TESTES - Modulo de Dias Uteis B3")
    print("=" * 70)

    # Teste 1: Ultimo dia util
    print("\n[TESTE 1] Ultimo dia util:")
    last_bd = get_last_business_day()
    print(f"  Resultado: {last_bd} ({last_bd.strftime('%A, %d de %B de %Y')})")

    # Teste 2: Proximo dia util
    print("\n[TESTE 2] Proximo dia util:")
    next_bd = get_next_business_day()
    print(f"  Resultado: {next_bd} ({next_bd.strftime('%A, %d de %B de %Y')})")

    # Teste 3: Verificar dia especifico
    print("\n[TESTE 3] Verificacoes de dias especificos:")
    test_dates = [
        date(2024, 1, 1),   # Ano novo (feriado)
        date(2024, 1, 2),   # Dia util
        date(2024, 11, 20), # Consciencia Negra SP (se configurado)
    ]

    for test_date in test_dates:
        is_bd = is_business_day(test_date)
        status = " DIA UTIL" if is_bd else " NAO UTIL"
        print(f"  {test_date} ({test_date.strftime('%A')}): {status}")

    # Teste 4: Contagem de dias uteis
    print("\n[TESTE 4] Dias uteis em janeiro de 2024:")
    jan_start = date(2024, 1, 1)
    jan_end = date(2024, 1, 31)
    bd_count = count_business_days(jan_start, jan_end)
    print(f"  {bd_count} dias uteis entre {jan_start} e {jan_end}")

    print("\n" + "=" * 70)
