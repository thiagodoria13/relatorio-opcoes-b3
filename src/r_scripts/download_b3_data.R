#!/usr/bin/env Rscript
# ==============================================================================
# Download COTAHIST da B3 usando rb3 (versao 0.0.8) e salva Parquet normalizado
# ==============================================================================
#
# Uso:
#   Rscript download_b3_data.R --date=YYYY-MM-DD
#   Rscript download_b3_data.R YYYY-MM-DD
#
# Responsabilidade: somente ingestao e normalizacao para Parquet. A logica de
# negocio permanece no lado Python.
# ==============================================================================

suppressPackageStartupMessages({
  library(rb3)
  library(arrow)
  library(dplyr)
  library(lubridate)
})

# ==============================================================================
# Parsing robusto dos argumentos de linha de comando
# ==============================================================================

parse_date_arg <- function(args) {
  if (length(args) == 0) {
    stop(
      "Uso: Rscript download_b3_data.R --date=YYYY-MM-DD\n",
      "   ou: Rscript download_b3_data.R YYYY-MM-DD"
    )
  }

  arg <- args[1]
  date_str <- gsub("^--date=", "", arg)

  if (!grepl("^\\d{4}-\\d{2}-\\d{2}$", date_str)) {
    stop(sprintf("Formato invalido: '%s'. Use YYYY-MM-DD (ex: 2024-01-10)", date_str))
  }

  target_date <- tryCatch(
    as.Date(date_str),
    error = function(e) {
      stop(sprintf("Data invalida: '%s'. Erro: %s", date_str, e$message))
    }
  )

  if (target_date > Sys.Date()) {
    warning(sprintf("Data no futuro: %s. Prosseguindo mesmo assim...", date_str))
  }

  target_date
}

# ==============================================================================
# Main
# ==============================================================================

args <- commandArgs(trailingOnly = TRUE)
target_date <- parse_date_arg(args)

cat("=====================================================\n")
cat("R Download Script - COTAHIST B3\n")
cat("=====================================================\n")
cat(sprintf("R home: %s\n", R.home()))
cat(sprintf("R version: %s\n", R.version.string))
cat(sprintf("rb3 version: %s\n", packageVersion("rb3")))
cat(sprintf("arrow version: %s\n", packageVersion("arrow")))
cat(sprintf(
  "Target date: %s (%s)\n",
  target_date,
  format(target_date, "%A, %d de %B de %Y")
))
cat("=====================================================\n\n")

dir.create("data/processed", recursive = TRUE, showWarnings = FALSE)
dir.create("data/raw", recursive = TRUE, showWarnings = FALSE)

target_date_str <- format(target_date, "%Y-%m-%d")
tmp_path <- sprintf("data/processed/.cotahist_%s.parquet.tmp", target_date_str)
final_path <- sprintf("data/processed/cotahist_%s.parquet", target_date_str)

# ==============================================================================
# Download e processamento
# ==============================================================================

tryCatch({
  cat(sprintf("[1/4] Baixando COTAHIST para %s...\n", target_date_str))

  cotahist <- cotahist_get(target_date, "daily")
  cat("[OK] COTAHIST baixado com sucesso\n\n")

  cat("[2/4] Obtendo curva de juros (yc_get)...\n")
  yield_curve <- yc_get(target_date)
  cat("[OK] Yield curve obtida\n\n")

  cat("[3/4] Processando opcoes de acoes...\n")
  options <- cotahist_equity_options_superset(cotahist, yield_curve) %>%
    select(
      trade_date = refdate,
      symbol,
      underlying = symbol.underlying,
      option_type = type,
      maturity_date,
      strike_price = strike,
      qtdneg = transactions_quantity,
      quatot = traded_contracts,
      voltot = volume,
      preult = close,
      premed = average
    ) %>%
    collect()

  cat(sprintf("[OK] %d opcoes de acoes encontradas\n\n", nrow(options)))

  cat("[4/4] Normalizando dados e salvando Parquet...\n")

  options <- options %>%
    mutate(
      trade_date = as.Date(trade_date),
      symbol = as.character(symbol),
      underlying = as.character(underlying),
      option_type = toupper(as.character(option_type)),
      qtdneg = as.integer(qtdneg),
      quatot = as.integer(quatot),
      voltot = as.numeric(voltot),
      preult = as.numeric(preult),
      premed = as.numeric(premed),
      strike_price = as.numeric(strike_price),
      maturity_date = as.Date(maturity_date)
    )

  if (file.exists(tmp_path)) {
    file.remove(tmp_path)
  }

  write_parquet(options, tmp_path, compression = "snappy")
  file.rename(tmp_path, final_path)

  file_size <- file.info(final_path)$size
  file_size_mb <- round(file_size / 1024 / 1024, 2)

  cat("=====================================================\n")
  cat("[OK] SUCESSO - Parquet salvo com sucesso!\n")
  cat("=====================================================\n")
  cat(sprintf("Arquivo: %s\n", final_path))
  cat(sprintf("Linhas: %s\n", format(nrow(options), big.mark = ".", decimal.mark = ",")))
  cat(sprintf("Tamanho: %s MB\n", format(file_size_mb, big.mark = ".", decimal.mark = ",")))
  cat(sprintf("Colunas: %s\n", paste(colnames(options), collapse = ", ")))
  cat("=====================================================\n")

  if (nrow(options) == 0) {
    writeLines(
      c(
        "Aviso: nenhuma opcao encontrada para esta data.",
        "Verifique se o pregrao ocorreu normalmente."
      ),
      con = stderr()
    )
  }

  quit(status = 0)

}, error = function(e) {
  if (file.exists(tmp_path)) {
    file.remove(tmp_path)
  }

  writeLines(rep("X", 50), con = stderr())
  writeLines("ERRO NO DOWNLOAD/PROCESSAMENTO B3", con = stderr())
  writeLines(rep("X", 50), con = stderr())
  writeLines(sprintf("Data solicitada: %s", target_date), con = stderr())
  writeLines(sprintf("Mensagem de erro:\n%s", e$message), con = stderr())
  writeLines("Possiveis solucoes:", con = stderr())
  writeLines("  1. Verifique se a data e um dia util da B3", con = stderr())
  writeLines("  2. Verifique conexao com a internet", con = stderr())
  writeLines("  3. Tente novamente mais tarde (dados podem estar indisponiveis)", con = stderr())
  writeLines("  4. Verifique se os pacotes rb3 e arrow estao instalados corretamente", con = stderr())

  quit(status = 1)
})
