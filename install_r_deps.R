#!/usr/bin/env Rscript
# ==============================================================================
# Instalação de Dependências R - Sistema de Relatórios B3 Opções
# ==============================================================================
#
# Uso:
#   Rscript install_r_deps.R
#
# Este script instala as dependências R necessárias com versões pinadas
# para garantir reprodutibilidade.
#
# ==============================================================================

cat("==============================================================================\n")
cat("Instalando Dependências R\n")
cat("==============================================================================\n\n")

# ==============================================================================
# Instala remotes (para install_version)
# ==============================================================================

if (!requireNamespace("remotes", quietly = TRUE)) {
  cat("Instalando pacote 'remotes'...\n")
  install.packages("remotes", repos = "https://cloud.r-project.org")
} else {
  cat("✓ Pacote 'remotes' já instalado\n")
}

library(remotes)

# ==============================================================================
# Instala Dependências com Versões Pinadas
# ==============================================================================

dependencies <- list(
  list(name = "rb3", version = "0.0.8"),
  list(name = "arrow", version = "14.0.0.2"),
  list(name = "dplyr", version = "1.1.4"),
  list(name = "lubridate", version = "1.9.3")
)

cat("\nInstalando dependências com versões pinadas:\n")
cat("----------------------------------------------------------------------\n")

for (dep in dependencies) {
  name <- dep$name
  version <- dep$version

  cat(sprintf("\n[%s] Versão: %s\n", name, version))

  # Verifica se já está instalado
  if (requireNamespace(name, quietly = TRUE)) {
    installed_version <- as.character(packageVersion(name))

    if (installed_version == version) {
      cat(sprintf("✓ Já instalado com versão correta: %s\n", installed_version))
      next
    } else {
      cat(sprintf("⚠ Versão diferente instalada: %s (alvo: %s)\n",
                  installed_version, version))
      cat("  Atualizando para versão pinada...\n")
    }
  }

  # Instala versão específica
  tryCatch({
    remotes::install_version(
      name,
      version = version,
      repos = "https://cloud.r-project.org",
      upgrade = "never"  # Não atualiza dependências (pode causar conflitos)
    )

    cat(sprintf("✓ %s %s instalado com sucesso\n", name, version))

  }, error = function(e) {
    cat(sprintf("✗ Erro ao instalar %s: %s\n", name, e$message))
    cat("  Tentando instalação sem versão pinada...\n")

    # Fallback: instala versão mais recente
    install.packages(name, repos = "https://cloud.r-project.org")
  })
}

# ==============================================================================
# Validação Final
# ==============================================================================

cat("\n==============================================================================\n")
cat("Validação das Instalações\n")
cat("==============================================================================\n\n")

all_ok <- TRUE

for (dep in dependencies) {
  name <- dep$name
  target_version <- dep$version

  if (requireNamespace(name, quietly = TRUE)) {
    installed_version <- as.character(packageVersion(name))

    if (installed_version == target_version) {
      cat(sprintf("✓ %s: %s (OK)\n", name, installed_version))
    } else {
      cat(sprintf("⚠ %s: %s (esperado: %s)\n", name, installed_version, target_version))
      all_ok <- FALSE
    }
  } else {
    cat(sprintf("✗ %s: NÃO INSTALADO\n", name))
    all_ok <- FALSE
  }
}

# ==============================================================================
# Teste Rápido
# ==============================================================================

cat("\n==============================================================================\n")
cat("Teste Rápido de Funcionalidade\n")
cat("==============================================================================\n\n")

test_passed <- TRUE

# Testa rb3
cat("Testando rb3...\n")
tryCatch({
  library(rb3)
  cat("  ✓ rb3 carregado com sucesso\n")
  cat(sprintf("  Versão: %s\n", packageVersion("rb3")))
}, error = function(e) {
  cat(sprintf("  ✗ Erro ao carregar rb3: %s\n", e$message))
  test_passed <- FALSE
})

# Testa arrow
cat("\nTestando arrow...\n")
tryCatch({
  library(arrow)
  cat("  ✓ arrow carregado com sucesso\n")
  cat(sprintf("  Versão: %s\n", packageVersion("arrow")))
}, error = function(e) {
  cat(sprintf("  ✗ Erro ao carregar arrow: %s\n", e$message))
  test_passed <- FALSE
})

# Testa dplyr
cat("\nTestando dplyr...\n")
tryCatch({
  library(dplyr)
  cat("  ✓ dplyr carregado com sucesso\n")
  cat(sprintf("  Versão: %s\n", packageVersion("dplyr")))
}, error = function(e) {
  cat(sprintf("  ✗ Erro ao carregar dplyr: %s\n", e$message))
  test_passed <- FALSE
})

# Testa lubridate
cat("\nTestando lubridate...\n")
tryCatch({
  library(lubridate)
  cat("  ✓ lubridate carregado com sucesso\n")
  cat(sprintf("  Versão: %s\n", packageVersion("lubridate")))
}, error = function(e) {
  cat(sprintf("  ✗ Erro ao carregar lubridate: %s\n", e$message))
  test_passed <- FALSE
})

# ==============================================================================
# Resultado Final
# ==============================================================================

cat("\n==============================================================================\n")

if (all_ok && test_passed) {
  cat("✅ SUCESSO - Todas as dependências foram instaladas e testadas!\n")
  cat("==============================================================================\n\n")
  cat("Próximos passos:\n")
  cat("  1. Instale dependências Python: pip install -r requirements.txt\n")
  cat("  2. Configure credenciais: cp .env.example .env (e preencha)\n")
  cat("  3. Teste o sistema: python src/python/orchestrator.py --help\n\n")
  quit(status = 0)
} else {
  cat("⚠ ATENÇÃO - Algumas dependências podem não estar corretas\n")
  cat("==============================================================================\n\n")
  cat("Verifique os avisos acima e tente reinstalar se necessário.\n")
  cat("O sistema pode funcionar mesmo com versões diferentes, mas não é garantido.\n\n")
  quit(status = 1)
}
