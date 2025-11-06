# 📊 Relatório Diário de Grandes Operações de Opções B3

Sistema automatizado robusto para identificar e reportar grandes operações ("boletas grandes") no mercado de opções da B3.

![Status](https://img.shields.io/badge/status-production--ready-brightgreen)
![Python](https://img.shields.io/badge/python-3.8+-blue)
![R](https://img.shields.io/badge/R-4.0+-blue)
![Platform](https://img.shields.io/badge/platform-windows-lightgrey)

---

## 🎯 Características

- ✅ **Download automatizado** de dados COTAHIST (B3) via pacote `rb3`
- ✅ **Identificação inteligente** de operações com alto ticket médio
- ✅ **Filtros configuráveis** (volume mínimo, máx. operações)
- ✅ **PDF profissional** com gráficos e tabelas (formatação pt-BR)
- ✅ **Envio automático por e-mail** (Gmail SMTP com SSL)
- ✅ **Retry robusto** para atrasos da B3 (backoff exponencial)
- ✅ **Formatação pt-BR** (R$ 1.234,56)
- ✅ **Lock por data** (evita execuções concorrentes)
- ✅ **Logs rotativos** (30 dias de histórico)
- ✅ **Calendário B3** (feriados nacionais + específicos)
- ✅ **CLI completo** (--date, --force, --no-email, --debug)
- ✅ **Observabilidade** (métricas detalhadas nos logs)
- ✅ **Fail-safe** (alertas por e-mail em caso de falha)

---

## 📋 Pré-requisitos

### Software Necessário

1. **Python 3.8+**
   - Download: https://www.python.org/downloads/
   - ✅ Adicionar ao PATH durante instalação

2. **R 4.0+**
   - Download: https://cran.r-project.org/bin/windows/base/
   - ✅ Adicionar ao PATH durante instalação

3. **Git** (opcional, para clonar o repositório)
   - Download: https://git-scm.com/downloads

### Conta Gmail

- E-mail Gmail com **autenticação de 2 fatores** ativada
- **App Password** criado (instruções abaixo)

---

## 🚀 Setup Rápido (15 minutos)

### 1. Clone o Repositório

```bash
git clone <url-do-repo>
cd relatorio-opcoes-b3
```

Ou baixe o ZIP e extraia.

### 2. Instale Dependências R

```bash
Rscript install_r_deps.R
```

Isso instalará:
- `rb3` (download dados B3)
- `arrow` (leitura/escrita Parquet)
- `dplyr`, `lubridate` (manipulação de dados)

### 3. Instale Dependências Python

**Recomendado: Use ambiente virtual**

```bash
# Criar ambiente virtual
python -m venv venv

# Ativar (Windows)
venv\Scripts\activate

# Ativar (Linux/Mac)
source venv/bin/activate

# Instalar dependências
pip install -r requirements.txt
```

### 4. Configure Credenciais

#### a) Crie App Password do Gmail

1. Acesse: https://myaccount.google.com/apppasswords
2. Faça login (pode pedir senha novamente)
3. Nome do app: "Relatorio B3"
4. Clique em "Criar"
5. **Copie a senha** de 16 caracteres (ex: `xxxx xxxx xxxx xxxx`)

#### b) Configure .env

```bash
# Copiar template
copy .env.example .env    # Windows
cp .env.example .env      # Linux/Mac

# Editar .env com seu editor favorito
notepad .env              # Windows
nano .env                 # Linux/Mac
```

Preencha:
```
GMAIL_USER=seu_email@gmail.com
GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx
```

**IMPORTANTE:** Nunca commite o arquivo `.env` no Git!

### 5. Configure Parâmetros

Edite `config/settings.yaml`:

```yaml
filters:
  max_operations: 5              # Máx. ops para "boleta grande"
  min_financial_volume: 100000   # Volume mínimo (R$ 100k)
  top_n: 20                      # Quantas listar

email:
  recipients:
    - "seu_destino@gmail.com"    # ← ALTERAR!
```

### 6. Teste o Sistema

```bash
# Teste com data específica (não envia e-mail)
python src/python/orchestrator.py --date=2024-01-10 --no-email

# Se funcionou, verifique:
# - data/processed/cotahist_2024-01-10.parquet
# - output/reports/relatorio_2024-01-10.pdf
# - logs/execution.log
```

### 7. Agende Execução Diária (Windows)

```bash
# Execute como Administrador
setup_task_scheduler.bat
```

Siga as instruções na tela. **Importante:** Configure manualmente:
1. Abra Task Scheduler (`taskschd.msc`)
2. Localize tarefa "B3_Opcoes_Relatorio_Diario"
3. Propriedades → Aba "Geral":
   - ☑ "Executar independentemente de o usuário estar conectado"
4. Aba "Ações" → Editar:
   - "Iniciar em": `C:\caminho\do\projeto`

---

## 📖 Uso

### Execução Manual

```bash
# Último dia útil (padrão)
python src/python/orchestrator.py

# Data específica
python src/python/orchestrator.py --date=2024-01-10

# Força re-download (ignora cache)
python src/python/orchestrator.py --date=2024-01-10 --force

# Gera PDF sem enviar e-mail
python src/python/orchestrator.py --no-email

# Modo debug (mais verboso)
python src/python/orchestrator.py --debug

# Ver ajuda
python src/python/orchestrator.py --help
```

### Estrutura do Relatório PDF

1. **Capa**: Sumário executivo, filtros aplicados
2. **Tabela**: Top N operações (símbolo, ativo, tipo, preço, volume, nº ops, ticket médio, % do dia)
3. **Gráfico de Barras**: Ticket médio por operação
4. **Gráfico de Pizza**: Distribuição por ativo-objeto
5. **Gráfico de Dispersão**: Volume vs. Nº operações

### E-mail Enviado

- **Assunto**: "Relatório Diário - Grandes Operações de Opções B3 - DD/MM/YYYY"
- **Corpo HTML**: Resumo executivo + Top 5 em tabela
- **Anexo**: PDF completo

---

## 🔧 Configuração Avançada

### `config/settings.yaml`

```yaml
filters:
  max_operations: 5              # Ajustar conforme necessidade
  min_financial_volume: 100000   # Pode aumentar para filtrar mais
  top_n: 20                      # Quantas operações exibir

scheduling:
  target_time: "08:00"           # Horário (sugerido: 08:20-08:30)
  retries: 6                     # Tentativas se B3 atrasar
  retry_interval_minutes: 10     # Intervalo (backoff aplicado)

email:
  enabled: true                  # false = não envia e-mail
  send_failure_alerts: true      # Enviar alerta se falhar

paths:
  rscript: "Rscript"  # Ou caminho completo no Windows
```

### `config/b3_holidays.yaml`

Adicione feriados específicos da B3:

```yaml
b3_specific_holidays:
  - '2025-11-20'  # Consciência Negra SP
  # Adicionar outros conforme calendário B3
```

---

## 📂 Estrutura do Projeto

```
relatorio-opcoes-b3/
├── config/
│   ├── settings.yaml          # Configuração principal
│   ├── b3_holidays.yaml       # Feriados B3
│   └── .env                   # Credenciais (NÃO commitar!)
├── src/
│   ├── r_scripts/
│   │   └── download_b3_data.R # Download B3 (rb3)
│   └── python/
│       ├── orchestrator.py    # Orquestração central
│       ├── business_days.py   # Calendário B3
│       ├── analyzer.py        # Filtros e métricas
│       ├── report_pdf.py      # Geração de PDF
│       └── mailer.py          # Envio de e-mail
├── data/
│   ├── raw/                   # Cache ZIPs (opcional)
│   └── processed/             # Parquets gerados
├── output/
│   └── reports/               # PDFs gerados
├── logs/
│   └── execution.log          # Logs (rotação 30 dias)
├── tests/
│   └── test_analyzer.py       # Testes unitários
├── requirements.txt           # Deps Python
├── install_r_deps.R           # Deps R
├── setup_task_scheduler.bat  # Agendamento Windows
└── README.md                  # Este arquivo
```

---

## 🧪 Testes

### Testes Unitários

```bash
# Com pytest (recomendado)
pip install pytest
pytest tests/ -v

# Ou execução direta
python tests/test_analyzer.py
```

### Teste End-to-End

```bash
# Testa fluxo completo com data histórica
python src/python/orchestrator.py --date=2024-01-10 --no-email
```

Verifique:
- ✅ Parquet criado em `data/processed/`
- ✅ PDF criado em `output/reports/`
- ✅ Sem erros em `logs/execution.log`

### Teste de E-mail

```bash
# Envia e-mail de teste (para si mesmo)
python src/python/mailer.py
```

---

## 🐛 Troubleshooting

### Erro: "Rscript não encontrado"

**Solução:**
1. Verifique se R está instalado: `R --version`
2. Adicione R ao PATH do Windows
3. Ou configure em `settings.yaml`:
   ```yaml
   paths:
     rscript: "C:\\Program Files\\R\\R-4.3.1\\bin\\Rscript.exe"
   ```

### Erro: "GMAIL_APP_PASSWORD não encontrado"

**Solução:**
1. Verifique se `.env` existe (não `.env.example`)
2. Certifique-se de que está na raiz do projeto
3. Formato correto:
   ```
   GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx
   ```
   (sem aspas, pode ter ou não espaços)

### Erro: "Nenhuma opção encontrada"

**Causas possíveis:**
1. Data é feriado/fim de semana → Use dia útil
2. Dados B3 ainda não disponíveis → Execute mais tarde (ex: 09:00)
3. Erro na conexão → Verifique internet

**Solução:**
```bash
# Teste com data conhecida (passada)
python src/python/orchestrator.py --date=2024-01-10
```

### Erro: "PDF muito pequeno"

**Causas:**
- Matplotlib sem backend Agg
- Nenhum dado para gráficos

**Solução:**
Verifique logs em `logs/execution.log` para detalhes.

### Task Scheduler não executa

**Checklist:**
1. ☑ "Executar independentemente de usuário conectado" marcado
2. ☑ "Iniciar em" configurado para diretório do projeto
3. ☑ Usuário tem permissões
4. ☑ `.env` está no diretório raiz
5. ☑ Teste manual funciona

**Debug:**
- Task Scheduler → Histórico da tarefa
- `logs/execution.log`

---

## 📊 Métricas e Observabilidade

### Logs

Todos os logs em `logs/execution.log` (rotação automática 30 dias).

**Formato:**
```
2025-01-10 08:00:00 [INFO] Início da execução
2025-01-10 08:00:05 [INFO] ✓ Parquet já existe
2025-01-10 08:00:10 [INFO] Top 20 selecionados
2025-01-10 08:00:15 [INFO] ✓ PDF gerado: 156.3 KB
2025-01-10 08:00:20 [INFO] ✓ E-mail enviado
2025-01-10 08:00:25 [INFO] ✅ Execução concluída
```

### Estatísticas Logadas

- Total de opções processadas
- Operações após filtros
- Volume total do dia (R$)
- Top 3 maiores tickets
- Tempo de execução
- Erros e warnings

---

## 🔒 Segurança

### Credenciais

- ✅ `.env` no `.gitignore` (nunca commitado)
- ✅ App Password (não senha real do Gmail)
- ✅ SSL/TLS para SMTP
- ✅ Variáveis de ambiente (não hardcoded)

### Dados

- ✅ Dados locais (não enviados a terceiros)
- ✅ PDFs armazenados localmente
- ✅ Logs não contêm dados sensíveis

---

## 🚀 Próximos Passos (Opcional)

### Deploy na Nuvem

**AWS Lambda + EventBridge:**
1. Containerizar (Docker)
2. Upload para ECR
3. Lambda com 10GB de memória
4. EventBridge cron: `cron(0 11 ? * MON-FRI *)` (08:00 BRT = 11:00 UTC)

**Google Cloud Functions + Cloud Scheduler:**
Similar ao AWS, mas com Cloud Scheduler.

### Melhorias Futuras

- [ ] Dashboard web com Streamlit
- [ ] Banco de dados (PostgreSQL) para histórico
- [ ] Análise de tendências (opções recorrentes)
- [ ] Alertas via Telegram/WhatsApp
- [ ] API REST para consultas
- [ ] Machine Learning (previsão de grandes ordens)

---

## 🤝 Contribuindo

Sugestões e melhorias são bem-vindas!

1. Fork o projeto
2. Crie um branch (`git checkout -b feature/melhoria`)
3. Commit suas mudanças (`git commit -m 'Adiciona nova feature'`)
4. Push para o branch (`git push origin feature/melhoria`)
5. Abra um Pull Request

---

## 📝 Licença

Este projeto é fornecido "como está", sem garantias.

Use por sua conta e risco. O autor não se responsabiliza por decisões de investimento baseadas neste sistema.

---

## 📧 Suporte

**Problemas ou dúvidas?**

1. Verifique este README
2. Consulte `logs/execution.log`
3. Abra uma issue no GitHub

---

## 🙏 Agradecimentos

- **B3**: Dados públicos COTAHIST
- **rb3**: Excelente pacote R para dados B3
- **Comunidade Python/R**: Bibliotecas open-source

---

## 📚 Referências

- [B3 - Dados Históricos](https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/historico/)
- [Pacote rb3 (R)](https://github.com/ropensci/rb3)
- [COTAHIST - Layout](https://www.b3.com.br/data/files/C8/F3/08/B4/297BE410F816C9E492D828A8/SeriesHistoricas_Layout.pdf)
- [Gmail App Passwords](https://support.google.com/accounts/answer/185833)

---

**Desenvolvido com 🤖 Claude Code + 💡 Expertise Humana**

*Última atualização: 2025-01-05*
