@echo off
REM ==============================================================================
REM Setup Task Scheduler para Windows
REM Sistema de Relatórios Diários B3 Opções
REM ==============================================================================
REM
REM Este script cria uma tarefa agendada no Windows Task Scheduler para
REM executar o relatório diariamente.
REM
REM IMPORTANTE: Execute como Administrador!
REM
REM ==============================================================================

setlocal

REM ==============================================================================
REM CONFIGURAÇÕES (AJUSTAR PARA SEU AMBIENTE!)
REM ==============================================================================

set TASK_NAME=B3_Opcoes_Relatorio_Diario
set PROJECT_DIR=%CD%
set PYTHON_EXE=python
set SCRIPT_PATH=src\python\orchestrator.py
set TIME=08:00
set USERNAME=%USERNAME%

REM ==============================================================================
REM Detecção Automática de Python
REM ==============================================================================

echo ==============================================================================
echo Detectando Python...
echo ==============================================================================
echo.

REM Tenta encontrar Python
where python >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    set PYTHON_EXE=python
    goto :python_found
)

where python3 >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    set PYTHON_EXE=python3
    goto :python_found
)

REM Python não encontrado no PATH
echo ✗ Python não encontrado no PATH do sistema
echo.
echo Opções:
echo   1. Instale Python: https://www.python.org/downloads/
echo   2. Adicione Python ao PATH
echo   3. Ou edite este arquivo e defina PYTHON_EXE manualmente
echo      (ex: set PYTHON_EXE=C:\Python311\python.exe)
echo.
pause
exit /b 1

:python_found
echo ✓ Python encontrado: %PYTHON_EXE%

REM Obtém versão do Python
for /f "tokens=2" %%i in ('%PYTHON_EXE% --version 2^>^&1') do set PYTHON_VERSION=%%i
echo   Versão: %PYTHON_VERSION%
echo.

REM ==============================================================================
REM Validações
REM ==============================================================================

echo ==============================================================================
echo Validando Ambiente...
echo ==============================================================================
echo.

REM Verifica se orchestrator.py existe
if not exist "%PROJECT_DIR%\%SCRIPT_PATH%" (
    echo ✗ Script não encontrado: %PROJECT_DIR%\%SCRIPT_PATH%
    echo   Execute este arquivo a partir do diretório raiz do projeto
    pause
    exit /b 1
)
echo ✓ Script encontrado: %SCRIPT_PATH%

REM Verifica se config/settings.yaml existe
if not exist "%PROJECT_DIR%\config\settings.yaml" (
    echo ✗ Configuração não encontrada: config\settings.yaml
    echo   Configure o sistema antes de agendar
    pause
    exit /b 1
)
echo ✓ Configuração encontrada: config\settings.yaml

REM Verifica se .env existe
if not exist "%PROJECT_DIR%\.env" (
    echo ⚠ ATENÇÃO: Arquivo .env não encontrado
    echo   Copie .env.example para .env e configure credenciais
    echo   A tarefa será criada, mas falhará sem credenciais!
    echo.
    set /p CONTINUE="Continuar mesmo assim? (S/N): "
    if /i not "%CONTINUE%"=="S" exit /b 1
) else (
    echo ✓ Credenciais encontradas: .env
)

echo.

REM ==============================================================================
REM Sumário da Configuração
REM ==============================================================================

echo ==============================================================================
echo Sumário da Configuração
echo ==============================================================================
echo.
echo Nome da tarefa:  %TASK_NAME%
echo Diretório:       %PROJECT_DIR%
echo Python:          %PYTHON_EXE% (%PYTHON_VERSION%)
echo Script:          %SCRIPT_PATH%
echo Horário:         %TIME% (diariamente)
echo Usuário:         %USERNAME%
echo.

set /p CONFIRM="Confirma criação da tarefa? (S/N): "
if /i not "%CONFIRM%"=="S" (
    echo Operação cancelada.
    pause
    exit /b 0
)

echo.

REM ==============================================================================
REM Remove Tarefa Existente (se houver)
REM ==============================================================================

echo ==============================================================================
echo Verificando Tarefas Existentes...
echo ==============================================================================
echo.

schtasks /query /tn "%TASK_NAME%" >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo Tarefa "%TASK_NAME%" já existe. Removendo...
    schtasks /delete /tn "%TASK_NAME%" /f
    if %ERRORLEVEL% EQU 0 (
        echo ✓ Tarefa antiga removida
    ) else (
        echo ✗ Erro ao remover tarefa antiga
        echo   Execute como Administrador
        pause
        exit /b 1
    )
) else (
    echo ✓ Nenhuma tarefa existente encontrada
)

echo.

REM ==============================================================================
REM Cria Nova Tarefa
REM ==============================================================================

echo ==============================================================================
echo Criando Nova Tarefa...
echo ==============================================================================
echo.

schtasks /create ^
    /tn "%TASK_NAME%" ^
    /tr "cmd /c \"cd /d %PROJECT_DIR% && %PYTHON_EXE% %SCRIPT_PATH%\"" ^
    /sc daily ^
    /st %TIME% ^
    /ru "%USERNAME%" ^
    /rl HIGHEST ^
    /f

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ==============================================================================
    echo ✓ Tarefa Criada com Sucesso!
    echo ==============================================================================
    echo.
) else (
    echo.
    echo ==============================================================================
    echo ✗ Erro ao Criar Tarefa
    echo ==============================================================================
    echo.
    echo Possíveis causas:
    echo   - Este script precisa ser executado como Administrador
    echo   - Permissões insuficientes do usuário %USERNAME%
    echo.
    pause
    exit /b 1
)

REM ==============================================================================
REM Configurações Manuais Necessárias
REM ==============================================================================

echo ==============================================================================
echo IMPORTANTE: Configurações Manuais Necessárias
echo ==============================================================================
echo.
echo A tarefa foi criada, mas você DEVE configurar manualmente:
echo.
echo 1. Abra o Agendador de Tarefas:
echo    - Pressione Win+R
echo    - Digite: taskschd.msc
echo    - Pressione Enter
echo.
echo 2. Localize a tarefa: "%TASK_NAME%"
echo.
echo 3. Clique com botão direito ^> Propriedades
echo.
echo 4. Aba "Geral":
echo    ☑ Marque: "Executar independentemente de o usuário estar conectado"
echo    ☑ Marque: "Executar com privilégios mais altos" (se necessário)
echo.
echo 5. Aba "Ações" ^> Editar:
echo    - "Iniciar em": %PROJECT_DIR%
echo    (Muito importante para carregar .env e paths corretamente!)
echo.
echo 6. Aba "Configurações":
echo    ☐ Desmarque: "Parar a tarefa se ela for executada por mais de"
echo    (Permite que a tarefa rode o tempo que for necessário)
echo.
echo 7. Clique "OK" e forneça sua senha se solicitado
echo.
echo ==============================================================================
REM Teste a Tarefa
echo ==============================================================================
echo.
echo Para testar a tarefa SEM esperar até %TIME%:
echo.
echo   1. No Agendador de Tarefas, localize "%TASK_NAME%"
echo   2. Clique com botão direito ^> Executar
echo   3. Verifique logs em: logs\execution.log
echo   4. Verifique relatório em: output\reports\
echo.
echo ==============================================================================
REM Desinstalação
echo ==============================================================================
echo.
echo Para REMOVER a tarefa agendada:
echo.
echo   schtasks /delete /tn "%TASK_NAME%" /f
echo.
echo Ou use o Agendador de Tarefas (taskschd.msc) e delete manualmente.
echo.
echo ==============================================================================

pause
