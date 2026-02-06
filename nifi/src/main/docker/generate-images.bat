@echo off
echo Descargando y migrando imágenes Docker al repositorio MTR...
echo ========================================================

REM Definir las imágenes identificadas - MTR Telekom
set MTR_REGISTRY=mtr.devops.telekom.de
set MTR_PROJECT=genomica

echo Autenticando con MTR Telekom...
echo %MTR_REGISTRY%
echo Usuario: genomica+genomica

REM Imágenes principales de Apache NiFi
echo Creando imagen personalizada para el MCP de Apache NiFi...
docker build ./mcp -t %MTR_REGISTRY%/%MTR_PROJECT%/nifi-mcp:1.0.0

echo.
echo Subiendo imágenes a MTR Telekom...
echo ==================================

REM Subir imágenes principales
docker push %MTR_REGISTRY%/%MTR_PROJECT%/nifi-mcp:1.0.0

echo.
echo ¡Migración completada!
echo ======================
echo Todas las imágenes han sido descargadas, retagueadas y subidas a MTR Telekom.
echo Registry: %MTR_REGISTRY%
echo Project: %MTR_PROJECT%
echo.
echo RESUMEN DE IMÁGENES MIGRADAS:
echo - Apache NiFi MCP: 1.0.0
echo.
echo Para autenticarte previamente, ejecuta:
echo docker login %MTR_REGISTRY%
echo Usuario: genomica+genomica
echo Contraseña: CIOS07P666TFBENJ05SK27M3S0RVQUKFCR281YAEM8NA09X52SZI1HV8E5VI98V
