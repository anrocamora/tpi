@echo off
REM Script para construir y publicar la imagen alpine-git-curl-jq en MTR

echo ================================================
echo Construyendo imagen alpine-git-curl-jq:1.0.0
echo ================================================

REM Definir variables
set IMAGE_NAME=alpine-git-curl-jq
set IMAGE_TAG=1.0.0
set REGISTRY=mtr.devops.telekom.de/genomica
set FULL_IMAGE=%REGISTRY%/%IMAGE_NAME%:%IMAGE_TAG%

REM Construir la imagen
echo.
.\..\setup-mtr-access.bat
echo [1/3] Construyendo imagen Docker...
docker build -t %IMAGE_NAME%:%IMAGE_TAG% -f Dockerfile .

if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Fallo al construir la imagen
    exit /b 1
)

echo.
echo [2/3] Etiquetando imagen para MTR...
docker tag %IMAGE_NAME%:%IMAGE_TAG% %FULL_IMAGE%

if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Fallo al etiquetar la imagen
    exit /b 1
)

REM Subir la imagen al registro
echo.
echo [3/3] Subiendo imagen a %REGISTRY%...
docker push %FULL_IMAGE%

if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Fallo al subir la imagen al registro
    echo NOTA: Asegurate de estar autenticado con: docker login %REGISTRY%
    exit /b 1
)

echo.
echo ================================================
echo EXITO: Imagen publicada correctamente
echo ================================================
echo.
echo Imagen disponible en: %FULL_IMAGE%
echo.
echo Para usarla en otros lugares:
echo   docker pull %FULL_IMAGE%
echo.

exit /b 0

