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
echo Descargando imágenes Docker reales de Stackable...
docker pull docker.stackable.tech/stackable/commons-operator:24.11.0
docker pull docker.stackable.tech/stackable/secret-operator:24.11.0
docker pull docker.stackable.tech/stackable/listener-operator:24.11.0
docker pull docker.stackable.tech/stackable/zookeeper-operator:24.11.0
docker pull docker.stackable.tech/stackable/nifi-operator:24.11.0
docker pull apache/nifi:2.6.0

echo.
echo Retagueando imágenes para MTR Telekom...
echo =========================================

REM Retagear imágenes principales - usando las rutas exactas del values.yaml
docker tag docker.stackable.tech/stackable/commons-operator:24.11.0 %MTR_REGISTRY%/%MTR_PROJECT%/stackable/commons-operator:24.11.0
docker tag docker.stackable.tech/stackable/secret-operator:24.11.0 %MTR_REGISTRY%/%MTR_PROJECT%/stackable/secret-operator:24.11.0
docker tag docker.stackable.tech/stackable/listener-operator:24.11.0 %MTR_REGISTRY%/%MTR_PROJECT%/stackable/listener-operator:24.11.0
docker tag docker.stackable.tech/stackable/zookeeper-operator:24.11.0 %MTR_REGISTRY%/%MTR_PROJECT%/stackable/zookeeper-operator:24.11.0
docker tag docker.stackable.tech/stackable/nifi-operator:24.11.0 %MTR_REGISTRY%/%MTR_PROJECT%/stackable/nifi-operator:24.11.0
docker tag apache/nifi:2.6.0 %MTR_REGISTRY%/%MTR_PROJECT%/nifi:2.6.0

echo.
echo Subiendo imágenes a MTR Telekom...
echo ==================================

REM Subir imágenes principales - usando las rutas exactas del values.yaml
docker push %MTR_REGISTRY%/%MTR_PROJECT%/stackable/commons-operator:24.11.0
docker push %MTR_REGISTRY%/%MTR_PROJECT%/stackable/secret-operator:24.11.0
docker push %MTR_REGISTRY%/%MTR_PROJECT%/stackable/listener-operator:24.11.0
docker push %MTR_REGISTRY%/%MTR_PROJECT%/stackable/zookeeper-operator:24.11.0
docker push %MTR_REGISTRY%/%MTR_PROJECT%/stackable/nifi-operator:24.11.0
docker push %MTR_REGISTRY%/%MTR_PROJECT%/nifi:2.6.0

echo.
echo ¡Migración completada!
echo ======================
echo Todas las imágenes han sido descargadas, retagueadas y subidas a MTR Telekom.
echo Registry: %MTR_REGISTRY%
echo Project: %MTR_PROJECT%
echo.
echo Rutas de imágenes migradas:
echo - %MTR_REGISTRY%/%MTR_PROJECT%/stackable/commons-operator:24.11.0
echo - %MTR_REGISTRY%/%MTR_PROJECT%/stackable/secret-operator:24.11.0
echo - %MTR_REGISTRY%/%MTR_PROJECT%/stackable/listener-operator:24.11.0
echo - %MTR_REGISTRY%/%MTR_PROJECT%/stackable/zookeeper-operator:24.11.0
echo - %MTR_REGISTRY%/%MTR_PROJECT%/stackable/nifi-operator:24.11.0
echo - %MTR_REGISTRY%/%MTR_PROJECT%/nifi:2.6.0
