﻿# Script para generar truststore.jks y keystore.jks desde los secrets de Kubernetes
# Configuración de Kafka con Strimzi en namespace tpi

$ErrorActionPreference = "Stop"

$NAMESPACE = "tpi"
$CLUSTER_NAME = "kafka-tls"
$USER_NAME = "kafka-tls-client-jks"
$KEYSTORE_PASSWORD = ""  # Set password as parameter

Write-Host "=== Generación de Certificados para Kafka TLS ===" -ForegroundColor Green
Write-Host ""

# Verificar que los secrets existen
Write-Host "[0/4] Verificando secrets..." -ForegroundColor Yellow
try {
    kubectl get secret "$CLUSTER_NAME-cluster-ca-cert" -n $NAMESPACE | Out-Null
    Write-Host "   ✓ Secret cluster-ca-cert encontrado" -ForegroundColor Green
} catch {
    Write-Host "   ✗ Secret cluster-ca-cert NO encontrado" -ForegroundColor Red
    exit 1
}

try {
    kubectl get secret $USER_NAME -n $NAMESPACE | Out-Null
    Write-Host "   ✓ Secret $USER_NAME encontrado" -ForegroundColor Green
} catch {
    Write-Host "   ✗ Secret $USER_NAME NO encontrado" -ForegroundColor Red
    Write-Host "   Ejecuta: kubectl get kafkauser $USER_NAME -n $NAMESPACE" -ForegroundColor Yellow
    exit 1
}

# 1. Generar truststore.jks
Write-Host "[1/4] Generando truststore.jks..." -ForegroundColor Yellow

kubectl -n $NAMESPACE get secret "$CLUSTER_NAME-cluster-ca-cert" -o jsonpath='{.data.ca\.crt}' | `
    ForEach-Object { [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($_)) } | `
    Out-File -Encoding ASCII ca.crt

if (-not (Test-Path ca.crt) -or (Get-Item ca.crt).Length -eq 0) {
    Write-Host "   ✗ Error al extraer ca.crt" -ForegroundColor Red
    exit 1
}

# Eliminar truststore anterior si existe
if (Test-Path truststore.jks) {
    Remove-Item truststore.jks -Force
}

keytool -import -trustcacerts -alias strimzi-ca -file ca.crt -keystore truststore.jks -storepass $KEYSTORE_PASSWORD -noprompt

if (Test-Path truststore.jks) {
    Write-Host "   ✓ truststore.jks creado correctamente" -ForegroundColor Green
} else {
    Write-Host "   ✗ Error al crear truststore.jks" -ForegroundColor Red
    exit 1
}

# 2. Extraer certificado del cliente
Write-Host "[2/4] Extrayendo certificado del cliente..." -ForegroundColor Yellow

kubectl -n $NAMESPACE get secret $USER_NAME -o jsonpath='{.data.user\.p12}' | `
    ForEach-Object { [System.Convert]::FromBase64String($_) } | `
    Set-Content -Encoding Byte -Path client.p12

if (-not (Test-Path client.p12) -or (Get-Item client.p12).Length -eq 0) {
    Write-Host "   ✗ Error al extraer client.p12" -ForegroundColor Red
    Write-Host "   El secret $USER_NAME no tiene el campo user.p12" -ForegroundColor Yellow
    exit 1
}

Write-Host "   ✓ client.p12 extraído correctamente" -ForegroundColor Green

# 3. Extraer contraseña del PKCS12
Write-Host "[3/4] Extrayendo contraseña del PKCS12..." -ForegroundColor Yellow

kubectl -n $NAMESPACE get secret $USER_NAME -o jsonpath='{.data.user\.password}' | `
    ForEach-Object { [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($_)) } | `
    Out-File -Encoding ASCII -NoNewline client.p12.pass

if (-not (Test-Path client.p12.pass) -or (Get-Item client.p12.pass).Length -eq 0) {
    Write-Host "   ✗ Error al extraer contraseña" -ForegroundColor Red
    exit 1
}

$p12pass = Get-Content client.p12.pass
Write-Host "   ✓ Contraseña extraída: $p12pass" -ForegroundColor Green

# 4. Generar keystore.jks
Write-Host "[4/4] Generando keystore.jks..." -ForegroundColor Yellow

# Eliminar keystore anterior si existe
if (Test-Path keystore.jks) {
    Remove-Item keystore.jks -Force
}

keytool -importkeystore -srckeystore client.p12 -srcstoretype PKCS12 -srcstorepass $p12pass `
    -destkeystore keystore.jks -deststoretype JKS -deststorepass $KEYSTORE_PASSWORD -destkeypass $KEYSTORE_PASSWORD -noprompt

if (Test-Path keystore.jks) {
    Write-Host "   ✓ keystore.jks creado correctamente" -ForegroundColor Green
} else {
    Write-Host "   ✗ Error al crear keystore.jks" -ForegroundColor Red
    exit 1
}

# Verificar contenido del keystore
Write-Host ""
Write-Host "=== Verificación de certificados ===" -ForegroundColor Green
Write-Host "Contenido del truststore:" -ForegroundColor Cyan
keytool -list -keystore truststore.jks -storepass $KEYSTORE_PASSWORD

Write-Host ""
Write-Host "Contenido del keystore:" -ForegroundColor Cyan
keytool -list -keystore keystore.jks -storepass $KEYSTORE_PASSWORD

# Limpiar archivo temporal ca.crt
Remove-Item ca.crt -ErrorAction SilentlyContinue

# Resumen
Write-Host ""
Write-Host "=== Resumen de archivos generados ===" -ForegroundColor Green
Get-ChildItem | Where-Object { $_.Extension -in '.jks','.p12','.pass' } | Format-Table Name, Length, LastWriteTime

Write-Host ""
Write-Host "✓ Certificados generados exitosamente" -ForegroundColor Green
Write-Host ""
Write-Host "Configuración para tu cliente:" -ForegroundColor Cyan
Write-Host "  bootstrap.servers=pregenomica-app.admon-cfnavarra.es:8002" -ForegroundColor White
Write-Host "  security.protocol=SSL" -ForegroundColor White
Write-Host "  ssl.truststore.location=truststore.jks" -ForegroundColor White
Write-Host "  ssl.truststore.password=$KEYSTORE_PASSWORD" -ForegroundColor White
Write-Host "  ssl.keystore.location=keystore.jks" -ForegroundColor White
Write-Host "  ssl.keystore.password=$KEYSTORE_PASSWORD" -ForegroundColor White
Write-Host "  ssl.key.password=$KEYSTORE_PASSWORD" -ForegroundColor White
Write-Host "  ssl.endpoint.identification.algorithm=" -ForegroundColor White
