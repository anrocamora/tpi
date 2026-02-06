# 🚀 Apache Airflow - TSuPreMe Platform

[![T-Systems](https://img.shields.io/badge/T--Systems-TSuPreMe-blue)](https://www.t-systems.com/)
[![Airflow Version](https://img.shields.io/badge/airflow-3.1.0-green)](https://airflow.apache.org/)
[![Helm Chart](https://img.shields.io/badge/helm-chart%201.19.0-blue)](https://helm.sh/)
[![Kubernetes](https://img.shields.io/badge/kubernetes-1.29+-blue)](https://kubernetes.io/)

Despliegue y configuración de **Apache Airflow** en la plataforma **TSuPreMe** de T-Systems para la gestión de flujos de trabajo distribuidos en entornos de salud y genómica.

## 📋 Tabla de Contenidos

- [🏗️ Arquitectura del Proyecto](#️-arquitectura-del-proyecto)
- [🌍 Entornos y Clientes](#-entornos-y-clientes)
- [⚡ Inicio Rápido](#-inicio-rápido)
- [🔧 Pipeline CI/CD](#-pipeline-cicd)
- [🚀 Despliegue Local](#-despliegue-local)
- [📁 Gestión de DAGs](#-gestión-de-dags)
- [📖 Documentación de DAGs](#-documentación-de-dags)
- [⚙️ Configuración por Entorno](#️-configuración-por-entorno)
- [📊 Monitoreo y Observabilidad](#-monitoreo-y-observabilidad)
- [🔐 Seguridad y Accesos](#-seguridad-y-accesos)
- [🛠️ Troubleshooting](#️-troubleshooting)
- [📚 Documentación Técnica](#-documentación-técnica)

## 🏗️ Arquitectura del Proyecto

### Tipo de Proyecto
Este es un **proyecto solo Helm** que despliega Apache Airflow sin build de aplicación propia, diseñado específicamente para la infraestructura TSuPreMe de T-Systems.

### Componentes Principales

```mermaid
graph TB
    subgraph "TSuPreMe Platform"
        subgraph "GitLab CI/CD"
            GIT[GitLab Repository]
            PIPE[Pipeline CI/CD]
        end
        
        subgraph "Harbor Registry"
            HELM[Helm Charts]
        end
        
        subgraph "MTR Registry"
            IMG[Docker Images]
        end
        
        subgraph "CICD Repository"
            CICD_PRIV[CICD Private]
            CICD_PUB[CICD Public Mirror]
        end
        
        subgraph "Kubernetes Clusters"
            subgraph "DEV Cluster"
                ARGO_DEV[ArgoCD DEV]
                DEV[Desarrollo]
            end
            
            subgraph "PRE Cluster"
                ARGO_PRE[ArgoCD PRE]
                PRE[Pre-producción]
            end
            
            subgraph "PRO Cluster"
                ARGO_PRO[ArgoCD PRO]
                PRO[Producción]
            end
        end
        
        subgraph "Apache Airflow Components"
            WEB[Webserver]
            SCHED[Scheduler]
            WORK[Workers]
            DB[(PostgreSQL)]
            REDIS[(Redis)]
        end
    end
    
    GIT --> PIPE
    PIPE --> HELM
    PIPE --> IMG
    PIPE --> CICD_PRIV
    CICD_PRIV --> CICD_PUB
    ARGO_DEV --> CICD_PUB
    ARGO_PRE --> CICD_PUB
    ARGO_PRO --> CICD_PUB
    ARGO_DEV --> DEV
    ARGO_PRE --> PRE
    ARGO_PRO --> PRO
    DEV --> WEB
    PRE --> WEB
    PRO --> WEB
```

### Stack Tecnológico

- **Orquestación**: Kubernetes + Helm 3.x + ArgoCD
- **CI/CD**: GitLab CI/CD
- **Registry Helm**: Harbor (harbor.apps.ocpdes.t-systems.es)
- **Registry Docker**: MTR - Magenta Trusted Registry (https://mtr.devops.telekom.de/repository/genomica)
- **Despliegue Continuo**: ArgoCD
- **CICD Repository**: https://setools.t-systems.es/gitlab/health/genomica/commons/cicd.git
- **Base de Datos**: PostgreSQL con PgBouncer
- **Message Broker**: Redis (CeleryExecutor)
- **Monitoreo**: Prometheus + Grafana + StatsD
- **Logging**: ELK Stack integrado

## 🌍 Entornos y Clientes

### Entornos Soportados

| Entorno | Descripción | Namespace | Recursos |
|---------|-------------|-----------|----------|
| **dev** | Desarrollo y pruebas | `tpi` | Mínimos |
| **pre** | Pre-producción | `tpi` | Medios |
| **pro** | Producción | `tpi` | Completos |

### Clientes Configurados

#### **sns-o** - Sistema Nacional de Salud Navarra
- Configuración específica para procesos de salud
- Integración con sistemas sanitarios
- Cumplimiento normativo GDPR/LOPD

#### **hsc** - Health Service Center
- Procesamiento de datos genómicos
- Análisis de alta performance
- Workflows de bioinformática

#### **otro** - Configuración genérica
- Template base personalizable
- Configuración estándar de Airflow

### Estructura de Configuración

```
src/main/helm/charts/airflow/
├── Chart.yaml                    # Definición del chart
├── values-common.yaml            # Valores comunes
├── helmfile.yaml.gotmpl         # Configuración Helmfile
├── clients/
│   ├── sns-o/                   # Sistema Nacional de Salud
│   │   ├── values-dev.yaml
│   │   ├── values-pre.yaml
│   │   └── values-pro.yaml
│   ├── hsc/                     # Health Service Center
│   │   ├── values-dev.yaml
│   │   ├── values-pre.yaml
│   │   └── values-pro.yaml
│   └── otro/                    # Configuración genérica
│       ├── values-dev.yaml
│       ├── values-pre.yaml
│       └── values-pro.yaml
└── templates/
    ├── deployment.yaml
    ├── service.yaml
    ├── configmap.yaml
    └── ingress.yaml
```

## ⚡ Inicio Rápido

### Prerrequisitos

```bash
# Herramientas necesarias
kubectl version --client  # >= 1.29
helm version              # >= 3.12
helmfile version          # >= 0.155

# Acceso a TSuPreMe
export KUBECONTEXT="tsupreme-dev"  # o tsupreme-pre, tsupreme-pro
```

### Variables de Entorno Requeridas

```bash
# Configuración del proyecto
export PROJECT_NAME="airflow"
export NAMESPACE="tpi"
export CLIENT="sns-o"              # sns-o, hsc, otro
export ENVIRONMENT="dev"           # dev, pre, pro
export KUBECONTEXT="tsupreme-dev"  # Contexto K8s correspondiente
```

### Despliegue Rápido

```bash
# 1. Clonar el repositorio
git clone <repository-url>
cd airflow

# 2. Navegar al chart
cd src/main/helm/charts/airflow/

# 3. Configurar variables
export CLIENT="sns-o"
export ENVIRONMENT="dev"
export KUBECONTEXT="tsupreme-dev"

# 4. Desplegar
helmfile -f helmfile.yaml.gotmpl \
  --state-values-set client=$CLIENT \
  --state-values-set useLocalCharts=true \
  -e $ENVIRONMENT \
  sync
```

## 🔧 Pipeline CI/CD

### Arquitectura del Pipeline

Como **proyecto solo Helm**, el pipeline sigue el siguiente flujo de despliegue:

```yaml
# Stages del Pipeline (Tipo 2: Solo Helm)
1. 📋 params     - Selección manual de parámetros (CLIENT/ENVIRONMENT)
2. 🚪 gate       - Validación de parámetros
3. 📦 push-chart - Empaquetado y push a Harbor Registry
4. 🔄 sync       - Commit al repositorio CICD
```

### Flujo de Despliegue Completo

```mermaid
sequenceDiagram
    participant Dev as Developer
    participant Git as GitLab Project
    participant Pipe as Pipeline CI/CD
    participant Harbor as Harbor Registry
    participant CICD as CICD Repository
    participant Mirror as CICD Public Mirror
    participant Argo as ArgoCD
    participant K8s as Kubernetes
    
    Dev->>Git: git push feature/update
    Git->>Pipe: Trigger CI/CD Pipeline
    Pipe->>Pipe: select_params (manual)
    Pipe->>Harbor: helm push chart
    Pipe->>CICD: git commit config
    CICD->>Mirror: auto mirror sync
    Argo->>Mirror: detect changes
    Argo->>K8s: deploy via helm
    K8s->>K8s: Airflow pods running
```

### Repositorios del Ecosistema

#### Repositorio Principal del Proyecto
- **URL**: Repositorio actual (proyecto Airflow)
- **Función**: Código fuente y charts Helm
- **Pipeline**: GitLab CI/CD

#### Harbor Registry
- **URL**: harbor.apps.ocpdes.t-systems.es
- **Función**: Almacén de charts Helm (y imágenes Docker para proyectos Java)
- **Propósito**: Organización y versionado de artefactos

#### MTR Registry
- **URL**: https://mtr.devops.telekom.de/repository/genomica
- **Función**: Almacén de imágenes Docker
- **Propósito**: Organización y versionado de imágenes Docker

#### Repositorio CICD
- **URL Privada**: https://setools.t-systems.es/gitlab/health/genomica/commons/cicd.git
- **URL Pública**: https://github.com/health/genomica/commons/cicd.git (mirror)
- **Función**: Configuraciones de despliegue por cliente/entorno
- **Consumidor**: ArgoCD

#### ArgoCD
- **Función**: Continuous Deployment
- **Despliegue**: ArgoCD desplegado **dentro de cada cluster** (DEV, PRE, PRO)
- **Fuente**: CICD Repository (mirror público)
- **Configuración**: Cada ArgoCD monitorea configuraciones específicas de su entorno
- **Autonomía**: Cada cluster gestiona sus propios despliegues independientemente

### Configuración GitLab CI/CD

#### Variables Requeridas en GitLab

```yaml
# Variables del proyecto
PROJECT_NAME: "airflow"
NAMESPACE: "tpi"
HELMFILEDIR: "src/main/helm/charts/airflow"

# Credenciales (GitLab Variables - Protected)
HARBOR_USERNAME: "usuario-harbor"
HARBOR_PASSWORD: "password-harbor"
GIT_TOKEN: "token-git-acceso"
```

#### Configuración del Pipeline

```yaml
# gitlab-ci.yml
image: harbor.apps.ocpdes.t-systems.es/devops/aws-kubectl-helm:1.0.0

variables:
  PROJECT_NAME: "airflow"
  NAMESPACE: "tpi"
  HELMFILEDIR: "src/main/helm/charts/airflow"

stages:
  - params
  - gate
  - push-chart
  - sync
```

#### Trigger del Pipeline

- **Automático**: Solo en merge requests a `main`
- **Manual**: Selección de CLIENT y ENVIRONMENT vía job `select_params`

### Ejemplo de Ejecución

```bash
# 1. Crear MR hacia main
git checkout -b feature/update-airflow-config
git commit -m "Update airflow configuration for sns-o"
git push origin feature/update-airflow-config

# 2. El pipeline se ejecuta automáticamente
# 3. Ejecutar manualmente 'select_params' y elegir:
#    - CLIENT: sns-o
#    - ENVIRONMENT: dev

# 4. El pipeline continúa automáticamente:
#    - Valida parámetros
#    - Empaqueta el chart
#    - Lo sube a Harbor
#    - Sincroniza con CICD
```

## 🚀 Despliegue Local

### Configuración Inicial

#### 1. Verificar Conectividad

```bash
# Verificar contexto K8s
kubectl config current-context
kubectl config use-context $KUBECONTEXT

# Probar conectividad
kubectl --context=$KUBECONTEXT get nodes
kubectl --context=$KUBECONTEXT get namespaces
```

#### 2. Validar Configuración

```bash
# Verificar archivos de configuración
ls -la clients/$CLIENT/values-$ENVIRONMENT.yaml
cat values-common.yaml

# Validar sintaxis del chart
helm lint .
```

### Proceso de Despliegue

#### Despliegue Estándar

```bash
# Navegar al directorio del chart
cd src/main/helm/charts/airflow/

# Configurar variables
export CLIENT="sns-o"
export ENVIRONMENT="dev"
export KUBECONTEXT="tsupreme-dev"

# Desplegar usando charts locales
helmfile -f helmfile.yaml.gotmpl \
  --state-values-set client=$CLIENT \
  --state-values-set useLocalCharts=true \
  -e $ENVIRONMENT \
  sync
```

#### Dry-run (Simulación)

```bash
# Ver cambios sin aplicar
helmfile -f helmfile.yaml.gotmpl \
  --state-values-set client=$CLIENT \
  --state-values-set useLocalCharts=true \
  -e $ENVIRONMENT \
  diff
```

#### Renderizado de Templates

```bash
# Ver YAML final generado
helm template airflow . \
  -f values-common.yaml \
  -f clients/$CLIENT/values-$ENVIRONMENT.yaml \
  --debug
```

### Comandos de Gestión

#### Verificar Estado

```bash
# Estado del release
helm --kube-context=$KUBECONTEXT list -n $NAMESPACE

# Pods de Airflow
kubectl --context=$KUBECONTEXT get pods -n $NAMESPACE -l app.kubernetes.io/name=airflow

# Servicios
kubectl --context=$KUBECONTEXT get svc -n $NAMESPACE
```

#### Logs y Debug

```bash
# Logs del scheduler
kubectl --context=$KUBECONTEXT logs -n $NAMESPACE -l component=scheduler -f

# Logs del webserver
kubectl --context=$KUBECONTEXT logs -n $NAMESPACE -l component=webserver -f

# Eventos del namespace
kubectl --context=$KUBECONTEXT get events -n $NAMESPACE --sort-by=.metadata.creationTimestamp
```

#### Desinstalación

```bash
# Desinstalar completamente
helmfile -f helmfile.yaml.gotmpl \
  --state-values-set client=$CLIENT \
  --state-values-set useLocalCharts=true \
  -e $ENVIRONMENT \
  destroy

# Verificar limpieza
kubectl --context=$KUBECONTEXT get all -n $NAMESPACE
```

## 📁 Gestión de DAGs

### Importación Automática de DAGs

Apache Airflow en TSuPreMe soporta múltiples métodos para gestionar y desplegar DAGs. La carpeta `src/main/helm/charts/airflow/dags/` contiene los DAGs que pueden ser importados automáticamente al cluster.

#### Métodos de Importación

##### 1. Git-Sync (Recomendado para Producción)

El método **Git-Sync** sincroniza automáticamente los DAGs desde un repositorio Git, ideal para entornos productivos con cambios frecuentes.

```yaml
# Configuración en values-{environment}.yaml
dags:
  gitSync:
    enabled: true
    repo: https://github.com/TSIB-Arch/genomics-cicd.git
    branch: main
    rev: HEAD
    depth: 1
    maxFailures: 0
    subPath: "airflow/sns-o/dev/dags"  # Ruta relativa dentro del repo
    wait: 60  # Sincroniza cada 60 segundos

    # Credenciales (si el repo es privado)
    credentialsSecret: airflow-git-credentials

    # Recursos del sidecar git-sync
    resources:
      limits:
        cpu: 100m
        memory: 128Mi
      requests:
        cpu: 50m
        memory: 64Mi
```

**Crear Secret para Git privado:**

```bash
# Crear secret con credenciales de Git
kubectl --context=$KUBECONTEXT create secret generic airflow-git-credentials \
  -n $NAMESPACE \
  --from-literal=GITSYNC_USERNAME='tu-usuario' \
  --from-literal=GITSYNC_PASSWORD='tu-token-git'

# O usando SSH key
kubectl --context=$KUBECONTEXT create secret generic airflow-ssh-secret \
  -n $NAMESPACE \
  --from-file=gitSshKey=$HOME/.ssh/id_rsa
```

##### 2. Persistent Volume (Desarrollo y Testing)

Para desarrollo local o cuando los DAGs cambian con frecuencia, usar un volumen persistente:

```yaml
# Configuración en values-{environment}.yaml
dags:
  persistence:
    enabled: true
    size: 1Gi
    storageClassName: standard  # o el storage class disponible
    accessMode: ReadWriteMany   # Importante para múltiples pods
    existingClaim: ~            # O usar un PVC existente
    subPath: ~
```

**Copiar DAGs al volumen:**

```bash
# Identificar el pod del scheduler
SCHEDULER_POD=$(kubectl --context=$KUBECONTEXT get pods -n $NAMESPACE \
  -l component=scheduler -o jsonpath='{.items[0].metadata.name}')

# Copiar DAGs desde la carpeta local
kubectl --context=$KUBECONTEXT cp \
  src/main/helm/charts/airflow/dags/ \
  $NAMESPACE/$SCHEDULER_POD:/opt/airflow/dags/ \
  -c scheduler

# Verificar que los DAGs se copiaron
kubectl --context=$KUBECONTEXT exec -n $NAMESPACE $SCHEDULER_POD \
  -c scheduler -- ls -la /opt/airflow/dags/
```

##### 3. ConfigMap (Para DAGs pequeños y estáticos)

Ideal para DAGs de configuración o testing que cambian poco:

```bash
# Crear ConfigMap desde la carpeta de DAGs
kubectl --context=$KUBECONTEXT create configmap airflow-dags \
  -n $NAMESPACE \
  --from-file=src/main/helm/charts/airflow/dags/

# Actualizar ConfigMap existente
kubectl --context=$KUBECONTEXT create configmap airflow-dags \
  -n $NAMESPACE \
  --from-file=src/main/helm/charts/airflow/dags/ \
  --dry-run=client -o yaml | kubectl apply -f -
```

Luego configurar en `values.yaml`:

```yaml
# Agregar extraVolumes y extraVolumeMounts
extraVolumes:
  - name: dags-configmap
    configMap:
      name: airflow-dags

extraVolumeMounts:
  - name: dags-configmap
    mountPath: /opt/airflow/dags
    readOnly: true
```

#### DAGs Incluidos en el Proyecto

##### tsupreme_data_pipeline.py

DAG principal para orquestar el flujo de datos de TSuPreMe con integración NiFi.

**Características:**
- Comunicación con NiFi para procesamiento de datos
- Monitorización de pipelines de NiFi
- Gestión de flujos de datos entre Kafka y NiFi
- Autenticación con NiFi vía tokens

**Variables de Airflow requeridas:**

```python
# Configurar en Airflow UI: Admin > Variables
nifi_process_group_id = "a153e6ea-019a-1000-ffff-ffffa209e4cd"  # ID del Process Group en NiFi
```

**Conexiones de Airflow requeridas:**

```bash
# Crear conexión NiFi desde CLI
airflow connections add 'nifi_default' \
    --conn-type 'http' \
    --conn-host 'https://nifi-node-default.tpi.svc.cluster.local' \
    --conn-login 'usuario-nifi' \
    --conn-password 'password-nifi'

# O configurar vía UI: Admin > Connections
# Connection Id: nifi_default
# Connection Type: HTTP
# Host: https://nifi-node-default.tpi.svc.cluster.local
# Login: usuario-nifi
# Password: password-nifi
```

**Trigger del DAG:**

```bash
# Ejecutar manualmente desde CLI
airflow dags trigger tsupreme_data_pipeline

# O desde la UI de Airflow
```

#### Verificación de DAGs Importados

```bash
# Listar DAGs disponibles
kubectl --context=$KUBECONTEXT exec -n $NAMESPACE \
  deploy/airflow-scheduler -- airflow dags list

# Ver detalles de un DAG específico
kubectl --context=$KUBECONTEXT exec -n $NAMESPACE \
  deploy/airflow-scheduler -- airflow dags show tsupreme_data_pipeline

# Verificar errores de parsing
kubectl --context=$KUBECONTEXT exec -n $NAMESPACE \
  deploy/airflow-scheduler -- airflow dags list-import-errors
```

#### Troubleshooting DAGs

##### DAG no aparece en la UI

```bash
# 1. Verificar que el archivo existe
kubectl --context=$KUBECONTEXT exec -n $NAMESPACE \
  deploy/airflow-scheduler -- ls -la /opt/airflow/dags/

# 2. Verificar errores de sintaxis Python
kubectl --context=$KUBECONTEXT exec -n $NAMESPACE \
  deploy/airflow-scheduler -- python /opt/airflow/dags/tsupreme_data_pipeline.py

# 3. Verificar logs del scheduler
kubectl --context=$KUBECONTEXT logs -n $NAMESPACE \
  -l component=scheduler --tail=100 | grep -i error

# 4. Forzar re-scan de DAGs
kubectl --context=$KUBECONTEXT exec -n $NAMESPACE \
  deploy/airflow-scheduler -- airflow dags reserialize
```

##### Error de importación de módulos

```bash
# Instalar dependencias faltantes vía extraPipPackages
# En values.yaml:
extraPipPackages:
  - "apache-airflow-providers-http==4.5.0"
  - "requests==2.31.0"
  - "urllib3==2.0.7"
```

##### Variables o Conexiones no encontradas

```bash
# Listar variables configuradas
kubectl --context=$KUBECONTEXT exec -n $NAMESPACE \
  deploy/airflow-scheduler -- airflow variables list

# Listar conexiones configuradas
kubectl --context=$KUBECONTEXT exec -n $NAMESPACE \
  deploy/airflow-scheduler -- airflow connections list

# Exportar variables y conexiones para backup
kubectl --context=$KUBECONTEXT exec -n $NAMESPACE \
  deploy/airflow-scheduler -- airflow variables export /tmp/variables.json

kubectl --context=$KUBECONTEXT exec -n $NAMESPACE \
  deploy/airflow-scheduler -- airflow connections export /tmp/connections.json
```

#### Mejores Prácticas

**Desarrollo:**
- Usar persistent volume para desarrollo local
- Validar DAGs localmente con `python -m py_compile dag_file.py`
- Probar en entorno `dev` antes de promover a `pre` o `pro`

**Producción:**
- Usar Git-Sync para sincronización automática
- Implementar CI/CD para validar DAGs antes del merge
- Mantener DAGs versionados en Git
- Documentar variables y conexiones requeridas

**Seguridad:**
- No hardcodear credenciales en DAGs
- Usar Airflow Variables para configuración
- Usar Airflow Connections para credenciales externas
- Implementar secrets en Kubernetes para datos sensibles

## 📖 Documentación de DAGs

Este proyecto incluye **documentación completa y detallada** de los DAGs disponibles, con diagramas, ejemplos prácticos y guías de troubleshooting.

### 📚 DAGs Documentados

La documentación completa de los DAGs se encuentra en: **[src/main/helm/charts/airflow/dag_docs/](./src/main/helm/charts/airflow/dag_docs/)**

#### Índice de Documentación

| DAG | Documentación | Descripción |
|-----|---------------|-------------|
| **DAG_TSUPREME_001_TPIAGENT_UPLOADS** | [📄 Ver Docs](./src/main/helm/charts/airflow/dag_docs/DAG_TSUPREME_001_TPIAGENT_UPLOADS.md) | DAG principal para gestión del ciclo de vida de Process Groups de NiFi |
| **STOP_NIFI_EMERGENCY** | [📄 Ver Docs](./src/main/helm/charts/airflow/dag_docs/STOP_NIFI_EMERGENCY.md) | DAG de emergencia para parada inmediata de NiFi |

### 🎯 Inicio Rápido - DAGs de NiFi

#### Configuración Inicial

```bash
# 1. Configurar conexión NiFi
airflow connections add nifi_default \
  --conn-type http \
  --conn-host https://nifi.example.com:8443 \
  --conn-login admin \
  --conn-password 'your-password'

# 2. Configurar timeout (opcional)
airflow variables set nifi_stop_after_minutes 240  # 4 horas

# 3. (Opcional) Filtrar Process Groups específicos
airflow variables set nifi_process_group_names '["TSuPreMe Pipeline"]'
```

#### Ejecución Básica

```bash
# Iniciar el flujo de NiFi (con timeout de 4 horas)
airflow dags trigger DAG_TSUPREME_001_TPIAGENT_UPLOADS

# Parar NiFi inmediatamente (emergencia)
airflow dags trigger STOP_NIFI_EMERGENCY
```

### 🏗️ Arquitectura de los DAGs de NiFi

```mermaid
graph TB
    User([Usuario]) --> Main[DAG_TSUPREME_001_TPIAGENT_UPLOADS]
    
    Main --> Check[Check NiFi Availability]
    Check --> Start[Start Process Groups]
    Start --> Monitor[Monitor Activity]
    Monitor --> Wait[Wait Timeout/Forever]
    Wait --> Stop[Stop Process Groups]
    Stop --> Trigger[Trigger Emergency DAG]
    
    Trigger --> Emergency[STOP_NIFI_EMERGENCY]
    Emergency --> StopAll[Emergency Stop All PGs]
    
    User -.Manual Trigger.-> Emergency
    
    style Main fill:#4CAF50,color:#fff
    style Emergency fill:#dc3545,color:#fff
    style StopAll fill:#ff6b6b,color:#fff
```

### 🛡️ Sistema de Seguridad Multinivel

Los DAGs de NiFi implementan **4 capas independientes** para garantizar que los Process Groups siempre se detengan:

| Capa | Mecanismo | Cuándo se Activa |
|------|-----------|------------------|
| **1** | Task explícita `stop_nifi_pipeline` | Al finalizar la espera (timeout o manual) |
| **2** | `TriggerDagRunOperator` → STOP_NIFI_EMERGENCY | Después de la parada explícita (safety net) |
| **3** | Callbacks del DAG (`on_success`, `on_failure`) | Cuando se marca el DAG como Success/Failed |
| **4** | Sensor `on_kill()` | Cuando se cancela una task manualmente |

### 📊 Características Principales

#### DAG_TSUPREME_001_TPIAGENT_UPLOADS

- ✅ **Inicio automático** de Process Groups de NiFi
- ✅ **Monitoreo de actividad** para verificar procesamiento
- ✅ **Modos de operación**: Timeout configurable o infinito
- ✅ **4 capas de seguridad** para garantizar parada
- ✅ **Integración automática** con DAG de emergencia
- ✅ **Filtrado de Process Groups** por nombre

**Casos de uso:**
- Procesamiento batch programado (con timeout)
- Modo desarrollo 24/7 (modo infinito)
- Parada de emergencia coordinada

#### STOP_NIFI_EMERGENCY

- 🚨 **Parada inmediata** de todos los Process Groups
- 🚨 **Ejecución simple** (1 sola task)
- 🚨 **Integración automática** con DAG principal
- 🚨 **Manejo robusto de errores** parciales
- 🚨 **Logging detallado** con resumen de resultados

**Casos de uso:**
- Parada manual urgente
- Cleanup después de fallos
- Workaround para Airflow 3.0+

### 🔧 Variables de Airflow Requeridas

| Variable | Tipo | Default | Descripción |
|----------|------|---------|-------------|
| `nifi_stop_after_minutes` | int | -1 | Timeout en minutos. `-1` = infinito, `>0` = parada automática |
| `nifi_process_group_names` | list[str] | null | Filtro opcional de PGs. Si no está configurada, controla **todos** |

**Ejemplos:**
```bash
# Parada automática después de 4 horas
airflow variables set nifi_stop_after_minutes 240

# Modo infinito (solo parada manual)
airflow variables set nifi_stop_after_minutes -1

# Controlar solo Process Groups específicos
airflow variables set nifi_process_group_names '["TSuPreMe Pipeline", "Data Ingestion"]'
```

### 🔍 Troubleshooting Rápido

| Problema | Solución Rápida | Documentación |
|----------|-----------------|---------------|
| "Failed to get NiFi token: 401" | Verificar credenciales en `nifi_default` | [Ver docs](./src/main/helm/charts/airflow/dag_docs/DAG_TSUPREME_001_TPIAGENT_UPLOADS.md#error-failed-to-get-nifi-token-401) |
| "No se encontraron Process Groups" | Verificar `nifi_process_group_names` | [Ver docs](./src/main/helm/charts/airflow/dag_docs/DAG_TSUPREME_001_TPIAGENT_UPLOADS.md#error-no-se-encontraron-process-groups-bajo-root) |
| "dag_id STOP_NIFI_EMERGENCY not found" | Instalar ambos DAGs | [Ver docs](./src/main/helm/charts/airflow/dag_docs/DAG_TSUPREME_001_TPIAGENT_UPLOADS.md#error-dag_id-stop_nifi_emergency-not-found) |
| NiFi sigue corriendo después de Success | Ejecutar `STOP_NIFI_EMERGENCY` manualmente | [Ver docs](./src/main/helm/charts/airflow/dag_docs/STOP_NIFI_EMERGENCY.md#caso-2-cleanup-después-de-fallo) |

### 📚 Documentación Completa

Para información detallada sobre cada DAG, consulta:

- **[📘 Índice General](./src/main/helm/charts/airflow/dag_docs/README.md)** - Vista general y guías rápidas
- **[📗 DAG_TSUPREME_001_TPIAGENT_UPLOADS](./src/main/helm/charts/airflow/dag_docs/DAG_TSUPREME_001_TPIAGENT_UPLOADS.md)** - Documentación completa con 8+ diagramas Mermaid
- **[📕 STOP_NIFI_EMERGENCY](./src/main/helm/charts/airflow/dag_docs/STOP_NIFI_EMERGENCY.md)** - Documentación del DAG de emergencia

**Contenido incluido en la documentación:**
- ✅ Arquitectura detallada con diagramas Mermaid
- ✅ Casos de uso prácticos con ejemplos
- ✅ Configuración paso a paso
- ✅ Troubleshooting exhaustivo con logs de ejemplo
- ✅ Mejores prácticas y recomendaciones
- ✅ Tabla de escenarios de parada completa

## ⚙️ Configuración por Entorno

### Configuración Base (values-common.yaml)

```yaml
# Configuración común a todos los entornos
executor: "CeleryExecutor"
airflowVersion: "2.10.5"

# Configuración de red corporativa T-Systems
webserver:
  service:
    type: ClusterIP
  
# Configuración de seguridad
serviceAccount:
  create: true
  
# Recursos base
resources:
  limits:
    cpu: 1000m
    memory: 2Gi
  requests:
    cpu: 500m
    memory: 1Gi
```

### Configuración por Cliente

#### sns-o (Sistema Nacional de Salud)

```yaml
# values-dev.yaml
webserver:
  replicas: 1
  ingress:
    enabled: true
    hosts:
      - airflow-sns-o-dev.tsupreme.es

scheduler:
  replicas: 1

workers:
  replicas: 2

# Base de datos específica para SNS-O
data:
  metadataConnection:
    host: "postgres-sns-o-dev.tsupreme.internal"
    db: "airflow_sns_o_dev"
```

#### hsc (Health Service Center)

```yaml
# values-dev.yaml  
webserver:
  replicas: 1
  ingress:
    enabled: true
    hosts:
      - airflow-hsc-dev.tsupreme.es

# Configuración para procesamiento intensivo
workers:
  replicas: 4
  resources:
    limits:
      cpu: 2000m
      memory: 4Gi

# PostgreSQL para cargas de genómica
data:
  metadataConnection:
    host: "postgres-hsc-dev.tsupreme.internal"
    db: "airflow_hsc_dev"
```

### Configuración de Producción

#### Diferencias Clave vs Desarrollo

```yaml
# Producción (values-pro.yaml)
webserver:
  replicas: 3                    # Alta disponibilidad
  autoscaling:
    enabled: true
    minReplicas: 2
    maxReplicas: 5

scheduler:
  replicas: 2                    # Scheduler redundante

workers:
  replicas: 6                    # Mayor capacidad
  autoscaling:
    enabled: true
    minReplicas: 3
    maxReplicas: 20

# PgBouncer para connection pooling
pgbouncer:
  enabled: true
  replicas: 2

# Configuración de seguridad avanzada
networkPolicies:
  enabled: true
  
# Monitoreo completo
monitoring:
  enabled: true
  serviceMonitor:
    enabled: true
```

## 📊 Monitoreo y Observabilidad

### Métricas Integradas

#### Prometheus + Grafana

```yaml
# Configuración de métricas
statsd:
  enabled: true
  service:
    type: ClusterIP
    
config:
  metrics:
    statsd_on: true
    statsd_host: "airflow-statsd"
    statsd_port: 9125
    statsd_prefix: "airflow"
```

#### Dashboard Flower (CeleryExecutor)

```yaml
flower:
  enabled: true
  service:
    type: ClusterIP
  ingress:
    enabled: true
    hosts:
      - flower-${CLIENT}-${ENVIRONMENT}.tsupreme.es
```

### Logging Centralizado

#### Configuración ELK Stack

```yaml
# Logs enviados a ElasticSearch
logs:
  persistence:
    enabled: true
    size: 100Gi
    
config:
  logging:
    remote_logging: true
    remote_log_conn_id: "elasticsearch_default"
    elasticsearch_host: "elasticsearch.logging.tsupreme.internal"
```

## 🔐 Seguridad y Accesos

### Autenticación y Autorización

#### LDAP Corporativo T-Systems

```yaml
# Integración con LDAP corporativo
config:
  webserver:
    authenticate: true
    auth_backend: "airflow.contrib.auth.backends.ldap_auth"
    
  ldap:
    uri: "ldaps://ldap.t-systems.com:636"
    user_filter: "objectClass=person"
    user_name_attr: "uid"
    group_member_attr: "memberOf"
    superuser_filter: "memberOf=cn=airflow-admins,ou=groups,dc=t-systems,dc=com"
```

#### RBAC (Role-Based Access Control)

```yaml
# Service Account con permisos específicos
serviceAccount:
  create: true
  name: "airflow-sa"
  annotations:
    "kubernetes.io/managed-by": "tsupreme"

# RBAC rules
rbac:
  create: true
  rules:
    - apiGroups: [""]
      resources: ["pods", "pods/log"]
      verbs: ["create", "get", "list", "watch", "delete"]
    - apiGroups: ["batch"]
      resources: ["jobs"]
      verbs: ["create", "get", "list", "watch", "delete"]
```

### Gestión de Secretos

#### Kubernetes Secrets

```yaml
# Secretos para conexiones de BD
data:
  metadataSecretName: "airflow-metadata-secret"
  
# Secretos para conexiones externas  
extraSecrets:
  airflow-connections:
    data:
      postgres_default: "postgresql://user:pass@host:5432/db"
      redis_default: "redis://redis-host:6379/0"
```

### Network Policies

```yaml
# Segmentación de red
networkPolicies:
  enabled: true
  
# Solo permitir tráfico necesario
ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: monitoring
    ports:
    - protocol: TCP
      port: 8080
```

## 🛠️ Troubleshooting

### Problemas Comunes del Pipeline

#### Error: "Falta ejecutar el job manual 'select_params'"

```bash
# Solución según AGENT.md:
1. Ir a GitLab CI/CD Pipeline
2. Buscar job "select_params" 
3. Hacer clic en "Play" manual
4. Seleccionar CLIENT y ENVIRONMENT
5. El pipeline continuará automáticamente
```

#### Error: "Chart validation failed"

```bash
# Debug local del chart
cd src/main/helm/charts/airflow/
helm lint .
helm template airflow . -f values-common.yaml -f clients/sns-o/values-dev.yaml --debug

# Verificar sintaxis YAML
yamllint values-common.yaml
yamllint clients/*/values-*.yaml
```

### Problemas de Despliegue

#### Error: "no such file or directory: helmfile.yaml.gotmpl"

```bash
# Verificar que estás en el directorio correcto
pwd
# Debe ser: /path/to/project/src/main/helm/charts/airflow/

# Verificar que el archivo existe
ls -la helmfile.yaml.gotmpl
```

#### Error de contexto Kubernetes

```bash
# Verificar contexto actual
kubectl config current-context

# Configurar contexto correcto
export KUBECONTEXT="tsupreme-dev"  # o tsupreme-pre, tsupreme-pro
kubectl config use-context $KUBECONTEXT
```

#### Error: "values file not found"

```bash
# Verificar que existe el archivo de configuración
ls -la clients/$CLIENT/values-$ENVIRONMENT.yaml

# Verificar variables de entorno
echo "CLIENT: $CLIENT"
echo "ENVIRONMENT: $ENVIRONMENT"

# Las combinaciones válidas son:
# CLIENT: sns-o, hsc, otro
# ENVIRONMENT: dev, pre, pro
```

### Debug Avanzado

#### Logs detallados de Helmfile

```bash
# Ejecutar con logging detallado
helmfile --log-level debug -f helmfile.yaml.gotmpl \
  --state-values-set client=$CLIENT \
  --state-values-set useLocalCharts=true \
  -e $ENVIRONMENT \
  sync
```

#### Verificar valores computados

```bash
# Ver valores finales que se aplicarán
helmfile -f helmfile.yaml.gotmpl \
  --state-values-set client=$CLIENT \
  --state-values-set useLocalCharts=true \
  -e $ENVIRONMENT \
  write-values
```

#### Acceso a Componentes

```bash
# Ejecutar bash en scheduler
kubectl --context=$KUBECONTEXT exec -it deploy/airflow-scheduler -n $NAMESPACE -- bash

# Comandos útiles dentro del contenedor:
airflow version
airflow config list
airflow connections list
airflow dags list
```

## 📚 Documentación Técnica

### Arquitectura Detallada

#### Flujo de Datos

```mermaid
sequenceDiagram
    participant Dev as Developer
    participant Git as GitLab
    participant Pipe as Pipeline
    participant Harbor as Harbor Registry
    participant K8s as Kubernetes
    participant AF as Airflow
    
    Dev->>Git: git push feature/update
    Git->>Pipe: Trigger CI/CD
    Pipe->>Harbor: helm push chart
    Pipe->>K8s: helmfile sync
    K8s->>AF: Deploy/Update pods
    AF->>AF: Start services
```

### Configuraciones Específicas por Cliente

#### SNS-O - Sistema Nacional de Salud

**Características específicas:**
- Cumplimiento GDPR/LOPD estricto
- Integración con sistemas sanitarios
- Workflows de procesamiento de datos de salud
- Backup y auditoría extendidos

```yaml
# Configuración específica SNS-O
config:
  core:
    # Seguridad para datos sanitarios
    sensitive_var_conn_mask: true
    hide_sensitive_var_conn_fields: true
    
  email:
    # Notificaciones al equipo sanitario
    default_email_on_retry: true
    default_email_on_failure: true
```

#### HSC - Health Service Center

**Características específicas:**
- Procesamiento intensivo de genómica
- Workflows de bioinformática
- Integración con clusters HPC
- Almacenamiento de alto rendimiento

```yaml
# Configuración específica HSC
workers:
  # Recursos para procesamiento genómico
  resources:
    limits:
      cpu: 4000m
      memory: 8Gi
      
# Configuración para genómica
dags:
  # DAGs específicos de bioinformática
  persistence:
    enabled: true
    size: 500Gi  # Datasets grandes
    storageClassName: fast-ssd
```

### Integración con Ecosistema TSuPreMe

#### Conectividad con Servicios Corporativos

```yaml
# Configuración de servicios T-Systems
extraEnv:
  # Proxy corporativo (según AGENT.md)
  - name: HTTP_PROXY
    value: "http://10.49.89.10:8080"
  - name: HTTPS_PROXY  
    value: "http://10.49.89.10:8080"
    
  # Time zone corporativo
  - name: TZ
    value: "Europe/Madrid"
```

### Mejores Prácticas (según AGENT.md)

#### Desarrollo
- Usar siempre el entorno `dev` para pruebas iniciales
- Validar cambios localmente antes del commit
- Mantener consistencia en nombres de valores entre entornos

#### Seguridad
- No hardcodear credenciales en código
- Usar GitLab Variables para secretos
- Revisar permisos de namespaces K8s

#### Operaciones
- Documentar cambios en valores específicos de cliente
- Mantener backup de configuraciones críticas
- Monitorear recursos después de despliegues

## 🤝 Soporte y Contacto

### Equipo Responsable

**DevOps Team - Genómica T-Systems**
- 📧 **Email**: genomica_pipe@t-systems.com
- 🔗 **GitLab**: [Airflow Project](https://gitlab.t-systems.com/genomica/airflow)
- 📚 **Wiki**: [Wiki interno T-Systems]

### Escalación de Issues

#### Nivel 1 - Soporte Básico
- Problemas de configuración
- Dudas sobre despliegue
- Documentación

#### Nivel 2 - Soporte Técnico
- Problemas de pipeline CI/CD
- Issues de conectividad
- Debugging avanzado

#### Nivel 3 - Soporte Crítico
- Caídas de producción
- Problemas de seguridad
- Escalación arquitectura

### Recursos Adicionales

- **[Apache Airflow Official Docs](https://airflow.apache.org/docs/)**
- **[Helm Charts Documentation](https://helm.sh/docs/)**
- **[TSuPreMe Platform Guide](https://docs.tsupreme.t-systems.com/)**
- **[T-Systems DevOps Standards](https://devops.t-systems.com/standards/)**

---

## 🏥 Casos de Uso Específicos

### Genómica y Bioinformática (HSC)

```mermaid
graph LR
    A[Secuenciación] --> B[Quality Control]
    B --> C[Alignment]
    C --> D[Variant Calling]
    D --> E[Annotation]
    E --> F[Report Generation]
    F --> G[Clinical Delivery]
```

**DAGs típicos:**
- `genomic_pipeline_wgs` - Whole Genome Sequencing
- `variant_annotation` - Anotación de variantes
- `clinical_report_gen` - Generación de informes clínicos

### Datos Sanitarios (SNS-O)

```mermaid
graph TD
    A[Ingesta Datos] --> B[Validación GDPR]
    B --> C[Anonimización]
    C --> D[Procesamiento]
    D --> E[Análisis Epidemiológico]
    E --> F[Reportes Sanitarios]
```

**DAGs típicos:**
- `health_data_ingestion` - Ingesta de datos sanitarios
- `epidemiological_analysis` - Análisis epidemiológico
- `health_reporting` - Reportes sanitarios automatizados

---

## 🏥 Casos de Uso Específicos

### Genómica y Bioinformática (HSC)

```mermaid
graph LR
    A[Secuenciación] --> B[Quality Control]
    B --> C[Alignment]
    C --> D[Variant Calling]
    D --> E[Annotation]
    E --> F[Report Generation]
    F --> G[Clinical Delivery]
```

**DAGs típicos:**
- `genomic_pipeline_wgs` - Whole Genome Sequencing
- `variant_annotation` - Anotación de variantes
- `clinical_report_gen` - Generación de informes clínicos

### Datos Sanitarios (SNS-O)

```mermaid
graph TD
    A[Ingesta Datos] --> B[Validación GDPR]
    B --> C[Anonimización]
    C --> D[Procesamiento]
    D --> E[Análisis Epidemiológico]
    E --> F[Reportes Sanitarios]
```

**DAGs típicos:**
- `health_data_ingestion` - Ingesta de datos sanitarios
- `epidemiological_analysis` - Análisis epidemiológico
- `health_reporting` - Reportes sanitarios automatizados

---

**📅 Última actualización**: 2 de octubre de 2025  
**📝 Versión del documento**: 2.0  
**👥 Mantenido por**: DevOps Team Genómica T-Systems

---

*Para contribuir a este proyecto o reportar issues, por favor utiliza el [sistema de GitLab Issues](https://gitlab.t-systems.com/genomica/airflow/-/issues) o contacta directamente con el equipo DevOps.*
