# AGENT.md - Guía de Despliegue para T-Systems TSuPreMe

## Introducción

Esta guía documenta los procesos de CI/CD y despliegue para proyectos en la plataforma TSuPreMe de T-Systems. Soporta dos tipos principales de proyectos:

1. **Proyectos de aplicación completa** (ej. terminology-server): Incluyen build de aplicación, imagen Docker y chart Helm
2. **Proyectos solo Helm**: Únicamente push de charts Helm sin build de aplicación

## Arquitectura y Estándares

### Entornos Soportados
- **dev**: Desarrollo
- **pre**: Pre-producción  
- **pro**: Producción

### Clientes Soportados
- **sns-o**: Sistema Nacional de Salud - Navarra
- **hsc**: Health Service Center
- **otro**: Otros clientes personalizados

### Tecnologías Base
- **CI/CD**: GitLab CI/CD
- **Contenedores**: Docker/Podman con Jib
- **Orquestación**: Kubernetes + Helm 3.x + ArgoCD
- **Registro**: Harbor (harbor.apps.ocpdes.t-systems.es)
- **Despliegue Continuo**: ArgoCD
- **Build**: Maven 3.x + OpenJDK 21
- **Calidad**: SonarQube + Jacoco

### Repositorios del Ecosistema

#### Harbor Registry
- **URL**: harbor.apps.ocpdes.t-systems.es
- **Función**: Almacén de charts Helm únicamente
- **Propósito**: Organización limpia y versionado de charts Helm

#### MTR Registry (Magenta Trusted Registry)
- **URL**: https://mtr.devops.telekom.de/repository/genomica
- **Función**: Almacén de imágenes Docker
- **Propósito**: Organización y versionado de imágenes Docker para proyectos Java

#### Repositorio CICD
- **URL Privada**: https://setools.t-systems.es/gitlab/health/genomica/commons/cicd.git
- **URL Pública (Mirror)**: https://github.com/health/genomica/commons/cicd.git
- **Función**: Configuraciones de despliegue por cliente/entorno
- **Mirror**: Sincronización automática del repo privado al público
- **Consumidor**: ArgoCD

#### ArgoCD
- **Función**: Continuous Deployment
- **Despliegue**: ArgoCD desplegado **dentro de cada cluster** (DEV, PRE, PRO)
- **Fuente**: CICD Repository (mirror público de arquitectura)
- **Configuración**: Cada ArgoCD monitorea configuraciones específicas de su entorno
- **Autonomía**: Cada cluster gestiona sus propios despliegues independientemente
- **Trigger**: Detecta cambios en el repo CICD y despliega automáticamente en su cluster

## Configuración de Proyecto

### Variables de Entorno Requeridas

#### Para GitLab CI/CD
```yaml
# Configuración del proyecto
PROJECT_NAME: "nombre-del-proyecto"
NAMESPACE: "namespace-k8s" 
IMAGE_REPO: "mtr.devops.telekom.de/genomica/$PROJECT_NAME"
HELMFILEDIR: "src/main/helm/charts/$PROJECT_NAME"

# Credenciales (configurar en GitLab Variables)
HARBOR_USERNAME: "usuario-harbor"
HARBOR_PASSWORD: "password-harbor" 
ARTIFACTORY_USERNAME: "usuario-artifactory"
ARTIFACTORY_IDENTITY_KEY: "token-artifactory"
SONAR_AUTH_TOKEN: "token-sonar"
GIT_TOKEN: "token-git"
```

#### Para Despliegues Locales
```bash
# Variables de entorno necesarias
export CLIENT="sns-o"           # o "hsc", "otro"
export ENVIRONMENT="dev"        # o "pre", "pro"
export KUBECONTEXT="mi-contexto-k8s"
```

### Estructura de Directorios Estándar

```
src/main/helm/charts/[PROJECT_NAME]/
├── Chart.yaml                 # Definición del chart
├── values-common.yaml         # Valores comunes a todos los entornos
├── helmfile.yaml.gotmpl      # Configuración Helmfile (si aplica)
├── clients/
│   ├── sns-o/
│   │   ├── values-dev.yaml
│   │   ├── values-pre.yaml
│   │   └── values-pro.yaml
│   ├── hsc/
│   │   ├── values-dev.yaml
│   │   ├── values-pre.yaml
│   │   └── values-pro.yaml
│   └── otro/
│       ├── values-dev.yaml
│       ├── values-pre.yaml
│       └── values-pro.yaml
├── templates/
│   ├── deployment.yaml
│   ├── service.yaml
│   ├── configmap.yaml
│   └── ingress.yaml
└── tests/
    └── connection-test.yaml
```

## Pipelines de CI/CD

### Pipeline Tipo 1: Aplicación Completa

Para proyectos con aplicación Java (ej. terminology-server):

#### Stages del Pipeline

1. **params**: Selección manual de parámetros (CLIENT, ENVIRONMENT)
2. **gate**: Validación de parámetros seleccionados
3. **setup**: Extracción de versión del proyecto
4. **build**: Compilación con Maven
5. **verify**: Ejecución de tests y cobertura
6. **analyse**: Análisis de calidad con SonarQube
7. **push-image**: Build y push de imagen Docker con Jib
8. **push-chart**: Empaquetado y push del chart Helm
9. **sync**: Sincronización con repositorio CICD

#### Características Clave
- **Trigger**: Solo en merge requests a main
- **Selección manual**: Cliente y entorno via job manual
- **Proxy corporativo**: Configurado para Maven y Docker
- **Versionado**: Automático desde pom.xml
- **Seguridad**: Análisis con SonarQube obligatorio

### Pipeline Tipo 2: Solo Helm

Para proyectos que únicamente despliegan charts Helm:

#### Stages Simplificados
1. **params**: Selección de parámetros
2. **gate**: Validación
3. **push-chart**: Push del chart a Harbor
4. **sync**: Sincronización con CICD

### Configuración Específica por Tipo

#### Para Proyectos con Aplicación Java
```yaml
# En gitlab-ci.yml
image: harbor.apps.ocpdes.t-systems.es/genomica/maven-eclipse-temurin-21:latest

variables:
  PROJECT_NAME: "mi-aplicacion"
  IMAGE_REPO: "mtr.devops.telekom.de/genomica/$PROJECT_NAME"
  MAVEN_CLI_OPTS: "--batch-mode --errors --fail-at-end"
```

#### Para Proyectos Solo Helm
```yaml
# En gitlab-ci.yml  
image: harbor.apps.ocpdes.t-systems.es/devops/aws-kubectl-helm:1.0.0

variables:
  PROJECT_NAME: "mi-chart"
  # Sin IMAGE_REPO ni configuración Maven
```

## Despliegues Locales

### Prerrequisitos

#### Herramientas Necesarias
```bash
# Instalar herramientas
curl -L https://get.helm.sh/helm-v3.12.0-linux-amd64.tar.gz | tar xz
curl -L https://github.com/helmfile/helmfile/releases/download/v0.155.0/helmfile_0.155.0_linux_amd64.tar.gz | tar xz

# Verificar instalación
helm version
helmfile version
kubectl version --client
```

#### Configuración de Kubectl
```bash
# Configurar contexto de Kubernetes
kubectl config current-context
kubectl config use-context mi-contexto-tsupreme
```

### Proceso de Despliegue Local

#### 1. Configurar Variables de Entorno
```bash
# Configuración requerida
export CLIENT="sns-o"                    # Cliente objetivo
export ENVIRONMENT="dev"                 # Entorno objetivo  
export KUBECONTEXT="tsupreme-dev"       # Contexto K8s
```

#### 2. Validar Configuración
```bash
# Verificar que existen los archivos de valores
ls -la clients/$CLIENT/values-$ENVIRONMENT.yaml
cat values-common.yaml

# Verificar conectividad K8s
kubectl --context=$KUBECONTEXT get nodes
```

#### 3. Ejecutar Despliegue con Helmfile
```bash
# Navegar al directorio del chart
cd src/main/helm/charts/[PROJECT_NAME]/

# Despliegue usando charts locales
helmfile -f helmfile.yaml.gotmpl \
  --state-values-set client=$CLIENT \
  --state-values-set useLocalCharts=true \
  -e $ENVIRONMENT \
  sync
```

#### 4. Verificar Despliegue
```bash
# Verificar estado del release
helm --kube-context=$KUBECONTEXT list -n [NAMESPACE]

# Verificar pods
kubectl --context=$KUBECONTEXT get pods -n [NAMESPACE]

# Ver logs si es necesario
kubectl --context=$KUBECONTEXT logs -n [NAMESPACE] -l app=[PROJECT_NAME]
```

### Comandos Útiles para Despliegues Locales

#### Simulación (Dry-run)
```bash
# Ver qué se va a desplegar sin aplicar cambios
helmfile -f helmfile.yaml.gotmpl \
  --state-values-set client=$CLIENT \
  --state-values-set useLocalCharts=true \
  -e $ENVIRONMENT \
  diff
```

#### Desinstalación
```bash
# Desinstalar release
helmfile -f helmfile.yaml.gotmpl \
  --state-values-set client=$CLIENT \
  --state-values-set useLocalCharts=true \
  -e $ENVIRONMENT \
  destroy
```

#### Debug y Troubleshooting
```bash
# Renderizar templates localmente
helm template mi-release . \
  -f values-common.yaml \
  -f clients/$CLIENT/values-$ENVIRONMENT.yaml

# Validar sintaxis del chart
helm lint .

# Ver valores computados finales
helmfile -f helmfile.yaml.gotmpl \
  --state-values-set client=$CLIENT \
  --state-values-set useLocalCharts=true \
  -e $ENVIRONMENT \
  write-values
```

## Troubleshooting

### Problemas Comunes

#### Pipeline CI/CD

**Error: "Falta ejecutar el job manual 'select_params'"**
- Solución: Ejecutar manualmente el job `select_params` y seleccionar CLIENT y ENVIRONMENT

**Error de proxy Maven**
- Verificar configuración de proxy en `MAVEN_CLI_OPTS`
- Comprobar conectividad: `curl -x http://10.49.89.10:8080 https://repo1.maven.org`

**Fallo en push de imagen Docker**
- Verificar credenciales `ARTIFACTORY_USERNAME` y `ARTIFACTORY_IDENTITY_KEY`
- Comprobar permisos en el registro de destino

#### Despliegues Locales

**Error: "no such file or directory: helmfile.yaml.gotmpl"**
- Verificar que estás en el directorio correcto del chart
- Comprobar que el archivo existe y tiene permisos de lectura

**Error de contexto Kubernetes**
- Verificar: `kubectl config current-context`
- Configurar: `export KUBECONTEXT="contexto-correcto"`

**Error: "values file not found"**
- Verificar que existe `clients/$CLIENT/values-$ENVIRONMENT.yaml`
- Comprobar variables CLIENT y ENVIRONMENT

### Logs y Debugging

```bash
# Ver logs detallados de helmfile
helmfile --log-level debug -f helmfile.yaml.gotmpl ...

# Ver estado de recursos K8s
kubectl --context=$KUBECONTEXT describe pod [POD_NAME] -n [NAMESPACE]

# Ver eventos del cluster
kubectl --context=$KUBECONTEXT get events -n [NAMESPACE] --sort-by=.metadata.creationTimestamp
```

## Mejores Prácticas

### Desarrollo
- Usar siempre el entorno `dev` para pruebas iniciales
- Validar cambios localmente antes del commit
- Mantener consistencia en nombres de valores entre entornos

### Seguridad
- No hardcodear credenciales en código
- Usar GitLab Variables para secretos
- Revisar permisos de namespaces K8s

### Operaciones
- Documentar cambios en valores específicos de cliente
- Mantener backup de configuraciones críticas
- Monitorear recursos después de despliegues

### Versionado
- Seguir versionado semántico para aplicaciones
- Usar tags Git para releases importantes
- Mantener changelog actualizado

---

## Contacto y Soporte

Para soporte técnico contactar:
- **Equipo DevOps**: genomica_pipe@t-systems.com
- **Documentación**: [Wiki interno T-Systems]
- **Issues**: [GitLab Issues del proyecto]

---

*Documento actualizado: {{ date }}*
*Versión: 1.0*
