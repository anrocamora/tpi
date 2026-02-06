# NiFi Helm Chart

Este chart despliega Apache NiFi 2.6.0 utilizando el operador de Stackable. Se ha configurado para utilizar imágenes alojadas en el registro privado de Telekom (MTR), reutiliza los charts oficiales de Stackable y crea automáticamente el secreto `mtr-credentials` requerido para las descargas de imágenes.

## Estructura de valores

- `values.yaml`: Valores por defecto del chart.
- `values-common.yaml`: Valores comunes compartidos por todos los entornos.
- `clients/sns-o/values-dev.yaml`: Ajustes específicos para el entorno de desarrollo (EKS + ALB + gp3).
- `clients/sns-o/values-pre.yaml` y `clients/sns-o/values-pro.yaml`: Archivos preparados para futuros ajustes de preproducción y producción.

Combina los ficheros según el entorno objetivo, por ejemplo:

```bash
helm dependency build ./nifi

helm install nifi ./nifi \
  --namespace tpi \
  -f values-common.yaml \
  -f clients/sns-o/values-dev.yaml
```

## Probando en un clúster local

Para validar el chart en un entorno local sin acceso al registro privado, desactiva el registro MTR y usa imágenes públicas:

```bash
helm dependency build ./nifi

helm upgrade --install nifi ./nifi \
  --namespace tpi \
  --set global.imageRegistry="" \
  --set nifi.image.repository=apache/nifi \
  --set nifi.image.tag="2.6.0" \
  -f values-common.yaml \
  -f clients/sns-o/values-dev.yaml
```

Asegúrate de disponer de un clúster Kubernetes accesible (por ejemplo Kind o Minikube) y del binario de `helm` antes de ejecutar el comando.

## Recursos instalados

- Dependencias `commons-operator`, `secret-operator`, `listener-operator` y `nifi-operator` desplegadas en `stackable-operators` (se pueden deshabilitar individualmente con `operators.<nombre>.enabled=false`). Los charts se resuelven desde `oci://oci.stackable.tech/sdp-charts` y se fuerzan a usar las imágenes internas `mtr.devops.telekom.de/genomica/stackable/*`.
- Recursos `Namespace` para `stackable-operators` y `tpi` cuando `operators.createNamespace` y `nifi.createNamespace` están activos.
- Recurso `NifiCluster` que describe el despliegue gestionado por el operador.
- Secretos `mtr-credentials` y opcionalmente el usuario único (`single-user`).
- Ingress HTTP en la ruta `/nifi` cuando está habilitado.
- Despliegue opcional `*-mcp` con su servicio y ruta `/mcp` para publicar el servidor MCP cuando `mcp.enabled=true`.
- **ConfigMap y Job de importación de pipelines** cuando `nifi.pipelines.enabled=true` (habilitado por defecto).

Cuando el MCP está activado se genera automáticamente un `ConfigMap` con la
configuración `config.yaml` esperada por el proyecto
[NiFiMCP](https://github.com/ms82119/NiFiMCP). Si no se definen servidores
explícitos en `mcp.config.servers`, se crea una entrada que referencia al
servicio de NiFi desplegado por el chart (incluyendo usuario y contraseña del
modo *single-user* cuando está habilitado). Esto permite que el MCP arranque
apuntando directamente al clúster sin configuraciones manuales adicionales.

## Importación automática de pipelines

El chart incluye funcionalidad para importar automáticamente pipelines (flows) de NiFi durante el despliegue.

**Nota importante**: A partir de **NiFi 1.27.0**, las pipelines se exportan en formato **JSON** (Flow Definitions) en lugar de XML (Templates). Este chart está configurado para trabajar con el nuevo formato JSON.

### ¿Cómo funciona?

1. **ConfigMap de pipelines**: Todos los archivos `.json` en la carpeta `pipelines/` se empaquetan automáticamente en un ConfigMap.
2. **Job de importación**: Después de la instalación/actualización del chart (usando hooks de Helm), se ejecuta un Job que:
   - Espera a que NiFi esté disponible
   - Se autentica usando las credenciales configuradas
   - Crea un Process Group para cada archivo JSON
   - Importa la definición del flow dentro del Process Group usando la API de NiFi 1.27.0

### Configuración

La importación de pipelines está habilitada por defecto. Puedes controlarla con los siguientes valores:

```yaml
nifi:
  pipelines:
    enabled: true  # Habilita/deshabilita la importación automática
    job:
      image: "curlimages/curl:latest"  # Imagen para el job de importación
      backoffLimit: 5  # Número de reintentos si falla
```

### Añadir nuevas pipelines

1. **Exportar desde NiFi UI** (NiFi 1.27.0+):
   - Haz clic derecho en el Process Group que deseas exportar
   - Selecciona **"Download flow definition"**
   - Guarda el archivo JSON descargado

2. **Agregar al chart**:
   - Coloca el archivo JSON en la carpeta `pipelines/`
   - Renombra el archivo con un nombre descriptivo (ej: `Mi_Pipeline.json`)
   - Los archivos se importarán automáticamente en el siguiente despliegue

Ejemplo:
```bash
# Copiar pipeline exportada desde NiFi
cp ~/Downloads/flow_snapshot.json pipelines/TSuPreMe_Pipeline.json

# Verificar que el JSON es válido
cat pipelines/TSuPreMe_Pipeline.json | jq .

# Desplegar
helm upgrade nifi ./nifi -n tpi -f values-common.yaml
```

### Formato de archivo JSON

Los archivos JSON deben seguir el formato de **Flow Definition** de NiFi 1.27.0:

```json
{
  "flowContents": {
    "identifier": "uuid-del-flow",
    "name": "Nombre del Pipeline",
    "processors": [...],
    "connections": [...],
    "controllerServices": [...]
  },
  "externalControllerServices": {},
  "parameterContexts": {},
  "flowEncodingVersion": "1.0"
}
```

### Deshabilitar la importación

Si prefieres gestionar las pipelines manualmente:

```bash
helm install nifi ./nifi \
  --namespace tpi \
  --set nifi.pipelines.enabled=false \
  -f values-common.yaml
```

### Verificar la importación

Después del despliegue:

```bash
# Ver logs del job de importación
kubectl logs -n tpi -l job=pipeline-importer --tail=100

# Verificar que el job completó exitosamente
kubectl get jobs -n tpi

# Acceder a NiFi UI y verificar los Process Groups importados
```

Consulta el fichero `templates/` para ver el detalle de los recursos renderizados.
