# TPI Agent Service Helm Chart

## Descripción

Chart de Helm para desplegar el servicio TPI Agent en Kubernetes. Este agente monitoriza directorios y sube automáticamente archivos grandes a S3, publicando eventos de progreso en Kafka.

## Prerrequisitos

1. **Cluster Kubernetes** (K3s, EKS, etc.)
2. **CSI SMB Driver** instalado en el clúster (`smb.csi.k8s.io`)
3. **Acceso a un share SMB/Samba** (puerto 445) y credenciales
4. **Kafka cluster** con SSL configurado
5. **Credenciales AWS S3** para cada entorno
6. **Pull Secret** `mtr-credentials` para el registry de imágenes

## Estructura de Directorios

El agente utiliza la siguiente estructura de directorios en el volumen SMB:

```
/data/tpi-agent-snso-{env}/
├── source/        # Archivos a subir (monitoreado)
├── completed/     # Archivos subidos exitosamente
├── failed/        # Archivos que fallaron
└── logs/          # Logs de la aplicación
```

## Configuración de almacenamiento SMB (Samba)

A partir de esta versión, el chart soporta dos modos:

- **dynamic (por defecto)**: crea una **StorageClass** (`<release-name>-smb`) y un **PVC**, y el driver SMB CSI aprovisiona el PV automáticamente.
- **static**: crea **PV + PVC** como hasta ahora (sin StorageClass).

### Parámetros principales

- `storage.smb.provisioning`: `dynamic` | `static`
- `storage.smb.storageClassName`: opcional. Si está vacío, usa `<release-name>-smb`.
- `storage.smb.reclaimPolicy`: recomendado `Delete` en modo `dynamic`.
- `storage.smb.mountOptions`: opciones de montaje SMB (se aplican en la StorageClass y también en el modo estático).

### Ejemplo: forzar modo estático (fallback)

```bash
helm upgrade --install tpi-agent-service-pre ./tpi-agent-service \
  -f ./tpi-agent-service/clients/sns-o/values-pre.yaml \
  --set storage.smb.provisioning=static \
  -n tpi
```

## Selección de backend de almacenamiento

El chart soporta tres backends seleccionables con `storage.type`:

- `smb` (**por defecto**): SMB/Samba (con soporte `dynamic` y `static`).
- `nfs`: NFS (actualmente con PV+PVC estático en el chart).
- `local`: almacenamiento local efímero (`emptyDir`) dentro del Pod.

### Ejemplo: usar `local` (emptyDir)

```bash
helm upgrade --install tpi-agent-service-pre ./tpi-agent-service \
  -f ./tpi-agent-service/clients/sns-o/values-pre.yaml \
  --set storage.type=local \
  -n tpi
```

### Ejemplo: usar `nfs`

```bash
helm upgrade --install tpi-agent-service-pre ./tpi-agent-service \
  -f ./tpi-agent-service/clients/sns-o/values-pre.yaml \
  --set storage.type=nfs \
  --set storage.nfs.server=10.0.0.1 \
  --set storage.nfs.path=/export/data \
  -n tpi
```

## Configuración por Entorno

### Entorno DEV

```bash
helm install tpi-agent-service-dev ./tpi-agent-service \
  -f ./tpi-agent-service/clients/sns-o/values-dev.yaml \
  -n tpi --create-namespace
```

### Entorno PRE

```bash
helm install tpi-agent-service-pre ./tpi-agent-service \
  -f ./tpi-agent-service/clients/sns-o/values-pre.yaml \
  -n tpi --create-namespace
```

### Entorno PRO

```bash
helm install tpi-agent-service-pro ./tpi-agent-service \
  -f ./tpi-agent-service/clients/sns-o/values-pro.yaml \
  -n tpi --create-namespace
```

## Verificación del Despliegue

### 1. Verificar el Pod

```bash
kubectl get pods -n tpi
kubectl logs -f <pod-name> -n tpi
```

### 2. Verificar los Directorios

Desde el pod:

```bash
kubectl exec -it <pod-name> -n tpi -- sh
ls -la /data/tpi-agent-snso-pre/
```

## Troubleshooting

### El pod no inicia (CrashLoopBackOff)

1. Verifica los logs del initContainer:
   ```bash
   kubectl logs <pod-name> -n tpi -c init-directories
   ```

2. Verifica que el CSI SMB driver está instalado:
   ```bash
   kubectl get csidriver | grep smb
   ```

3. Si hay errores de montaje SMB, revisa los eventos del Pod:
   ```bash
   kubectl describe pod <pod-name> -n tpi
   ```

## Actualización del Chart

Para actualizar una release existente:

```bash
helm upgrade tpi-agent-service-pre ./tpi-agent-service \
  -f ./tpi-agent-service/clients/sns-o/values-pre.yaml \
  -n tpi
```

## Desinstalación

```bash
helm uninstall tpi-agent-service-pre -n tpi
```
