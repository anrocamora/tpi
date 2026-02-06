# Redis Graceful Shutdown Fix

## Problema

Cuando se apaga el cluster de Kubernetes, el pod de Redis se quedaba colgado y no terminaba correctamente a menos que se forzara el apagado. Esto afectaba especialmente a:

- Autoscaling en AWS cuando se apagan y encienden los clusters diariamente
- Operaciones de mantenimiento del cluster
- Escalado de nodos

## Solución Implementada

Se han realizado dos cambios principales para solucionar este problema:

### 1. Reducción del terminationGracePeriodSeconds

**Archivo:** `values.yaml`

Se redujo el tiempo de gracia de terminación de **600 segundos (10 minutos)** a **30 segundos**:

```yaml
redis:
  enabled: true
  terminationGracePeriodSeconds: 30  # Antes era 600
```

Este cambio evita que Kubernetes espere demasiado tiempo antes de forzar el cierre del pod.

### 2. Implementación de preStop Hook

**Archivo:** `templates/redis/redis-statefulset.yaml`

Se agregó un hook `preStop` en el lifecycle del contenedor de Redis para ejecutar un shutdown correcto:

```yaml
lifecycle:
  preStop:
    exec:
      command:
        - sh
        - -c
        - redis-cli -a "${REDIS_PASSWORD}" shutdown save
```

Este hook:
- Ejecuta el comando `redis-cli shutdown save` antes de que Kubernetes mate el proceso
- Guarda los datos en disco antes de cerrar
- Permite que Redis se apague limpiamente
- Utiliza la variable de entorno `REDIS_PASSWORD` para autenticarse

## Beneficios

✅ Redis se apaga correctamente sin necesidad de forzar el cierre  
✅ Se guardan los datos antes del shutdown  
✅ Mejora la estabilidad en escenarios de autoscaling  
✅ Reduce el tiempo de espera durante el apagado del cluster  
✅ Compatible con configuraciones personalizadas mediante `containerLifecycleHooks`

## Notas

- Si ya tienes configurado `redis.containerLifecycleHooks` en tus values, el hook personalizado tiene prioridad sobre el preStop por defecto
- El cambio es compatible con versiones anteriores
- Se recomienda probar el cambio en un entorno de desarrollo antes de aplicarlo en producción

