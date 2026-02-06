# ArgoCD Jobs Sync Fix

## Problema

Cuando se despliega Airflow con ArgoCD, los Jobs (`create-user-job` y `migrate-database-job`) pueden presentar dos problemas:

1. **OutOfSync permanente**: Si los Jobs se configuran como hooks `Sync`, se ejecutan en CADA sincronización de ArgoCD, aunque solo deberían ejecutarse la primera vez o cuando hay cambios reales.

2. **Jobs no se ejecutan en el primer despliegue**: Si ArgoCD no reconoce correctamente los hooks, los Jobs pueden no ejecutarse durante la instalación inicial.

Esto afecta a:
- Sincronización de ArgoCD mostrando siempre el cluster como OutOfSync
- Ejecución innecesaria de jobs en cada sync (desperdicio de recursos)
- Jobs que no se ejecutan cuando deberían (primer despliegue)
- Confusión sobre el estado real del deployment

## Solución Implementada

Se han configurado los Jobs como **PreSync hooks** con política de eliminación **BeforeHookCreation** cuando `useHelmHooks` está deshabilitado (caso de ArgoCD).

### Cambios en los Templates

**Archivos modificados:**
- `templates/jobs/migrate-database-job.yaml`
- `templates/jobs/create-user-job.yaml`

Se modificó la lógica de anotaciones para usar PreSync hooks de ArgoCD cuando `useHelmHooks: false`:

```yaml
{{- $annotations := dict }}
{{- if .Values.migrateDatabaseJob.useHelmHooks }}
  {{- $_ := set $annotations "helm.sh/hook" "post-install,post-upgrade" }}
  {{- $_ := set $annotations "helm.sh/hook-weight" "1" }}
  {{- $_ := set $annotations "helm.sh/hook-delete-policy" "before-hook-creation,hook-succeeded" }}
{{- else }}
  {{- /* Para ArgoCD: usar PreSync hook con política de eliminación BeforeHookCreation */}}
  {{- $_ := set $annotations "argocd.argoproj.io/hook" "PreSync" }}
  {{- $_ := set $annotations "argocd.argoproj.io/hook-delete-policy" "BeforeHookCreation" }}
  {{- $_ := set $annotations "argocd.argoproj.io/sync-wave" "-1" }}
{{- end }}
```

### Anotaciones de ArgoCD Aplicadas

Cuando `useHelmHooks: false`, los jobs tendrán estas anotaciones:

#### migrate-database-job
```yaml
annotations:
  argocd.argoproj.io/hook: PreSync
  argocd.argoproj.io/hook-delete-policy: BeforeHookCreation
  argocd.argoproj.io/sync-wave: "-1"
```

#### create-user-job
```yaml
annotations:
  argocd.argoproj.io/hook: PreSync
  argocd.argoproj.io/hook-delete-policy: BeforeHookCreation
  argocd.argoproj.io/sync-wave: "0"
```

## Explicación de las Anotaciones

| Anotación | Valor | Descripción |
|-----------|-------|-------------|
| `argocd.argoproj.io/hook` | `PreSync` | Define que este recurso es un hook que se ejecuta ANTES de la sincronización principal. Esto garantiza que los Jobs se ejecuten antes de desplegar otros componentes. |
| `argocd.argoproj.io/hook-delete-policy` | `BeforeHookCreation` | Elimina el Job anterior ANTES de crear uno nuevo. Esto permite que el Job se reejecute cuando hay cambios reales en el manifiesto. |
| `argocd.argoproj.io/sync-wave` | `"-1"` o `"0"` | Define el orden de ejecución: primero la migración de BD (wave -1), luego la creación de usuario (wave 0), finalmente el resto de recursos (wave por defecto 0 o mayor). |

## Comportamiento

### Con PreSync + BeforeHookCreation:

**Primera instalación:**
1. ArgoCD detecta que los Jobs son PreSync hooks
2. Ejecuta `migrate-database-job` (sync-wave -1)
3. Ejecuta `create-user-job` (sync-wave 0)  
4. Una vez completados con éxito, despliega el resto de recursos
5. Los Jobs completados permanecen en el cluster

**Sincronizaciones posteriores SIN cambios:**
1. ArgoCD compara el manifiesto del Job con el Job existente
2. Si NO hay cambios, NO hace nada (el Job anterior sigue ahí, completado)
3. Estado: **Synced** ✅

**Sincronizaciones posteriores CON cambios:**
1. ArgoCD detecta cambios en el manifiesto del Job
2. Elimina el Job anterior (política `BeforeHookCreation`)
3. Crea y ejecuta el nuevo Job
4. Espera a que complete exitosamente
5. Continúa con el sync del resto de recursos

**Apagado/Encendido del cluster:**
1. Al apagar el cluster, los Jobs completados se pierden (como todo)
2. Al encender y hacer sync, ArgoCD detecta que no existen los Jobs
3. Los crea y ejecuta como en la primera instalación
4. ✅ Funciona correctamente con el ciclo diario de AWS

## Configuración en values.yaml

Los archivos de configuración de clientes deben tener:

**values-dev.yaml / values-pre.yaml / values-pro.yaml:**
```yaml
createUserJob:
  useHelmHooks: false  # ✅ Correcto para ArgoCD

migrateDatabaseJob:
  useHelmHooks: false  # ✅ Correcto para ArgoCD
```

## Beneficios

✅ Los Jobs se ejecutan correctamente en el primer despliegue  
✅ ArgoCD muestra el estado correcto (Synced) cuando no hay cambios  
✅ Los Jobs NO se reejecutan innecesariamente en cada sync  
✅ Se reejecutarán automáticamente si hay cambios reales en el Job  
✅ Orden de ejecución garantizado mediante sync-waves  
✅ Compatible con el flujo de trabajo de apagado/encendido diario del cluster en AWS  
✅ No requiere cambios en la configuración de ArgoCD  
✅ No desperdicia recursos ejecutando Jobs innecesarios

## Diferencias con Sync Hook

La configuración anterior usaba `hook: Sync`, lo cual causaba problemas:

| Característica | Sync Hook (❌ Anterior) | PreSync Hook (✅ Actual) |
|----------------|----------------------|------------------------|
| ¿Cuándo se ejecuta? | Durante cada sync | Solo cuando no existe o hay cambios |
| ¿Se elimina después? | Sí (`HookSucceeded`) | Solo antes de recrear (`BeforeHookCreation`) |
| ¿Se recrea en cada sync? | Sí, siempre | No, solo si hay cambios o no existe |
| Estado de ArgoCD | OutOfSync frecuente | Synced cuando no hay cambios |
| Recursos desperdiciados | Sí | No |

## Troubleshooting

### Los Jobs no se ejecutan en el primer despliegue

**Verificar:**
1. Que `useHelmHooks: false` esté configurado en values.yaml
2. Que los Jobs tengan las anotaciones de ArgoCD correctas
3. Que ArgoCD tenga permisos para crear Jobs
4. Revisar los logs de ArgoCD Application Controller

### ArgoCD muestra OutOfSync

**Verificar:**
1. Que los Jobs tengan `hook: PreSync` (no `Sync`)
2. Que la política sea `BeforeHookCreation` (no `HookSucceeded`)
3. Hacer un hard refresh en ArgoCD UI
4. Verificar que no haya drift en otros recursos

### Los Jobs se ejecutan en cada sync

**Verificar:**
1. Que NO tengan `hook: Sync`
2. Que la configuración use `PreSync` como se documenta aquí
3. Que los manifiestos del Job no cambien entre syncs (revisa templates con valores dinámicos)## Notas Importantes

- Esta solución solo aplica cuando `useHelmHooks: false`
- Si usas Helm directamente (sin ArgoCD), mantén `useHelmHooks: true`
- Los jobs se ejecutarán en cada Sync de ArgoCD, pero se limpiarán automáticamente
- Las migraciones de BD son idempotentes, por lo que es seguro ejecutarlas múltiples veces
- La creación de usuario también es idempotente (no crea duplicados)

## Testing

Para verificar que funciona correctamente:

```bash
# Generar template con ArgoCD hooks
helm template test . \
  --set migrateDatabaseJob.useHelmHooks=false \
  --set createUserJob.useHelmHooks=false \
  --set executor=CeleryExecutor | grep -A5 "argocd.argoproj.io"
```

Deberías ver las anotaciones de ArgoCD en los Jobs.

