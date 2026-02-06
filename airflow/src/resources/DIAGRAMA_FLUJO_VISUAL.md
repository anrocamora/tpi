# Diagrama de Flujo - Solución Integrada

## Flujo Normal (con timeout de 4 horas)

```
┌─────────────────────────────────────────────────────────────────────┐
│           DAG: DAG_TSUPREME_001_TPIAGENT_UPLOADS                    │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │ check_nifi_availability│
                    │      (PythonOp)        │
                    └───────────┬────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  start_nifi_pipeline   │
                    │  ⚡ INICIA Process     │
                    │     Groups en NiFi     │
                    └───────────┬────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │ monitor_nifi_pipeline  │
                    │  👀 Espera actividad  │
                    └───────────┬────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │   wait_before_stop     │
                    │ ⏱️ Espera 4 horas     │
                    │   (TimeDeltaSensor)    │
                    └───────────┬────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  stop_nifi_pipeline    │
                    │  🛑 PARA Process      │
                    │     Groups en NiFi     │
                    │ (trigger_rule=ALL_DONE)│
                    └───────────┬────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │trigger_emergency_stop  │
                    │    🚨 SAFETY NET      │
                    │ (trigger_rule=ALL_DONE)│
                    │   wait_for_completion  │
                    └───────────┬────────────┘
                                │
                                │ Dispara →
                                │
┌───────────────────────────────▼──────────────────────────────────┐
│              DAG: STOP_NIFI_EMERGENCY                            │
└──────────────────────────────────────────────────────────────────┘
                                │
                                ▼
                    ┌───────────────────────────┐
                    │emergency_stop_all_nifi_pgs │
                    │    🔴 PARA TODOS LOS      │
                    │    Process Groups NiFi     │
                    │    (100% garantizado)      │
                    └────────────┬───────────────┘
                                 │
                                 ▼
                            ✅ SUCCESS
                    NiFi completamente parado


## Flujo con Parada Manual (Marca como Success)

┌─────────────────────────────────────────────────────────────────────┐
│  Usuario marca DAG como SUCCESS/FAILED desde UI de Airflow          │
└─────────────────────────────────────────────────────────────────────┘
                                │
                ┌───────────────┴───────────────┐
                │                               │
                ▼                               ▼
    ┌───────────────────┐          ┌──────────────────────┐
    │  DAG Callback     │          │ Task: trigger_       │
    │  (si se ejecuta)  │          │ emergency_stop       │
    │                   │          │ (siempre se ejecuta) │
    │ trigger_stop_dag_ │          │                      │
    │ callback()        │          │ trigger_rule=        │
    │                   │          │ ALL_DONE             │
    └────────┬──────────┘          └──────────┬───────────┘
             │                                 │
             │ Dispara DAG →                   │ Dispara DAG →
             │                                 │
             └──────────────┬──────────────────┘
                            │
                            ▼
            ┌───────────────────────────────────┐
            │    DAG: STOP_NIFI_EMERGENCY       │
            │    🚨 Se ejecuta desde 2 lugares  │
            │    (redundancia intencional)      │
            └────────────┬──────────────────────┘
                         │
                         ▼
             ┌─────────────────────────┐
             │emergency_stop_all_nifi  │
             │   🔴 PARA NiFi          │
             └────────┬────────────────┘
                      │
                      ▼
                 ✅ SUCCESS
         NiFi parado correctamente


## Flujo con Cancelación de Task

┌─────────────────────────────────────────────────────────────────────┐
│  Usuario cancela una task (kill) desde UI de Airflow                │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  Airflow mata la task │
                    │  (ej: wait_forever)   │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  Sensor.on_kill()     │
                    │  se ejecuta           │
                    │  automáticamente      │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │ stop_nifi_processors  │
                    │ (best_effort=True)    │
                    │ 🛑 PARA NiFi         │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │ trigger_emergency_stop│
                    │ también se ejecuta    │
                    │ (ALL_DONE rule)       │
                    └───────────┬───────────┘
                                │
                                ▼
            ┌───────────────────────────────────┐
            │    DAG: STOP_NIFI_EMERGENCY       │
            │    🚨 Safety net adicional        │
            └────────────┬──────────────────────┘
                         │
                         ▼
                    ✅ SUCCESS
         NiFi parado por múltiples mecanismos


## Capas de Seguridad (Safety Layers)

┌──────────────────────────────────────────────────────────────────┐
│                    CAPA 1: Task Explícita                        │
│  stop_nifi_pipeline (PythonOperator, trigger_rule=ALL_DONE)      │
│  → Se ejecuta siempre al final del DAG                           │
└──────────────────────────────────────────────────────────────────┘
                                ↓ Si falla ↓
┌──────────────────────────────────────────────────────────────────┐
│              CAPA 2: Trigger DAG de Emergencia                   │
│  trigger_emergency_stop (TriggerDagRunOperator, ALL_DONE)        │
│  → Dispara STOP_NIFI_EMERGENCY automáticamente                   │
└──────────────────────────────────────────────────────────────────┘
                                ↓ Si falla ↓
┌──────────────────────────────────────────────────────────────────┐
│                   CAPA 3: DAG Callbacks                          │
│  on_success_callback / on_failure_callback                       │
│  → Dispara STOP_NIFI_EMERGENCY desde callback                    │
└──────────────────────────────────────────────────────────────────┘
                                ↓ Si falla ↓
┌──────────────────────────────────────────────────────────────────┐
│                  CAPA 4: Sensor on_kill()                        │
│  StopNiFiOnKillTimeDeltaSensor.on_kill()                         │
│  → Se ejecuta si Airflow cancela la task                         │
└──────────────────────────────────────────────────────────────────┘

💪 RESULTADO: 4 capas independientes = Alta confiabilidad


## Estados del DAG y Acciones

┌─────────────────┬──────────────────────────────────────────────────┐
│  Estado DAG     │  Mecanismo de Parada Activado                    │
├─────────────────┼──────────────────────────────────────────────────┤
│  ✅ Success     │  1. stop_nifi_pipeline (task)                    │
│                 │  2. trigger_emergency_stop (task)                │
│                 │  3. on_success_callback (dispara DAG emergency)  │
├─────────────────┼──────────────────────────────────────────────────┤
│  ❌ Failed      │  1. trigger_emergency_stop (ALL_DONE rule)       │
│                 │  2. on_failure_callback (dispara DAG emergency)  │
├─────────────────┼──────────────────────────────────────────────────┤
│  🚫 Cancelled   │  1. on_kill() en sensors                         │
│  (kill task)    │  2. trigger_emergency_stop (ALL_DONE rule)       │
├─────────────────┼──────────────────────────────────────────────────┤
│  🕐 Timeout     │  1. wait_before_stop termina                     │
│  alcanzado      │  2. stop_nifi_pipeline ejecuta                   │
│                 │  3. trigger_emergency_stop ejecuta               │
└─────────────────┴──────────────────────────────────────────────────┘


## Verificación Visual en UI de Airflow

Grafo del DAG Principal:
┌────────────────────────────────────────────────────────────────┐
│  ○ check_nifi_availability                                      │
│           ↓                                                     │
│  ○ start_nifi_pipeline                                          │
│           ↓                                                     │
│  ○ monitor_nifi_pipeline                                        │
│           ↓                                                     │
│  ○ wait_before_stop (o wait_forever)                            │
│           ↓                                                     │
│  ○ stop_nifi_pipeline                                           │
│           ↓                                                     │
│  ○ trigger_emergency_stop_dag  ←─── 🎯 AQUÍ ESTÁ LA MAGIA      │
│                                                                 │
│  Esta task dispara → STOP_NIFI_EMERGENCY                        │
└────────────────────────────────────────────────────────────────┘

Cuando ejecutes, verás:
  ○ = queued (gris)
  ● = running (verde claro)
  ✓ = success (verde)
  ✗ = failed (rojo)
  ⊗ = skipped (rosa)

La task "trigger_emergency_stop_dag" siempre debe estar en ✓ (verde)


## Logs para Monitorear

### En stop_nifi_pipeline:
```
[NiFi][STOP] Stopping NiFi processors... source='explicit_stop_task'
[NiFi][STOP] ✓ Stopped 3 process groups successfully
```

### En trigger_emergency_stop_dag:
```
[trigger_dagrun] Triggering DAG: STOP_NIFI_EMERGENCY
[trigger_dagrun] Waiting for DAG to complete...
[trigger_dagrun] ✓ DAG completed successfully
```

### En DAG callbacks (si se ejecutan):
```
[DAG][callback] Triggering STOP_NIFI_EMERGENCY as safety net...
[DAG][callback] ✓ Successfully triggered STOP_NIFI_EMERGENCY
```

### En STOP_NIFI_EMERGENCY:
```
============================================================
EMERGENCY STOP - Deteniendo todos los Process Groups de NiFi
============================================================
✓ Stopped process group: pg-abc123
✓ Stopped process group: pg-def456
============================================================
EMERGENCY STOP COMPLETED
Stopped: 2 process groups
Errors: 0 process groups
============================================================
```
