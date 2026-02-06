# Diseño de ingeniería de software — UC-DS-001 y UC-DS-002

**Ámbito**: Este documento cubre exclusivamente los casos de uso **UC-DS-001 (subida a landing zone)** y **UC-DS-002 (catalogación de ficheros)**.

<!-- TOC -->
* [Diseño de ingeniería de software — UC-DS-001 y UC-DS-002](#diseño-de-ingeniería-de-software--uc-ds-001-y-uc-ds-002)
  * [1. Objetivo y alcance](#1-objetivo-y-alcance)
  * [2. Trazabilidad funcional](#2-trazabilidad-funcional)
  * [3. Arquitectura de referencia](#3-arquitectura-de-referencia)
  * [4. UC-DS-001 — Subida de datos a landing zone](#4-uc-ds-001--subida-de-datos-a-landing-zone)
    * [4.1 Contrato de entrada](#41-contrato-de-entrada)
    * [4.2 Flujo principal](#42-flujo-principal)
    * [4.3 Máquina de estados y resiliencia](#43-máquina-de-estados-y-resiliencia)
    * [4.4 Reglas de particionado multipart](#44-reglas-de-particionado-multipart)
    * [4.5 Eventos y contrato Kafka](#45-eventos-y-contrato-kafka)
    * [4.6 Riesgos técnicos y mitigaciones](#46-riesgos-técnicos-y-mitigaciones)
  * [5. UC-DS-002 — Catalogación de ficheros](#5-uc-ds-002--catalogación-de-ficheros)
    * [5.1 Orquestación Airflow](#51-orquestación-airflow)
    * [5.2 Diseño funcional de pipeline NiFi](#52-diseño-funcional-de-pipeline-nifi)
    * [5.3 Secuencia de catalogación en Tomic](#53-secuencia-de-catalogación-en-tomic)
    * [5.4 Idempotencia, concurrencia y orden](#54-idempotencia-concurrencia-y-orden)
    * [5.5 Gestión de errores y DLQ](#55-gestión-de-errores-y-dlq)
  * [6. Arquitectura de datos](#6-arquitectura-de-datos)
  * [7. Operación, observabilidad y SRE](#7-operación-observabilidad-y-sre)
  * [8. Criterios de aceptación técnicos](#8-criterios-de-aceptación-técnicos)
  * [9. Decisiones de diseño (ADR-lite)](#9-decisiones-de-diseño-adr-lite)
<!-- TOC -->

## 1. Objetivo y alcance

Este diseño especifica cómo se implementan y operan, de extremo a extremo, los dos casos de uso actualmente productivos de la cadena de subida/catálogo:

- **UC-DS-001**: detección de runs finalizados y subida confiable a S3 compatible.
- **UC-DS-002**: consumo del evento de completitud y catalogación en Tomic/OpenCGA.

Quedan explícitamente fuera de alcance en este documento los UC-DS-003..007.

## 2. Trazabilidad funcional

| Caso de uso | Componente principal | Componentes de soporte | Evidencia técnica |
|---|---|---|---|
| UC-DS-001 | TPI Agent Service | Kafka events/state, S3 | Scheduler + UploadService + UploadStateStore |
| UC-DS-002 | NiFi PG_TSUPREME_001_TPIAGENT_UPLOADS | Airflow DAG, Tomic API, DistributedMapCache | DAG_TSUPREME_001_TPIAGENT_UPLOADS + NiFi PG documentado |

## 3. Arquitectura de referencia

```mermaid
%% Fuente canónica: diagrams/contexto_uc1_uc2.mmd
graph TB
    U[Usuario operador] --> SMB[Unidad de red SMB/CIFS]
    SMB --> AG[TPI Agent]
    AG -->|multipart upload| S3[THealthLake S3]
    AG -->|UPLOAD_*| KE[Kafka Events]
    AG -->|estado compactado| KS[Kafka State]

    AF[Airflow DAG] -->|start/monitor/stop| NF[NiFi PG]
    NF -->|consume UPLOAD_COMPLETED| KE
    NF -->|create/link/update| TO[Tomic API]
    TO --> DB[(TCatalog)]
```

## 4. UC-DS-001 — Subida de datos a landing zone

### 4.1 Contrato de entrada

**Trigger de run listo**: presencia de `RunCompletionStatus.xml` en raíz del run.

**Precondiciones**:
- El run es legible desde el punto de montaje SMB/NFS del agente.
- Existe conectividad al endpoint S3 y al broker Kafka.

### 4.2 Flujo principal

```mermaid
%% Fuente canónica: diagrams/secuencia_uc1_happy_path.mmd
sequenceDiagram
    participant Op as Operador
    participant Fs as Unidad de red
    participant Ag as TPI Agent
    participant S3 as S3
    participant K as Kafka

    Op->>Fs: Copia run completo + RunCompletionStatus.xml
    Ag->>Fs: Escaneo periódico (30s)
    Ag->>K: UPLOAD_STARTED (+folder opcional)
    loop cada fichero
      Ag->>S3: upload (single-part o multipart)
      Ag->>K: UPLOAD_PROGRESS
    end
    Ag->>K: UPLOAD_COMPLETED (+folder completo)
    Ag->>Fs: mover run a completed/
```

### 4.3 Máquina de estados y resiliencia

```mermaid
%% Fuente canónica: diagrams/estados_upload_uc1.mmd
stateDiagram-v2
    [*] --> DETECTED
    DETECTED --> STARTED
    STARTED --> IN_PROGRESS
    IN_PROGRESS --> COMPLETED
    IN_PROGRESS --> FAILED
    IN_PROGRESS --> ABORTED
    IN_PROGRESS --> ABANDONED
    COMPLETED --> [*]
    FAILED --> [*]
    ABORTED --> [*]
    ABANDONED --> [*]
```

Aspectos de resiliencia:
- Persistencia de estado de subida en tópico compactado para recuperación post-restart.
- Reintentos en operaciones S3 multipart por parte.
- Abort explícito de multipart ante fallo irrecuperable.

### 4.4 Reglas de particionado multipart

```mermaid
%% Fuente canónica: diagrams/diagrama_flujo_multipart.mmd
sequenceDiagram
    participant C as Cliente S3
    participant M as Multipart Session
    participant F as File Stream

    C->>M: CreateMultipartUpload
    loop partNumber 1..N
      F->>C: read chunk (ej. 64MB)
      C->>M: UploadPart(partNumber, bytes)
      M-->>C: ETag
    end
    C->>M: CompleteMultipartUpload(ETags)
```

Reglas:
- `size == 0`: subida simple.
- `size < threshold`: single-part.
- `size >= threshold`: multipart.

### 4.5 Eventos y contrato Kafka

Eventos funcionales de UC-DS-001:
- `UPLOAD_STARTED`
- `UPLOAD_PROGRESS`
- `UPLOAD_COMPLETED`
- `UPLOAD_FAILED`

Contrato mínimo esperado para activar UC-DS-002:
- `eventType == "UPLOAD_COMPLETED"`
- `uploadId`, `agentId`
- `folder` recursivo con `files[]` y `folders[]`

### 4.6 Riesgos técnicos y mitigaciones

| Riesgo | Impacto | Mitigación |
|---|---|---|
| Corte de red con S3 | subida incompleta | retry + resume + estado en Kafka |
| Run corrupto/incompleto | catálogo inconsistente | trigger estricto por `RunCompletionStatus.xml` |
| Volumen alto de ficheros pequeños | latencia elevada | paralelismo acotado + eventos ligeros en progreso |

## 5. UC-DS-002 — Catalogación de ficheros

### 5.1 Orquestación Airflow

```mermaid
%% Fuente canónica: diagrams/orquestacion_uc2_airflow_nifi.mmd
graph LR
    C[check_nifi_availability] --> S[start_nifi_pipeline]
    S --> M[monitor_nifi_pipeline]
    M --> W{stop_after_minutes}
    W -->|>0| T[wait_before_stop]
    W -->|-1| I[wait_forever]
    T --> P[stop_nifi_pipeline]
    I --> P
    P --> E[trigger_emergency_stop_dag]
```

La orquestación separa gobierno operativo (Airflow) de procesamiento de datos (NiFi).

### 5.2 Diseño funcional de pipeline NiFi

```mermaid
%% Fuente canónica: diagrams/pipeline_nifi_detallado_uc2.mmd
graph TD
    CK[ConsumeKafkaRecord] --> FLT{eventType==UPLOAD_COMPLETED}
    FLT -->|sí| PARSE[Parse árbol Folder recursivo]
    FLT -->|no| DROP[Drop/ignore]

    PARSE --> DQ[Build queue directorios padre->hijo]
    PARSE --> FQ[Build queue ficheros]

    DQ --> AUTH[Ensure JWT token cacheado]
    AUTH --> CR[InvokeHTTP POST /files/create]

    FQ --> LK[InvokeHTTP POST /files/link]
    LK --> TG[InvokeHTTP POST /files/update tags]

    CR --> OK[Success]
    TG --> OK
    CR --> ERR[Retry/penalize/DLQ]
    LK --> ERR
    TG --> ERR
```

### 5.3 Secuencia de catalogación en Tomic

```mermaid
%% Fuente canónica: diagrams/secuencia_uc2_catalogacion.mmd
sequenceDiagram
    participant N as NiFi
    participant T as Tomic API
    participant C as Cache Token

    N->>C: get token (cache)
    alt token ausente/expirado
      N->>T: POST /users/login
      T-->>N: JWT
      N->>C: put JWT
    end

    loop cada directorio (orden padre->hijo)
      N->>T: POST /files/create (DIRECTORY)
      T-->>N: 200/409
    end

    loop cada fichero
      N->>T: POST /files/link
      T-->>N: 200/409
      N->>T: POST /files/update (tags run/sample)
      T-->>N: 200
    end
```

### 5.4 Idempotencia, concurrencia y orden

Reglas clave:
- Orden estricto de directorios (`padre -> hijo`) para evitar dependencias rotas.
- Manejo idempotente de respuestas `409` (recurso ya existente).
- Reintento con backoff ante errores transitorios HTTP.
- Granularidad de unidad de trabajo por run para mantener trazabilidad.

### 5.5 Gestión de errores y DLQ

```mermaid
%% Fuente canónica: diagrams/errores_uc2_dlq.mmd
graph TD
    P[Processor InvokeHTTP] --> R{HTTP status}
    R -->|2xx| OK[success]
    R -->|409| IDEM[idempotent success]
    R -->|429/5xx| RETRY[retry con backoff]
    R -->|4xx no recuperable| DLQ[enviar a DLQ]
    RETRY --> LIM{max reintentos}
    LIM -->|no| P
    LIM -->|sí| DLQ
```

## 6. Arquitectura de datos

**Evento de integración**: Avro `UploadEvent` con árbol recursivo `Folder`.

**Claves de correlación recomendadas**:
- `uploadId` (unidad técnica)
- `runId` (unidad funcional)
- `agentId` (origen)

**Taxonomía de tags de catálogo**:
- `run_id`
- `sample_id`
- tags operativos de trazabilidad (`source_id`, `agent_id`, etc.)

## 7. Operación, observabilidad y SRE

Métricas recomendadas:
- UC-001: tiempo total de subida por run, throughput MB/s, ratio de fallos por fichero.
- UC-002: lag de consumidor Kafka, tiempo de catalogación por run, tasa DLQ.

Alertas recomendadas:
- ausencia de `UPLOAD_COMPLETED` para runs detectados en ventana esperada;
- crecimiento continuo de DLQ;
- token/login failures repetidos en Tomic.

## 8. Criterios de aceptación técnicos

- Run detectado y transferido a S3 con estructura de paths preservada.
- Publicación de eventos Kafka de inicio/progreso/fin.
- Consumo de `UPLOAD_COMPLETED` y catalogación completa en Tomic.
- Reejecución segura (idempotente) sin duplicados funcionales dañinos.
- Evidencia operativa en logs/metrics para auditoría.

## 9. Decisiones de diseño (ADR-lite)

1. **Event-driven entre UC-001 y UC-002** para desacoplar tiempos de subida y catalogación.
2. **Airflow para control de ciclo de vida de NiFi**, evitando acoplar control operativo al flujo de datos.
3. **Persistencia de estado de subida en Kafka compactado** para recuperación tras reinicios.
4. **Catalogación idempotente** como criterio de robustez ante reintentos y reprocesados.
