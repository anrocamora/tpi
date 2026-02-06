# sync_pg_dev_to_pre.py

Script para copiar `PG_*.json` de `DEV` a `PRE` cambiando **solo** `parameterContexts` (nombres y valores).

---

## 📑 Convención de Documentación

> **Importante**: Cada Process Group principal debe tener su documentación funcional en un archivo `PG_{NOMBRE_COMPLETO}.md` ubicado en el directorio del cliente correspondiente.
>
> **Ejemplo**: `pipelines/sns-o/PG_TSUPREME_001_TPIAGENT_UPLOADS.md`
>
> Esta documentación debe contener: objetivo funcional, configuración por entorno, contrato de datos, flujo operativo, reglas de catalogación, guía de troubleshooting y referencias a los JSONs DEV/PRE.

---

## Qué hace

- Busca ficheros:
  - `src/main/helm/charts/nifi/pipelines/*/dev/PG_*.json`
- Copia a:
  - `src/main/helm/charts/nifi/pipelines/*/pre/`
- Renombra el fichero destino:
  - `*_DEV.json` → `*_PRE.json`
- Dentro del JSON, modifica únicamente:
  - `parameterContexts`: renombra claves y `name` sustituyendo `_DEV` → `_PRE` (para todos los contextos que lo contengan)
  - `parameterContexts[*].parameters[].value`: sustituye por valores PRE del catálogo (por nombre de parámetro)

## Catálogo PRE (fuente de valores)

El script construye un catálogo leyendo los `parameterContexts` de **todos** los ficheros que ya existan en PRE:

- `pipelines/*/pre/PG_*.json`

El catálogo se indexa por:
- `contextName` (el `name` del contexto; si falta, usa la clave del diccionario)
- `parameterName` (el `parameters[].name`)

> Importante: si no hay **ningún** `PG_*.json` en `*/pre/`, el script fallará porque no tiene de dónde sacar valores PRE.

## Reglas de actualización (importante)

- El fichero PRE se **regenera completo** a partir del DEV (se copia el JSON DEV entero y solo se tocan `parameterContexts`).
- Preservación de valores PRE:
  - Si en PRE ya existe un parámetro con `value`, ese `value` tiene prioridad y se conserva.
  - Esto aplica también a parámetros sensibles (p. ej. `mongo.pwd`): aunque en DEV no venga `value`, si PRE lo trae, se reinyecta.
- Si el parámetro no existe en el catálogo PRE → se conserva el valor DEV (si lo tenía).
- El script no inventa valores: si PRE no trae `value` y DEV tampoco, no se añade.

- No se toca nada fuera de `parameterContexts` (no se cambian nombres de flow, comentarios, etc.).

## Uso

Desde el directorio `src/main/helm/charts/nifi/pipelines`:

```powershell
python .\sync_pg_dev_to_pre.py --dry-run --verbose
```

Para escribir/sobrescribir destinos ya existentes:

```powershell
python .\sync_pg_dev_to_pre.py --overwrite --verbose
```

## Limitaciones

- El JSON se reserializa con `indent=2`, por lo que el formato/espaciado del fichero resultante puede cambiar.
  La estructura se mantiene y el script solo altera los datos bajo `parameterContexts`.
- El catálogo PRE se toma de los `PG_*.json` ya existentes en `*/pre/`. Si necesitas valores PRE “base” para un cliente nuevo,
  primero tendrás que tener al menos un `PG_*.json` en PRE (aunque sea de referencia).
