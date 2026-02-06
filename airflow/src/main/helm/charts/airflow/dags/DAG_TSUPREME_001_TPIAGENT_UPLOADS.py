"""
DAG para aprovisionar, operar y orquestar el flujo de datos de TSuPreMe, gestionando tópicos de Kafka y flujos de NiFi.

VERSIÓN INTEGRADA: Dispara automáticamente el DAG de parada al finalizar (éxito, fallo o cancelación).

Reglas:
- El DAG se llama: DAG_TSUPREME_001_TPIAGENT_UPLOADS
- nifi_stop_after_minutes == -1 => infinito (solo parada manual o por trigger del stop DAG)
- nifi_stop_after_minutes > 0 => timeout automático
- Al finalizar SIEMPRE dispara STOP_NIFI_EMERGENCY como safety net
"""

from __future__ import annotations

from datetime import datetime, timedelta
import json
import logging
import time
import urllib3

import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.utils.trigger_rule import TriggerRule

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)


# ------------------------
# Config
# ------------------------

def get_stop_after_minutes() -> int:
    """Lee nifi_stop_after_minutes desde Variables.

    - -1 => infinito (solo parada manual)
    - >0 => minutos
    - inválido => -1
    """
    raw = None
    try:
        raw = Variable.get('nifi_stop_after_minutes', default_var='-1')
        value = int(str(raw).strip())
    except Exception:
        logger.warning("Variable nifi_stop_after_minutes inválida (%r). Usando -1.", raw)
        return -1

    if value < -1:
        logger.warning("Variable nifi_stop_after_minutes=%s < -1. Usando -1.", value)
        return -1

    logger.info("nifi_stop_after_minutes=%s", value)
    return value


STOP_AFTER_MINUTES = get_stop_after_minutes()


# ------------------------
# NiFi helpers
# ------------------------

def get_nifi_token() -> str:
    connection = BaseHook.get_connection('nifi_default')
    nifi_base_url = connection.host

    token_url = f"{nifi_base_url}/nifi-api/access/token"

    resp = requests.post(
        token_url,
        data={'username': connection.login, 'password': connection.password},
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        verify=False,
        timeout=30,
    )

    if resp.status_code != 201:
        raise Exception(f'Failed to get NiFi token: {resp.status_code} - {resp.text}')

    return resp.text


def _dedupe_stable(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _parse_listish(value) -> list[str]:
    if value is None:
        return []

    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]

    text = str(value).strip()
    if not text:
        return []

    if text.startswith('[') and text.endswith(']'):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            pass

    return [part.strip() for part in text.split(',') if part.strip()]


def get_nifi_process_group_ids(*, token: str, nifi_base_url: str) -> list[str]:
    """IDs bajo root, opcionalmente filtrados por Variable `nifi_process_group_names`."""

    try:
        raw_names = Variable.get('nifi_process_group_names', default_var=None, deserialize_json=True)
    except TypeError:
        raw_names = Variable.get('nifi_process_group_names', default_var=None)

    target_names = set(_parse_listish(raw_names))

    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {token}'}
    url = f"{nifi_base_url}/nifi-api/flow/process-groups/root"
    resp = requests.get(url, headers=headers, verify=False, timeout=30)

    if resp.status_code != 200:
        raise Exception(f"Failed to list root process group children: {resp.status_code} - {resp.text}")

    data = resp.json() if resp.content else {}
    pgs = data.get('processGroupFlow', {}).get('flow', {}).get('processGroups', [])

    ids: list[str] = []
    for pg in pgs:
        component = pg.get('component', {}) or {}
        pg_id = component.get('id')
        pg_name = (component.get('name') or '').strip()

        if not pg_id:
            continue
        if target_names and pg_name not in target_names:
            continue
        ids.append(pg_id)

    ids = _dedupe_stable(ids)
    if not ids:
        raise ValueError('No se encontraron Process Groups bajo root en NiFi (o no coinciden con nifi_process_group_names).')

    logger.info("Resolved process group IDs (count=%s): %s", len(ids), ids)
    return ids


def check_nifi_availability():
    token = get_nifi_token()
    connection = BaseHook.get_connection('nifi_default')
    nifi_base_url = connection.host

    url = f'{nifi_base_url}/nifi-api/system-diagnostics'
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {token}'}
    resp = requests.get(url, headers=headers, verify=False, timeout=30)

    if resp.status_code != 200:
        raise Exception(f"NiFi returned non-200 status: {resp.status_code} - {resp.text[:200]}")

    logger.info("NiFi is available")
    return True


def start_nifi_processors():
    token = get_nifi_token()
    connection = BaseHook.get_connection('nifi_default')
    nifi_base_url = connection.host

    process_group_ids = get_nifi_process_group_ids(token=token, nifi_base_url=nifi_base_url)

    headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': f'Bearer {token}'}
    results = {}

    for pg_id in process_group_ids:
        url = f'{nifi_base_url}/nifi-api/flow/process-groups/{pg_id}'
        payload = {'id': pg_id, 'state': 'RUNNING'}
        resp = requests.put(url, headers=headers, json=payload, verify=False, timeout=30)
        if resp.status_code not in (200, 201):
            raise Exception(f'Failed to start NiFi process group {pg_id}: {resp.status_code} - {resp.text}')
        results[pg_id] = resp.json() if resp.content else {'status': 'success'}

    logger.info("[NiFi] Started %d process groups", len(results))
    return results


def monitor_nifi_pipeline():
    """Monitor simple: espera actividad durante un tiempo corto."""
    token = get_nifi_token()
    connection = BaseHook.get_connection('nifi_default')
    nifi_base_url = connection.host

    process_group_ids = get_nifi_process_group_ids(token=token, nifi_base_url=nifi_base_url)

    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {token}'}

    max_attempts = 10
    sleep_seconds = 10

    remaining = set(process_group_ids)

    for attempt in range(max_attempts):
        if not remaining:
            return True

        logger.info("Monitor attempt %s/%s remaining=%s", attempt + 1, max_attempts, sorted(remaining))

        done = set()
        for pg_id in sorted(remaining):
            url = f'{nifi_base_url}/nifi-api/flow/process-groups/{pg_id}/status'
            resp = requests.get(url, headers=headers, verify=False, timeout=30)
            if resp.status_code != 200 or not resp.content:
                continue

            status = resp.json()
            stats = status.get('processGroupStatus', {}).get('aggregateSnapshot', {})
            if any((stats.get(k, 0) or 0) > 0 for k in ('bytesIn', 'bytesOut', 'bytesRead', 'bytesWritten', 'flowFilesIn', 'flowFilesOut')):
                done.add(pg_id)

        remaining -= done
        if remaining:
            time.sleep(sleep_seconds)

    logger.warning("Monitor finished with no activity in PGs=%s. Continuing.", sorted(remaining))
    return True


def stop_nifi_processors(*, best_effort: bool = False, source: str = 'task'):
    """Parar todos los PGs configurados."""
    logger.warning("[NiFi][STOP] Stopping NiFi processors... source=%r best_effort=%s", source, best_effort)

    try:
        token = get_nifi_token()
        connection = BaseHook.get_connection('nifi_default')
        nifi_base_url = connection.host

        process_group_ids = get_nifi_process_group_ids(token=token, nifi_base_url=nifi_base_url)
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': f'Bearer {token}'}

        results = {}
        errors = {}

        for pg_id in process_group_ids:
            url = f'{nifi_base_url}/nifi-api/flow/process-groups/{pg_id}'
            payload = {'id': pg_id, 'state': 'STOPPED'}
            resp = requests.put(url, headers=headers, json=payload, verify=False, timeout=30)

            if resp.status_code not in (200, 201):
                errors[pg_id] = f"{resp.status_code} - {(resp.text or '')[:500]}"
                continue

            results[pg_id] = resp.json() if resp.content else {'status': 'stop requested'}

        if errors:
            msg = f"Failed to stop one or more NiFi process groups: {json.dumps(errors)}"
            if best_effort:
                logger.error(msg)
                return {'stopped': list(results.keys()), 'errors': errors}
            raise Exception(msg)

        logger.warning('[NiFi][STOP] ✓ Stopped %d process groups successfully', len(results))
        return results

    except Exception as e:
        if best_effort:
            logger.exception('[NiFi][STOP] Stop failed (swallowed): %s', str(e))
            return {'stopped': [], 'errors': {'_fatal': str(e)}}
        raise


def stop_nifi_processors_task_wrapper():
    """Wrapper para task explícita de stop."""
    return stop_nifi_processors(best_effort=False, source='explicit_stop_task')


# ------------------------
# Callbacks que disparan el DAG de parada
# ------------------------

def trigger_stop_dag_callback(context):
    """Dispara el DAG de parada de emergencia."""
    from airflow.api.common.trigger_dag import trigger_dag
    
    logger.warning("[DAG][callback] Triggering STOP_NIFI_EMERGENCY as safety net...")
    try:
        trigger_dag(
            dag_id='STOP_NIFI_EMERGENCY',
            run_id=f"auto_triggered_by_{context['dag_run'].run_id}",
            conf={
                'triggered_by': 'DAG_TSUPREME_001_TPIAGENT_UPLOADS',
                'source': 'dag_callback',
                'reason': 'safety_net'
            },
            execution_date=None,
            replace_microseconds=False,
        )
        logger.warning("[DAG][callback] ✓ Successfully triggered STOP_NIFI_EMERGENCY")
    except Exception as e:
        logger.exception("[DAG][callback] Failed to trigger STOP_NIFI_EMERGENCY: %s", e)


# ------------------------
# Sensor con on_kill
# ------------------------

class StopNiFiOnKillTimeDeltaSensor(TimeDeltaSensor):
    """TimeDeltaSensor que para NiFi cuando es cancelado."""

    def on_kill(self) -> None:
        try:
            logger.warning('[NiFi][on_kill] Task %s KILLED. Stopping NiFi...', self.task_id)
            stop_nifi_processors(best_effort=True, source=f'sensor_on_kill:{self.task_id}')
        except Exception:
            logger.exception('[NiFi][on_kill] Stop failed')


# ------------------------
# DAG definition
# ------------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'DAG_TSUPREME_001_TPIAGENT_UPLOADS',
    default_args=default_args,
    description='DAG para NiFi que dispara DAG de parada automáticamente',
    schedule=None,
    catchup=False,
    tags=['nifi-integration'],
    # Callbacks que disparan el DAG de parada
    on_success_callback=trigger_stop_dag_callback,
    on_failure_callback=trigger_stop_dag_callback,
)


# ------------------------
# Tasks
# ------------------------

check_nifi = PythonOperator(
    task_id='check_nifi_availability',
    python_callable=check_nifi_availability,
    dag=dag
)

start_nifi = PythonOperator(
    task_id='start_nifi_pipeline',
    python_callable=start_nifi_processors,
    dag=dag
)

monitor_nifi = PythonOperator(
    task_id='monitor_nifi_pipeline',
    python_callable=monitor_nifi_pipeline,
    dag=dag
)

# Task explícita de stop (redundancia)
stop_nifi_pipeline = PythonOperator(
    task_id='stop_nifi_pipeline',
    python_callable=stop_nifi_processors_task_wrapper,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Trigger del DAG de emergencia (SAFETY NET - siempre se ejecuta)
trigger_emergency_stop = TriggerDagRunOperator(
    task_id='trigger_emergency_stop_dag',
    trigger_dag_id='STOP_NIFI_EMERGENCY',
    trigger_rule=TriggerRule.ALL_DONE,  # Siempre se ejecuta
    wait_for_completion=True,  # Espera a que termine
    poke_interval=10,
    conf={
        'triggered_by': 'DAG_TSUPREME_001_TPIAGENT_UPLOADS',
        'source': 'trigger_task',
        'reason': 'automatic_safety_net'
    },
    dag=dag,
)

if STOP_AFTER_MINUTES == -1:
    # Modo infinito: solo parada manual
    wait_forever = StopNiFiOnKillTimeDeltaSensor(
        task_id='wait_forever',
        delta=timedelta(days=365),
        mode='reschedule',
        dag=dag,
    )

    # Flujo: check -> start -> monitor -> wait_forever -> stop -> trigger_emergency
    check_nifi >> start_nifi >> monitor_nifi >> wait_forever >> stop_nifi_pipeline >> trigger_emergency_stop

else:
    # Modo con timeout
    wait_before_stop = StopNiFiOnKillTimeDeltaSensor(
        task_id='wait_before_stop',
        delta=timedelta(minutes=int(STOP_AFTER_MINUTES)),
        mode='reschedule',
        dag=dag,
    )

    # Flujo: check -> start -> monitor -> wait -> stop -> trigger_emergency
    check_nifi >> start_nifi >> monitor_nifi >> wait_before_stop >> stop_nifi_pipeline >> trigger_emergency_stop

# Exponer dag
globals()['dag'] = dag
