"""
DAG de emergencia para PARAR NiFi manualmente.

USO:
1. Ve a la UI de Airflow
2. Busca el DAG: STOP_NIFI_EMERGENCY
3. Click en "Trigger DAG"
4. Se detendrán todos los Process Groups de NiFi

Este DAG existe como workaround para Airflow 3.0+ donde la parada manual
del DAG principal no siempre detiene las tareas en ejecución.
"""

from datetime import datetime, timedelta
import json
import logging
import urllib3

import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)


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


def emergency_stop_nifi():
    """Detener TODOS los Process Groups de NiFi inmediatamente."""
    logger.warning("=" * 80)
    logger.warning("EMERGENCY STOP - Deteniendo todos los Process Groups de NiFi")
    logger.warning("=" * 80)

    token = get_nifi_token()
    connection = BaseHook.get_connection('nifi_default')
    nifi_base_url = connection.host

    process_group_ids = get_nifi_process_group_ids(token=token, nifi_base_url=nifi_base_url)
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': f'Bearer {token}'}

    stopped = []
    errors = {}

    for pg_id in process_group_ids:
        try:
            url = f'{nifi_base_url}/nifi-api/flow/process-groups/{pg_id}'
            payload = {'id': pg_id, 'state': 'STOPPED'}
            resp = requests.put(url, headers=headers, json=payload, verify=False, timeout=30)

            if resp.status_code in (200, 201):
                stopped.append(pg_id)
                logger.info("✓ Stopped process group: %s", pg_id)
            else:
                errors[pg_id] = f"{resp.status_code} - {resp.text[:500]}"
                logger.error("✗ Failed to stop process group %s: %s", pg_id, errors[pg_id])

        except Exception as e:
            errors[pg_id] = str(e)
            logger.exception("✗ Exception stopping process group %s", pg_id)

    logger.warning("=" * 80)
    logger.warning("EMERGENCY STOP COMPLETED")
    logger.warning("Stopped: %d process groups", len(stopped))
    logger.warning("Errors: %d process groups", len(errors))
    logger.warning("=" * 80)

    if stopped:
        logger.info("Successfully stopped: %s", stopped)
    if errors:
        logger.error("Failed to stop: %s", errors)

    result = {
        'stopped': stopped,
        'errors': errors,
        'total_attempted': len(process_group_ids),
        'success_count': len(stopped),
        'error_count': len(errors),
    }

    if errors:
        raise Exception(f"Failed to stop {len(errors)} process groups. Details: {json.dumps(errors, indent=2)}")

    return result


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'STOP_NIFI_EMERGENCY',
    default_args=default_args,
    description='🚨 DAG de emergencia para DETENER NiFi manualmente',
    schedule=None,
    catchup=False,
    tags=['nifi-integration', 'emergency'],
)

stop_task = PythonOperator(
    task_id='emergency_stop_all_nifi_process_groups',
    python_callable=emergency_stop_nifi,
    dag=dag,
)

globals()['dag'] = dag
