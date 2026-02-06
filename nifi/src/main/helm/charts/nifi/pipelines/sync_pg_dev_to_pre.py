#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Sincroniza PG_*.json de DEV a PRE cambiando SOLO parameterContexts.

- Encuentra PG_*.json en:  <root>/*/dev/PG_*.json
- Copia a:               <root>/*/pre/ (crea carpeta)
- Renombra:    *_DEV.json -> *_PRE.json
- Modifica únicamente (dentro del JSON):
    - root.parameterContexts: renombra claves y campo name: *_DEV -> *_PRE
    - root.parameterContexts[*].parameters[].value: sustituye por valores PRE de catálogo

Catálogo PRE:
- Se construye leyendo LOS `parameterContexts` de los ficheros ya existentes en PRE:
    <root>/*/pre/PG_*.json

Reglas:
- Si un parámetro no existe en catálogo PRE -> se conserva el valor DEV.
- Si el parámetro no tiene 'value' (p.ej. sensibles) -> no se añade/modifica.
- No se toca nada fuera de 'parameterContexts'.

Nota sobre formato:
- Se reserializa el JSON completo (no se pretende preservar exactamente espacios/orden de claves).
  La estructura (campos y arrays) se mantiene, y solo cambia el contenido de parameterContexts.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple


ROOT_DEFAULT = Path(__file__).resolve().parent


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def iter_pg_pre_files(root: Path) -> Iterable[Path]:
    # root/*/pre/PG_*.json
    for pre_dir in root.glob("*/pre"):
        if not pre_dir.is_dir():
            continue
        for pg in pre_dir.glob("PG_*.json"):
            if pg.is_file():
                yield pg


def build_pre_catalog(root: Path) -> Dict[str, Dict[str, Any]]:
    """Devuelve catálogo: contextName -> paramName -> valor PRE existente.

    IMPORTANTE:
    - El catálogo se construye leyendo TODOS los `PG_*.json` que ya existan en PRE.
    - A diferencia de una aproximación "solo si hay value", aquí indexamos parámetros aunque
      el campo `value` no exista (guardando un sentinel de "no-value").

    Esto permite 2 comportamientos clave:
    - Si PRE YA tiene un `value` para un parámetro sensible (p.ej. mongo.pwd), lo preservamos
      al regenerar el PRE desde DEV.
    - Si PRE NO tiene `value`, no inventamos ninguno.
    """

    catalog: Dict[str, Dict[str, Any]] = {}

    pre_files = list(iter_pg_pre_files(root))
    if not pre_files:
        raise FileNotFoundError(
            "No se han encontrado ficheros de catálogo PRE (PG_*.json) en */pre/. "
            "Ejecuta el script en un repo donde ya exista al menos un PRE o pasa --root correctamente."
        )

    NO_VALUE = object()

    for p in pre_files:
        data = _load_json(p)
        if not isinstance(data, dict):
            continue

        pcs = data.get("parameterContexts") or {}
        if not isinstance(pcs, dict):
            continue

        for ctx_key, ctx in pcs.items():
            if not isinstance(ctx, dict):
                continue
            ctx_name = str(ctx.get("name") or ctx_key)

            params = ctx.get("parameters") or []
            if not isinstance(params, list):
                continue

            ctx_map = catalog.setdefault(ctx_name, {})
            for param in params:
                if not isinstance(param, dict):
                    continue
                pname = param.get("name")
                if not pname:
                    continue
                pname = str(pname)

                # Guardamos explícitamente si PRE traía value o no.
                if "value" in param:
                    ctx_map[pname] = (True, param.get("value"))
                else:
                    # Solo registramos (False, NO_VALUE) si aún no existe una entrada con value.
                    # Así, si un PRE tiene value en otro fichero, prevalece.
                    if pname not in ctx_map:
                        ctx_map[pname] = (False, NO_VALUE)

    # Convertimos NO_VALUE a None solo para no filtrar el object() hacia fuera
    for ctx_name, params in list(catalog.items()):
        for pname, (has_value, val) in list(params.items()):
            if not has_value:
                params[pname] = (False, None)

    return catalog


def rename_dev_to_pre(name: str) -> str:
    return name.replace("_DEV", "_PRE")


def update_parameter_contexts_only(doc: Dict[str, Any], pre_catalog: Dict[str, Dict[str, Any]]) -> Tuple[int, int, int]:
    """Modifica doc in-place aplicando nombres PRE y preservando valores PRE existentes.

    Reglas de merge (por parámetro):
    - Si en PRE existe `value` -> se fuerza ese `value` en el doc resultante.
      - Incluso si en DEV no venía `value` (caso típico de sensibles).
    - Si en PRE NO existe `value` -> no se añade `value` si el parámetro no lo traía.
      Si el parámetro lo traía (no-sensible), se conserva el de DEV.

    Devuelve (contexts_renamed, params_updated, params_missing_in_catalog).
    """
    pcs = doc.get("parameterContexts")
    if not isinstance(pcs, dict):
        return (0, 0, 0)

    new_pcs: Dict[str, Any] = {}
    contexts_renamed = 0
    params_updated = 0
    params_missing = 0

    for ctx_key, ctx in pcs.items():
        if not isinstance(ctx, dict):
            new_pcs[ctx_key] = ctx
            continue

        old_key = str(ctx_key)
        new_key = rename_dev_to_pre(old_key) if "_DEV" in old_key else old_key

        new_ctx = dict(ctx)

        old_name = new_ctx.get("name")
        if isinstance(old_name, str) and "_DEV" in old_name:
            new_ctx["name"] = rename_dev_to_pre(old_name)

        if new_key != old_key:
            contexts_renamed += 1

        ctx_name_for_lookup = new_ctx.get("name") if isinstance(new_ctx.get("name"), str) else new_key
        ctx_name_for_lookup = str(ctx_name_for_lookup)

        params = new_ctx.get("parameters")
        if isinstance(params, list):
            cat_for_ctx = pre_catalog.get(ctx_name_for_lookup, {})
            for param in params:
                if not isinstance(param, dict):
                    continue
                pname = param.get("name")
                if not pname:
                    continue
                pname = str(pname)

                if pname in cat_for_ctx:
                    has_val, pre_val = cat_for_ctx[pname]
                    if has_val:
                        # Preservar PRE: forzamos value aunque en DEV no existiese.
                        if ("value" not in param) or (param.get("value") != pre_val):
                            param["value"] = pre_val
                            params_updated += 1
                    else:
                        # PRE no tiene value: no añadimos value; si viene, dejamos DEV.
                        pass
                else:
                    params_missing += 1

        new_pcs[new_key] = new_ctx

    doc["parameterContexts"] = new_pcs
    return (contexts_renamed, params_updated, params_missing)


def iter_pg_dev_files(root: Path) -> Iterable[Path]:
    # root/*/dev/PG_*.json
    for dev_dir in root.glob("*/dev"):
        if not dev_dir.is_dir():
            continue
        for pg in dev_dir.glob("PG_*.json"):
            if pg.is_file():
                yield pg


def dev_to_pre_dest(dev_file: Path) -> Path:
    # .../<client>/dev/PG_XXX_DEV.json -> .../<client>/pre/PG_XXX_PRE.json
    client_dir = dev_file.parent.parent
    pre_dir = client_dir / "pre"
    dest_name = dev_file.name.replace("_DEV.json", "_PRE.json")
    return pre_dir / dest_name


def process_one_file(src: Path, dest: Path, pre_catalog: Dict[str, Dict[str, Any]], overwrite: bool, dry_run: bool) -> Tuple[bool, int, int, int]:
    """Genera PRE completo desde DEV y luego preserva valores PRE en parameterContexts."""
    if dest.exists() and not overwrite:
        return (False, 0, 0, 0)

    # 1) Base: copiar DEV entero (no tocamos estructura fuera de parameterContexts).
    data = _load_json(src)
    if not isinstance(data, dict):
        raise ValueError(f"El JSON raíz no es objeto en {src}")

    # 2) Ajustar solo parameterContexts (nombres DEV->PRE y valores preservados de PRE).
    ctx_renamed, params_updated, params_missing = update_parameter_contexts_only(data, pre_catalog)

    if dry_run:
        return (True, ctx_renamed, params_updated, params_missing)

    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")

    return (True, ctx_renamed, params_updated, params_missing)


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Copia PG_*.json DEV->PRE cambiando solo parameterContexts (nombres y valores).")
    parser.add_argument("--root", type=Path, default=ROOT_DEFAULT, help="Directorio pipelines (por defecto: donde está el script)")
    parser.add_argument("--dry-run", action="store_true", help="No escribe ficheros, solo informa")
    parser.add_argument("--overwrite", action="store_true", help="Sobrescribe destinos si existen")
    parser.add_argument("--verbose", action="store_true", help="Log detallado")

    args = parser.parse_args(list(argv) if argv is not None else None)

    root: Path = args.root
    pre_catalog = build_pre_catalog(root)

    total = 0
    copied = 0
    sum_ctx = 0
    sum_updated = 0
    sum_missing = 0

    for src in iter_pg_dev_files(root):
        total += 1
        dest = dev_to_pre_dest(src)
        would_write, ctx_renamed, params_updated, params_missing = process_one_file(
            src=src,
            dest=dest,
            pre_catalog=pre_catalog,
            overwrite=args.overwrite,
            dry_run=args.dry_run,
        )

        if would_write:
            copied += 1
            sum_ctx += ctx_renamed
            sum_updated += params_updated
            sum_missing += params_missing

        if args.verbose:
            status = "SKIP" if (dest.exists() and not args.overwrite) else ("DRY" if args.dry_run else "WRITE")
            print(f"[{status}] {src} -> {dest} | ctx_renamed={ctx_renamed} params_updated={params_updated} params_missing={params_missing}")

    print(
        f"Procesados PG DEV: {total}. Generados: {copied}. "
        f"Contexts renombrados: {sum_ctx}. Params actualizados: {sum_updated}. Params sin catálogo (se deja DEV): {sum_missing}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
