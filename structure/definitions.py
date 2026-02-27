"""
definitions.py — Punto de entrada de Dagster.

Registra todos los assets, jobs y schedules del proyecto.
Para lanzar: dagster dev -f definitions.py
"""
from dagster import (
    AssetKey,
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)

# Importar cada asset explícitamente para que Dagster los reconozca sin ambigüedad
from pr2_assets.git_ops import clone_repository, configure_git, github_token, pull_repository
from pr2_assets.renta import guardar_renta_limpia, ingestar_renta, limpiar_renta
from pr2_assets.codislas import (
    guardar_codislas_limpia,
    guardar_renta_integrada,
    ingestar_codislas,
    integrar_renta_codislas,
    limpiar_codislas,
)
from pr2_assets.nivelestudios import (
    commit_ejercicio3,
    enriquecer_nivelestudios,
    generar_graficos_ejercicio3,
    guardar_nivelestudios_limpio,
    ingestar_nivelestudios,
    limpiar_nivelestudios,
)

# Lista explícita — Dagster ve exactamente qué assets existen
all_assets = [
    # git
    github_token,
    clone_repository,
    configure_git,
    pull_repository,
    # renta
    ingestar_renta,
    limpiar_renta,
    guardar_renta_limpia,
    # codislas
    ingestar_codislas,
    limpiar_codislas,
    guardar_codislas_limpia,
    integrar_renta_codislas,
    guardar_renta_integrada,
    # nivelestudios
    ingestar_nivelestudios,
    limpiar_nivelestudios,
    enriquecer_nivelestudios,
    guardar_nivelestudios_limpio,
    generar_graficos_ejercicio3,
    commit_ejercicio3,
]


def _sel(*asset_names: str) -> AssetSelection:
    """Helper: construye un AssetSelection a partir de nombres de asset."""
    return AssetSelection.keys(*[AssetKey(name) for name in asset_names])


# ── Jobs ───────────────────────────────────────────────────────────────────────

# Job completo: desde la inicialización del repo hasta el commit final
job_completo = define_asset_job(
    name="pipeline_completo",
    description="Ejecuta todo el pipeline: git setup → ingestión → limpieza → gráficos → commit.",
    selection=AssetSelection.all(),
)

# Job de gráficos + commit: todo lo necesario para producir y subir los gráficos
job_graficos_y_commit = define_asset_job(
    name="graficos_y_commit",
    description="Regenera los gráficos del ejercicio 3 y hace commit/push.",
    selection=AssetSelection.keys(AssetKey("commit_ejercicio3")).upstream(),
)

# Job de solo limpieza: ingesta + limpieza, sin gráficos ni commit
job_limpieza = define_asset_job(
    name="solo_limpieza",
    description="Ingesta y limpia todos los datasets, sin generar gráficos.",
    selection=AssetSelection.keys(AssetKey("guardar_nivelestudios_limpio")).upstream(),
)


# ── Schedules (opcionales) ─────────────────────────────────────────────────────

# Ejecutar el pipeline completo cada día a las 06:00 (comentar si no se necesita)
# schedule_diario = ScheduleDefinition(
#     job=job_completo,
#     cron_schedule="0 6 * * *",
#     name="schedule_pipeline_diario",
# )


# ── Definitions ────────────────────────────────────────────────────────────────
defs = Definitions(
    assets=all_assets,
    jobs=[
        job_completo,
        job_graficos_y_commit,
        job_limpieza,
    ],
    # schedules=[schedule_diario],
)
