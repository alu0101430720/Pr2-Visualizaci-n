"""
definitions.py — Punto de entrada de Dagster.
Para lanzar: dagster dev -f definitions.py
"""
from dagster import AssetKey, AssetSelection, Definitions, define_asset_job

from pr2_assets.git_ops import clone_repository, configure_git, pull_repository
from pr2_assets.renta import guardar_renta_limpia, ingestar_renta, limpiar_renta
from pr2_assets.codislas import (
    guardar_codislas_limpia, guardar_renta_integrada,
    ingestar_codislas, integrar_renta_codislas, limpiar_codislas,
)
from pr2_assets.nivelestudios import (
    commit_ejercicio3, enriquecer_nivelestudios, generar_graficos_ejercicio3,
    guardar_nivelestudios_limpio, ingestar_nivelestudios, limpiar_nivelestudios,
)
from pr2_assets.checks import (
    # ── Carga (Raw) ──────────────────────────────────────────────────────────
    check_estandarizacion_territorio_renta,
    check_nulos_criticos_renta,
    check_estandarizacion_isla_codislas,
    check_nulos_criticos_codislas,
    check_nulos_criticos_nivelestudios,
    # ── Transformación (Curated) ─────────────────────────────────────────────
    check_cardinalidad_fuente_renta,
    check_continuidad_serie_temporal_renta,
    check_escala_y_renta,
    check_label_text_territorio,
    check_cardinalidad_islas,
    check_integridad_join_renta_codislas,
    check_continuidad_serie_temporal_nivelestudios,
    check_cardinalidad_nivel_estudios,
    check_dominance_otros_nivelestudios,
    check_label_text_municipio,
    # ── Visualización (Asset) — validan el gráfico definido en config.Dashboard
    check_eje_cero_renta,
    check_datos_dashboard_no_vacios,
    check_escala_y_dashboard,
    check_cardinalidad_dashboard,
    check_orden_magnitud_dashboard,
    check_graficos_generados,
    check_consistencia_color_paleta,
)

all_assets = [
    clone_repository, configure_git, pull_repository,
    ingestar_renta, limpiar_renta, guardar_renta_limpia,
    ingestar_codislas, limpiar_codislas, guardar_codislas_limpia,
    integrar_renta_codislas, guardar_renta_integrada,
    ingestar_nivelestudios, limpiar_nivelestudios, enriquecer_nivelestudios,
    guardar_nivelestudios_limpio, generar_graficos_ejercicio3, commit_ejercicio3,
]

all_checks = [
    # Carga
    check_estandarizacion_territorio_renta,
    check_nulos_criticos_renta,
    check_estandarizacion_isla_codislas,
    check_nulos_criticos_codislas,
    check_nulos_criticos_nivelestudios,
    # Transformación
    check_cardinalidad_fuente_renta,
    check_continuidad_serie_temporal_renta,
    check_escala_y_renta,
    check_label_text_territorio,
    check_cardinalidad_islas,
    check_integridad_join_renta_codislas,
    check_continuidad_serie_temporal_nivelestudios,
    check_cardinalidad_nivel_estudios,
    check_dominance_otros_nivelestudios,
    check_label_text_municipio,
    # Visualización
    check_eje_cero_renta,
    check_datos_dashboard_no_vacios,
    check_escala_y_dashboard,
    check_cardinalidad_dashboard,
    check_orden_magnitud_dashboard,
    check_graficos_generados,
    check_consistencia_color_paleta,
]

# ── Jobs ───────────────────────────────────────────────────────────────────────
job_completo = define_asset_job(
    name="pipeline_completo",
    selection=AssetSelection.all(),
    description="git setup → ingestión → limpieza → gráficos → commit.",
)
job_graficos_y_commit = define_asset_job(
    name="graficos_y_commit",
    selection=AssetSelection.keys(AssetKey("commit_ejercicio3")).upstream(),
    description="Regenera gráficos y hace commit/push.",
)
job_limpieza = define_asset_job(
    name="solo_limpieza",
    selection=AssetSelection.keys(AssetKey("guardar_nivelestudios_limpio")).upstream(),
    description="Ingesta y limpia datasets, sin gráficos.",
)

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    jobs=[job_completo, job_graficos_y_commit, job_limpieza],
)
