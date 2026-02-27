from dagster import (
    AssetKey, AssetSelection, Definitions, define_asset_job,
    load_assets_from_modules, load_asset_checks_from_modules,
)
from pr2_assets import git_ops, renta, codislas, nivelestudios, checks

all_assets = load_assets_from_modules([git_ops, renta, codislas, nivelestudios])
all_checks = load_asset_checks_from_modules([checks])

job_completo = define_asset_job(
    name="pipeline_completo",
    selection=AssetSelection.all(),
)
job_graficos_y_commit = define_asset_job(
    name="graficos_y_commit",
    selection=AssetSelection.keys(AssetKey("commit_ejercicio3")).upstream(),
)
job_limpieza = define_asset_job(
    name="solo_limpieza",
    selection=AssetSelection.keys(AssetKey("guardar_nivelestudios_limpio")).upstream(),
)

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    jobs=[job_completo, job_graficos_y_commit, job_limpieza],
)