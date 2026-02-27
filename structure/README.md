# Pr2-Visualizacion · Pipeline Dagster

## Estructura del proyecto

```
pr2_dagster/
│
├── config.py                  # Constantes globales (rutas, rama, usuario git, fuentes…)
│
├── definitions.py             # Punto de entrada Dagster: assets + jobs + schedules
│
├── assets/                    # Un módulo por capa de datos
│   ├── git_ops.py             #   github_token → clone → configure → pull
│   ├── renta.py               #   ingestar → limpiar → guardar
│   ├── codislas.py            #   ingestar → limpiar → integrar con renta → guardar
│   └── nivelestudios.py       #   ingestar → limpiar → enriquecer → gráficos → commit
│
├── charts/                    # Funciones de visualización (sin Dagster, testeables solas)
│   ├── renta_charts.py        #   graficar_renta, graficar_renta_territorial
│   └── social_charts.py       #   graficar_social
│
└── utils/
    └── git.py                 # Helpers: git_add, git_commit, git_push, commit_and_push
```

## DAG de assets

```
github_token ──┬──► clone_repository ──► configure_git ──► pull_repository
               │                                                    │
               │         ┌──────────────────────────────────────────┤
               │         │                                           │
               │    ingestar_renta                        ingestar_codislas
               │         │                                           │
               │    limpiar_renta ◄──────────────────── limpiar_codislas
               │         │       \                           │
               │  guardar_renta   integrar_renta_codislas    │
               │  _limpia          /          \    guardar_codislas_limpia
               │                 /             \
               │   guardar_renta_integrada  ingestar_nivelestudios
               │                                     │
               │                            limpiar_nivelestudios
               │                                     │
               │                         enriquecer_nivelestudios
               │                            /              \
               │               guardar_nivelestudios  generar_graficos
               │                   _limpio              _ejercicio3
               │                        \               /
               └─────────────────────────► commit_ejercicio3
```

## Uso

```bash
# Lanzar la UI de Dagster
dagster dev -f definitions.py

# O ejecutar un job concreto por CLI
dagster job execute -f definitions.py -j pipeline_completo
dagster job execute -f definitions.py -j graficos_y_commit
dagster job execute -f definitions.py -j solo_limpieza
```

## Jobs disponibles

| Job | Descripción |
|-----|-------------|
| `pipeline_completo` | Todo desde cero: git → datos → gráficos → commit |
| `graficos_y_commit` | Solo regenera gráficos y hace push (datos ya procesados) |
| `solo_limpieza` | Ingesta y limpieza de datasets, sin gráficos ni commit |

## Añadir checks (futuro)

Dagster soporta `AssetChecks` de forma nativa. Para añadir validaciones:

```python
# assets/checks.py
from dagster import asset_check, AssetCheckResult

@asset_check(asset="limpiar_renta")
def check_renta_no_nulos(limpiar_renta):
    n_nulos = limpiar_renta["Porcentaje"].isna().sum()
    return AssetCheckResult(passed=n_nulos == 0, metadata={"nulos": n_nulos})
```

Luego registrar en `definitions.py`:
```python
from assets.checks import check_renta_no_nulos
defs = Definitions(assets=..., asset_checks=[check_renta_no_nulos])
```

## Variables de entorno

| Variable | Descripción |
|----------|-------------|
| `GITHUB_TOKEN` | Token de acceso a GitHub (obligatorio) |
