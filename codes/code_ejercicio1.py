
import os
import subprocess
import pandas as pd
from plotnine import (
    ggplot, aes, geom_line, geom_point, scale_x_continuous,
    scale_color_brewer, labs, theme_minimal, theme, expand_limits
)
from dagster import asset, OpExecutionContext


class Fuentes:
    """Catálogo de códigos de fuentes de renta para evitar errores de escritura."""
    SALARIOS           = 'SUELDOS_SALARIOS'
    PENSIONES          = 'PENSIONES'
    DESEMPLEO          = 'PRESTACIONES_DESEMPLEO'
    OTROS_INGRESOS     = 'OTROS_INGRESOS'
    OTRAS_PRESTACIONES = 'OTRAS_PRESTACIONES'
    TODOS = [SALARIOS, PENSIONES, DESEMPLEO, OTROS_INGRESOS, OTRAS_PRESTACIONES]


def graficar_renta(df, territorio='Canarias', fuentes_codigo=None, mostrar_leyenda=True, eje_y_cero=True):
    datos = df[df['Territorio'] == territorio].copy()
    if fuentes_codigo:
        if isinstance(fuentes_codigo, str):
            fuentes_codigo = [fuentes_codigo]
        datos = datos[datos['Fuente_Renta_Code'].isin(fuentes_codigo)]
    grafico = (
        ggplot(datos, aes(x='Año', y='Porcentaje', color='Fuente_Renta', group='Fuente_Renta'))
        + geom_line(size=1.5)
        + geom_point(size=3, fill="white", stroke=1.5)
        + scale_x_continuous(breaks=range(2015, 2024))
        + scale_color_brewer(type='qual', palette='Set1')
        + labs(title="Distribución de la renta bruta media por persona según fuente de ingresos (por año) en " + territorio, y="%", x="Año", color="Fuente")
        + theme_minimal()
        + theme(legend_position='bottom', figure_size=(10, 6))
    )
    if not mostrar_leyenda:
        grafico += theme(legend_position='none')
    if eje_y_cero:
        grafico += expand_limits(y=0)
    return grafico


@asset(deps=["pull_repository"])
def ingestar_renta(context: OpExecutionContext, pull_repository: str) -> pd.DataFrame:
    """Lee el CSV original de distribución de renta."""
    ruta = "Pr2-Visualizacion/distribucion-renta-canarias.csv"
    df = pd.read_csv(ruta)
    context.log.info("CSV cargado: " + str(df.shape[0]) + " filas, " + str(df.shape[1]) + " columnas.")
    return df


@asset(deps=["ingestar_renta"])
def limpiar_renta(context: OpExecutionContext, ingestar_renta: pd.DataFrame) -> pd.DataFrame:
    """Elimina columnas innecesarias y renombra las restantes."""
    df = ingestar_renta.drop(columns=[
        'ESTADO_OBSERVACION#es',
        'CONFIDENCIALIDAD_OBSERVACION#es',
        'TIME_PERIOD_CODE',
        'TERRITORIO_CODE'
    ])
    df = df.rename(columns={
        'TERRITORIO#es':  'Territorio',
        'TIME_PERIOD#es': 'Año',
        'MEDIDAS#es':     'Fuente_Renta',
        'MEDIDAS_CODE':   'Fuente_Renta_Code',
        'OBS_VALUE':      'Porcentaje',
    })
    df['Territorio'] = df['Territorio'].str.title()
    context.log.info("Limpieza completada.")
    return df


@asset(deps=["limpiar_renta"])
def guardar_dataset_limpio(context: OpExecutionContext, limpiar_renta: pd.DataFrame) -> str:
    """Persiste el DataFrame limpio en datasets-clean/ dentro del repo."""
    directorio = "Pr2-Visualizacion/datasets-clean"
    os.makedirs(directorio, exist_ok=True)
    ruta_csv = directorio + "/distribucion-renta-canarias-clean.csv"
    limpiar_renta.to_csv(ruta_csv, index=False)
    context.log.info("Dataset limpio guardado en: " + ruta_csv)
    return ruta_csv


@asset(deps=["limpiar_renta"])
def generar_grafico_renta(context: OpExecutionContext, limpiar_renta: pd.DataFrame) -> str:
    """Genera y guarda el gráfico de renta para territorio."""
    territorio = 'Canarias'
    directorio = "Pr2-Visualizacion/graficos"
    os.makedirs(directorio, exist_ok=True)

    grafico = graficar_renta(limpiar_renta, territorio=territorio, fuentes_codigo=Fuentes.TODOS)

    nombre_archivo = "renta_" + territorio.lower().replace(' ', '_') + ".png"
    ruta_grafico = directorio + "/" + nombre_archivo
    grafico.save(ruta_grafico, dpi=150)
    context.log.info("Gráfico guardado en: " + ruta_grafico)
    return ruta_grafico


@asset(deps=["guardar_dataset_limpio", "generar_grafico_renta", "github_token"])
def commit_ejercicio1(
    context: OpExecutionContext,
    github_token: str,
    guardar_dataset_limpio: str,
    generar_grafico_renta: str,
) -> None:
    """Hace add, commit y push de los archivos generados en el ejercicio 1."""
    repo_dir = "Pr2-Visualizacion"
    repo_url = "https://" + github_token + "@github.com/alu0101430720/Pr2-Visualizacion.git"

    for archivo in [guardar_dataset_limpio, generar_grafico_renta]:
        # Convertimos la ruta absoluta/relativa en relativa al repo
        ruta_relativa = os.path.relpath(archivo, repo_dir)
        result = subprocess.run(
            ["git", "-C", repo_dir, "add", ruta_relativa],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            msg = "Error en git add (" + ruta_relativa + "): " + result.stderr
            raise RuntimeError(msg)
        context.log.info("git add OK: " + ruta_relativa)

    result = subprocess.run(
        ["git", "-C", repo_dir, "commit", "-m", "ejercicio1: dataset limpio y grafico de renta"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        if "nothing to commit" in result.stdout or "nothing to commit" in result.stderr:
            context.log.info("Nada nuevo que commitear.")
            return
        msg = "Error en git commit: " + result.stderr
        raise RuntimeError(msg)

    context.log.info("Commit realizado: " + result.stdout)

    result = subprocess.run(
        ["git", "-C", repo_dir, "push", repo_url],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        msg = "Error en git push: " + result.stderr
        raise RuntimeError(msg)

    context.log.info("Push completado: " + result.stdout)
