
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
    #ruta = "Pr2-Visualizacion/datasets-check/distribucion-renta-canarias-checks.csv"
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
