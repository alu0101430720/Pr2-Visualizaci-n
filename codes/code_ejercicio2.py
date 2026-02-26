
import os
import subprocess
import pandas as pd
import warnings
from plotnine import (
    ggplot, aes, geom_line, geom_point, scale_x_continuous,
    scale_color_brewer, labs, theme_minimal, theme, expand_limits,
    facet_wrap, element_text, scale_color_manual
)
from dagster import asset, OpExecutionContext


# ── Funciones auxiliares (sin modificar) ──────────────────────────────────────

def limpiar_formato_coma(texto):
    if pd.isna(texto):
        return ""
    texto = str(texto)
    if ',' in texto:
        partes = texto.split(',')
        nombre = partes[0].strip()
        articulo = partes[1].strip()
        return articulo + " " + nombre
    return texto.strip()


def procesar_nombres_codislas(df_codislas):
    df = df_codislas.copy()
    df['ISLA_clean'] = df['ISLA'].apply(limpiar_formato_coma)
    df['ISLA_clean'] = df['ISLA_clean'].str.title()
    df['NOMBRE_clean'] = df['NOMBRE'].apply(limpiar_formato_coma)
    df['NOMBRE_clean'] = df['NOMBRE_clean'].str.title()
    return df


def integrar_df(df_codislas_clean, df_renta_clean):
    df_integrado = pd.merge(
        df_renta_clean,
        df_codislas_clean[['Territorio', 'ISLA_clean']],
        on='Territorio',
        how='left'
    )
    return df_integrado


def graficar_renta_territorial(df, territorio, fuentes_codigo=None,
                               comparar_subterritorios=False,
                               mostrar_leyenda=True,
                               eje_y_cero=True,
                               desglosar_municipios=True):
    ISLAS_LAS_PALMAS = ['Gran Canaria', 'Lanzarote', 'Fuerteventura']
    ISLAS_SCT = ['Tenerife', 'La Gomera', 'La Palma', 'El Hierro']
    TODAS_ISLAS = ISLAS_LAS_PALMAS + ISLAS_SCT

    es_region = territorio == 'Canarias'
    es_provincia_lp = territorio == 'Las Palmas'
    es_provincia_sct = territorio == 'Santa Cruz de Tenerife'

    datos = df.copy()
    nivel_detalle = "Municipios"

    if es_region:
        datos = datos[datos['Territorio'].isin(TODAS_ISLAS)]
        nivel_detalle = "Islas de Canarias"
    elif es_provincia_lp:
        datos = datos[datos['Territorio'].isin(ISLAS_LAS_PALMAS)]
        nivel_detalle = "Islas de Las Palmas"
    elif es_provincia_sct:
        datos = datos[datos['Territorio'].isin(ISLAS_SCT)]
        nivel_detalle = "Islas de S.C. de Tenerife"
    else:
        if desglosar_municipios:
            datos = datos[datos['ISLA_clean'] == territorio]
            datos = datos[datos['Territorio'] != territorio]
            nivel_detalle = "Municipios de " + territorio
        else:
            datos = datos[datos['Territorio'] == territorio]
            nivel_detalle = "Isla de " + territorio
            comparar_subterritorios = False

    if fuentes_codigo:
        codigos = fuentes_codigo if isinstance(fuentes_codigo, list) else [fuentes_codigo]
        datos = datos[datos['Fuente_Renta_Code'].isin(codigos)]

    if datos.empty:
        print("Aviso: No hay datos para " + territorio + " con la configuración actual.")
        return None

    num_entidades = datos['Territorio'].nunique()
    if comparar_subterritorios and num_entidades > 10:
        warnings.warn("Estás comparando " + str(num_entidades) + " entidades. Podría ser ilegible.")

    if comparar_subterritorios:
        aes_config = aes(x='Año', y='Porcentaje', color='Territorio', group='Territorio')
        titulo = "Distribución de la renta bruta media por persona según fuente de ingresos (por año): " + nivel_detalle

        capa_extra = theme()
        etiqueta_color = "Territorio"
        tamanio_linea = 1.2
    else:
        aes_config = aes(x='Año', y='Porcentaje', color='Fuente_Renta', group='Fuente_Renta')
        titulo = "Evolución Renta: " + territorio if not desglosar_municipios else "Detalle: " + nivel_detalle
        etiqueta_color = "Fuente de Renta"
        tamanio_linea = 1.5
        if not desglosar_municipios and not es_region and not es_provincia_lp and not es_provincia_sct:
            capa_extra = theme()
        else:
            capa_extra = facet_wrap('~Territorio', scales='free_y')

    grafico = (
        ggplot(datos, aes_config)
        + geom_line(size=tamanio_linea)
        + geom_point(size=3 if not comparar_subterritorios else 1.5, fill="white", stroke=1)
        + scale_x_continuous(breaks=range(2015, 2024))
        + capa_extra
        + labs(title=titulo, x="Año", y=f"% ({fuentes_codigo})", color=etiqueta_color)
        + theme_minimal()
        + theme(
            figure_size=(12, 8),
            legend_position='bottom' if mostrar_leyenda else 'none',
            axis_text_x=element_text(rotation=45)
        )
    )

    if not comparar_subterritorios:
        grafico += scale_color_brewer(type='qual', palette='Set1')

    if eje_y_cero:
        grafico += expand_limits(y=0)

    return grafico

class Fuentes:
    """Catálogo de códigos de fuentes de renta para evitar errores de escritura."""
    SALARIOS           = 'SUELDOS_SALARIOS'
    PENSIONES          = 'PENSIONES'
    DESEMPLEO          = 'PRESTACIONES_DESEMPLEO'
    OTROS_INGRESOS     = 'OTROS_INGRESOS'
    OTRAS_PRESTACIONES = 'OTRAS_PRESTACIONES'
    TODOS = [SALARIOS, PENSIONES, DESEMPLEO, OTROS_INGRESOS, OTRAS_PRESTACIONES]


# ── Assets Dagster ─────────────────────────────────────────────────────────────

@asset(deps=["pull_repository"])
def ingestar_codislas(context: OpExecutionContext, pull_repository: str) -> pd.DataFrame:
    """Lee el CSV de códigos de islas."""
    #ruta = "Pr2-Visualizacion/datasets-check/codislas-checks.csv"
    ruta = "Pr2-Visualizacion/codislas.csv"
    df = pd.read_csv(ruta, encoding='latin-1', sep=';')
    context.log.info("codislas.csv cargado: " + str(df.shape[0]) + " filas.")
    return df


@asset(deps=["ingestar_codislas"])
def limpiar_codislas(context: OpExecutionContext, ingestar_codislas: pd.DataFrame) -> pd.DataFrame:
    """Procesa y limpia el DataFrame de códigos de islas."""
    df = procesar_nombres_codislas(ingestar_codislas)
    df = df.rename(columns={"NOMBRE_clean": "Territorio"})
    context.log.info("codislas limpio: " + str(df.shape[0]) + " filas.")
    return df


@asset(deps=["limpiar_codislas", "limpiar_renta"])
def construir_ejercicio2(
    context: OpExecutionContext,
    limpiar_codislas: pd.DataFrame,
    limpiar_renta: pd.DataFrame,
) -> pd.DataFrame:
    """Integra el DataFrame de renta con el de códigos de islas."""
    df = integrar_df(limpiar_codislas, limpiar_renta)
    context.log.info("df_ejercicio2 construido: " + str(df.shape[0]) + " filas.")
    return df
