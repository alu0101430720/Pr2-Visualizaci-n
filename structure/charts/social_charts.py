"""
charts/social_charts.py — Funciones de visualización social (educación, sexo, nacionalidad).
"""
import pandas as pd
from plotnine import (
    ggplot, aes,
    geom_area, geom_tile, geom_text,
    facet_wrap,
    scale_x_continuous, scale_fill_brewer, scale_fill_cmap,
    labs, theme_minimal, theme, element_blank,
)

from config import ISLAS_LAS_PALMAS, ISLAS_SCT, TODAS_ISLAS, MAPA_EDUCACION

# Dimensiones soportadas
DIMENSIONES_VALIDAS = {"Estudios", "Sexo", "Nacionalidad"}


def _filtrar_territorio(datos: pd.DataFrame, territorio: str, desglosar_municipios: bool) -> tuple[pd.DataFrame, str]:
    """Filtra y asigna la columna 'Territorio' según la entidad pedida."""
    col_isla = "ISLA_clean"
    col_muni = "Municipio_clean"

    if territorio == "Canarias":
        datos["Territorio"] = datos[col_isla] if desglosar_municipios else "Canarias"
    elif territorio == "Las Palmas":
        datos = datos[datos[col_isla].isin(ISLAS_LAS_PALMAS)].copy()
        datos["Territorio"] = datos[col_isla] if desglosar_municipios else "Las Palmas"
    elif territorio == "Santa Cruz de Tenerife":
        datos = datos[datos[col_isla].isin(ISLAS_SCT)].copy()
        datos["Territorio"] = datos[col_isla] if desglosar_municipios else "S.C. Tenerife"
    elif territorio in TODAS_ISLAS:
        datos = datos[datos[col_isla] == territorio].copy()
        datos["Territorio"] = datos[col_muni] if desglosar_municipios else territorio
    else:
        datos = datos[datos[col_muni] == territorio].copy()
        datos["Territorio"] = territorio

    return datos


def _agregar_dimension(datos: pd.DataFrame, dimension: str) -> pd.DataFrame:
    """Agrega por (Periodo, Territorio, Categoria) según la dimensión elegida."""
    if dimension == "Estudios":
        temp = datos[
            (datos["Sexo"] == "Total") &
            (datos["Nivel de estudios en curso"] != "Total")
        ].copy()
        temp["Categoria"] = temp["Nivel de estudios en curso"].map(MAPA_EDUCACION)
        df_plot = temp.groupby(["Periodo", "Territorio", "Categoria"])["Total"].sum().reset_index()
        df_plot["Categoria"] = pd.Categorical(
            df_plot["Categoria"],
            categories=["Sin Estudios/Otros", "Básicos", "Medios", "Superiores"],
            ordered=True,
        )

    elif dimension == "Sexo":
        temp = datos[
            (datos["Nivel de estudios en curso"] == "Total") &
            (datos["Sexo"] != "Total")
        ].copy()
        df_plot = temp.groupby(["Periodo", "Territorio", "Sexo"])["Total"].sum().reset_index()
        df_plot.rename(columns={"Sexo": "Categoria"}, inplace=True)

    elif dimension == "Nacionalidad":
        temp = datos[
            (datos["Nivel de estudios en curso"] == "Total") &
            (datos["Sexo"] == "Total") &
            (datos["Nacionalidad"] != "Total")
        ].copy()
        df_plot = temp.groupby(["Periodo", "Territorio", "Nacionalidad"])["Total"].sum().reset_index()
        df_plot.rename(columns={"Nacionalidad": "Categoria"}, inplace=True)

    else:
        raise ValueError(f"Dimensión '{dimension}' no válida. Usa: {DIMENSIONES_VALIDAS}")

    # Calcular porcentaje
    totales = df_plot.groupby(["Periodo", "Territorio"])["Total"].transform("sum")
    df_plot["Porcentaje"] = (df_plot["Total"] / totales) * 100
    df_plot["Etiqueta"] = df_plot["Porcentaje"].round(0).astype(int).astype(str) + "%"
    return df_plot


def graficar_social(
    df_educacion: pd.DataFrame,
    territorio: str,
    dimension_social: str = "Estudios",
    desglosar_municipios: bool = True,
    comparar_subterritorios: bool = True,
):
    """
    Genera gráfico social (heatmap o área apilada) para el territorio dado.

    Args:
        comparar_subterritorios: True → heatmap por territorio.
                                 False → área apilada (facetado si hay varios territorios).
    """
    datos = _filtrar_territorio(df_educacion.copy(), territorio, desglosar_municipios)
    df_plot = _agregar_dimension(datos, dimension_social)

    titulo_base = f"Distribución de nivel de estudios (por {dimension_social}): {territorio}"

    if comparar_subterritorios:
        grafico = (
            ggplot(df_plot, aes(x="Periodo", y="Territorio", fill="Porcentaje"))
            + geom_tile(color="white", size=0.5)
            + geom_text(aes(label="Etiqueta"), size=7, color="white")
            + facet_wrap("~Categoria", nrow=1)
            + scale_fill_cmap(name="magma")
            + scale_x_continuous(breaks=sorted(df_plot["Periodo"].unique()))
            + labs(title=titulo_base, x="Año", y="", fill="%")
            + theme_minimal()
            + theme(figure_size=(14, 6), panel_grid=element_blank())
        )
    else:
        facet_layer = (
            facet_wrap("~Territorio", scales="free_y")
            if df_plot["Territorio"].nunique() > 1
            else theme()
        )
        grafico = (
            ggplot(df_plot, aes(x="Periodo", y="Porcentaje", fill="Categoria"))
            + geom_area(alpha=0.85, color="white")
            + scale_x_continuous(breaks=sorted(df_plot["Periodo"].unique()))
            + facet_layer
            + scale_fill_brewer(type="qual", palette="Set2")
            + labs(title=titulo_base, x="Año", y="%", fill=dimension_social)
            + theme_minimal()
            + theme(figure_size=(12, 5), legend_position="right")
        )
    return grafico
