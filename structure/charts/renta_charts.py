"""
charts/renta_charts.py — Funciones de visualización de renta.
"""
import warnings

import pandas as pd
from plotnine import (
    ggplot, aes,
    geom_line, geom_point,
    scale_x_continuous, scale_color_brewer,
    labs, theme_minimal, theme, expand_limits,
    facet_wrap, element_text,
)

from config import ISLAS_LAS_PALMAS, ISLAS_SCT, TODAS_ISLAS


def graficar_renta(
    df: pd.DataFrame,
    territorio: str = "Canarias",
    fuentes_codigo: list[str] | str | None = None,
    mostrar_leyenda: bool = True,
    eje_y_cero: bool = True,
):
    """Gráfico simple de renta para un territorio (sin desglose geográfico)."""
    datos = df[df["Territorio"] == territorio].copy()
    if fuentes_codigo:
        codigos = [fuentes_codigo] if isinstance(fuentes_codigo, str) else fuentes_codigo
        datos = datos[datos["Fuente_Renta_Code"].isin(codigos)]

    grafico = (
        ggplot(datos, aes(x="Año", y="Porcentaje", color="Fuente_Renta", group="Fuente_Renta"))
        + geom_line(size=1.5)
        + geom_point(size=3, fill="white", stroke=1.5)
        + scale_x_continuous(breaks=range(2015, 2024))
        + scale_color_brewer(type="qual", palette="Set1")
        + labs(
            title=f"Distribución de la renta bruta media por persona según fuente de ingresos: {territorio}",
            y="%", x="Año", color="Fuente",
        )
        + theme_minimal()
        + theme(legend_position="bottom" if mostrar_leyenda else "none", figure_size=(10, 6))
    )
    if eje_y_cero:
        grafico += expand_limits(y=0)
    return grafico


def graficar_renta_territorial(
    df: pd.DataFrame,
    territorio: str,
    fuentes_codigo: list[str] | str | None = None,
    comparar_subterritorios: bool = False,
    mostrar_leyenda: bool = True,
    eje_y_cero: bool = True,
    desglosar_municipios: bool = True,
):
    """
    Gráfico de renta con soporte para regiones, provincias e islas.
    Si comparar_subterritorios=True, cada línea es un territorio distinto.
    """
    es_region      = territorio == "Canarias"
    es_prov_lp     = territorio == "Las Palmas"
    es_prov_sct    = territorio == "Santa Cruz de Tenerife"
    es_provincia   = es_prov_lp or es_prov_sct

    datos = df.copy()
    nivel_detalle = "Municipios"

    if es_region:
        datos = datos[datos["Territorio"].isin(TODAS_ISLAS)]
        nivel_detalle = "Islas de Canarias"
    elif es_prov_lp:
        datos = datos[datos["Territorio"].isin(ISLAS_LAS_PALMAS)]
        nivel_detalle = "Islas de Las Palmas"
    elif es_prov_sct:
        datos = datos[datos["Territorio"].isin(ISLAS_SCT)]
        nivel_detalle = "Islas de S.C. de Tenerife"
    else:
        if desglosar_municipios:
            datos = datos[(datos["ISLA_clean"] == territorio) & (datos["Territorio"] != territorio)]
            nivel_detalle = f"Municipios de {territorio}"
        else:
            datos = datos[datos["Territorio"] == territorio]
            nivel_detalle = f"Isla de {territorio}"
            comparar_subterritorios = False

    if fuentes_codigo:
        codigos = [fuentes_codigo] if isinstance(fuentes_codigo, str) else fuentes_codigo
        datos = datos[datos["Fuente_Renta_Code"].isin(codigos)]

    if datos.empty:
        warnings.warn(f"No hay datos para '{territorio}' con la configuración actual.")
        return None

    n_entidades = datos["Territorio"].nunique()
    if comparar_subterritorios and n_entidades > 10:
        warnings.warn(f"Comparando {n_entidades} entidades — el gráfico puede quedar ilegible.")

    if comparar_subterritorios:
        aes_cfg = aes(x="Año", y="Porcentaje", color="Territorio", group="Territorio")
        titulo  = f"Distribución de la renta bruta media por persona ({nivel_detalle})"
        color_layer = theme()     # cada llamada a esta función puede sobreescribir la paleta si lo necesita
        capa_extra  = theme()
        etiqueta_color = "Territorio"
        tam_linea = 1.2
    else:
        aes_cfg = aes(x="Año", y="Porcentaje", color="Fuente_Renta", group="Fuente_Renta")
        titulo  = f"Evolución Renta: {nivel_detalle}" if desglosar_municipios or es_region or es_provincia \
                  else f"Evolución Renta: {territorio}"
        color_layer = scale_color_brewer(type="qual", palette="Set1")
        capa_extra  = facet_wrap("~Territorio", scales="free_y") \
                      if (desglosar_municipios or es_region or es_provincia) else theme()
        etiqueta_color = "Fuente de Renta"
        tam_linea = 1.5

    grafico = (
        ggplot(datos, aes_cfg)
        + geom_line(size=tam_linea)
        + geom_point(size=3 if not comparar_subterritorios else 1.5, fill="white", stroke=1)
        + scale_x_continuous(breaks=range(2015, 2024))
        + color_layer
        + capa_extra
        + labs(title=titulo, x="Año", y=f"% ({fuentes_codigo})", color=etiqueta_color)
        + theme_minimal()
        + theme(
            figure_size=(12, 8),
            legend_position="bottom" if mostrar_leyenda else "none",
            axis_text_x=element_text(rotation=45),
        )
    )
    if eje_y_cero:
        grafico += expand_limits(y=0)
    return grafico
