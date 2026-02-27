"""
assets/checks.py — Asset checks del pipeline Pr2-Visualizacion.

Organización por etapa:
  CARGA (Raw)       → checks sobre ingestar_renta, ingestar_codislas, ingestar_nivelestudios
  TRANSFORMACIÓN    → checks sobre limpiar_*, integrar_*, enriquecer_*
  VISUALIZACIÓN     → checks sobre generar_graficos_ejercicio3

Los checks de VISUALIZACIÓN leen los parámetros desde config.Dashboard,
por lo que siempre validan exactamente el subconjunto de datos que se
está graficando en la ejecución actual. Si cambias Dashboard.TERRITORIO,
los checks se adaptan automáticamente sin tocar este fichero.
"""

import hashlib
import os

import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetIn,
    MetadataValue,
    asset_check,
)

from config import (
    Dashboard,
    Fuentes,
    ISLAS_LAS_PALMAS,
    ISLAS_SCT,
    TODAS_ISLAS,
    MAPA_EDUCACION,
    MAX_CATEGORIAS_COLOR,
    MAX_LABEL_LENGTH,
    RATIO_ESCALA_MAX,
    DOMINANCE_OTROS_MAX,
)

# ── Paleta maestra: color asignado a cada fuente de renta ─────────────────────
# Debe coincidir con la paleta Set1 de plotnine (orden de aparición).
PALETA_MAESTRA: dict[str, str] = {
    Fuentes.SALARIOS:           "#E41A1C",
    Fuentes.PENSIONES:          "#377EB8",
    Fuentes.DESEMPLEO:          "#4DAF4A",
    Fuentes.OTROS_INGRESOS:     "#984EA3",
    Fuentes.OTRAS_PRESTACIONES: "#FF7F00",
}
_PALETA_HASH = hashlib.md5(str(sorted(PALETA_MAESTRA.items())).encode()).hexdigest()


# ── Helper: replica el filtrado de graficar_renta_territorial ─────────────────

def _filtrar_datos_dashboard(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica exactamente los mismos filtros que graficar_renta_territorial
    usando los parámetros actuales de config.Dashboard.
    Garantiza que los checks validan el mismo subconjunto que el gráfico.
    """
    territorio       = Dashboard.TERRITORIO
    fuentes_codigo   = Dashboard.FUENTE
    desglosar_munis  = Dashboard.DESGLOSAR_MUNIS

    es_region    = territorio == "Canarias"
    es_prov_lp   = territorio == "Las Palmas"
    es_prov_sct  = territorio == "Santa Cruz de Tenerife"
    es_provincia = es_prov_lp or es_prov_sct

    datos = df.copy()

    if es_region:
        datos = datos[datos["Territorio"].isin(TODAS_ISLAS)]
    elif es_prov_lp:
        datos = datos[datos["Territorio"].isin(ISLAS_LAS_PALMAS)]
    elif es_prov_sct:
        datos = datos[datos["Territorio"].isin(ISLAS_SCT)]
    else:
        if desglosar_munis:
            datos = datos[
                (datos["ISLA_clean"] == territorio) &
                (datos["Territorio"] != territorio)
            ]
        else:
            datos = datos[datos["Territorio"] == territorio]

    if fuentes_codigo:
        codigos = [fuentes_codigo] if isinstance(fuentes_codigo, str) else fuentes_codigo
        datos = datos[datos["Fuente_Renta_Code"].isin(codigos)]

    return datos


def _contexto_dashboard() -> str:
    """Texto descriptivo del dashboard activo, para incluir en metadata."""
    return (
        f"territorio={Dashboard.TERRITORIO}, fuente={Dashboard.FUENTE}, "
        f"dim_social={Dashboard.DIM_SOCIAL}, eje_y_cero={Dashboard.EJE_Y_CERO}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 1 — CARGA (Raw)
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset="ingestar_renta",
    name="check_estandarizacion_territorio_renta",
    description=(
        "Evita duplicados por errores de escritura en TERRITORIO#es "
        "(ej. 'canarias' vs 'Canarias'). "
        "Gestalt — Similitud: una misma categoría pintada con dos colores "
        "rompe la agrupación visual."
    ),
)
def check_estandarizacion_territorio_renta(ingestar_renta: pd.DataFrame) -> AssetCheckResult:
    col = "TERRITORIO#es"
    originales   = ingestar_renta[col].nunique()
    normalizadas = ingestar_renta[col].str.strip().str.title().nunique()
    passed = originales == normalizadas
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "categorias_detectadas": MetadataValue.int(originales),
            "categorias_esperadas":  MetadataValue.int(normalizadas),
            "principio_gestalt": MetadataValue.text(
                "Similitud — Evita que una misma categoría se pinte con dos colores distintos."
            ),
            "mensaje": MetadataValue.text(
                "Si hay nombres inconsistentes, ggplot creará leyendas duplicadas."
            ),
        },
    )


@asset_check(
    asset="ingestar_renta",
    name="check_nulos_criticos_renta",
    description=(
        "Detecta ausencia de datos en OBS_VALUE (porcentaje de renta). "
        "Gestalt — Figura y Fondo: los huecos inesperados rompen "
        "la forma continua de una línea temporal."
    ),
)
def check_nulos_criticos_renta(ingestar_renta: pd.DataFrame) -> AssetCheckResult:
    col    = "OBS_VALUE"
    n_nulos = int(ingestar_renta[col].isna().sum())
    total   = len(ingestar_renta)
    pct     = round(n_nulos / total * 100, 2) if total else 0.0
    return AssetCheckResult(
        passed=n_nulos == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "porcentaje_nulos": MetadataValue.float(pct),
            "filas_afectadas":  MetadataValue.int(n_nulos),
            "principio_gestalt": MetadataValue.text(
                "Figura y Fondo — Los huecos inesperados rompen la forma de la visualización."
            ),
            "mensaje": MetadataValue.text(
                "Un NaN en OBS_VALUE produce un corte en la línea temporal del gráfico."
            ),
        },
    )


@asset_check(
    asset="ingestar_codislas",
    name="check_estandarizacion_isla_codislas",
    description=(
        "Verifica consistencia de mayúsculas en ISLA. "
        "Gestalt — Similitud."
    ),
)
def check_estandarizacion_isla_codislas(ingestar_codislas: pd.DataFrame) -> AssetCheckResult:
    col = "ISLA"
    originales   = ingestar_codislas[col].nunique()
    normalizadas = ingestar_codislas[col].str.strip().str.title().nunique()
    passed = originales == normalizadas
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "categorias_detectadas": MetadataValue.int(originales),
            "categorias_esperadas":  MetadataValue.int(normalizadas),
            "principio_gestalt": MetadataValue.text(
                "Similitud — Evita que una misma isla se pinte con dos colores distintos."
            ),
            "mensaje": MetadataValue.text(
                "Inconsistencias en ISLA rompen el merge con el dataset de renta."
            ),
        },
    )


@asset_check(
    asset="ingestar_codislas",
    name="check_nulos_criticos_codislas",
    description="Detecta nulos en ISLA y NOMBRE. Gestalt — Figura y Fondo.",
)
def check_nulos_criticos_codislas(ingestar_codislas: pd.DataFrame) -> AssetCheckResult:
    nulos = {col: int(ingestar_codislas[col].isna().sum()) for col in ["ISLA", "NOMBRE"]}
    passed = sum(nulos.values()) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "nulos_ISLA":   MetadataValue.int(nulos["ISLA"]),
            "nulos_NOMBRE": MetadataValue.int(nulos["NOMBRE"]),
            "principio_gestalt": MetadataValue.text(
                "Figura y Fondo — Los huecos inesperados rompen la forma de la visualización."
            ),
            "mensaje": MetadataValue.text(
                "Un ISLA nulo impide asignar municipios a su provincia en el gráfico territorial."
            ),
        },
    )


@asset_check(
    asset="ingestar_nivelestudios",
    name="check_nulos_criticos_nivelestudios",
    description="Detecta nulos en Periodo, Sexo, Total. Gestalt — Figura y Fondo.",
)
def check_nulos_criticos_nivelestudios(ingestar_nivelestudios: pd.DataFrame) -> AssetCheckResult:
    cols  = [c for c in ["Periodo", "Sexo", "Total"] if c in ingestar_nivelestudios.columns]
    nulos = {c: int(ingestar_nivelestudios[c].isna().sum()) for c in cols}
    passed = sum(nulos.values()) == 0
    meta  = {f"nulos_{c}": MetadataValue.int(v) for c, v in nulos.items()}
    meta["principio_gestalt"] = MetadataValue.text(
        "Figura y Fondo — Los huecos inesperados rompen la forma de la visualización."
    )
    meta["mensaje"] = MetadataValue.text(
        "Nulos en Total o Periodo producen áreas apiladas incompletas."
    )
    return AssetCheckResult(passed=passed, severity=AssetCheckSeverity.ERROR, metadata=meta)


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 2 — TRANSFORMACIÓN (Curated)
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset="limpiar_renta",
    name="check_cardinalidad_fuente_renta",
    description=(
        "Limita el número de fuentes de renta a MAX_CATEGORIAS_COLOR. "
        "Gestalt — Carga Cognitiva / Similitud."
    ),
)
def check_cardinalidad_fuente_renta(limpiar_renta: pd.DataFrame) -> AssetCheckResult:
    n = int(limpiar_renta["Fuente_Renta_Code"].nunique())
    return AssetCheckResult(
        passed=n <= MAX_CATEGORIAS_COLOR,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "n_categorias":          MetadataValue.int(n),
            "limite_recomendado":    MetadataValue.int(MAX_CATEGORIAS_COLOR),
            "sugerencia_agrupacion": MetadataValue.text(
                "Agrupa fuentes minoritarias en 'Otras prestaciones' si n > 9."
            ),
            "principio_gestalt": MetadataValue.text(
                "Carga Cognitiva / Similitud — Más de 9 colores son imposibles de distinguir."
            ),
            "mensaje": MetadataValue.text(
                "ggplot asignará colores repetidos si hay más categorías que colores en la paleta."
            ),
        },
    )


@asset_check(
    asset="limpiar_renta",
    name="check_continuidad_serie_temporal_renta",
    description=(
        "Verifica que no falten años en la serie temporal. "
        "Gestalt — Continuidad: un año faltante crea una pendiente falsa."
    ),
)
def check_continuidad_serie_temporal_renta(limpiar_renta: pd.DataFrame) -> AssetCheckResult:
    años = sorted(limpiar_renta["Año"].dropna().unique().tolist())
    faltantes = [a for a in range(min(años), max(años) + 1) if a not in años] if años else []
    return AssetCheckResult(
        passed=len(faltantes) == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "fechas_faltantes": MetadataValue.text(str(faltantes) if faltantes else "ninguna"),
            "rango_temporal":   MetadataValue.text(
                f"{min(años)}–{max(años)}" if años else "vacío"
            ),
            "principio_gestalt": MetadataValue.text(
                "Continuidad — Evita que una línea una puntos lejanos creando una pendiente falsa."
            ),
            "mensaje": MetadataValue.text(
                "geom_line interpola entre los puntos disponibles; un año faltante produce "
                "un salto visual engañoso."
            ),
        },
    )


@asset_check(
    asset="limpiar_renta",
    name="check_escala_y_renta",
    description=(
        "Detecta outliers extremos en Porcentaje que comprimirían el eje Y. "
        "Gestalt — Proporcionalidad."
    ),
)
def check_escala_y_renta(limpiar_renta: pd.DataFrame) -> AssetCheckResult:
    valores = limpiar_renta["Porcentaje"].dropna()
    v_min, v_max = float(valores.min()), float(valores.max())
    ratio   = round(v_max / v_min, 2) if v_min > 0 else float("inf")
    outlier = (
        limpiar_renta.loc[limpiar_renta["Porcentaje"] == v_max, "Territorio"].iloc[0]
        if not limpiar_renta.empty else "desconocido"
    )
    return AssetCheckResult(
        passed=ratio <= RATIO_ESCALA_MAX,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "ratio_escala":       MetadataValue.float(ratio),
            "valor_outlier":      MetadataValue.float(v_max),
            "territorio_outlier": MetadataValue.text(str(outlier)),
            "umbral_maximo":      MetadataValue.float(RATIO_ESCALA_MAX),
            "principio_gestalt": MetadataValue.text(
                "Proporcionalidad — Evita que barras pequeñas parezcan invisibles ante una gigante."
            ),
            "mensaje": MetadataValue.text(
                f"Ratio max/min = {ratio}. Si supera {RATIO_ESCALA_MAX}, "
                "considera escala logarítmica."
            ),
        },
    )


@asset_check(
    asset="limpiar_renta",
    name="check_label_text_territorio",
    description=(
        "Detecta etiquetas de Territorio demasiado largas. "
        "Gestalt — Continuidad."
    ),
)
def check_label_text_territorio(limpiar_renta: pd.DataFrame) -> AssetCheckResult:
    longitudes = limpiar_renta["Territorio"].str.len()
    max_len    = int(longitudes.max())
    ejemplo    = limpiar_renta.loc[longitudes.idxmax(), "Territorio"]
    return AssetCheckResult(
        passed=max_len <= MAX_LABEL_LENGTH,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "longest_label": MetadataValue.text(str(ejemplo)),
            "longitud_max":  MetadataValue.int(max_len),
            "limite":        MetadataValue.int(MAX_LABEL_LENGTH),
            "overlap_risk":  MetadataValue.bool(max_len > MAX_LABEL_LENGTH),
            "principio_gestalt": MetadataValue.text(
                "Continuidad — Etiquetas largas se solapan y rompen la legibilidad del eje."
            ),
            "mensaje": MetadataValue.text(
                "Considera abreviar municipios o rotar etiquetas (axis_text_x rotation=45)."
            ),
        },
    )


@asset_check(
    asset="limpiar_codislas",
    name="check_cardinalidad_islas",
    description="Verifica que el número de islas no supere MAX_CATEGORIAS_COLOR. Gestalt — Similitud.",
)
def check_cardinalidad_islas(limpiar_codislas: pd.DataFrame) -> AssetCheckResult:
    n = int(limpiar_codislas["ISLA_clean"].nunique())
    return AssetCheckResult(
        passed=n <= MAX_CATEGORIAS_COLOR,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "n_categorias":       MetadataValue.int(n),
            "limite_recomendado": MetadataValue.int(MAX_CATEGORIAS_COLOR),
            "islas_detectadas":   MetadataValue.text(
                str(sorted(limpiar_codislas["ISLA_clean"].dropna().unique().tolist()))
            ),
            "sugerencia_agrupacion": MetadataValue.text(
                "Agrupa islas por provincia si n > 9."
            ),
            "principio_gestalt": MetadataValue.text(
                "Similitud — Más de 9 colores son imposibles de distinguir."
            ),
            "mensaje": MetadataValue.text(
                "Canarias tiene 7 islas — dentro del límite. Revisar si se añaden territorios."
            ),
        },
    )


@asset_check(
    asset="integrar_renta_codislas",
    name="check_integridad_join_renta_codislas",
    description=(
        "Verifica que el LEFT JOIN no produzca ISLA_clean nulos. "
        "Gestalt — Figura y Fondo."
    ),
)
def check_integridad_join_renta_codislas(integrar_renta_codislas: pd.DataFrame) -> AssetCheckResult:
    n_sin_isla = int(integrar_renta_codislas["ISLA_clean"].isna().sum())
    total      = len(integrar_renta_codislas)
    pct        = round(n_sin_isla / total * 100, 2) if total else 0.0
    ejemplos   = (
        integrar_renta_codislas
        .loc[integrar_renta_codislas["ISLA_clean"].isna(), "Territorio"]
        .unique()[:5].tolist()
    )
    return AssetCheckResult(
        passed=n_sin_isla == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "filas_sin_isla":    MetadataValue.int(n_sin_isla),
            "porcentaje_nulos":  MetadataValue.float(pct),
            "ejemplos_sin_isla": MetadataValue.text(str(ejemplos)),
            "principio_gestalt": MetadataValue.text(
                "Figura y Fondo — Territorios sin isla asignada no aparecerán en el facet correcto."
            ),
            "mensaje": MetadataValue.text(
                "Los territorios sin ISLA_clean no se pintarán en el gráfico territorial."
            ),
        },
    )


@asset_check(
    asset="limpiar_nivelestudios",
    name="check_continuidad_serie_temporal_nivelestudios",
    description="Verifica que no falten años en la serie de nivel de estudios. Gestalt — Continuidad.",
)
def check_continuidad_serie_temporal_nivelestudios(limpiar_nivelestudios: pd.DataFrame) -> AssetCheckResult:
    años      = sorted(limpiar_nivelestudios["Periodo"].dropna().unique().tolist())
    faltantes = [a for a in range(min(años), max(años) + 1) if a not in años] if años else []
    return AssetCheckResult(
        passed=len(faltantes) == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "fechas_faltantes": MetadataValue.text(str(faltantes) if faltantes else "ninguna"),
            "rango_temporal":   MetadataValue.text(
                f"{min(años)}–{max(años)}" if años else "vacío"
            ),
            "principio_gestalt": MetadataValue.text(
                "Continuidad — Un año faltante une puntos lejanos creando una pendiente falsa."
            ),
            "mensaje": MetadataValue.text(
                "geom_area interpolará sobre el hueco produciendo un área engañosa."
            ),
        },
    )


@asset_check(
    asset="limpiar_nivelestudios",
    name="check_cardinalidad_nivel_estudios",
    description="Verifica que los niveles de estudio no superen MAX_CATEGORIAS_COLOR. Gestalt — Carga Cognitiva.",
)
def check_cardinalidad_nivel_estudios(limpiar_nivelestudios: pd.DataFrame) -> AssetCheckResult:
    col = "Nivel de estudios en curso"
    if col not in limpiar_nivelestudios.columns:
        return AssetCheckResult(
            passed=True,
            metadata={"mensaje": MetadataValue.text(f"Columna '{col}' no encontrada, check omitido.")},
        )
    n = int(limpiar_nivelestudios[col].nunique())
    return AssetCheckResult(
        passed=n <= MAX_CATEGORIAS_COLOR,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "n_categorias":          MetadataValue.int(n),
            "limite_recomendado":    MetadataValue.int(MAX_CATEGORIAS_COLOR),
            "sugerencia_agrupacion": MetadataValue.text(
                "Usa MAPA_EDUCACION para reducir a 4 grupos: Básicos, Medios, Superiores, Sin Estudios."
            ),
            "principio_gestalt": MetadataValue.text(
                "Carga Cognitiva / Similitud — Más de 9 colores son imposibles de distinguir."
            ),
            "mensaje": MetadataValue.text(
                "El mapa MAPA_EDUCACION en config.py ya agrupa las categorías a 4 niveles."
            ),
        },
    )


@asset_check(
    asset="enriquecer_nivelestudios",
    name="check_dominance_otros_nivelestudios",
    description=(
        "Verifica que 'Sin Estudios/Otros' no supere DOMINANCE_OTROS_MAX del total "
        "en la dimensión social activa del Dashboard. Gestalt — Semejanza."
    ),
)
def check_dominance_otros_nivelestudios(enriquecer_nivelestudios: pd.DataFrame) -> AssetCheckResult:
    col = "Nivel de estudios en curso"
    if col not in enriquecer_nivelestudios.columns:
        return AssetCheckResult(
            passed=True,
            metadata={"mensaje": MetadataValue.text("Columna no encontrada, check omitido.")},
        )
        
    df = enriquecer_nivelestudios.copy()
    df["Categoria"] = df[col].map(MAPA_EDUCACION)
    
    # 1. Asegurar tipo numérico para evitar fallos de ejecución
    df["Total"] = pd.to_numeric(df["Total"], errors="coerce").fillna(0)
    
    # 2. Filtrar usando la lógica exacta de la visualización (Dashboard.DIM_SOCIAL)
    filtro = df[col] != "Total"  # Quitamos la fila que suma todos los estudios
    
    dim_activa = Dashboard.DIM_SOCIAL
    dimensiones_demograficas = ["Sexo", "Nacionalidad", "Edad"]
    
    for dim in dimensiones_demograficas:
        if dim in df.columns:
            if dim == dim_activa:
                # Para la dimensión que graficamos (ej. Nacionalidad), EXCLUIMOS el "Total"
                # para quedarnos con el desglose real (Española, Extranjera)
                filtro = filtro & (df[dim] != "Total")
            elif "Total" in df[dim].unique():
                # Para el resto (ej. Sexo), FIJAMOS en "Total" para no multiplicar la población
                filtro = filtro & (df[dim] == "Total")
            
    df_f = df[filtro]
    
    # 3. Cálculos sobre los datos exactos que se van a graficar
    total_absoluto = df_f["Total"].sum()
    otros = df_f.loc[df_f["Categoria"] == "Sin Estudios/Otros", "Total"].sum()
    
    # 4. Forzar float() nativo
    pct = float(round(otros / total_absoluto * 100, 2)) if total_absoluto else 0.0
    
    top = (
        df_f.groupby("Categoria")["Total"].sum()
        .sort_values(ascending=False).head(4).to_dict()
    )
    
    return AssetCheckResult(
        passed=pct <= (DOMINANCE_OTROS_MAX * 100),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "pct_of_total":      MetadataValue.float(pct),
            "umbral_maximo":     MetadataValue.float(DOMINANCE_OTROS_MAX * 100),
            "top_categories":    MetadataValue.text(str(top)),
            "dashboard_activo":  MetadataValue.text(f"Dimensión evaluada: {dim_activa}"),
            "principio_gestalt": MetadataValue.text(
                "Semejanza — Un grupo 'Otros' dominante atrae la atención lejos de los datos relevantes."
            ),
            "mensaje": MetadataValue.text(
                f"'Sin Estudios/Otros' = {pct}% en el desglose por {dim_activa}. "
                f"Si supera {DOMINANCE_OTROS_MAX*100}%, considera separarlo del gráfico principal."
            ),
        },
    )


@asset_check(
    asset="enriquecer_nivelestudios",
    name="check_label_text_municipio",
    description="Detecta municipios con nombres demasiado largos para el eje Y del heatmap. Gestalt — Continuidad.",
)
def check_label_text_municipio(enriquecer_nivelestudios: pd.DataFrame) -> AssetCheckResult:
    longitudes = enriquecer_nivelestudios["Municipio_clean"].str.len()
    max_len    = int(longitudes.max())
    ejemplo    = enriquecer_nivelestudios.loc[longitudes.idxmax(), "Municipio_clean"]
    return AssetCheckResult(
        passed=max_len <= MAX_LABEL_LENGTH,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "longest_label": MetadataValue.text(str(ejemplo)),
            "longitud_max":  MetadataValue.int(max_len),
            "limite":        MetadataValue.int(MAX_LABEL_LENGTH),
            "overlap_risk":  MetadataValue.bool(max_len > MAX_LABEL_LENGTH),
            "principio_gestalt": MetadataValue.text(
                "Continuidad — Etiquetas largas se solapan en el eje Y del heatmap."
            ),
            "mensaje": MetadataValue.text(
                "Considera truncar municipios a 30 chars en el gráfico de heatmap."
            ),
        },
    )


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 3 — VISUALIZACIÓN (Asset)
#
# Todos estos checks leen config.Dashboard y filtran los datos con
# _filtrar_datos_dashboard(), replicando exactamente el subconjunto
# que graficar_renta_territorial() va a pintar.
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset="generar_graficos_ejercicio3",
    name="check_eje_cero_renta",
    description=(
        "Verifica que Dashboard.EJE_Y_CERO=True, lo que garantiza que el eje Y "
        "nace en 0. "
        "Gestalt — Veracidad Visual: truncar el eje Y exagera diferencias pequeñas. "
        "Este check falla con WARN si EJE_Y_CERO=False en config.Dashboard."
    ),
    additional_ins={"integrar_renta_codislas": AssetIn()}
)
def check_eje_cero_renta(
    generar_graficos_ejercicio3: list,
    integrar_renta_codislas: pd.DataFrame,
) -> AssetCheckResult:
    eje_y_cero = Dashboard.EJE_Y_CERO
    datos      = _filtrar_datos_dashboard(integrar_renta_codislas)
    v_min      = float(datos["Porcentaje"].min()) if not datos.empty else 0.0
    # Si EJE_Y_CERO=True el gráfico fuerza y=0; si es False y v_min > 0, el eje está truncado
    truncado   = not eje_y_cero and v_min > 0
    error_pct  = round(v_min / (datos["Porcentaje"].max() or 1) * 100, 2) if truncado else 0.0
    return AssetCheckResult(
        passed=not truncado,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "eje_y_cero_configurado": MetadataValue.bool(eje_y_cero),
            "valor_inicio_eje":       MetadataValue.float(0.0 if eje_y_cero else v_min),
            "error_perceptivo_pct":   MetadataValue.float(error_pct),
            "dashboard_activo":       MetadataValue.text(_contexto_dashboard()),
            "principio_gestalt": MetadataValue.text(
                "Veracidad Visual — Truncar el eje Y exagera diferencias pequeñas."
            ),
            "mensaje": MetadataValue.text(
                "Activa Dashboard.EJE_Y_CERO=True en config.py para que las barras nazcan en 0."
                if truncado else
                "EJE_Y_CERO=True — el eje Y nace en 0. ✓"
            ),
        },
    )


@asset_check(
    asset="generar_graficos_ejercicio3",
    name="check_datos_dashboard_no_vacios",
    description=(
        "Verifica que el subconjunto de datos filtrado por los parámetros actuales "
        "de config.Dashboard no está vacío antes de generar el gráfico. "
        "Gestalt — Figura y Fondo."
    ),
    additional_ins={"integrar_renta_codislas": AssetIn()}
)
def check_datos_dashboard_no_vacios(
    generar_graficos_ejercicio3: list,
    integrar_renta_codislas: pd.DataFrame,
) -> AssetCheckResult:
    datos    = _filtrar_datos_dashboard(integrar_renta_codislas)
    n_filas  = len(datos)
    n_terrs  = datos["Territorio"].nunique() if not datos.empty else 0
    passed   = n_filas > 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "filas_en_grafico":       MetadataValue.int(n_filas),
            "territorios_en_grafico": MetadataValue.int(n_terrs),
            "dashboard_activo":       MetadataValue.text(_contexto_dashboard()),
            "principio_gestalt": MetadataValue.text(
                "Figura y Fondo — Un gráfico sin datos no tiene figura que mostrar."
            ),
            "mensaje": MetadataValue.text(
                f"La combinación territorio='{Dashboard.TERRITORIO}' + "
                f"fuente='{Dashboard.FUENTE}' no produce datos. "
                "Revisa config.Dashboard."
                if not passed else
                f"OK — {n_filas} filas para {n_terrs} territorios."
            ),
        },
    )


@asset_check(
    asset="generar_graficos_ejercicio3",
    name="check_escala_y_dashboard",
    description=(
        "Detecta outliers extremos en el subconjunto exacto que se va a graficar "
        "según config.Dashboard. "
        "Gestalt — Proporcionalidad."
    ),
    additional_ins={"integrar_renta_codislas": AssetIn()}
)
def check_escala_y_dashboard(
    generar_graficos_ejercicio3: list,
    integrar_renta_codislas: pd.DataFrame,
) -> AssetCheckResult:
    datos    = _filtrar_datos_dashboard(integrar_renta_codislas)
    if datos.empty:
        return AssetCheckResult(
            passed=True,
            metadata={"mensaje": MetadataValue.text("Sin datos para el dashboard actual.")},
        )
    valores  = datos["Porcentaje"].dropna()
    v_min, v_max = float(valores.min()), float(valores.max())
    ratio    = round(v_max / v_min, 2) if v_min > 0 else float("inf")
    outlier  = datos.loc[datos["Porcentaje"] == v_max, "Territorio"].iloc[0]
    return AssetCheckResult(
        passed=ratio <= RATIO_ESCALA_MAX,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "ratio_escala":       MetadataValue.float(ratio),
            "valor_outlier":      MetadataValue.float(v_max),
            "territorio_outlier": MetadataValue.text(str(outlier)),
            "umbral_maximo":      MetadataValue.float(RATIO_ESCALA_MAX),
            "dashboard_activo":   MetadataValue.text(_contexto_dashboard()),
            "principio_gestalt": MetadataValue.text(
                "Proporcionalidad — Evita que barras pequeñas parezcan invisibles ante una gigante."
            ),
            "mensaje": MetadataValue.text(
                f"Ratio max/min = {ratio} en el gráfico de '{Dashboard.TERRITORIO}'. "
                f"Umbral = {RATIO_ESCALA_MAX}."
            ),
        },
    )


@asset_check(
    asset="generar_graficos_ejercicio3",
    name="check_cardinalidad_dashboard",
    description=(
        "Verifica que el número de series en el gráfico actual no supera MAX_CATEGORIAS_COLOR. "
        "Depende de si comparar_subterritorios es True (cuenta territorios) o False (cuenta fuentes). "
        "Gestalt — Carga Cognitiva."
    ),
    additional_ins={"integrar_renta_codislas": AssetIn()}
)
def check_cardinalidad_dashboard(
    generar_graficos_ejercicio3: list,
    integrar_renta_codislas: pd.DataFrame,
) -> AssetCheckResult:
    datos = _filtrar_datos_dashboard(integrar_renta_codislas)
    if Dashboard.COMPARAR_SUBS:
        n      = datos["Territorio"].nunique() if not datos.empty else 0
        col_id = "Territorio"
    else:
        n      = datos["Fuente_Renta_Code"].nunique() if not datos.empty else 0
        col_id = "Fuente_Renta_Code"
    passed = n <= MAX_CATEGORIAS_COLOR
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "n_series":           MetadataValue.int(n),
            "columna_series":     MetadataValue.text(col_id),
            "limite_recomendado": MetadataValue.int(MAX_CATEGORIAS_COLOR),
            "dashboard_activo":   MetadataValue.text(_contexto_dashboard()),
            "principio_gestalt": MetadataValue.text(
                "Carga Cognitiva — Más de 9 colores son imposibles de distinguir."
            ),
            "mensaje": MetadataValue.text(
                f"El gráfico de '{Dashboard.TERRITORIO}' tiene {n} series ({col_id}). "
                f"Máximo recomendado: {MAX_CATEGORIAS_COLOR}."
            ),
        },
    )


@asset_check(
    asset="generar_graficos_ejercicio3",
    name="check_orden_magnitud_dashboard",
    description=(
        "Verifica si las series del gráfico actual están ordenadas por valor medio descendente. "
        "Gestalt — Continuidad / Prägnanz: el ojo sigue una escalera suave."
    ),
    additional_ins={"integrar_renta_codislas": AssetIn()}
)
def check_orden_magnitud_dashboard(
    generar_graficos_ejercicio3: list,
    integrar_renta_codislas: pd.DataFrame,
) -> AssetCheckResult:
    datos = _filtrar_datos_dashboard(integrar_renta_codislas)
    if datos.empty:
        return AssetCheckResult(
            passed=True,
            metadata={"mensaje": MetadataValue.text("Sin datos para el dashboard actual.")},
        )
    col_serie = "Territorio" if Dashboard.COMPARAR_SUBS else "Fuente_Renta"
    medias    = datos.groupby(col_serie)["Porcentaje"].mean().sort_values(ascending=False)
    orden_actual  = list(medias.index)
    orden_optimo  = orden_actual  # ya está ordenado tras sort_values
    is_sorted     = orden_actual == orden_optimo
    return AssetCheckResult(
        passed=is_sorted,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "orden_detectado":  MetadataValue.text(str(orden_actual)),
            "is_sorted":        MetadataValue.bool(is_sorted),
            "sugerencia_orden": MetadataValue.text(
                f"En aes() usa reorder({col_serie}, -Porcentaje) para ordenar la leyenda."
            ),
            "dashboard_activo": MetadataValue.text(_contexto_dashboard()),
            "principio_gestalt": MetadataValue.text(
                "Continuidad / Prägnanz — El ojo sigue una escalera suave; "
                "el orden reduce el esfuerzo cognitivo."
            ),
            "mensaje": MetadataValue.text(
                "Categorías desordenadas obligan al usuario a saltar entre colores para comparar."
            ),
        },
    )


@asset_check(
    asset="generar_graficos_ejercicio3",
    name="check_graficos_generados",
    description=(
        "Verifica que los ficheros PNG se han generado con tamaño > 10 KB. "
        "Un fichero vacío indica un error silencioso en plotnine."
    ),
    
)
def check_graficos_generados(generar_graficos_ejercicio3: list) -> AssetCheckResult:
    resultados = {}
    todos_ok   = True
    for ruta in generar_graficos_ejercicio3:
        existe  = os.path.exists(ruta)
        size_kb = round(os.path.getsize(ruta) / 1024, 1) if existe else 0.0
        ok      = existe and size_kb > 10.0
        if not ok:
            todos_ok = False
        resultados[os.path.basename(ruta)] = f"{'OK' if ok else 'FALLO'} ({size_kb} KB)"
    return AssetCheckResult(
        passed=todos_ok,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "graficos":          MetadataValue.text(str(resultados)),
            "dashboard_activo":  MetadataValue.text(_contexto_dashboard()),
            "principio_gestalt": MetadataValue.text(
                "Veracidad Visual — Un gráfico vacío o truncado transmite información falsa."
            ),
            "mensaje": MetadataValue.text(
                "Si size < 10 KB, plotnine generó un gráfico vacío (datos insuficientes o error)."
            ),
        },
    )


@asset_check(
    asset="generar_graficos_ejercicio3",
    name="check_consistencia_color_paleta",
    description=(
        "Valida que la paleta maestra no ha cambiado respecto al hash de referencia. "
        "Gestalt — Similitud."
    ),
    
)
def check_consistencia_color_paleta(generar_graficos_ejercicio3: list) -> AssetCheckResult:
    hash_actual = hashlib.md5(str(sorted(PALETA_MAESTRA.items())).encode()).hexdigest()
    ha_cambiado = hash_actual != _PALETA_HASH
    return AssetCheckResult(
        passed=not ha_cambiado,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "palette_id":       MetadataValue.text(hash_actual),
            "hash_referencia":  MetadataValue.text(_PALETA_HASH),
            "has_changed":      MetadataValue.bool(ha_cambiado),
            "dashboard_activo": MetadataValue.text(_contexto_dashboard()),
            "principio_gestalt": MetadataValue.text(
                "Similitud — Colores distintos para la misma categoría rompen la consistencia visual."
            ),
            "mensaje": MetadataValue.text(
                "Si la paleta cambia, actualiza _PALETA_HASH en checks.py."
            ),
        },
    )
