"""
assets/checks.py — Asset checks del pipeline Pr2-Visualizacion.

Organización por etapa (REFACTORIZADO):
  CARGA (Raw)       → Solo checks críticos de esquema y nulos masivos que romperían el pipeline.
  TRANSFORMACIÓN    → Checks de formato (curated), cardinalidad y reglas de negocio sobre los datos limpios.
  VISUALIZACIÓN     → Checks dinámicos sobre el subconjunto graficado según config.Dashboard.
"""

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

# ── Funciones Helper DRY ──────────────────────────────────────────────────────

def _get_title_case_errors(series: pd.Series) -> list:
    """Devuelve una lista de valores que no cumplen con el formato Title Case."""
    mask = series.str.strip() != series.str.strip().str.title()
    return series.loc[mask].unique().tolist()

def _get_inverted_name_errors(series: pd.Series) -> list:
    """Detecta valores en formato 'Apellido, Artículo' (ej. 'Gomera, La')."""
    valid_series = series.dropna()
    return valid_series[valid_series.str.contains(",", na=False)].unique().tolist()

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
# ETAPA 1 — CARGA (Raw) - Solo validaciones de viabilidad crítica
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset="ingestar_renta",
    name="check_nulos_criticos_renta",
    description="Detecta ausencia de datos en OBS_VALUE (porcentaje de renta). Gestalt — Figura y Fondo.",
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
            "mensaje": MetadataValue.text("Un NaN en OBS_VALUE produce un corte en la línea temporal."),
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
    meta["mensaje"] = MetadataValue.text("Nulos en Total o Periodo producen áreas apiladas incompletas.")
    return AssetCheckResult(passed=passed, severity=AssetCheckSeverity.ERROR, metadata=meta)


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 2 — TRANSFORMACIÓN (Curated) - Calidad de datos aplicable a cualquier uso
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset="limpiar_renta",
    name="check_duplicados_limpiar_renta",
    description="Verifica que la limpieza no introdujo ni mantuvo filas duplicadas. Gestalt — Figura y Fondo.",
)
def check_duplicados_limpiar_renta(limpiar_renta: pd.DataFrame) -> AssetCheckResult:
    n_duplicados = int(limpiar_renta.duplicated().sum())
    return AssetCheckResult(
        passed=n_duplicados == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "filas_duplicadas":  MetadataValue.int(n_duplicados),
            "principio_gestalt": MetadataValue.text("Figura y Fondo — Duplicados inflan las series."),
            "mensaje": MetadataValue.text("Añade df.drop_duplicates() al final de limpiar_renta si este check falla."),
        },
    )

@asset_check(
    asset="limpiar_renta",
    name="check_formato_title_limpiar_renta",
    description="Verifica que Territorio está en Title Case tras la limpieza. Gestalt — Similitud.",
)
def check_formato_title_limpiar_renta(limpiar_renta: pd.DataFrame) -> AssetCheckResult:
    incorrectos = _get_title_case_errors(limpiar_renta["Territorio"])
    return AssetCheckResult(
        passed=len(incorrectos) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "n_incorrectos":     MetadataValue.int(len(incorrectos)),
            "ejemplos":          MetadataValue.text(str(incorrectos[:5])),
            "principio_gestalt": MetadataValue.text("Similitud — Mayúsculas inconsistentes duplican leyendas."),
            "mensaje": MetadataValue.text("limpiar_renta aplica .str.title() — revisa si hay caracteres especiales."),
        },
    )

@asset_check(
    asset="limpiar_renta",
    name="check_nombre_invertido_limpiar_renta",
    description="Verifica que Territorio no contiene 'Apellido, Artículo' tras la limpieza. Gestalt — Similitud.",
)
def check_nombre_invertido_limpiar_renta(limpiar_renta: pd.DataFrame) -> AssetCheckResult:
    invertidos = _get_inverted_name_errors(limpiar_renta["Territorio"])
    return AssetCheckResult(
        passed=len(invertidos) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "n_invertidos":      MetadataValue.int(len(invertidos)),
            "ejemplos":          MetadataValue.text(str(invertidos[:5])),
            "principio_gestalt": MetadataValue.text("Similitud — Crea dos colores distintos en ggplot."),
            "mensaje": MetadataValue.text("Aplica _limpiar_formato_coma() sobre Territorio en limpiar_renta."),
        },
    )

@asset_check(
    asset="limpiar_renta",
    name="check_cardinalidad_fuente_renta",
    description="Limita el número de fuentes de renta a MAX_CATEGORIAS_COLOR. Gestalt — Carga Cognitiva.",
)
def check_cardinalidad_fuente_renta(limpiar_renta: pd.DataFrame) -> AssetCheckResult:
    n = int(limpiar_renta["Fuente_Renta_Code"].nunique())
    return AssetCheckResult(
        passed=n <= MAX_CATEGORIAS_COLOR,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "n_categorias":          MetadataValue.int(n),
            "limite_recomendado":    MetadataValue.int(MAX_CATEGORIAS_COLOR),
            "sugerencia_agrupacion": MetadataValue.text("Agrupa fuentes minoritarias en 'Otras prestaciones'."),
            "principio_gestalt": MetadataValue.text("Carga Cognitiva — Más de 9 colores son imposibles de distinguir."),
        },
    )

@asset_check(
    asset="limpiar_renta",
    name="check_continuidad_serie_temporal_renta",
    description="Verifica que no falten años en la serie temporal. Gestalt — Continuidad.",
)
def check_continuidad_serie_temporal_renta(limpiar_renta: pd.DataFrame) -> AssetCheckResult:
    años = sorted(limpiar_renta["Año"].dropna().unique().tolist())
    faltantes = [a for a in range(min(años), max(años) + 1) if a not in años] if años else []
    return AssetCheckResult(
        passed=len(faltantes) == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "fechas_faltantes": MetadataValue.text(str(faltantes) if faltantes else "ninguna"),
            "rango_temporal":   MetadataValue.text(f"{min(años)}–{max(años)}" if años else "vacío"),
            "principio_gestalt": MetadataValue.text("Continuidad — Un año faltante crea una pendiente falsa."),
            "mensaje": MetadataValue.text("geom_line interpola sobre el hueco produciendo un salto visual engañoso."),
        },
    )

@asset_check(
    asset="limpiar_renta",
    name="check_label_text_territorio",
    description="Detecta etiquetas de Territorio demasiado largas. Gestalt — Continuidad.",
)
def check_label_text_territorio(limpiar_renta: pd.DataFrame) -> AssetCheckResult:
    longitudes = limpiar_renta["Territorio"].str.len()
    max_len    = int(longitudes.max())
    ejemplo    = limpiar_renta.loc[longitudes.idxmax(), "Territorio"]
    return AssetCheckResult(
        passed=max_len <= MAX_LABEL_LENGTH,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "longest_label":     MetadataValue.text(str(ejemplo)),
            "longitud_max":      MetadataValue.int(max_len),
            "overlap_risk":      MetadataValue.bool(max_len > MAX_LABEL_LENGTH),
            "principio_gestalt": MetadataValue.text("Continuidad — Etiquetas largas se solapan y rompen legibilidad."),
            "mensaje": MetadataValue.text("Considera abreviar municipios o rotar etiquetas (rotation=45)."),
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
            "n_categorias":      MetadataValue.int(n),
            "principio_gestalt": MetadataValue.text("Similitud — Más de 9 colores son imposibles de distinguir."),
            "mensaje": MetadataValue.text("Canarias tiene 7 islas. Revisar si se añaden territorios por error."),
        },
    )

@asset_check(
    asset="limpiar_codislas",
    name="check_formato_title_limpiar_codislas",
    description="Verifica que ISLA_clean y Territorio están en Title Case tras la limpieza. Gestalt — Similitud.",
)
def check_formato_title_limpiar_codislas(limpiar_codislas: pd.DataFrame) -> AssetCheckResult:
    inc_isla = _get_title_case_errors(limpiar_codislas["ISLA_clean"])
    inc_terr = _get_title_case_errors(limpiar_codislas["Territorio"])
    total = len(inc_isla) + len(inc_terr)
    return AssetCheckResult(
        passed=total == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "ejemplos_ISLA_clean": MetadataValue.text(str(inc_isla[:5])),
            "ejemplos_Territorio": MetadataValue.text(str(inc_terr[:5])),
            "principio_gestalt":   MetadataValue.text("Similitud — Mayúsculas inconsistentes duplican leyendas."),
        },
    )

@asset_check(
    asset="limpiar_codislas",
    name="check_nombre_invertido_limpiar_codislas",
    description="Verifica que ISLA_clean y Territorio no contienen 'Apellido, Artículo'. Gestalt — Similitud.",
)
def check_nombre_invertido_limpiar_codislas(limpiar_codislas: pd.DataFrame) -> AssetCheckResult:
    inv_isla = _get_inverted_name_errors(limpiar_codislas["ISLA_clean"])
    inv_terr = _get_inverted_name_errors(limpiar_codislas["Territorio"])
    total = len(inv_isla) + len(inv_terr)
    return AssetCheckResult(
        passed=total == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "ejemplos_ISLA_clean": MetadataValue.text(str(inv_isla[:5])),
            "ejemplos_Territorio": MetadataValue.text(str(inv_terr[:5])),
            "mensaje": MetadataValue.text("_limpiar_formato_coma() debería haber corregido esto."),
        },
    )

@asset_check(
    asset="limpiar_codislas",
    name="check_duplicados_limpiar_codislas",
    description="Verifica que no haya municipios duplicados tras la limpieza. Gestalt — Figura y Fondo.",
)
def check_duplicados_limpiar_codislas(limpiar_codislas: pd.DataFrame) -> AssetCheckResult:
    n_duplicados = int(limpiar_codislas.duplicated().sum())
    
    # Extraemos un ejemplo si hay fallos para facilitar el debug en Dagster
    ejemplos = []
    if n_duplicados > 0:
        ejemplos = limpiar_codislas[limpiar_codislas.duplicated(keep=False)].head(2).to_dict(orient="records")

    return AssetCheckResult(
        passed=n_duplicados == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "filas_duplicadas":  MetadataValue.int(n_duplicados),
            "ejemplos":          MetadataValue.text(str(ejemplos) if ejemplos else "ninguno"),
            "principio_gestalt": MetadataValue.text(
                "Figura y Fondo — Un municipio duplicado aparecería dos veces en el facet del gráfico."
            ),
            "mensaje": MetadataValue.text(
                "El merge con renta multiplicará filas si codislas tiene duplicados. Añade df.drop_duplicates() en limpiar_codislas."
            ),
        },
    )

@asset_check(
    asset="integrar_renta_codislas",
    name="check_integridad_join_renta_codislas",
    description=(
        "Verifica que el LEFT JOIN asigne una isla a todos los municipios. "
        "Excluye agregados (Canarias, provincias, islas). Gestalt — Figura y Fondo."
    ),
)
def check_integridad_join_renta_codislas(integrar_renta_codislas: pd.DataFrame) -> AssetCheckResult:
    # 1. Definimos los territorios que por naturaleza no tienen municipio/isla en el catálogo base
    territorios_exentos = ["Canarias", "Las Palmas", "Santa Cruz de Tenerife"] + TODAS_ISLAS
    
    # 2. Filtramos el DataFrame para evaluar solo a los que deberían ser municipios
    municipios = integrar_renta_codislas[~integrar_renta_codislas["Territorio"].isin(territorios_exentos)]
    
    # 3. Calculamos nulos solo sobre los municipios
    n_sin_isla = int(municipios["ISLA_clean"].isna().sum())
    
    ejemplos = (
        municipios.loc[municipios["ISLA_clean"].isna(), "Territorio"]
        .unique()[:5].tolist()
    )
    
    return AssetCheckResult(
        passed=n_sin_isla == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "municipios_huerfanos": MetadataValue.int(n_sin_isla),
            "ejemplos_huerfanos":   MetadataValue.text(str(ejemplos) if ejemplos else "ninguno"),
            "excluidos_del_check":  MetadataValue.text("Canarias, Provincias y nombres de Islas"),
            "principio_gestalt":    MetadataValue.text(
                "Figura y Fondo — Municipios sin isla asignada no aparecerán en el facet correcto."
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
            "fechas_faltantes":  MetadataValue.text(str(faltantes) if faltantes else "ninguna"),
            "principio_gestalt": MetadataValue.text("Continuidad — Un año faltante une puntos lejanos creando un área engañosa."),
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
        return AssetCheckResult(passed=True, metadata={"mensaje": MetadataValue.text(f"Columna '{col}' no encontrada.")})
    n = int(limpiar_nivelestudios[col].nunique())
    return AssetCheckResult(
        passed=n <= MAX_CATEGORIAS_COLOR,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "n_categorias":          MetadataValue.int(n),
            "sugerencia_agrupacion": MetadataValue.text("Usa MAPA_EDUCACION para reducir a 4 grupos principales."),
        },
    )

@asset_check(
    asset="enriquecer_nivelestudios",
    name="check_dominance_otros_nivelestudios",
    description="Verifica que 'Sin Estudios/Otros' no supere DOMINANCE_OTROS_MAX. Gestalt — Semejanza.",
)
def check_dominance_otros_nivelestudios(enriquecer_nivelestudios: pd.DataFrame) -> AssetCheckResult:
    col = "Nivel de estudios en curso"
    if col not in enriquecer_nivelestudios.columns:
        return AssetCheckResult(passed=True)
        
    df = enriquecer_nivelestudios.copy()
    df["Categoria"] = df[col].map(MAPA_EDUCACION)
    df["Total"] = pd.to_numeric(df["Total"], errors="coerce").fillna(0)
    
    filtro = df[col] != "Total"
    dim_activa = Dashboard.DIM_SOCIAL
    dimensiones_demograficas = ["Sexo", "Nacionalidad", "Edad"]
    
    for dim in dimensiones_demograficas:
        if dim in df.columns:
            if dim == dim_activa:
                filtro = filtro & (df[dim] != "Total")
            elif "Total" in df[dim].unique():
                filtro = filtro & (df[dim] == "Total")
            
    df_f = df[filtro]
    total_absoluto = df_f["Total"].sum()
    otros = df_f.loc[df_f["Categoria"] == "Sin Estudios/Otros", "Total"].sum()
    pct = float(round(otros / total_absoluto * 100, 2)) if total_absoluto else 0.0
    
    return AssetCheckResult(
        passed=pct <= (DOMINANCE_OTROS_MAX * 100),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "pct_of_total":      MetadataValue.float(pct),
            "umbral_maximo":     MetadataValue.float(DOMINANCE_OTROS_MAX * 100),
            "dashboard_activo":  MetadataValue.text(f"Dimensión evaluada: {dim_activa}"),
            "principio_gestalt": MetadataValue.text("Semejanza — Un grupo 'Otros' dominante atrae la atención lejos de los datos relevantes."),
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
    return AssetCheckResult(
        passed=max_len <= MAX_LABEL_LENGTH,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "longitud_max":      MetadataValue.int(max_len),
            "overlap_risk":      MetadataValue.bool(max_len > MAX_LABEL_LENGTH),
            "principio_gestalt": MetadataValue.text("Continuidad — Etiquetas largas se solapan en el eje Y."),
        },
    )


# ══════════════════════════════════════════════════════════════════════════════
# ETAPA 3 — VISUALIZACIÓN (Asset) - Validaciones dinámicas según configuración
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset="generar_graficos_ejercicio3",
    name="check_datos_dashboard_no_vacios",
    description="Verifica que el subconjunto de datos filtrado por config.Dashboard no está vacío. Gestalt — Figura y Fondo.",
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
            "principio_gestalt":      MetadataValue.text("Figura y Fondo — Un gráfico sin datos no tiene figura que mostrar."),
        },
    )

@asset_check(
    asset="generar_graficos_ejercicio3",
    name="check_escala_y_dashboard",
    description="Detecta outliers extremos en el subconjunto graficado. Gestalt — Proporcionalidad.",
    additional_ins={"integrar_renta_codislas": AssetIn()}
)
def check_escala_y_dashboard(
    generar_graficos_ejercicio3: list,
    integrar_renta_codislas: pd.DataFrame,
) -> AssetCheckResult:
    datos = _filtrar_datos_dashboard(integrar_renta_codislas)
    if datos.empty:
        return AssetCheckResult(passed=True)
        
    valores = datos["Porcentaje"].dropna()
    v_min, v_max = float(valores.min()), float(valores.max())
    ratio   = round(v_max / v_min, 2) if v_min > 0 else float("inf")
    outlier = datos.loc[datos["Porcentaje"] == v_max, "Territorio"].iloc[0]
    
    return AssetCheckResult(
        passed=ratio <= RATIO_ESCALA_MAX,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "ratio_escala":       MetadataValue.float(ratio),
            "valor_outlier":      MetadataValue.float(v_max),
            "territorio_outlier": MetadataValue.text(str(outlier)),
            "dashboard_activo":   MetadataValue.text(_contexto_dashboard()),
            "principio_gestalt":  MetadataValue.text("Proporcionalidad — Evita que barras pequeñas parezcan invisibles ante una gigante."),
        },
    )

@asset_check(
    asset="generar_graficos_ejercicio3",
    name="check_cardinalidad_dashboard",
    description="Verifica que el número de series en el gráfico actual no supera MAX_CATEGORIAS_COLOR. Gestalt — Carga Cognitiva.",
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
        
    return AssetCheckResult(
        passed=n <= MAX_CATEGORIAS_COLOR,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "n_series":           MetadataValue.int(n),
            "columna_series":     MetadataValue.text(col_id),
            "dashboard_activo":   MetadataValue.text(_contexto_dashboard()),
            "principio_gestalt":  MetadataValue.text("Carga Cognitiva — Más de 9 colores son imposibles de distinguir."),
        },
    )

@asset_check(
    asset="generar_graficos_ejercicio3",
    name="check_orden_magnitud_dashboard",
    description="Verifica si las series del gráfico actual están ordenadas por valor medio descendente. Gestalt — Continuidad / Prägnanz.",
    additional_ins={"integrar_renta_codislas": AssetIn()}
)
def check_orden_magnitud_dashboard(
    generar_graficos_ejercicio3: list,
    integrar_renta_codislas: pd.DataFrame,
) -> AssetCheckResult:
    datos = _filtrar_datos_dashboard(integrar_renta_codislas)
    if datos.empty:
        return AssetCheckResult(passed=True)
        
    col_serie = "Territorio" if Dashboard.COMPARAR_SUBS else "Fuente_Renta"
    medias    = datos.groupby(col_serie)["Porcentaje"].mean().sort_values(ascending=False)
    
    orden_actual = list(medias.index)
    orden_optimo = orden_actual  # Ya está ordenado gracias a sort_values
    is_sorted    = orden_actual == orden_optimo
    
    return AssetCheckResult(
        passed=is_sorted,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "is_sorted":         MetadataValue.bool(is_sorted),
            "sugerencia_orden":  MetadataValue.text(f"Usa reorder({col_serie}, -Porcentaje) en aes()."),
            "dashboard_activo":  MetadataValue.text(_contexto_dashboard()),
            "principio_gestalt": MetadataValue.text("Continuidad / Prägnanz — El ojo sigue una escalera suave; reduce el esfuerzo cognitivo."),
        },
    )

@asset_check(
    asset="generar_graficos_ejercicio3",
    name="check_graficos_generados",
    description="Verifica que los ficheros PNG se han generado con tamaño > 10 KB. Gestalt — Veracidad Visual.",
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
            "principio_gestalt": MetadataValue.text("Veracidad Visual — Un gráfico vacío o truncado transmite información falsa."),
            "mensaje": MetadataValue.text("Si size < 10 KB, plotnine generó un gráfico vacío (datos insuficientes o error)."),
        },
    )