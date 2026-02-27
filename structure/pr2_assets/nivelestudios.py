"""
assets/nivelestudios.py — Pipeline de nivel de estudios (ejercicio 3).

Assets:
  ingestar_nivelestudios → limpiar_nivelestudios → enriquecer_nivelestudios
                                                           ↓
                                              guardar_nivelestudios_limpio
                                                           ↓
                                              generar_graficos_ejercicio3
                                                           ↓
                                              commit_ejercicio3
"""
import os
import warnings

import pandas as pd
from dagster import asset, OpExecutionContext

from config import DIR_CLEAN, DIR_GRAFICOS, GIT_BRANCH, REPO_DIR, RAW_NIVELESTUDIOS, Fuentes, repo_url
from charts import graficar_renta_territorial, graficar_social
from utils import commit_and_push


# ── Helpers ────────────────────────────────────────────────────────────────────

def _limpiar_formato_coma(texto) -> str:
    if pd.isna(texto):
        return ""
    texto = str(texto)
    if "," in texto:
        nombre, articulo = [p.strip() for p in texto.split(",", 1)]
        return f"{articulo} {nombre}"
    return texto.strip()


def _slug(texto: str) -> str:
    return texto.lower().replace(" ", "_")


# ── Assets ─────────────────────────────────────────────────────────────────────

@asset
def ingestar_nivelestudios(context: OpExecutionContext, pull_repository: str) -> pd.DataFrame:
    """Lee el Excel de nivel de estudios."""
    df = pd.read_excel(RAW_NIVELESTUDIOS)
    context.log.info(f"nivelestudios.xlsx cargado: {df.shape[0]} filas.")
    return df


@asset
def limpiar_nivelestudios(
    context: OpExecutionContext,
    ingestar_nivelestudios: pd.DataFrame,
) -> pd.DataFrame:
    """Divide código/municipio, extrae año, normaliza nombres."""
    df = ingestar_nivelestudios.copy()
    df[["Codigo", "Municipio"]] = df["Municipios de 500 habitantes o más"].str.split(" ", n=1, expand=True)
    df["Periodo"] = df["Periodo"].dt.year
    df["Municipio_clean"] = df["Municipio"].apply(_limpiar_formato_coma).str.title()
    df.drop(columns=["Municipios de 500 habitantes o más", "Municipio", "Codigo"], inplace=True)
    context.log.info(f"nivelestudios limpio: {df.shape[0]} filas.")
    return df


@asset
def enriquecer_nivelestudios(
    context: OpExecutionContext,
    limpiar_nivelestudios: pd.DataFrame,
    integrar_renta_codislas: pd.DataFrame,
) -> pd.DataFrame:
    """
    Añade ISLA_clean a nivelestudios usando el mapa Territorio→Isla
    obtenido del DataFrame integrado de renta.
    """
    mapa_islas = (
        integrar_renta_codislas[["Territorio", "ISLA_clean"]]
        .dropna()
        .drop_duplicates()
        .set_index("Territorio")["ISLA_clean"]
        .to_dict()
    )
    df = limpiar_nivelestudios.copy()
    df["ISLA_clean"] = df["Municipio_clean"].map(mapa_islas)
    sin_isla = df["ISLA_clean"].isna().sum()
    if sin_isla:
        context.log.warning(f"{sin_isla} filas sin ISLA_clean (municipios no encontrados en el mapa).")
    context.log.info(f"nivelestudios enriquecido: {df.shape[0]} filas.")
    return df


@asset
def guardar_nivelestudios_limpio(
    context: OpExecutionContext,
    enriquecer_nivelestudios: pd.DataFrame,
) -> str:
    """Guarda nivelestudios enriquecido en datasets-clean/."""
    os.makedirs(DIR_CLEAN, exist_ok=True)
    ruta = f"{DIR_CLEAN}/nivelestudios-clean.csv"
    enriquecer_nivelestudios.to_csv(ruta, index=False)
    context.log.info(f"nivelestudios-clean guardado en: {ruta}")
    return ruta


@asset
def generar_graficos_ejercicio3(
    context: OpExecutionContext,
    enriquecer_nivelestudios: pd.DataFrame,
    integrar_renta_codislas: pd.DataFrame,
) -> list[str]:
    """
    Genera y guarda los dos gráficos del dashboard final (renta + social).

    Configura territorio, fuente y dimensión social aquí o pásalos
    como RunConfig para mayor flexibilidad.
    """
    warnings.filterwarnings("ignore")

    # ── Parámetros del dashboard ───────────────────────────────────────────────
    TERRITORIO      = "Tenerife"
    FUENTE          = Fuentes.SALARIOS
    DIM_SOCIAL      = "Nacionalidad"   # "Estudios" | "Sexo" | "Nacionalidad"
    COMPARAR_SUBS   = True
    DESGLOSAR_MUNIS = True
    # ──────────────────────────────────────────────────────────────────────────

    os.makedirs(DIR_GRAFICOS, exist_ok=True)
    rutas = []

    # Gráfico de renta
    g_renta = graficar_renta_territorial(
        integrar_renta_codislas,
        territorio=TERRITORIO,
        fuentes_codigo=FUENTE,
        comparar_subterritorios=COMPARAR_SUBS,
        desglosar_municipios=DESGLOSAR_MUNIS,
        mostrar_leyenda=True,
        eje_y_cero=True,
    )
    if g_renta is None:
        raise RuntimeError(f"Gráfico de renta vacío para '{TERRITORIO}'.")
    ruta_renta = f"{DIR_GRAFICOS}/ejercicio3_renta_{_slug(TERRITORIO)}.png"
    g_renta.save(ruta_renta, dpi=150)
    context.log.info(f"Gráfico renta → {ruta_renta}")
    rutas.append(ruta_renta)

    # Gráfico social
    g_social = graficar_social(
        enriquecer_nivelestudios,
        territorio=TERRITORIO,
        dimension_social=DIM_SOCIAL,
        desglosar_municipios=DESGLOSAR_MUNIS,
        comparar_subterritorios=COMPARAR_SUBS,
    )
    ruta_social = f"{DIR_GRAFICOS}/ejercicio3_social_{_slug(DIM_SOCIAL)}_{_slug(TERRITORIO)}.png"
    g_social.save(ruta_social, dpi=150)
    context.log.info(f"Gráfico social → {ruta_social}")
    rutas.append(ruta_social)

    return rutas


@asset
def commit_ejercicio3(
    context: OpExecutionContext,
    github_token: str,
    guardar_nivelestudios_limpio: str,
    guardar_renta_limpia: str,
    guardar_codislas_limpia: str,
    guardar_renta_integrada: str,
    generar_graficos_ejercicio3: list[str],
) -> None:
    """
    Añade todos los ficheros generados, hace commit y push a la rama de trabajo.

    Incluye datasets limpios (opcional — elimina los que no necesites) y gráficos.
    """
    archivos = [
        # Datasets limpios — comenta las líneas que no quieras commitear
        guardar_renta_limpia,
        guardar_codislas_limpia,
        guardar_renta_integrada,
        guardar_nivelestudios_limpio,
        # Gráficos (siempre)
        *generar_graficos_ejercicio3,
    ]

    commit_and_push(
        repo_dir=REPO_DIR,
        remote_url=repo_url(github_token),
        branch=GIT_BRANCH,
        files=archivos,
        message="ejercicio3: datasets limpios + graficos dashboard renta/social",
        ctx=context,
    )
