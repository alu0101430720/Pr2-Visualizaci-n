"""
assets/codislas.py — Pipeline de códigos de islas + integración con renta.

Assets:
  ingestar_codislas → limpiar_codislas → guardar_codislas_limpia
                                        ↘
                       limpiar_renta  → integrar_renta_codislas
"""
import os

import pandas as pd
from dagster import asset, OpExecutionContext

from config import DIR_CLEAN, RAW_CODISLAS


# ── Helpers ────────────────────────────────────────────────────────────────────

def _limpiar_formato_coma(texto) -> str:
    """Convierte 'Apellido, Articulo' → 'Articulo Apellido'."""
    if pd.isna(texto):
        return ""
    texto = str(texto)
    if "," in texto:
        nombre, articulo = [p.strip() for p in texto.split(",", 1)]
        return f"{articulo} {nombre}"
    return texto.strip()


# ── Assets ─────────────────────────────────────────────────────────────────────

@asset
def ingestar_codislas(context: OpExecutionContext, pull_repository: str) -> pd.DataFrame:
    """Lee el CSV de códigos de islas."""
    df = pd.read_csv(RAW_CODISLAS, encoding="latin-1", sep=";")
    context.log.info(f"codislas.csv cargado: {df.shape[0]} filas.")
    return df


@asset
def limpiar_codislas(context: OpExecutionContext, ingestar_codislas: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nombres de isla y municipio, renombra columna clave."""
    df = ingestar_codislas.copy()
    df["ISLA_clean"]   = df["ISLA"].apply(_limpiar_formato_coma).str.title()
    df["NOMBRE_clean"] = df["NOMBRE"].apply(_limpiar_formato_coma).str.title()
    df = df.rename(columns={"NOMBRE_clean": "Territorio"})
    context.log.info(f"codislas limpio: {df.shape[0]} filas.")
    return df


@asset
def guardar_codislas_limpia(context: OpExecutionContext, limpiar_codislas: pd.DataFrame) -> str:
    """Guarda codislas limpio y devuelve la ruta."""
    os.makedirs(DIR_CLEAN, exist_ok=True)
    ruta = f"{DIR_CLEAN}/codislas-clean.csv"
    limpiar_codislas.to_csv(ruta, index=False)
    context.log.info(f"codislas-clean guardado en: {ruta}")
    return ruta


@asset
def integrar_renta_codislas(
    context: OpExecutionContext,
    limpiar_renta: pd.DataFrame,
    limpiar_codislas: pd.DataFrame,
) -> pd.DataFrame:
    """
    Une renta con codislas para añadir la columna ISLA_clean.
    Este DataFrame es la base para los ejercicios 2 y 3.
    """
    df = pd.merge(
        limpiar_renta,
        limpiar_codislas[["Territorio", "ISLA_clean"]],
        on="Territorio",
        how="left",
    )
    context.log.info(f"Integración renta+codislas: {df.shape[0]} filas.")
    return df


@asset
def guardar_renta_integrada(context: OpExecutionContext, integrar_renta_codislas: pd.DataFrame) -> str:
    """Persiste el DataFrame integrado de renta+codislas."""
    os.makedirs(DIR_CLEAN, exist_ok=True)
    ruta = f"{DIR_CLEAN}/renta-integrada.csv"
    integrar_renta_codislas.to_csv(ruta, index=False)
    context.log.info(f"Renta integrada guardada en: {ruta}")
    return ruta
