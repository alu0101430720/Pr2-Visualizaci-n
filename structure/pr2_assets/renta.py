"""
assets/renta.py — Pipeline de datos de distribución de renta.

Assets:
  ingestar_renta → limpiar_renta → guardar_renta_limpia
"""
import os

import pandas as pd
from dagster import asset, OpExecutionContext

from config import DIR_CLEAN, RAW_RENTA


@asset
def ingestar_renta(context: OpExecutionContext, pull_repository: str) -> pd.DataFrame:
    """Lee el CSV original de distribución de renta."""
    df = pd.read_csv(RAW_RENTA)
    context.log.info(f"CSV cargado: {df.shape[0]} filas, {df.shape[1]} columnas.")
    return df


@asset
def limpiar_renta(context: OpExecutionContext, ingestar_renta: pd.DataFrame) -> pd.DataFrame:
    """Elimina columnas innecesarias, renombra y normaliza."""
    df = ingestar_renta.drop(columns=[
        "ESTADO_OBSERVACION#es",
        "CONFIDENCIALIDAD_OBSERVACION#es",
        "TIME_PERIOD_CODE",
        "TERRITORIO_CODE",
    ])
    df = df.rename(columns={
        "TERRITORIO#es":  "Territorio",
        "TIME_PERIOD#es": "Año",
        "MEDIDAS#es":     "Fuente_Renta",
        "MEDIDAS_CODE":   "Fuente_Renta_Code",
        "OBS_VALUE":      "Porcentaje",
    })
    df["Territorio"] = df["Territorio"].str.title()
    context.log.info("Limpieza de renta completada.")
    return df


@asset
def guardar_renta_limpia(context: OpExecutionContext, limpiar_renta: pd.DataFrame) -> str:
    """Persiste el DataFrame limpio y devuelve la ruta del fichero."""
    os.makedirs(DIR_CLEAN, exist_ok=True)
    ruta = f"{DIR_CLEAN}/distribucion-renta-canarias-clean.csv"
    limpiar_renta.to_csv(ruta, index=False)
    context.log.info(f"Renta limpia guardada en: {ruta}")
    return ruta
