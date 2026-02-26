
import os
import subprocess
import warnings
import pandas as pd
from plotnine import (
    ggplot, aes, geom_area, geom_tile, geom_text, facet_wrap,
    scale_x_continuous, scale_fill_brewer, scale_fill_cmap,
    labs, theme_minimal, theme, element_blank, element_text
)
from dagster import asset, OpExecutionContext


# ── Funciones auxiliares (sin modificar) ──────────────────────────────────────

MAPA_EDUCACION = {
    'Educación primaria e inferior': 'Básicos',
    'Primera etapa de Educación Secundaria y similar': 'Básicos',
    'Segunda etapa de Educación Secundaria, con orientación profesional (con y sin continuidad en la educación superior); Educación postsecundaria no superior': 'Medios',
    'Segunda etapa de educación secundaria, con orientación general': 'Medios',
    'Educación superior': 'Superiores',
    'No cursa estudios': 'Sin Estudios/Otros',
    'Cursa estudios pero no hay información sobre los mismos': 'Sin Estudios/Otros'
}

ISLAS_LAS_PALMAS = ['Gran Canaria', 'Lanzarote', 'Fuerteventura']
ISLAS_SCT = ['Tenerife', 'La Gomera', 'La Palma', 'El Hierro']
TODAS_ISLAS = ISLAS_LAS_PALMAS + ISLAS_SCT

class Fuentes:
    """Catálogo de códigos de fuentes de renta para evitar errores de escritura."""
    SALARIOS           = 'SUELDOS_SALARIOS'
    PENSIONES          = 'PENSIONES'
    DESEMPLEO          = 'PRESTACIONES_DESEMPLEO'
    OTROS_INGRESOS     = 'OTROS_INGRESOS'
    OTRAS_PRESTACIONES = 'OTRAS_PRESTACIONES'
    TODOS = [SALARIOS, PENSIONES, DESEMPLEO, OTROS_INGRESOS, OTRAS_PRESTACIONES]

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


def procesar_nombres_nivelestudios(df_nivelestudios):
    df = df_nivelestudios.copy()
    df['Municipio_clean'] = df['Municipio'].apply(limpiar_formato_coma)
    df['Municipio_clean'] = df['Municipio_clean'].str.title()
    return df


def graficar_social(df_educacion, territorio,
                    dimension_social='Estudios',
                    desglosar_municipios=True,
                    comparar_subterritorios=True):
    datos = df_educacion.copy()
    col_isla = 'ISLA_clean' if 'ISLA_clean' in datos.columns else 'Isla_Normalizada'
    col_muni = 'Municipio_clean' if 'Municipio_clean' in datos.columns else 'Municipio_Normalizado'

    if territorio == 'Canarias':
        datos['Territorio'] = datos[col_isla] if desglosar_municipios else 'Canarias'
    elif territorio == 'Las Palmas':
        datos = datos[datos[col_isla].isin(ISLAS_LAS_PALMAS)]
        datos['Territorio'] = datos[col_isla] if desglosar_municipios else 'Las Palmas'
    elif territorio == 'Santa Cruz de Tenerife':
        datos = datos[datos[col_isla].isin(ISLAS_SCT)]
        datos['Territorio'] = datos[col_isla] if desglosar_municipios else 'S.C. Tenerife'
    elif territorio in TODAS_ISLAS:
        datos = datos[datos[col_isla] == territorio]
        datos['Territorio'] = datos[col_muni] if desglosar_municipios else territorio
    else:
        datos = datos[datos[col_muni] == territorio]
        datos['Territorio'] = territorio

    df_plot = pd.DataFrame()
    if dimension_social == 'Estudios':
        temp = datos[(datos['Sexo'] == 'Total') & (datos['Nivel de estudios en curso'] != 'Total')].copy()
        temp['Categoria'] = temp['Nivel de estudios en curso'].map(MAPA_EDUCACION)
        df_plot = temp.groupby(['Periodo', 'Territorio', 'Categoria'])['Total'].sum().reset_index()
        df_plot['Categoria'] = pd.Categorical(
            df_plot['Categoria'],
            categories=['Sin Estudios/Otros', 'Básicos', 'Medios', 'Superiores'],
            ordered=True
        )
    elif dimension_social == 'Sexo':
        temp = datos[(datos['Nivel de estudios en curso'] == 'Total') & (datos['Sexo'] != 'Total')].copy()
        df_plot = temp.groupby(['Periodo', 'Territorio', 'Sexo'])['Total'].sum().reset_index()
        df_plot.rename(columns={'Sexo': 'Categoria'}, inplace=True)
    elif dimension_social == 'Nacionalidad':
        temp = datos[
            (datos['Nivel de estudios en curso'] == 'Total') &
            (datos['Sexo'] == 'Total') &
            (datos['Nacionalidad'] != 'Total')
        ].copy()
        df_plot = temp.groupby(['Periodo', 'Territorio', 'Nacionalidad'])['Total'].sum().reset_index()
        df_plot.rename(columns={'Nacionalidad': 'Categoria'}, inplace=True)

    totales = df_plot.groupby(['Periodo', 'Territorio'])['Total'].transform('sum')
    df_plot['Porcentaje'] = (df_plot['Total'] / totales) * 100
    df_plot['Etiqueta'] = df_plot['Porcentaje'].round(0).astype(int).astype(str) + "%"

    if comparar_subterritorios:
        titulo = "Distribución de Nivel de Estudios en curso (por " + dimension_social + "): " + territorio
        grafico = (
            ggplot(df_plot, aes(x='Periodo', y='Territorio', fill='Porcentaje'))
            + geom_tile(color="white", size=0.5)
            + geom_text(aes(label='Etiqueta'), size=7, color="white")
            + facet_wrap('~Categoria', nrow=1)
            + scale_fill_cmap(name='magma')
            + scale_x_continuous(breaks=sorted(df_plot['Periodo'].unique()))
            + labs(title=titulo, x="Año", y="", fill="%")
            + theme_minimal()
            + theme(figure_size=(14, 6), panel_grid=element_blank())
        )
    else:
        titulo = "Distribución Nivel de Estudios en curso (por " + dimension_social + "): " + territorio
        if df_plot['Territorio'].nunique() > 1:
            facet_config = facet_wrap('~Territorio', scales='free_y')
        else:
            facet_config = theme()
        grafico = (
            ggplot(df_plot, aes(x='Periodo', y='Porcentaje', fill='Categoria'))
            + geom_area(alpha=0.85, color="white")
            + scale_x_continuous(breaks=sorted(df_plot['Periodo'].unique()))
            + facet_config
            + scale_fill_brewer(type='qual', palette='Set2')
            + labs(title=titulo, x="Año", y="%", fill=dimension_social)
            + theme_minimal()
            + theme(figure_size=(12, 5), legend_position='right')
        )
    return grafico


def dashboard_renta_social_final(df_renta, df_educacion,
                                 territorio='Canarias',
                                 fuentes_codigo=None,
                                 dimension_social='Estudios',
                                 desglosar_municipios=True,
                                 comparar_subterritorios=False,
                                 mostrar_leyenda=True,
                                 eje_y_cero=True):
    from code_ejercicio2 import graficar_renta_territorial
    g_renta = graficar_renta_territorial(
        df_renta, territorio, fuentes_codigo,
        comparar_subterritorios=comparar_subterritorios,
        desglosar_municipios=desglosar_municipios,
        mostrar_leyenda=mostrar_leyenda,
        eje_y_cero=eje_y_cero
    )
    g_social = graficar_social(
        df_educacion, territorio, dimension_social,
        desglosar_municipios=desglosar_municipios,
        comparar_subterritorios=comparar_subterritorios
    )
    return g_renta, g_social


# ── Assets Dagster ─────────────────────────────────────────────────────────────

@asset(deps=["pull_repository"])
def ingestar_nivelestudios(context: OpExecutionContext, pull_repository: str) -> pd.DataFrame:
    """Lee el Excel de nivel de estudios."""
    ruta = "Pr2-Visualizacion/nivelestudios.xlsx"
    df = pd.read_excel(ruta)
    context.log.info("nivelestudios.xlsx cargado: " + str(df.shape[0]) + " filas.")
    return df


@asset(deps=["ingestar_nivelestudios"])
def limpiar_nivelestudios(
    context: OpExecutionContext,
    ingestar_nivelestudios: pd.DataFrame,
) -> pd.DataFrame:
    """Limpia y transforma el DataFrame de nivel de estudios."""
    df = ingestar_nivelestudios.copy()
    df[['Codigo', 'Municipio']] = df['Municipios de 500 habitantes o más'].str.split(' ', n=1, expand=True)
    df['Periodo'] = df['Periodo'].dt.year
    df = procesar_nombres_nivelestudios(df)
    df.drop(columns=['Municipios de 500 habitantes o más', 'Municipio', 'Codigo'], inplace=True)
    context.log.info("nivelestudios limpio: " + str(df.shape[0]) + " filas.")
    return df


@asset(deps=["limpiar_nivelestudios", "construir_ejercicio2"])
def construir_ejercicio3(
    context: OpExecutionContext,
    limpiar_nivelestudios: pd.DataFrame,
    construir_ejercicio2: pd.DataFrame,
) -> pd.DataFrame:
    """Añade la columna ISLA_clean a nivelestudios usando el mapa del ejercicio2."""
    mapa_islas = construir_ejercicio2[['Territorio', 'ISLA_clean']].dropna()
    mapa_islas = dict(zip(mapa_islas['Territorio'], mapa_islas['ISLA_clean']))
    df = limpiar_nivelestudios.copy()
    df['ISLA_clean'] = df['Municipio_clean'].map(mapa_islas)
    context.log.info("ejercicio3 construido: " + str(df.shape[0]) + " filas.")
    return df


@asset(deps=["construir_ejercicio3"])
def guardar_nivelestudios_limpio(
    context: OpExecutionContext,
    construir_ejercicio3: pd.DataFrame,
) -> str:
    """Guarda df_nivelestudios_clean en datasets-clean/."""
    directorio = "Pr2-Visualizacion/datasets-clean"
    os.makedirs(directorio, exist_ok=True)
    ruta = directorio + "/nivelestudios-clean.csv"
    construir_ejercicio3.to_csv(ruta, index=False)
    context.log.info("nivelestudios-clean guardado en: " + ruta)
    return ruta


@asset(deps=["construir_ejercicio3", "construir_ejercicio2"])
def generar_graficos_ejercicio3(
    context: OpExecutionContext,
    construir_ejercicio3: pd.DataFrame,
    construir_ejercicio2: pd.DataFrame,
) -> list:
    """Genera y guarda los dos gráficos del dashboard del ejercicio 3."""
    warnings.filterwarnings('ignore')

    territorio = 'Fuerteventura'
    dimension_social = 'Nacionalidad'
    directorio = "Pr2-Visualizacion/graficos"
    os.makedirs(directorio, exist_ok=True)

    g_renta, g_social = dashboard_renta_social_final(
        construir_ejercicio2,
        construir_ejercicio3,
        territorio=territorio,
        fuentes_codigo=Fuentes.SALARIOS,
        dimension_social=dimension_social,
        desglosar_municipios=True,
        comparar_subterritorios=True
    )

    territorio_slug = territorio.lower().replace(' ', '_')
    dimension_slug = dimension_social.lower().replace(' ', '_')

    ruta_renta = directorio + "/ejercicio3_renta_" + territorio_slug + ".png"
    ruta_social = directorio + "/ejercicio3_social_" + dimension_slug + "_" + territorio_slug + ".png"

    g_renta.save(ruta_renta, dpi=150)
    context.log.info("Gráfico renta guardado en: " + ruta_renta)

    g_social.save(ruta_social, dpi=150)
    context.log.info("Gráfico social guardado en: " + ruta_social)

    return [ruta_renta, ruta_social]


@asset(deps=["guardar_nivelestudios_limpio", "generar_graficos_ejercicio3", "github_token"])
def commit_ejercicio3(
    context: OpExecutionContext,
    github_token: str,
    guardar_nivelestudios_limpio: str,
    generar_graficos_ejercicio3: list,
) -> None:
    """Hace add, commit y push de los archivos generados en el ejercicio 3."""
    repo_dir = "Pr2-Visualizacion"
    repo_url = "https://" + github_token + "@github.com/alu0101430720/Pr2-Visualizacion.git"

    archivos = [guardar_nivelestudios_limpio] + generar_graficos_ejercicio3

    for archivo in archivos:
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
        ["git", "-C", repo_dir, "commit", "-m", "ejercicio3: nivelestudios limpio y graficos dashboard"],
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
        ["git", "-C", repo_dir, "push", repo_url, "HEAD:practica3/data-checks"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        msg = "Error en git push: " + result.stderr
        raise RuntimeError(msg)

    context.log.info("Push completado: " + result.stdout)
