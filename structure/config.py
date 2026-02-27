# config.py — Constantes y configuración compartida del proyecto Pr2-Visualizacion.

# ── Repositorio Git ────────────────────────────────────────────────────────────
REPO_DIR   = "/content/Pr2-Visualizacion"
REPO_OWNER = "alu0101430720"
REPO_NAME  = "Pr2-Visualizacion"
GIT_BRANCH = "practica3_refactorizado"
GIT_EMAIL  = "alu0101430720@ull.edu.es"
GIT_NAME   = "Carlos Yanes"

def repo_url(token: str) -> str:
    return f"https://{token}@github.com/{REPO_OWNER}/{REPO_NAME}.git"


# ── Rutas de datasets ──────────────────────────────────────────────────────────
RAW_RENTA         = f"{REPO_DIR}/distribucion-renta-canarias.csv"
RAW_CODISLAS      = f"{REPO_DIR}/codislas.csv"
RAW_NIVELESTUDIOS = f"{REPO_DIR}/nivelestudios.xlsx"

DIR_CLEAN    = f"{REPO_DIR}/datasets-clean"
DIR_GRAFICOS = f"{REPO_DIR}/graficos"


# ── Fuentes de renta ───────────────────────────────────────────────────────────
class Fuentes:
    SALARIOS           = "SUELDOS_SALARIOS"
    PENSIONES          = "PENSIONES"
    DESEMPLEO          = "PRESTACIONES_DESEMPLEO"
    OTROS_INGRESOS     = "OTROS_INGRESOS"
    OTRAS_PRESTACIONES = "OTRAS_PRESTACIONES"
    TODOS = [SALARIOS, PENSIONES, DESEMPLEO, OTROS_INGRESOS, OTRAS_PRESTACIONES]


# ── Geografía ──────────────────────────────────────────────────────────────────
ISLAS_LAS_PALMAS = ["Gran Canaria", "Lanzarote", "Fuerteventura"]
ISLAS_SCT        = ["Tenerife", "La Gomera", "La Palma", "El Hierro"]
TODAS_ISLAS      = ISLAS_LAS_PALMAS + ISLAS_SCT


# ── Mapas de categorías ────────────────────────────────────────────────────────
MAPA_EDUCACION = {
    "Educación primaria e inferior": "Básicos",
    "Primera etapa de Educación Secundaria y similar": "Básicos",
    (
        "Segunda etapa de Educación Secundaria, con orientación profesional "
        "(con y sin continuidad en la educación superior); "
        "Educación postsecundaria no superior"
    ): "Medios",
    "Segunda etapa de educación secundaria, con orientación general": "Medios",
    "Educación superior": "Superiores",
    "No cursa estudios": "Sin Estudios/Otros",
    "Cursa estudios pero no hay información sobre los mismos": "Sin Estudios/Otros",
}