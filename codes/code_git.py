
import os
import subprocess
from dagster import asset, OpExecutionContext
from google.colab import userdata


@asset
def github_token() -> str:
    """Recupera el token de GitHub desde la variable de entorno establecida en Colab."""
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise ValueError("No se encontró la variable de entorno 'GITHUB_TOKEN'.")
    return token


@asset(deps=["github_token"])
def clone_repository(context: OpExecutionContext, github_token: str) -> str:
    """Clona el repositorio de GitHub si no existe ya."""
    repo_url = "https://" + github_token + "@github.com/alu0101430720/Pr2-Visualizacion.git"
    repo_dir = "Pr2-Visualizacion"

    result = subprocess.run(
        ["git", "clone", repo_url, repo_dir],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        if "already exists" in result.stderr:
            context.log.info("El repositorio ya existe, se omite el clone.")
        else:
            msg = "Error en git clone: " + result.stderr
            raise RuntimeError(msg)
    else:
        context.log.info("Repositorio clonado correctamente: " + result.stdout)

    return repo_dir


@asset(deps=["clone_repository"])
def configure_git(context: OpExecutionContext, clone_repository: str) -> str:
    """Configura el usuario y email globales de git."""
    configs = [
        ["git", "config", "--global", "user.email", "alu0101430720@ull.edu.es"],
        ["git", "config", "--global", "user.name", "Carlos Yanes"],
    ]

    for cmd in configs:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            msg = "Error en git config: " + result.stderr
            raise RuntimeError(msg)

    context.log.info("Configuración de git establecida correctamente.")
    return clone_repository


@asset(deps=["configure_git", "github_token"])
def pull_repository(context: OpExecutionContext, configure_git: str, github_token: str) -> str:
    """Realiza git pull sobre el repositorio."""
    repo_dir = configure_git
    repo_url = "https://" + github_token + "@github.com/alu0101430720/Pr2-Visualizacion.git"

    result = subprocess.run(
        ["git", "-C", repo_dir, "pull", repo_url],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        msg = "Error en git pull: " + result.stderr
        raise RuntimeError(msg)

    context.log.info("git pull completado: " + result.stdout)

    result = subprocess.run(
    ["git", "-C", repo_dir, "checkout", "-B", "practica3/data-checks"],
    capture_output=True, text=True
    )

    if result.returncode != 0:
        raise RuntimeError("Error en git checkout: " + result.stderr)
        
    context.log.info("Branch activo: practica3/data-checks")

    return repo_dir
