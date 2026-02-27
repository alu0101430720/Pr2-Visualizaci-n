"""
assets/git_ops.py — Assets de inicialización del repositorio Git.

Orden de ejecución:
  clone_repository → configure_git → pull_repository

IMPORTANTE: el token de GitHub se lee directamente desde la variable de entorno
en cada función que lo necesita — nunca se pasa como output de un asset para
evitar que Dagster lo persista en disco y Github Push Protection lo bloquee.
"""
import os
import subprocess

from dagster import asset, OpExecutionContext

from config import GIT_BRANCH, GIT_EMAIL, GIT_NAME, REPO_DIR, repo_url


def get_github_token() -> str:
    """Lee el token de GitHub desde la variable de entorno. No es un asset."""
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise ValueError("No se encontró la variable de entorno 'GITHUB_TOKEN'.")
    return token


@asset
def clone_repository(context: OpExecutionContext) -> str:
    """Clona el repositorio si no existe todavía."""
    url = repo_url(get_github_token())
    result = subprocess.run(
        ["git", "clone", url, REPO_DIR],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        if "already exists" in result.stderr:
            context.log.info("Repositorio ya existe, se omite el clone.")
        else:
            raise RuntimeError(f"git clone: {result.stderr.strip()}")
    else:
        context.log.info(f"Repositorio clonado: {result.stdout.strip()}")
    return REPO_DIR


@asset
def configure_git(context: OpExecutionContext, clone_repository: str) -> str:
    """Configura usuario y email globales de git."""
    for cmd in [
        ["git", "config", "--global", "user.email", GIT_EMAIL],
        ["git", "config", "--global", "user.name", GIT_NAME],
    ]:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"git config: {result.stderr.strip()}")
    context.log.info("Configuración git OK.")
    return clone_repository


@asset
def pull_repository(context: OpExecutionContext, configure_git: str) -> str:
    """
    Hace pull y activa (o crea) la rama de trabajo.
    Devuelve el path del repo_dir para que otros assets lo usen como dependencia.
    """
    repo_dir = configure_git
    url = repo_url(get_github_token())

    result = subprocess.run(
        ["git", "-C", repo_dir, "pull", url],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"git pull (rc={result.returncode}):\n"
            f"STDOUT: {result.stdout.strip()}\nSTDERR: {result.stderr.strip()}"
        )
    context.log.info(f"git pull OK: {result.stdout.strip()}")

    result = subprocess.run(
        ["git", "-C", repo_dir, "checkout", "-B", GIT_BRANCH],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        result = subprocess.run(
            ["git", "-C", repo_dir, "checkout", GIT_BRANCH],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"git checkout (rc={result.returncode}):\n"
                f"STDOUT: {result.stdout.strip()}\nSTDERR: {result.stderr.strip()}"
            )
    context.log.info(f"Branch activo: {GIT_BRANCH}")
    return repo_dir
