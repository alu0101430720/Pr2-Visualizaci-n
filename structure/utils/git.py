"""
utils/git.py — Helpers para operaciones Git reutilizables.
"""
import subprocess
from dagster import OpExecutionContext


def _run(cmd: list[str], ctx: OpExecutionContext | None = None, error_prefix: str = "") -> subprocess.CompletedProcess:
    """Ejecuta un comando de shell, lanza RuntimeError si falla."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        prefix = error_prefix or " ".join(cmd[:3])
        raise RuntimeError(f"{prefix}: {result.stderr.strip()}")
    if ctx:
        ctx.log.info((result.stdout or result.stderr).strip())
    return result


def git_add(repo_dir: str, file_path: str, ctx: OpExecutionContext | None = None) -> None:
    """Añade un fichero al índice git."""
    import os
    rel = os.path.relpath(file_path, repo_dir)
    _run(["git", "-C", repo_dir, "add", rel], ctx, f"git add {rel}")
    if ctx:
        ctx.log.info(f"git add OK: {rel}")


def git_commit(repo_dir: str, message: str, ctx: OpExecutionContext | None = None) -> bool:
    """
    Hace commit. Devuelve True si hubo cambios, False si no había nada.
    """
    result = subprocess.run(
        ["git", "-C", repo_dir, "commit", "-m", message],
        capture_output=True, text=True,
    )
    combined = result.stdout + result.stderr
    if result.returncode != 0:
        if "nothing to commit" in combined:
            if ctx:
                ctx.log.info("Nada nuevo que commitear.")
            return False
        raise RuntimeError(f"git commit: {result.stderr.strip()}")
    if ctx:
        ctx.log.info(f"Commit OK: {result.stdout.strip()}")
    return True


def git_push(repo_dir: str, remote_url: str, branch: str, ctx: OpExecutionContext | None = None) -> None:
    """Hace push al remote indicado."""
    _run(
        ["git", "-C", repo_dir, "push", remote_url, f"HEAD:{branch}"],
        ctx, "git push",
    )


def commit_and_push(
    repo_dir: str,
    remote_url: str,
    branch: str,
    files: list[str],
    message: str,
    ctx: OpExecutionContext | None = None,
) -> None:
    """
    Añade *files*, hace commit con *message* y push a *branch*.
    Uso típico desde un asset de commit.
    """
    for f in files:
        git_add(repo_dir, f, ctx)
    committed = git_commit(repo_dir, message, ctx)
    if committed:
        git_push(repo_dir, remote_url, branch, ctx)
