from .git_ops import clone_repository, configure_git, github_token, pull_repository
from .renta import guardar_renta_limpia, ingestar_renta, limpiar_renta
from .codislas import (
    guardar_codislas_limpia,
    guardar_renta_integrada,
    ingestar_codislas,
    integrar_renta_codislas,
    limpiar_codislas,
)
from .nivelestudios import (
    commit_ejercicio3,
    enriquecer_nivelestudios,
    generar_graficos_ejercicio3,
    guardar_nivelestudios_limpio,
    ingestar_nivelestudios,
    limpiar_nivelestudios,
)