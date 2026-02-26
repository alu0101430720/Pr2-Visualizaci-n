
from dagster import Definitions, load_assets_from_modules
import code_git, code_ejercicio1, code_ejercicio2, code_ejercicio3

defs = Definitions(
    assets=load_assets_from_modules([
        code_git,
        code_ejercicio1,
        code_ejercicio2,
        code_ejercicio3,
    ])
)
