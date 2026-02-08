# ui/pages/modules/general_view.py
import asyncio
from nicegui import ui

# Importar el módulo base
from .base_view import (
    mostrar_tabla_base,
    procesar_tabla_individual_base,
    procesar_todas_tablas_base,
    render_module_ui,
)

# Importar solo lo necesario del cliente específico
from services.general_client import TABLAS_GENERAL, obtener_datos_tabla

# 1. Adaptar las funciones base a General
# async def mostrar_tabla(nombre_logico: str):
#     await mostrar_tabla_base(nombre_logico, obtener_datos_tabla)


async def mostrar_tabla(nombre_logico: str):
    async def datos_view(nombre):
        return await obtener_datos_tabla(nombre, export=False)

    await mostrar_tabla_base(nombre_logico, datos_view)


# async def procesar_tabla_individual(nombre_logico: str):
#     await procesar_tabla_individual_base(
#         nombre_logico, obtener_datos_tabla, TABLAS_GENERAL
#     )


async def procesar_tabla_individual(nombre_logico: str):
    async def datos_export(nombre: str):
        return await obtener_datos_tabla(nombre, export=True)

    await procesar_tabla_individual_base(nombre_logico, datos_export, TABLAS_GENERAL)


async def procesar_todas_tablas():
    # Pasa obtener_datos_tabla directamente para que la función base maneje la exportación
    async def datos_export(nombre: str):
        return await obtener_datos_tabla(nombre, export=True)

    await procesar_todas_tablas_base(TABLAS_GENERAL, datos_export, "General")


# 2. Reemplazar la función 'show' con el renderizador base
def show():
    render_module_ui(
        titulo="General",
        subtitulo='Consulta y genera los JSON de las tablas del modulo "General"',
        tablas_map=TABLAS_GENERAL,
        mostrar_func=mostrar_tabla,
        exportar_individual_func=procesar_tabla_individual,
        exportar_todas_func=procesar_todas_tablas,
    )
