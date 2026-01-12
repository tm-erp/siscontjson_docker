# ui/pages/modules/general_view.py
import asyncio
from nicegui import ui

# Importar el módulo base
from .base_view import (
    mostrar_tabla_base,
    procesar_tabla_individual_base,
    procesar_todas_tablas_base,
    descargar_csv_base,
    render_module_ui,
)

# Importar solo lo necesario del cliente específico
from services.general_client import TABLAS_GENERAL, obtener_datos_tabla


# 1. Adaptar las funciones base a General
async def mostrar_tabla(nombre_logico: str):
    await mostrar_tabla_base(nombre_logico, obtener_datos_tabla)


async def procesar_tabla_individual(nombre_logico: str):
    await procesar_tabla_individual_base(
        nombre_logico, obtener_datos_tabla, TABLAS_GENERAL
    )

async def procesar_todas_tablas():
    await procesar_todas_tablas_base(TABLAS_GENERAL, procesar_tabla_individual)

'''async def descargar_factura_compra_csv():
    await descargar_csv_base()
'''

async def descargar_csv(nombre_logico: str):
    await descargar_csv_base(nombre_logico, obtener_datos_tabla, TABLAS_GENERAL)

# 2. Reemplazar la función 'show' con el renderizador base
def show():
    render_module_ui(
        titulo="General",
        subtitulo='Consulta y genera los JSON de las tablas del modulo "General"',
        tablas_map=TABLAS_GENERAL,
        mostrar_func=mostrar_tabla,
        exportar_individual_func=procesar_tabla_individual,
        exportar_todas_func=procesar_todas_tablas,
        descargar_csv_func=descargar_csv,
    )
