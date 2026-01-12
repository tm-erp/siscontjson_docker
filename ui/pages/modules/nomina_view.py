# ui/pages/modules/nomina_view.py
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
from services.nomina_client import TABLAS_NOMINA, obtener_datos_tabla


# 1. Adaptar las funciones base a Nómina
# La función 'mostrar_tabla' llama a la base con la función específica de Nómina.
async def mostrar_tabla(nombre_logico: str):
    await mostrar_tabla_base(nombre_logico, obtener_datos_tabla)


# La función 'procesar_tabla_individual' llama a la base con la función de datos y el mapa de Nómina.
async def procesar_tabla_individual(nombre_logico: str):
    # Se usa el nombre real de la función original en el stack trace
    await procesar_tabla_individual_base(
        nombre_logico, obtener_datos_tabla, TABLAS_NOMINA
    )


# Envolver la función base de 'procesar_todas_tablas'
async def procesar_todas_tablas():
    # Nota: procesar_todas_tablas_base necesita la *función* individual adaptada
    await procesar_todas_tablas_base(TABLAS_NOMINA, procesar_tabla_individual)

'''async def descargar_factura_compra_csv():
    await descargar_factura_compra_csv_base()
'''

async def descargar_csv(nombre_logico: str):
    await descargar_csv_base(nombre_logico, obtener_datos_tabla, TABLAS_NOMINA)


# 2. Reemplazar la función 'show' con el renderizador base
def show():
    render_module_ui(
        titulo="Nómina",
        subtitulo="Consulta y genera los JSON de las tablas de Nomina",
        tablas_map=TABLAS_NOMINA,
        mostrar_func=mostrar_tabla,
        exportar_individual_func=procesar_tabla_individual,
        exportar_todas_func=procesar_todas_tablas,
        descargar_csv_func=descargar_csv,
        )
