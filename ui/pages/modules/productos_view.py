# Importar el módulo base
from .base_view import (
    mostrar_tabla_base,
    procesar_tabla_individual_base,
    procesar_todas_tablas_base,
    descargar_csv_base,
    render_module_ui,
)

# Importar solo lo necesario del cliente específico
from services.productos_client import TABLAS_PRODUCTOS, obtener_datos_tabla


# 1. Adaptar las funciones base a General
async def mostrar_tabla(nombre_logico: str):
    await mostrar_tabla_base(nombre_logico, obtener_datos_tabla)


async def procesar_tabla_individual(nombre_logico: str):
    await procesar_tabla_individual_base(
        nombre_logico, obtener_datos_tabla, TABLAS_PRODUCTOS
    )

async def procesar_todas_tablas():
    await procesar_todas_tablas_base(TABLAS_PRODUCTOS, procesar_tabla_individual)

async def descargar_csv(nombre_logico: str):
    await descargar_csv_base(nombre_logico, obtener_datos_tabla, TABLAS_PRODUCTOS)

# 2. Reemplazar la función 'show' con el renderizador base
def show():
    render_module_ui(
        titulo="Productos",
        subtitulo='Consulta y genera los JSON de las tablas del modulo "Productos"',
        tablas_map=TABLAS_PRODUCTOS,
        mostrar_func=mostrar_tabla,
        exportar_individual_func=procesar_tabla_individual,
        exportar_todas_func=procesar_todas_tablas,
        descargar_csv_func=descargar_csv,
    )