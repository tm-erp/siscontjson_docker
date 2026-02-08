from .base_view import (
    mostrar_tabla_base,
    procesar_tabla_individual_base,
    procesar_todas_tablas_base,
    render_module_ui,
)


from services.activos_fijos_client import TABLAS_ACTIVOS, obtener_datos_tabla


# 1. Adaptar las funciones base a General
async def mostrar_tabla(nombre_logico: str):
    await mostrar_tabla_base(nombre_logico, obtener_datos_tabla)


async def procesar_tabla_individual(nombre_logico: str):
    await procesar_tabla_individual_base(
        nombre_logico, obtener_datos_tabla, TABLAS_ACTIVOS
    )


async def procesar_todas_tablas():
    async def datos_export(nombre: str):
        return await obtener_datos_tabla(nombre, export=True)

    await procesar_todas_tablas_base(TABLAS_ACTIVOS, datos_export)


# 2. Reemplazar la función 'show' con el renderizador base
def show():
    render_module_ui(
        titulo="Activos Fijos",
        subtitulo='Consulta y genera los JSON de las tablas del modulo "Activos Fijos"',
        tablas_map=TABLAS_ACTIVOS,
        mostrar_func=mostrar_tabla,
        exportar_individual_func=procesar_tabla_individual,
        exportar_todas_func=procesar_todas_tablas,
    )
