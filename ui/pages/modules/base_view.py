# ui/pages/modules/base_view.py

import asyncio
import json
import csv
import io

from collections import OrderedDict
from nicegui.functions import javascript
from nicegui import ui
from utils.download_manager import save_to_download_cache

from collections import OrderedDict
from services.cp_client import get_current_conexion_params


# --- Funciones de Utilidad ---


def _generate_json_string(data_list: list, doctype: str) -> str:
    """
    Genera el contenido JSON como un string con la estructura de exportación
    Reemplaza la función idéntica en ambas vistas.
    """
    content = OrderedDict()
    content["doctype"] = doctype
    content["data"] = data_list
    return json.dumps(content, indent=4, ensure_ascii=False)


# --- Lógica de Visualización ---


async def mostrar_tabla_base(nombre_logico: str, obtener_datos_func):
    """
    Función base para mostrar datos de una tabla en un diálogo.
    Acepta 'obtener_datos_func' como una función asíncrona para obtener datos específicos del módulo.
    """
    try:
        # Obtener datos usando la función específica del cliente
        result = await obtener_datos_func(nombre_logico)
        records = result["data"]
        doctype_name = result["doctype"]

        ui.notify(f"{nombre_logico} consultado correctamente.")

        if not records or not all(isinstance(r, dict) for r in records):
            ui.notify(f"Datos inválidos para {nombre_logico}", type="negative")
            return

        # Lógica de NiceGUI para mostrar tabla (idéntica en ambas vistas)
        columns = [
            {"name": key, "label": key, "field": key, "align": "left"}
            for key in records[0].keys()
        ]

        data_length = len(records)
        pagination = (
            {"rows_per_page": min(10, data_length)}
            if data_length > 10
            else {"rows_per_page": data_length}
        )

        column_count = len(columns)
        card_classes = "w-full h-full max-h-screen "
        card_classes += (
            "min-w-[90vw] max-w-none" if column_count > 8 else "max-w-screen-xl"
        )

        with ui.dialog() as dialog:
            with ui.card().classes(card_classes):
                ui.label(f"Datos de {nombre_logico}").classes("text-lg font-bold")

                async def show_json_dump():
                    json_str = _generate_json_string(records, doctype_name)

                    with ui.dialog() as json_dialog:
                        with ui.card().classes("w-[80vw] max-w-none"):
                            ui.label(f"JSON Raw de {nombre_logico}").classes(
                                "text-lg font-bold"
                            )
                            ui.markdown(f"```json\n{json_str}\n```").classes(
                                "overflow-auto max-h-[70vh] bg-gray-100 p-4 rounded"
                            )
                            ui.button("Cerrar", on_click=json_dialog.close)
                    json_dialog.open()

                ui.button("Ver JSON Raw", on_click=show_json_dump).props(
                    "color=info size=sm outline icon=code"
                )

                ui.table(columns=columns, rows=records, pagination=pagination).classes(
                    "w-full h-full overflow-x-auto"
                )

                ui.button("Cerrar", on_click=dialog.close)
        dialog.open()

    except Exception as e:
        ui.notify(f"Error al consultar {nombre_logico}: {e}", type="negative")
        print(f"Error al consultar {nombre_logico}: {e}")


# --- Lógica de Exportación ---


async def procesar_tabla_individual_base(
    nombre_logico: str, obtener_datos_func, tablas_map
):
    """
    Función base para exportar datos individuales.
    Acepta la función de obtener datos y el diccionario de mapeo de tablas (TABLAS_NOMINA/GENERAL).
    """
    try:
        ui.notify(f"Preparando descarga de {nombre_logico}...")
        result = await obtener_datos_func(nombre_logico)

        if not result:
            ui.notify(f"No hay datos para exportar en {nombre_logico}.", type="warning")
            return

        # 1. Obtener nombre real del archivo del mapa específico
        sqlserver_name = tablas_map.get(nombre_logico, "error_nombre_tabla")
        if sqlserver_name == "error_nombre_tabla":
            ui.notify(
                f"Error: Mapeo no encontrado para '{nombre_logico}'.", type="negative"
            )
            return

        # 2. Preparar los datos (Bytes)
        json_str = _generate_json_string(result["data"], result["doctype"])
        json_bytes = json_str.encode("utf-8")
        file_name = f"{sqlserver_name}.json"

        download_url = save_to_download_cache(json_bytes, file_name)

        ui.run_javascript(f'window.location.href = "{download_url}"')

        ui.notify(f"Descargando {file_name}...", type="positive")

    except Exception as e:
        ui.notify(f"Error al exportar {nombre_logico}: {e}", type="negative")
        print(f"Error al exportar {nombre_logico}: {e}")


async def procesar_todas_tablas_base(tablas_map, procesar_individual_func):
    """
    Función base para exportar todas las tablas.
    Acepta el mapa de tablas (TABLAS_NOMINA/GENERAL) y la función de procesamiento individual.
    """
    with ui.dialog() as dialog, ui.card():
        ui.label("Exportando todas las tablas...").classes("text-lg font-semibold")
        ui.spinner(size="lg", color="green")

    dialog.open()

    try:
        # Itera sobre el mapa de tablas específico
        for nombre_logico in tablas_map.keys():
            await procesar_individual_func(nombre_logico)
            await asyncio.sleep(0.1)

        dialog.close()
        ui.notify("✅ Todas las tablas fueron exportadas exitosamente", type="positive")
    except Exception as e:
        dialog.close()
        ui.notify(f"❌ Error al exportar: {str(e)}", type="negative")

async def descargar_csv_base(nombre_logico: str, obtener_datos_func, tablas_map):
    """
    Función base para exportar datos individuales a CSV.
    Acepta la función de obtener datos y el diccionario de mapeo de tablas (TABLAS_NOMINA/GENERAL).
    """
    try:
        ui.notify(f"Preparando descarga de {nombre_logico}...")
        result = await obtener_datos_func(nombre_logico)

        if not result:
            ui.notify(f"No hay datos para exportar en {nombre_logico}.", type="warning")
            return

        # 1. Obtener nombre real del archivo del mapa específico
        sqlserver_name = tablas_map.get(nombre_logico, "error_nombre_tabla")
        if sqlserver_name == "error_nombre_tabla":
            ui.notify(
                f"Error: Mapeo no encontrado para '{nombre_logico}'.", type="negative"
            )
            return

        # 2. Preparar los datos para el archivo CSV
        data = result["data"]

        if not data:
            ui.notify(f"No hay datos para exportar en {nombre_logico}.", type="warning")
            return

        # Obtener las cabeceras (suponiendo que los datos son una lista de diccionarios)
        headers = data[0].keys()

        # Crear el archivo CSV en memoria
        csv_bytes = io.StringIO()
        writer = csv.DictWriter(csv_bytes, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)

        # Obtener el contenido CSV como bytes
        csv_bytes.seek(0)  # Volver al principio del archivo
        file_name = f"{sqlserver_name}.csv"

        # Guardar en el caché de descargas (deberás implementar esta función)
        csv_data = csv_bytes.getvalue().encode('utf-8')

        download_url = save_to_download_cache(csv_data, file_name)

        ui.run_javascript(f'window.location.href = "{download_url}"')

        ui.notify(f"Descargando {file_name}...", type="positive")

    except Exception as e:
        ui.notify(f"Error al exportar {nombre_logico}: {e}", type="negative")
        print(f"Error al exportar {nombre_logico}: {e}")


# Otros elementos necesarios (como ui y save_to_download_cache) deben ser implementados según tu contexto.

# --- Generación de Interfaz de Usuario ---

def render_module_ui(
    titulo: str,
    subtitulo: str,
    tablas_map,
    mostrar_func,
    exportar_individual_func,
    exportar_todas_func,
    descargar_csv_func = None,  # Indica que este boton es opcional,
):
    """
    Genera la interfaz de usuario repetitiva (títulos, botón de exportar todo, lista de tablas).
    """
    ui.label(titulo).classes("text-2xl font-bold mb-1")
    ui.label(subtitulo).classes("text-sm mb-4")
    ui.separator()

    # Botón para exportar todas las tablas
    ui.button(
        "Exportar todas las tablas a JSON",
        on_click=exportar_todas_func,
    ).props(
        "color=blue size=md icon=cloud_download"
    ).classes("mt-4 mb-6")

    # Display table names and buttons
    with ui.column().classes("mt-6 gap-2 w-full"):
        for nombre_logico in tablas_map.keys():
            with ui.row().classes("items-center justify-between w-full"):
                ui.label(nombre_logico).classes("text-md font-semibold")

                with ui.row():
                    ui.button(
                        "Visualizar datos",
                        # Aquí usamos la función de mostrar del módulo específico
                        on_click=lambda n=nombre_logico: mostrar_func(n),
                    ).props("color=primary outline size=sm")

                    ui.button(
                        "Exportar a JSON",
                        # Aquí usamos la función de exportar individual del módulo específico
                        on_click=lambda n=nombre_logico: exportar_individual_func(n),
                    ).props("color=green outline size=sm icon=cloud_download")

                    # Lista de nombres lógicos que deben mostrar el botón
                    nombres_con_csv = ["Facturas de Compra", "Facturas de Venta", "Existencias"]

                    if nombre_logico in nombres_con_csv:
                        ui.button(
                            "Exportar a CSV",
                            on_click=lambda n=nombre_logico: descargar_csv_func(n),
                        ).props("color=orange outline size=sm icon=download").classes("ml-2")