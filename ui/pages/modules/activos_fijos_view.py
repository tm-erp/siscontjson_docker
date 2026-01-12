import asyncio
import csv
import io
import json
from nicegui import ui
import pandas as pd
import numpy as np
from services.activos_fijos_client import TABLAS_ACTIVOS, obtener_datos_tabla, get_current_conexion_params
from db.db_manager import ConexionParams 

from utils.jsons_utils import save_json_file2
from utils.csv_utils import save_csv_file


# Muestra ventana modal con los datos de la tabla especificada
async def mostrar_tabla(nombre_logico: str, format: str):
    try:
        result = await obtener_datos_tabla(nombre_logico, format=format)        
        ui.notify(f"{nombre_logico} consultado correctamente.")

        if not result:
            ui.notify("No se encontraron datos para mostrar.", type="info")
            return

        if format == "CSV":
            df =  pd.read_csv(io.StringIO(result["data"])) 
            rows = df.replace({np.nan: None}).to_dict('records')
            columns = [{'name': col, 'label': col, 'field': col} for col in df.columns]
        else:
            data = result["data"]
            rows = data
            columns = [{"name": key, "label": key, "field": key, "align": "left"}
                   for key in data[0].keys()]

        # Configuración de paginación dinámica        
        data_length = len(rows)
        pagination = {
            "rows_per_page": min(10, data_length)
        } if data_length > 10 else {"rows_per_page": data_length}

        # Determinar ancho según cantidad de columnas
        column_count = len(columns)
        card_classes = "w-full h-full max-h-screen "
        if column_count > 8:
            card_classes += "min-w-[90vw] max-w-none"
        else:
            card_classes += "max-w-screen-xl"

        with ui.dialog() as dialog:
            with ui.card().classes(card_classes):
                ui.label(f"Datos de {nombre_logico}").classes(
                    "text-lg font-bold")
                
                ui.table(
                    columns=columns,
                    rows=rows,
                    pagination=pagination
                ).classes("w-full h-full overflow-x-auto")

                ui.button("Cerrar", on_click=dialog.close)
        dialog.open()

    except Exception as e:
        ui.notify(f"Error al consultar {nombre_logico}: {e}", type="negative")
        print(f"Error al consultar {nombre_logico}: {e}")

# funcion que obtiene los datos de la tabla especificada
async def procesar_tabla_individual(nombre_logico: str, format: str):
    try:
        ui.notify(f"Preparando exportación de {nombre_logico}...")
        data = await obtener_datos_tabla(nombre_logico, format) 
        file_name = f"{nombre_logico.replace(' ', '_').lower()}" 
        
        if not data:
                ui.notify(f"No hay datos para exportar en {nombre_logico}.",
                        type="warning")
                return None
        
        if format=="JSON":            
            file_name = file_name + ".json"
            save_json_file2(data, file_name)            
        else:           
            file_name = file_name + ".csv"
            save_csv_file(data["data"], file_name)

        ui.notify(
                f"Datos de {nombre_logico} exportados a '{file_name}' "
                f"correctamente.",
                type="positive")

    except Exception as e:
        ui.notify(f"Error desde nomina_view al exportar {nombre_logico}: {e}",
                  type="negative")
        print(f"Error al exportar {nombre_logico}: {e}")


async def procesar_todas_tablas():
    with ui.dialog() as dialog, ui.card():
        ui.label("Exportando todas las tablas...").classes(
            "text-lg font-semibold")
        ui.spinner(size='lg', color='green')

    dialog.open()

    try:
        for nombre_logico in TABLAS_ACTIVOS.keys():
            await procesar_tabla_individual(
                nombre_logico)  # suponiendo que exportar_json es async
            await asyncio.sleep(0.1)  # pequeña pausa para permitir feedback UI

        dialog.close()
        ui.notify("✅ Todas las tablas fueron exportadas exitosamente",
                  type="positive")
    except Exception as e:
        dialog.close()
        ui.notify(f"❌ Error al exportar: {str(e)}", type="negative")


def descargar_activos_fijos_csv():
    conexion_params = get_current_conexion_params()
    payload = conexion_params.model_dump()
    payload_json = json.dumps(payload)  # convierte a string JSON seguro
    
    js = f"""fetch('/api/activos_fijos/apertura_csv', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({payload_json})
    }}).then(response => {{
        if (!response.ok) throw new Error('Error en descarga');
        return response.blob();
    }}).then(blob => {{
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'activos_fijos.csv';
        document.body.appendChild(a);
        a.click();
        a.remove();
        window.URL.revokeObjectURL(url);
    }}).catch(e => {{
        alert('Error al descargar CSV: ' + e.message);
    }});
    """
    ui.run_javascript(js)


def show():
    ui.label("Activos Fijos").classes("text-2xl font-bold mb-1")
    ui.label("Consulta y genera los JSON de las tablas de Activos Fijos").classes(
        "text-sm mb-4")
    ui.separator()

    # Botón para exportar todas las tablas de Cuentas
    ui.button(
        "Exportar todas las tablas a JSON",
        on_click=lambda: procesar_todas_tablas()
        # ¡Aquí pasas TABLAS_NOMINA!
    ).props("color=blue size=md icon=cloud_download").classes("mt-4 mb-6")

    # Display table names and buttons
    with ui.column().classes("mt-6 gap-2 w-full"):  # Use w-full for full width
        for nombre_logico in TABLAS_ACTIVOS.keys():
            with ui.row().classes(
                    "items-center justify-between w-full"):  # Row for table
                # name and buttons
                ui.label(nombre_logico).classes(
                    "text-md font-semibold")  # Display the table name

                with ui.row():  # Group the buttons together
                    #Filtro los modulos que tienen visualizacion
                    if nombre_logico != "Categoria de Activos Fijos":
                        #Filtro aquellos con formato de salida csv y json
                        if nombre_logico != "Activos Fijos":
                            ui.button(
                                    "Visualizar datos",
                                    on_click=lambda n=nombre_logico: mostrar_tabla(n, format="JSON")
                                ).props(
                                    "color=primary outline size=sm")  # Added size for
                                # smaller buttons      
                            
                    if nombre_logico == "Activos Fijos":
                        ui.button(
                            "Exportar a CSV",
                            on_click= descargar_activos_fijos_csv
                        ).props("color=orange outline size=sm icon=download").classes("ml-2")
                    else:
                        ui.button(
                        "Exportar a JSON",
                        on_click=lambda
                            n=nombre_logico: procesar_tabla_individual(n, format="JSON")
                            ).props("color=green outline size=sm icon=cloud_download")

                    
