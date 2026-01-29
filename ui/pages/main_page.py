# ui/pages/main_page.py
from nicegui import app, ui

from config import DEFAULT_MODULE
from ui.components import header, sidebar
from ui.pages.modules import (
    cobros_pagos_view,
    contabilidad_general_view,
    general_view,
    inventarios_view,
    nomina_view,
    recursos_humanos_view,
    inicio_view,
    costo_view,
    almacen_view,
    productos_view,
    activos_fijos_view,
    ventas_view,
    compras_view,
    cuentas_view, 
    activos_fijos_view
)

# Diccionario que mapea los nombres de módulos a sus funciones .show()
MODULES_VIEW = {
    "Inicio": inicio_view.show,
    "General": general_view.show,
    "Contabilidad General": contabilidad_general_view.show,
    "Nómina": nomina_view.show,
    "Recursos Humanos": recursos_humanos_view.show,
    "Cobros y Pagos": cobros_pagos_view.show,
    "Inventarios": inventarios_view.show,
    "Costo": costo_view.show,
    "Almacen": almacen_view.show,
    "Productos": productos_view.show,
    "Cuentas": cuentas_view.show,
    "Activos Fijos": activos_fijos_view.show,
    "Ventas": ventas_view.show,
    "Compras": compras_view.show,
}


# Definimos la función show_module_content como "refrescable"
# Esto significa que podemos llamarla con .refresh() para que NiceGUI la redibuje
@ui.refreshable
def show_module_content(module_name: str):
    """
    Renderiza el contenido del módulo seleccionado.
    Usa un diccionario para simplificar el manejo de vistas.
    """

    module_func = MODULES_VIEW.get(module_name)
    if module_func:
        module_func()
    else:
        ui.label(f"Módulo '{module_name}' no encontrado.").classes(
            "text-red-500 text-lg"
        )


def show():
    """
    Función principal que construye la interfaz de usuario de la página principal.
    """
    # Obtiene el módulo actual del almacenamiento de usuario'
    current_module = app.storage.user.get("current_view", DEFAULT_MODULE)

    # Crea el encabezado de la aplicación
    header.create_header(
        server_ip=app.storage.user.get("server_ip_display", ""), on_logout=handle_logout
    )

    # Área principal de contenido (barra lateral + contenido del módulo)
    with ui.row().classes("w-full h-[calc(100vh-4rem)]"):
        # Crea la barra lateral
        sidebar.create_sidebar(
            selected_module=current_module, on_module_select=change_module
        )

        # Contenedor para el contenido del módulo
        with ui.column().classes("flex-grow p-6 bg-slate-100 overflow-auto"):
            # Llama la función refrescable para mostrar el contenido del módulo
            show_module_content(current_module)


def handle_logout():
    """
    Maneja el cierre de sesión del usuario.
    """
    # Actualiza el estado de inicio de sesión y limpia el IP del servidor
    app.storage.user.update(
        {
            "connected": False,
            "server_ip_display": "",
            "current_view": "General",
            # Restablece la vista actual al cerrar sesión
        }
    )
    # Redirige al usuario a la página de inicio de sesión
    ui.navigate.to("/")


def change_module(module_name: str):
    """
    Cambia el módulo activo y actualiza la interfaz de usuario.
    """
    # print(f"DEBUG: Intentando cambiar a módulo: {module_name}")
    # Actualiza el módulo actual en el almacenamiento de usuario
    app.storage.user["current_view"] = module_name

    # Llama al método .refresh() de la función refrescable para redibujar solo el contenido del módulo
    show_module_content.refresh(
        module_name
    )  # Pasamos el nuevo module_name a la función refrescable
    sidebar.update_active_module(module_name)
    # print(f"DEBUG: Módulo cambiado a: {app.storage.user.get('current_view')}")
