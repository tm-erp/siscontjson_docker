from nicegui import app, ui

from config import MODULES


def create_sidebar(selected_module: str, on_module_select):
    """Crea la barra lateral con los módulos disponibles."""
    with ui.column().classes("w-64 bg-slate-50 border-r border-slate-200 h-full"):
        ui.label("Módulos").classes(
            "text-lg font-bold p-4 text-slate-700 border-b border-slate-200"
        )

        for module_name, icon_name in MODULES.items():
            is_selected = module_name == selected_module

            # Clases base y condicionales para cada botón del sidebar
            item_classes = (
                "w-full flex items-center gap-3 px-4 py-2.5 "
                "rounded-md cursor-pointer transition-all "
                "duration-150 ease-in-out"
            )
            if is_selected:
                item_classes += " bg-primary text-white font-semibold shadow-sm"
            else:
                item_classes += (
                    " text-slate-600 hover:bg-slate-200 hover:text-slate-800"
                )

            # Botón clickeable
            with ui.element("div").classes(item_classes).on(
                "click", lambda _, name=module_name: on_module_select(name)
            ) as btn:
                ui.icon(icon_name).classes("text-xl")
                ui.label(module_name).classes("text-sm")
