from typing import Any

import httpx

from config import get_module_api_url

# Importar la función de utilidad desde base_client
from .base_client import get_current_conexion_params

# Lista de endpoints disponibles para iterar si quieres hacer algo dinámico
TABLAS_ACTIVOS = {
    "Activos Fijos": "apertura",
    "Categoria de Activos Fijos": "categorias",
    "Libro Finanzas": "libro_finanzas",
    "Location": "location",
}


async def obtener_datos_tabla(
    nombre_tabla: str, modulo: str | None = None, export: bool = False
) -> Any:
    """
    Función de fachada para CP que llama a la función base.
    """
    modulo = modulo or "activos_fijos"
    endpoint = TABLAS_ACTIVOS[nombre_tabla]
    base_url = get_module_api_url(modulo)
    url = f"{base_url}/{endpoint}"

    conexion_params = get_current_conexion_params()
    payload = conexion_params.model_dump()

    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=payload)
        response.raise_for_status()
        return response.json()
