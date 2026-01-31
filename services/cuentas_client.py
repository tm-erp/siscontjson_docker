from typing import Any

import httpx

from config import get_module_api_url

# Importar la función de utilidad desde base_client
from .base_client import get_current_conexion_params

# Lista de endpoints disponibles para iterar si quieres hacer algo dinámico
TABLAS_CUENTAS = {
    "Cuentas": "cuentas",
    # "Relaciones": "relaciones-trabajadores",
    "Elementos de gastos": "elementos_gastos",
}


async def obtener_datos_tabla(nombre_tabla: str, modulo: str | None = None) -> Any:
    modulo = modulo or "cuentas"
    endpoint = TABLAS_CUENTAS[nombre_tabla]
    base_url = get_module_api_url(modulo)
    url = f"{base_url}/{endpoint}"

    conexion_params = get_current_conexion_params()
    payload = conexion_params.model_dump()

    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=payload)
        response.raise_for_status()
        return response.json()
