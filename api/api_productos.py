import json


from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from starlette.responses import StreamingResponse

from db import db_productos as productos
from db.db_connection import create_db_managerAlchemy
from db.db_manager import ConexionParams

import io
import csv

router = APIRouter()

#Funcion para listar el json de productos
@router.post(
    "/productos",
    summary="Lista los productos",
    description="Muestra listado de los productos",
    tags=["PRODUCTOS"],
)
async def get_productos_endpoint(params: ConexionParams):
    try:
        with create_db_managerAlchemy(params) as db:
            data = productos.get_productos(db)
            return JSONResponse(content=data)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SIVNOMPROD:" f" {str(e)}",
        )



#Funcion pata listar el json de grupo de producto
@router.post(
    "/grupo_de_productos",
    summary="Lista los grupos de productos",
    description="Muestra listado de los grupos de productos",
    tags=["PRODUCTOS"],
)
async def get_grupo_productos_endpoint(params: ConexionParams):
    try:
        with create_db_managerAlchemy(params) as db:
            data = productos.get_grupo_productos(db)
            return JSONResponse(content=data)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de Grupos de productos:" f" {str(e)}",
        )


#Funcion para listar el json de existencias
@router.post(
    "/existencias",
    summary="Lista las existencias de los productos",
    description="Muestra listado de las existencias de los productos",
    tags=["PRODUCTOS"],
)

async def get_existencias_endpoint(params: ConexionParams):
    try:
        with create_db_managerAlchemy(params) as db:
            data = productos.get_existencias(db)
            return JSONResponse(content=data)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de Existencias:"
                   f" {str(e)}",
        )


#Funcion para listar el csv de existencias
@router.post(
    "/existencias_csv",
    summary="Lista las existencias de los productos formato csv",
    description="Muestra listado de las existencias de los productos formato csv",
    tags=["PRODUCTOS"],
)

async def get_existencias_csv(params: ConexionParams):
    try:
        with create_db_managerAlchemy(params) as db:
            data = productos.get_existencias(db)
            # Si data es JSON string, parsear así:
            if isinstance(data, str):
                data = json.loads(data)

            if not data:
                raise HTTPException(status_code=404, detail="No hay datos para exportar")

            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=data[0].keys())

            # Filas fijas antes del header
            filas_fijas = [
                ['Bulk Edit Items', '', ''],
                ['Barcode','Item Code', 'Item Name', 'Item Group', 'Warehouse',
                 'Quantity', 'Valuation Rate', 'Amount',
                 'Use Serial No / Batch Fields'],
                ['barcode','item_code', 'item_name', 'item_group', 'warehouse',
                 'qty', 'valuation_rate', 'amount', 'use_serial_batch_fields'],
                ['El formato CSV es sensible a mayúsculas y minúsculas'],
                ['No edite los encabezados que están preestablecidos en la plantilla'],
                ['------']
            ]

            csv_writer = csv.writer(output)
            csv_writer.writerows(filas_fijas)

            writer.writerows(data)

            output.seek(0)

            return StreamingResponse(
                output,
                media_type="text/csv",
                headers={
                    "Content-Disposition": 'attachment; filename="existencias_siscont.csv"'
                },
            )

            return JSONResponse(content=data)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SIVEXISTENCIAMOVIMIENTO: {str(e)}",
        )

#Funcion pata listar el json de Lista de precios
@router.post(
    "/lista_de_precios",
    summary="Lista de precios",
    description="Muestra listas de precios de ventas",
    tags=["PRODUCTOS"],
)

async def get_lista_precios_endpoint(params: ConexionParams):
    try:
        with create_db_managerAlchemy(params) as db:
            data = productos.get_lista_precios(db)
            return JSONResponse(content=data)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de Grupos de productos:" f" {str(e)}",
        )

#Funcion pata listar el json de Precios de los items
@router.post(
    "/precios_productos_lista",
    summary="Lista de precios por productos",
    description="Muestra listas de precios de los productos",
    tags=["PRODUCTOS"],
)
async def get_precio_productos_endpoint(params: ConexionParams):
    try:
        with create_db_managerAlchemy(params) as db:
            data = productos.get_precio_productos(db)
            return JSONResponse(content=data)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de Grupos de productos:" f" {str(e)}",
        )