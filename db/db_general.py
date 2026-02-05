import datetime
import logging
from typing import Dict, List

from utils.jsons_utils import (
    export_table_to_json,
    export_table_to_json_paginated,
    fetch_table_data,
    save_json_file,
)


# Para obtener las unidades de medida y poniendo alias con el nombre
# del campo en el doctype
def get_unidad_medida(db, export=False):
    doctype_name = "UOM"
    sqlserver_name = "SMGNOMENCLADORUNIDADMEDIDA"
    module_name = "Setup"

    field_mapping = [
        # Campos del doctype principal (trabajador)
        # (alias, (sql_field, doctype_field_type))
        ("uom_name", ("UMedDescrip", "string"))
    ]
    # Construimos la cláusula SELECT
    select_clauses = [
        f"{sql_field} as {alias}" for alias, (sql_field, _) in field_mapping
    ]

    query = f"""
       SELECT
           {', '.join(select_clauses)}
        FROM SMGNOMENCLADORUNIDADMEDIDA
        WHERE UMedactiva = 1
    """

    return export_table_to_json(
        db=db,
        doctype_name=doctype_name,
        sqlserver_name=sqlserver_name,
        module_name=module_name,
        field_mapping=field_mapping,
        table_query=query,
        save=export,
    )


# Obtener los clientes y ponerle el alias con el nombre del campo en el doctype
def get_clientes(db, export=False):
    doctype_name = "Customer"
    sqlserver_name = "SMGCLIENTEPROVEEDOR"
    module_name = "Selling"

    # Mapeo de campos: alias -> (campo SQL, tipo)
    field_mapping = [
        ("reup_code", (None, "string")),
        ("customer_name", ("CP.CliDescripcion", "string")),
        ("territory", ("P.PaisDescripcion", "string")),
        ("default_currency", (None, "string")),
        ("customer_type", (None, "string")),
        ("customer_group", ("TE.TipifiDescripcion", "string")),
        ("nit_code", (None, "string")),
        ("identity_number", (None, "string")),
    ]

    # Construcción de SELECT
    select_clauses = []
    for alias, (sql_field, _) in field_mapping:
        if alias == "reup_code":
            clause = f"""
                CASE
                    WHEN COALESCE(TRIM(UPPER(CP.CliTcpMiPyme)), '') <> 'T' 
                        THEN CAST(CP.CliCodigo AS NVARCHAR(100))
                    ELSE NULL
                END AS {alias}
            """

        elif alias == "customer_type":
            clause = f"""
                CASE
                    WHEN CP.CliTcpMiPyme = 'T' THEN 'Individual'
                    ELSE 'Company'
                END AS {alias}
            """

        elif alias == "default_currency":
            clause = f"""
                CASE
                    WHEN P.PaisDescripcion = 'Cuba' THEN 'CUP'
                    ELSE 'USD'
                END AS {alias}
            """

        elif alias == "nit_code":
            clause = f"""
                CASE
                    WHEN CP.CliTcpMiPyme = 'T' AND COALESCE(TRIM(CP.CliNit), '') = '' 
                        THEN CAST(CP.CliCodigo AS NVARCHAR(100))
                    WHEN CP.CliTcpMiPyme = 'T' 
                        THEN CAST(CP.CliNit AS NVARCHAR(100))
                    ELSE CAST(CP.CliNit AS NVARCHAR(100))
                END AS {alias}
            """

        elif alias == "identity_number":
            clause = f"""
                CASE
                    WHEN CP.CliTcpMiPyme = 'T' AND COALESCE(TRIM(CP.CliNit), '') = '' 
                        THEN CAST(CP.CliCodigo AS NVARCHAR(100))
                    WHEN CP.CliTcpMiPyme = 'T' 
                        THEN CAST(CP.CliNit AS NVARCHAR(100))
                    ELSE NULL
                END AS {alias}
            """

        else:
            clause = f"{sql_field} AS {alias}"

        select_clauses.append(clause)

    # 🔧 Query corregido
    query = f"""
    SELECT
        {', '.join(select_clauses)}
    FROM SMGCLIENTEPROVEEDOR AS CP
    LEFT JOIN SCOCLIENTEDBANCARIOS AS DB
        ON CAST(CP.CliCuentaMN AS NVARCHAR(100)) = CAST(DB.CLIDBCUENTA AS NVARCHAR(100))
    LEFT JOIN SMGNOMMONEDAS AS M
        ON DB.CliDbCodMon = M.MonCodigo
    LEFT JOIN SCOPAIS AS P
        ON CP.CliPaisCodIntern = P.PaisCodIntern
    LEFT JOIN SCOTIPIFEMPRESA AS TE
        ON CP.TipifiCodigo = TE.TipifiCodigo
    WHERE
        TRIM(UPPER(CP.CliCategoria)) IN ('C', 'CP', 'L', 'LP', 'LR', 'A')
        AND CP.CliActivo = 1
"""

    print(query)

    return export_table_to_json(
        db=db,
        doctype_name=doctype_name,
        sqlserver_name=sqlserver_name + "-cliente",
        module_name=module_name,
        field_mapping=field_mapping,
        table_query=query,
        save=export,
    )


# Obtener los proveedores y ponerle el alias con el nombre del campo en el doctype
def get_proveedores(db, export=False):
    doctype_name = "Supplier"
    sqlserver_name = "SMGCLIENTEPROVEEDOR"
    module_name = "Buying"

    # Mapeo de campos: alias -> (campo SQL, tipo)
    field_mapping = [
        ("reup_code", ("CP.CliCodigo", "string")),
        ("supplier_name", ("CP.CliDescripcion", "string")),
        ("supplier_group", ("TE.TipifiDescripcion", "string")),
        ("country", ("P.PaisDescripcion", "string")),
        # ("supplier_primary_address", ("CP.CliDireccion", "string")),
        # ("mobile_no", ("CP.CliTelefono", "string")),
        # ("email_id", ("CP.CliEmail", "string")),
        ("default_currency", ("MAX(M.MonSiglas)", "string")),
        ("supplier_type", (None, "string")),  # Valor fijo
        ("nit_code", ("NIT.NitDescrip", "string")),
    ]

    # Construcción de SELECT
    select_clauses = []
    for alias, (sql_field, _) in field_mapping:
        if alias == "supplier_type":
            clause = f"'Company' AS {alias}"
        elif alias == "default_currency":
            clause = f"""
                CASE 
                    WHEN P.PaisDescripcion = 'Cuba' THEN 'CUP'
                    ELSE 'USD'
                END AS {alias}
            """
        else:
            clause = f"{sql_field} AS {alias}"
        select_clauses.append(clause)

    query = f"""
        SELECT 
            {', '.join(select_clauses)}
        FROM SMGCLIENTEPROVEEDOR AS CP
        LEFT JOIN SCOCLIENTEDBANCARIOS AS DB
            ON CP.CliCuentaMN = DB.CLIDBCUENTA
        LEFT JOIN SMGNOMMONEDAS AS M
            ON DB.CliDbCodMon = M.MonCodigo
        LEFT JOIN SCOPAIS AS P
            ON CP.CliPaisCodIntern = P.PaisCodIntern
        LEFT JOIN SMGNIT AS NIT 
            ON CP.CliCodigo = NIT.NitCodigo
        LEFT JOIN SCOTIPIFEMPRESA AS TE 
            ON CP.TipifiCodigo = TE.TipifiCodigo
        WHERE 
            TRIM(UPPER(CP.CliCategoria)) IN ('P', 'CP', 'R', 'CR', 'LR', 'A')
            AND CP.CliActivo = 1
        GROUP BY 
            CP.CliCodigo,
            CP.CliDescripcion,
            CP.CliDireccion,
            CP.CliTelefono,
            CP.CliEmail,
            P.PaisDescripcion,
            CP.CliCategoria,
            NIT.NITDescrip,
            TE.TipifiDescripcion
    """

    # print(query)

    return export_table_to_json(
        db=db,
        doctype_name=doctype_name,
        sqlserver_name=sqlserver_name + "-proveedor",
        module_name=module_name,
        field_mapping=field_mapping,
        table_query=query,
        save=export,
    )


def get_bank_accounts(db, export=False):
    doctype_name = "Bank Account"
    sqlserver_name = "SCOCIENTEDATOSBANCARIOS"
    module_name = "Accounts"

    field_mapping = [
        ("account_name", ("DB.CliDBTitular", "string")),
        ("account_number", ("DB.CliDBCuenta", "string")),
        ("bank", ("DB.CliDBTitular", "string")),
        ("party_type", (None, "string")),
        ("party", ("CP.CliCodigo", "string")),
    ]

    select_clauses = []
    for alias, (sql_field, _) in field_mapping:
        if alias == "party_type":
            clause = f"'Customer' AS {alias}"
        elif alias == "nit_code":
            clause = f"""
                CASE
                    WHEN NIT.NitDescrip IS NOT NULL AND NIT.NitDescrip != '' THEN NIT.NitDescrip
                    ELSE CP.CliCodigo
                END AS {alias}
            """
        else:
            clause = f"{sql_field} AS {alias}"
        select_clauses.append(clause)

    query = f"""
        SELECT
            {', '.join(select_clauses)}
        FROM SCOCLIENTEDBANCARIOS AS DB
        LEFT JOIN SMGCLIENTEPROVEEDOR AS CP
            ON CP.CliCuentaMN = DB.CLIDBCUENTA
        WHERE CP.CliActivo = 1
            AND TRIM(UPPER(CP.CliCategoria)) IN ('C', 'CP', 'L', 'LP', 'LR')
    """

    return export_table_to_json(
        db=db,
        doctype_name=doctype_name,
        sqlserver_name=sqlserver_name + "-cuentas_bancarias",
        module_name=module_name,
        field_mapping=field_mapping,
        table_query=query,
        save=export,
    )


def get_contactos(db, export=False):
    doctype_name = "Contact"
    sqlserver_name = "SCOCLIENTECONTACTOS"
    module_name = "CRM"

    # Mapeo de campos: alias -> (campo SQL, tipo)
    field_mapping = [
        ("first_name", ("C.CliContacNombre", "string")),
        ("last_name", ("C.CliContacApellidos", "string")),
        ("mobile_no", ("C.CliContacTlfno", "string")),
        ("email_id", ("C.CliContacEmail", "string")),
        ("cli_codigo", ("CP.CliCodigo", "string")),
        ("cli_categoria", ("CP.CliCategoria", "string")),
        ("cli_descripcion", ("CP.CliDescripcion", "string")),
    ]

    # Construcción de SELECT
    select_clauses = [
        f"{sql_field} as {alias}" for alias, (sql_field, _) in field_mapping
    ]

    query = f"""
        SELECT
            {', '.join(select_clauses)}
        FROM SCOCLIENTECONTACTOS AS C
        LEFT JOIN SMGCLIENTEPROVEEDOR AS CP
            ON C.CliCodigo = CP.CliCodigo
        WHERE C.CliContacTlfno != '' AND C.CliContacEmail != ''
            AND CP.CliActivo = 1
    """

    print(query)

    try:
        # 1) Extraer datos planos
        rows = fetch_table_data(db, field_mapping, query)

        if export:
            # 2) Transformar a formato padre-hijo de Frappe
            contactos = [transform_contact_row(row) for row in rows]

            # 3) Guardar con la estructura esperada
            output_path = save_json_file(
                doctype_name, contactos, module_name, sqlserver_name
            )
            logging.info(f"{doctype_name}.json guardado correctamente en {output_path}")

            return contactos
        else:
            return rows

    except Exception as e:
        logging.error(f"Error exportando {doctype_name}: {e}")
        raise


def transform_contact_row(row: dict) -> dict:
    """Convierte en estructura padre-hijo de Contact"""
    contact = {
        "first_name": row.get("first_name"),
        "last_name": row.get("last_name"),
        "phone_nos": [],
        "email_ids": [],
        "links": [],
    }

    if row.get("mobile_no"):
        contact["phone_nos"].append(
            {
                "phone": row["mobile_no"],
                "is_primary_mobile_no": 1,
            }
        )

    if row.get("email_id"):
        contact["email_ids"].append({"email_id": row["email_id"], "is_primary": 1})

    # Agregar links basado en la categoría del cliente/proveedor
    categoria = row.get("cli_categoria", "").strip().upper()
    cli_codigo = row.get("cli_codigo")
    cli_descripcion = row.get("cli_descripcion")

    if cli_codigo and cli_descripcion:
        if categoria in ["C", "CP", "L", "LP", "LR"]:
            # Es cliente
            contact["links"].append(
                {"link_doctype": "Customer", "link_name": cli_descripcion}
            )
        if categoria in ["P", "CP", "R", "CR", "LR"]:
            # Es proveedor
            contact["links"].append(
                {"link_doctype": "Supplier", "link_name": cli_descripcion}
            )

    return contact


# def transform_contact_row(row: dict) -> dict:
#     """Convierte un row plano en estructura padre-hijo simplificada"""
#     contact = {
#         "first_name": row.get("first_name"),
#         "last_name": row.get("last_name"),
#         # Inicializar como None en lugar de listas vacías
#         "phone_nos": None,
#         "email_ids": None,
#     }

#     if row.get("mobile_no"):
#         contact["phone_nos"] = {  # ← Objeto directo, no lista
#             "doctype": "Contact Phone",
#             "phone": row["mobile_no"],
#             "is_primary_mobile_no": 1,
#         }

#     if row.get("email_id"):
#         contact["email_ids"] = {  # ← Objeto directo, no lista
#             "doctype": "Contact Email",
#             "email_id": row["email_id"],
#             "is_primary": 1,
#         }

#     return contact


# Funcion que muestra los contactos por clientes/proveedores pero solo para el endpoint
def get_clientes_con_contactos(db):
    # Mapeo de campos: alias -> (campo SQL, tipo)
    field_mapping = [
        ("cli_codigo", ("CP.CliCodigo", "string")),
        ("cli_descripcion", ("CP.CliDescripcion", "string")),
        ("cli_categoria", ("CP.CliCategoria", "string")),
        ("cli_direccion", ("CP.CliDireccion", "string")),
        ("categoria", (None, "string")),  # Valor derivado con CASE
        ("cantidad_contactos", ("COUNT(C.CliCodigo)", "integer")),
        (
            "contactos",
            (
                "STRING_AGG(CONCAT(C.CliContacNombre COLLATE DATABASE_DEFAULT, ' ', C.CliContacApellidos COLLATE DATABASE_DEFAULT, ' (', C.CliContacTlfno COLLATE DATABASE_DEFAULT, ', ', C.CliContacEmail COLLATE DATABASE_DEFAULT, ')'), ' | ')",
                "string",
            ),
        ),
    ]

    # Construcción de SELECT
    select_clauses = []
    for alias, (sql_field, _) in field_mapping:
        if alias == "categoria":
            clause = """
                CASE
                    WHEN TRIM(UPPER(CP.CliCategoria)) = 'C' THEN 'Cliente'
                    WHEN TRIM(UPPER(CP.CliCategoria)) = 'P' THEN 'Proveedor'
                    WHEN TRIM(UPPER(CP.CliCategoria)) = 'A' THEN 'Ambos'
                    WHEN TRIM(UPPER(CP.CliCategoria)) = 'CP' THEN 'Cliente y Proveedor'
                    WHEN TRIM(UPPER(CP.CliCategoria)) = 'L' THEN 'Cliente Potencial'
                    WHEN TRIM(UPPER(CP.CliCategoria)) = 'R' THEN 'Proveedor Potencial'
                    WHEN TRIM(UPPER(CP.CliCategoria)) = 'CR' THEN 'Cliente y proveedor potencial'
                    WHEN TRIM(UPPER(CP.CliCategoria)) = 'LP' THEN 'Cliente potencial y proveedor'
                    WHEN TRIM(UPPER(CP.CliCategoria)) = 'LR' THEN 'Cliente potencial y Proveedor potencial'
                    ELSE NULL
                END AS categoria
            """
        else:
            clause = f"{sql_field} AS {alias}"
        select_clauses.append(clause)

    query = f"""
    SELECT
        {', '.join(select_clauses)}
    FROM SCOCLIENTECONTACTOS AS C
    LEFT JOIN SMGCLIENTEPROVEEDOR AS CP
        ON C.CliCodigo = CP.CliCodigo
    WHERE
        C.CliContacTlfno IS NOT NULL AND C.CliContacTlfno != ''
        AND C.CliContacEmail IS NOT NULL AND C.CliContacEmail != ''
        AND CP.CliActivo = 1
        AND TRIM(UPPER(CP.CliCategoria)) IN ('C', 'P', 'A', 'CP', 'L', 'R', 'CR', 'LP', 'LR')
    GROUP BY
        CP.CliCodigo, CP.CliDescripcion, CP.CliCategoria,
        CP.CliDireccion
    ORDER BY CP.CliDescripcion;
    """

    return fetch_table_data(db, field_mapping, query)


def get_banks(db, export=False):
    doctype_name = "Bank"
    sqlserver_name = "SNOCONFIGURACION"
    module_name = "Accounts"

    field_mapping = [
        ("bank_name", (None, "string")),
        ("bank_type", (None, "string")),
    ]

    select_clauses = []
    for alias, (sql_field, _) in field_mapping:
        if alias == "bank_name":
            clause = """
                CASE DB.ConfigTBanco
                    WHEN 1 THEN 'Metropolitano'
                    WHEN 2 THEN 'BANDEC'
                    WHEN 3 THEN 'BPA'
                END AS bank_name
            """
        elif alias == "bank_type":
            clause = """
                CASE DB.ConfigTBanco
                    WHEN 1 THEN 'BANMET'
                    WHEN 2 THEN 'BANDEC'
                    WHEN 3 THEN 'BPA'
                END AS bank_type
            """
        else:
            clause = f"{sql_field} AS {alias}"
        select_clauses.append(clause.strip())

    query = f"""
        SELECT DISTINCT
            {', '.join(select_clauses)}
        FROM SNOCONFIGURACION AS DB
        WHERE DB.ConfigTBanco IS NOT NULL AND DB.ConfigTBanco != ''
    """

    print(query)
    return export_table_to_json(
        db=db,
        doctype_name=doctype_name,
        sqlserver_name=sqlserver_name + "-bancos",
        module_name=module_name,
        field_mapping=field_mapping,
        table_query=query,
        save=export,
    )
