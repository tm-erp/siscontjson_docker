import datetime
import logging
from typing import Dict, List

from utils.jsons_utils import export_table_to_json, \
    export_table_to_json_paginated


# Para obtener los centros de costo y poniendo alias con el nombre
# del campo en el doctype
def get_centro_costo(db, export: bool = False):
    doctype_name = "Cost Center"
    sqlserver_name = "SCCCENTROCOSTO"
    module_name = "Setup"

    field_mapping = [
        # Campos del doctype principal (Centro Costo)
        ("cost_center_number", ("OC.OCostCodigo", 'string')),
        ("cost_center_name", ("OC.OCostDescrip", 'string')),
        ("parent_cost_center", ("DE.DatEntNom", 'string'))  #centro de Costo padre
    ]

    # Construimos la cláusula SELECT
    select_clauses = [
        f"{sql_field} as {alias}" for alias, (sql_field, _) in field_mapping
    ] + ["""(SELECT TOP 1 DatEntNom FROM SMGDatosEntidad) AS parent_cost_center
    """]

    query = f"""
       SELECT
           {', '.join(select_clauses)}
        FROM SCCOBJCOSTO as OC INNER JOIN
        SCCCENTROCOSTO as CC ON OC.OCostCodigo = CC.CcostOCcodigo CROSS JOIN
               SMGDATOSENTIDAD as DE
    """

    return export_table_to_json(
        db=db,
        doctype_name=doctype_name,
        sqlserver_name=sqlserver_name,
        module_name=module_name,
        field_mapping=field_mapping,
        table_query=query,
        save=export
    )