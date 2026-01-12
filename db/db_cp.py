import datetime
import logging
from typing import Dict, List
from utils.jsons_utils import export_table_to_json, \
    export_table_to_json_paginated

# Para obtener las Facturas de Compras desde Cobros y Pagos y poniendo alias con el nombre
# del campo en el doctype
def get_factura_compra(db):
    doctype_name = "Purchase Invoice"
    sqlserver_name = "SCPPAGODOCUMENTO"
    module_name = "Setup"

    field_mapping = [
        ("invoice_number", ("PagdoaeDoc", 'string')),
        ("party_type", ("Supplier", 'string')),        
        ("party", ("CliDescrip", 'string')),  
        ("temporary_opening_account", ("699005 - Apertura - T", 'string')),  
        ("posting_date", ("PagdoaeFecha", 'string')), 
        ("due_date", ("NULL", 'string')),  # Campo vacío reemplazado por NULL
        ("item_name", ("Opening Invoice Item", 'string')),  
        ("outstanding_amount", ("1234", 'string')), # analizar esto. es la cant restante 
        ("qty", ("1", 'string')),
        ("cost_center", ("NULL",'')),  # Campo vacío reemplazado por NULL
        ("expense_element", ("NULL",''))  # Campo vacío reemplazado por NULL
    ]

    query = f"""
    SELECT 
        PD.PagdoaeDoc as invoice_number, 
        'Supplier' as party_type, 
        CP.CliDescripcion as party, 
        '699005 - Apertura - T' as temporary_opening_account,
        PD.PagdoaeFecha as posting_date, 
        CONVERT(VARCHAR(10), PD.PagdoaeFecha, 103) as posting_date,
        '' as due_date, 
        'Opening Invoice Item' as item_name, 
        ISNULL(saldos.saldo, 0) as outstanding_amount, 
        '1' as qty, 
        '' as cost_center, 
        '' as expense_element
    FROM [SCPPAGODOCUMENTO] as PD 
    INNER JOIN [SMGCLIENTEPROVEEDOR] as CP ON PD.CliCodigo = CP.CliCodigo
    LEFT JOIN (
    SELECT 
        SCPPAGODOCUMENTO.PagdoaeId, 
        SCPPAGODOCUMENTO.PagdoaeDoc, 
        SCPPAGODOCUMENTO.PagdoaeImporteMN,
        (SCPPAGODOCUMENTO.PagdoaeImporteMN - COALESCE(SUM(SCPPAGOCONT.PagCtaeImporte), 0)) AS saldo
    FROM SCPPAGODOCUMENTO
    LEFT OUTER JOIN SCPPAGOFACTURA ON SCPPAGOFACTURA.PagFaaeIdDoc = SCPPAGODOCUMENTO.PagdoaeId
    LEFT OUTER JOIN SCPPAGOCONT ON SCPPAGOCONT.PagaeId = SCPPAGOFACTURA.PagaeId AND SCPPAGOCONT.PagCtaeDebito = 1 
    WHERE SCPPAGODOCUMENTO.PagdoaeEstado IN (0, 1)
    GROUP BY SCPPAGODOCUMENTO.PagdoaeId, SCPPAGODOCUMENTO.PagdoaeDoc, SCPPAGODOCUMENTO.PagdoaeImporteMN
    ) saldos ON saldos.PagdoaeId = PD.PagdoaeId
    WHERE PD.PagdoaeEstado IN (0, 1)
    """

    return export_table_to_json(
        db=db,
        doctype_name=doctype_name,
        sqlserver_name=sqlserver_name,
        module_name=module_name,
        field_mapping=field_mapping,
        table_query=query
    )

def get_factura_ventas(db):
    doctype_name = "Sales Invoice"
    sqlserver_name = "SCPCOBRODOCUMENTO"
    module_name = "Setup"

    field_mapping = [
        ("invoice_number", ("CobdoaeDoc", 'string')),
        ("party_type", ("Customer", 'string')),        
        ("party", ("CliDescrip", 'string')),  
        ("temporary_opening_account", ("699005 - Apertura - T", 'string')),  
        ("posting_date", ("CobdoaeFecha", 'string')), 
        ("due_date", ("NULL", 'string')),  # Campo vacío reemplazado por NULL
        ("item_name", ("Opening Invoice Item", 'string')),  
        ("outstanding_amount", ("1234", 'string')), # analizar esto. es la  cant restante 
        ("qty", ("1", 'string')),
        ("cost_center", ("NULL",'')),  # Campo vacío reemplazado por NULL
        ("expense_element", ("NULL",''))  # Campo vacío reemplazado por NULL
    ]

    query = f"""
    SELECT CD.CobdoaeDoc as invoice_number,
        'Customer' as party_type,
        CP.CliDescripcion as party,
        '699005 - Apertura - T' as temporary_opening_account,
        CONVERT(VARCHAR(10), CD.CobdoaeFecha, 103) as posting_date,
        '' as due_date,
        'Opening Invoice Item' as item_name,
        ISNULL(saldos.saldo, 0) as outstanding_amount,
        '1' as qty,
        '' as cost_center,
        '' as expense_element
    FROM[SCPCOBRODOCUMENTO] as CD
    INNER JOIN[SMGCLIENTEPROVEEDOR] as CP ON CD.CliCodigo = CP.CliCodigo
    LEFT JOIN(    
    SELECT 
        SCPCOBRODOCUMENTO.CobdoaeId, 
        SCPCOBRODOCUMENTO.CobdoaeDoc, SCPCOBRODOCUMENTO.CobdoaeImporteMN,
        (SCPCOBRODOCUMENTO.CobdoaeImporteMN - COALESCE(SUM(SCPCOBROCONT.CobCtaeImporte), 0)) AS saldo
    FROM SCPCOBRODOCUMENTO
    LEFT OUTER JOIN SCPCOBROFACTURA ON SCPCOBROFACTURA.CobFaaeIdDoc = SCPCOBRODOCUMENTO.CobdoaeId
    LEFT OUTER JOIN SCPCOBROCONT ON SCPCOBROCONT.CobaeId = SCPCOBROFACTURA.CobaeId
    AND SCPCOBROCONT.CobCtaeDebito = 1 
    WHERE SCPCOBRODOCUMENTO.CobdoaeEstado IN (0, 1)
    GROUP BY SCPCOBRODOCUMENTO.CobdoaeId,SCPCOBRODOCUMENTO.CobdoaeDoc,SCPCOBRODOCUMENTO.CobdoaeImporteMN)
    saldos on saldos.CobdoaeId = CD.CobdoaeId where CD.CobdoaeEstado IN (0, 1)
    """
    return export_table_to_json(
        db=db,
        doctype_name=doctype_name,
        sqlserver_name=sqlserver_name,
        module_name=module_name,
        field_mapping=field_mapping,
        table_query=query)

def get_pagos_anticipados(db):
    doctype_name = "Payment Entry"
    sqlserver_name = "SCPPAGO"
    module_name = "Setup"

    field_mapping = [
       ("payment_type", ("Pay", 'string')),
       ("posting_date",("P.PagaeFecha", 'string')),
       ("company",("DE.DatEntNom", 'string')),
       ("paid_from",("CC.ClCuDescripcion", 'string')),
       ("paid_from_account_currency",("CUP", 'string')),
       ("paid_to",("CC.ClCuDescripcion", 'string')),
       ("paid_to_account_currency",("CUP", 'string')),
       ("paid_amount",("PC.PagCtaeImporte", 'string')),
       ("received_amount",("PC.PagCtaeImporte", 'string')),
       ("base_received_amount",("PC.PagCtaeImporte", 'string'))
    ]

    query = f"""
    SELECT 
            P.PagaeId, 'Pay' as payment_type, 
            PagaeFecha as posting_date,
            SMGDATOSENTIDAD.DatEntNom as company, 
            Credito.ClCuDescripcion AS paid_from,
            'CUP' as paid_from_account_currency,
            Debito.ClCuDescripcion AS paid_to,
            'CUP' as paid_to_account_currency,
            Debito.PagCtaeImporte AS paid_amount, 
            Debito.PagCtaeImporte AS received_amount,
            Debito.PagCtaeImporte AS base_received_amount 
        FROM SCPPAGO AS P
        OUTER APPLY (
            SELECT TOP 1 
                PCONT.PagCtaeImporte, CC.ClCuCuenta, CC.ClCuDescripcion
            FROM SCPPAGOCONT AS PCONT
            INNER JOIN SCGCLASIFICADORDECUENTAS AS CC 
                ON PCONT.ClcuIDCuenta = CC.ClcuIDCuenta
            WHERE PCONT.PagaeId = P.PagaeId
              AND PCONT.PagCtaeDebito = 1
        ) AS Debito
        OUTER APPLY (
            SELECT TOP 1 
            PCONT.PagCtaeImporte, CC.ClCuCuenta, CC.ClCuDescripcion
            FROM SCPPAGOCONT AS PCONT
            INNER JOIN SCGCLASIFICADORDECUENTAS AS CC 
            ON PCONT.ClcuIDCuenta = CC.ClcuIDCuenta
            WHERE PCONT.PagaeId = P.PagaeId
            AND PCONT.PagCtaeDebito = 0
        ) AS Credito
        CROSS JOIN SMGDATOSENTIDAD
        WHERE P.PagaeFuncion = 8 AND P.PagaeCausa = 10 AND P.TransaeCodigo = 13
        ORDER BY P.PagaeId
    """
    return export_table_to_json(
        db=db,
        doctype_name=doctype_name,
        sqlserver_name=sqlserver_name,
        module_name=module_name,
        field_mapping=field_mapping,
        table_query=query,
    )


def get_cobros_anticipados(db):
        doctype_name = "Payment Entry"
        sqlserver_name = "SCPCOBRO"
        module_name = "Setup"

        field_mapping = [
            ("payment_type", ("Receive", 'string')),
            ("posting_date", ("C.CobaeFecha", 'string')),
            ("company", ("DE.DatEntNom", 'string')),
            ("paid_from", ("CC.ClCuDescripcion", 'string')),
            ("paid_from_account_currency", ("CUP", 'string')),
            ("paid_to", ("CC.ClCuDescripcion", 'string')),
            ("paid_to_account_currency", ("CUP", 'string')),
            ("paid_amount", ("CCont.CobCtaeImporte", 'string')),
            ("received_amount", ("CCont.PagCtaeImporte", 'string')),
            ("base_received_amount", ("CCont.PagCtaeImporte", 'string'))
        ]

        query = f"""
        SELECT 
                C.CobaeId, 'Receive' as payment_type, 
                CobaeFecha as posting_date,
                SMGDATOSENTIDAD.DatEntNom as company, 
                Credito.ClCuDescripcion AS paid_from,
                'CUP' as paid_from_account_currency,
                Debito.ClCuDescripcion AS paid_to,
                'CUP' as paid_to_account_currency,
                Debito.CobCtaeImporte AS paid_amount, 
                Debito.CobCtaeImporte AS received_amount,
                Debito.CobCtaeImporte AS base_received_amount 
            FROM SCPCobro AS C
            OUTER APPLY (
                SELECT TOP 1 
                    CCONT.CobCtaeImporte, CC.ClCuCuenta, CC.ClCuDescripcion
                FROM SCPCOBROCONT AS CCONT
                INNER JOIN SCGCLASIFICADORDECUENTAS AS CC 
                    ON CCONT.ClcuIDCuenta = CC.ClcuIDCuenta
                WHERE CCONT.CobaeId = C.CobaeId
                  AND CCONT.CobCtaeDebito = 0
            ) AS Debito
            OUTER APPLY (
                SELECT TOP 1 
                CCONT.CobCtaeImporte, CC.ClCuCuenta, CC.ClCuDescripcion
                FROM SCPCOBROCONT AS CCONT
                INNER JOIN SCGCLASIFICADORDECUENTAS AS CC 
                ON CCONT.ClcuIDCuenta = CC.ClcuIDCuenta
                WHERE CCONT.CobaeId = C.CobaeId
                AND CCONT.CobCtaeDebito = 1
            ) AS Credito
            CROSS JOIN SMGDATOSENTIDAD
            WHERE C.CobaeFuncion = 8 AND C.CobaeCausa = 10 AND C.TransaeCodigo = 10
            ORDER BY C.CobaeId
        """
        return export_table_to_json(
            db=db,
            doctype_name=doctype_name,
            sqlserver_name=sqlserver_name,
            module_name=module_name,
            field_mapping=field_mapping,
            table_query=query,
        )

#def get_doc_crediticios(db):

