from typing import List, Dict

# from db.database import DatabaseManager
from db.db_connection import runSQLQuery, createJSON
from db.db_connection import Connection
# Importo la clase StreamingResponse para implementar la descarga del fichero JSON
from fastapi.responses import StreamingResponse
from collections import OrderedDict

#Importo el modulo json
import os
import logging
import pandas as pd

def getAccounts(conn:Connection, name:str):
        """
        Este metodo retorna las cuentas basadas en el Clasificador de Cuentas
        """

        #Esta query obtiene las cuantas del clasificador de cuentas y la combina con las tablas del tipo de grupo y subgrupo
        queryClas = f"""SELECT [ClcuIDCuenta] AS ID,
                              [ClCuDescripcion] AS account_name,
                              CASE
                                WHEN [ClCuUltimoNivel] = 1 THEN 0 ELSE 1
                              END AS is_group,
                              [ClCuSubElemento] AS expense_element,
                              CAST([ClCuCuenta] AS varchar) AS cuentaId,
                              CAST([ClCuSubCuenta] AS varchar) AS subcuentaId,
                              [ClCuSubControl] AS subcontrol,
                              [ClCuAnalisis] AS analisis
                              FROM [{name}].[dbo].[SCGCLASIFICADORDECUENTAS] WHERE [ClCuActiva] = 0
                              ORDER BY ClCuCuenta, ClCuSubcuenta, ClCuSubControl, ClCuAnalisis"""

        # Esta query obtiene los nomencladores de moneda de cada cuenta.
        queryMon = f"""SELECT CAST([NomCuCuenta] AS varchar)  AS cuentaId,
                             CAST([NomCuSubCuenta] AS varchar) AS subcuentaId,
                             [MonSiglas] as currency
                        FROM [{name}].[dbo].[SCGCTASNOMENCCTAS] AS NOMENCTAS
                        RIGHT JOIN [{name}].[dbo].[SMGNOMMONEDAS] AS NOMMONEDAS ON NOMENCTAS.NomCuSubControl = CAST(NOMMONEDAS.MonCodigo AS varchar)
                    """
        queryRan= f"""SELECT [ClcuIDCuenta] AS ID,
                        [NomRaRangoI] AS InicioR,
                        [NomRaRangoF] AS FinalR
                    FROM [{name}].[dbo].[SCGCLASIFICADORDECUENTAS] INNER JOIN [{name}].[dbo].[SCGRANGOSNOMENCCTAS]
                    ON [{name}].[dbo].[SCGCLASIFICADORDECUENTAS].[SubgId] = [{name}].[dbo].[SCGRANGOSNOMENCCTAS].[SubgId] AND
                        [{name}].[dbo].[SCGCLASIFICADORDECUENTAS].[GrupoID] = [{name}].[dbo].[SCGRANGOSNOMENCCTAS].[GrupoID]
                    WHERE [ClCuActiva] = 0
                  """

        #Creo el DataFrame para obtener la tabla con el clasificador de cuentas
        df = runSQLQuery(queryClas, conn)
        #Creo el DataFrame con las nominaciones de todas las cuentas
        dfMon = runSQLQuery(queryMon, conn)
       
        #Ajusto la tabla del clasificador de cuentas
        #Aplico la conversion en el campo expense element; si es 0 se queda en 0; pero si es 2 o 3 se convierte en 1
        df = df.replace({'expense_element':{2:1, 3:1}})

        #Ajusto la tabla de las monedas
        #Las cuentas que estan en CUC pasan hacer en CUP
        dfMon = dfMon.replace({'currency':{'CUC':'CUP'}})
        #Elimino de las tabla las cuentas que no tienen monedas porque son padres
        dfMon = dfMon[dfMon['cuentaId'].notna()]
        #Elimino las filas duplicadas generadas durante la conversion
        dfMon.drop_duplicates(inplace= True)

        #Merge sobre ambas tablas
        df = dfMon.merge(df, on=['cuentaId', 'subcuentaId'], how='right')

        df.insert(1,"account_number","")
        df["account_number"] = [f'{row['cuentaId']}{row['subcuentaId'] if int(row['subcuentaId'])!=0 else ""}{row["subcontrol"] if int(row["subcontrol"])!=0 else ""}{row["analisis"] if int(row["analisis"])!=0 else ""}' for index, row in df.iterrows()]

        #Inserto el tipo de cuenta de cada cuenta segun el excel cuentas
        dfType = pd.read_excel(os.path.join(os.getcwd(), "db/cuentas.xlsx"))

        dfType["Rangos"] = dfType["Rangos"].astype(str)

        dictType = {tuple(str(row["Rangos"]).split('-')): row["Tipo"] for i, row in dfType.iterrows()}

        df.insert(1,'account_type','')
        for i in range(0, len(df)):
               codigo = int(df.at[i,"cuentaId"])
               for key, value in dictType.items():                    
                    if len(key)>1: 
                        if codigo >= int(key[0]) and codigo < int(key[1]):
                            df.at[i,'account_type'] = str(value)
                            break
                    else:
                         if int(key[0]) == codigo:
                            df.at[i,'account_type'] = str(value)
                            break
                      
        
        df.drop(["ID"], axis=1, inplace=True)

        #Creo el JSON
        result = OrderedDict()
        result["doctype"] = "Account"
        for col in [coln for coln in df.columns]:
            df[col] = df[col].astype(str)
        result["data"] =  df.to_dict(orient="records")

        return result


def getExpenseElement(conn:Connection, name:str):
        """
        Este metodo retorna los valores de la tabla SCGELEMENTODEGASTO
        """
        
        #Esta es la query para obtener los subelementos de gastos
        queryChilds = f"""SELECT [SubelCodigo] AS number
                        ,[SubelDescrip] AS title
                        ,[EGastoDescripcion] AS old_parent
                        ,[EGastoDescripcion] AS parent_expense_element
                        ,[EGastoCodigoDesde] AS lft
                        ,[EGastoCodigoHasta] AS rgt
                        FROM [{name}].[dbo].[SCGSUBELEMENTO]
                        INNER JOIN [{name}].[dbo].[SCGELEMENTODEGASTO]
                        ON [{name}].[dbo].[SCGSUBELEMENTO].[EGastoID] = [{name}].[dbo].[SCGELEMENTODEGASTO].[EGastoID] WHERE [SubelActivo] = ''"""
        #Obtengo el Dataframe de los elementos de gastos
        dfChilds= runSQLQuery(queryChilds, conn)
        dfChilds.insert(0,"is_group", 0)
                
        #Creo el diccionario para el Doctype
        result = OrderedDict()
        result["doctype"] = "Expense Element"        
        result["data"] =  dfChilds.to_dict(orient="records")
        return result







