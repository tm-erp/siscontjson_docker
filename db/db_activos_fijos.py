from typing import List, Dict

# from db.database import DatabaseManager
from datetime import datetime
from dateutil.relativedelta import relativedelta
from db.db_connection import runSQLQuery, createJSON
from db.db_connection import Connection
from collections import OrderedDict
import logging
import pandas as pd


def getSAFApertura(conn: Connection, name: str):
        data = getSAFAperturaCSV(conn, name)
        result = OrderedDict()  
        result["doctype"] = "Asset"        
        result["data"] = data.to_csv(index=False)

        return result

    
def getSAFAperturaCSV(conn: Connection, name:str):
        
        query=f"""SELECT * FROM (SELECT [NomGrupCodigo] AS codeGroup
                    ,[SubGrupoCodigo] AS codeSubGroup
                    ,[AFDescripcion] AS descrip
                    ,[AFTasaDepreciacion] AS tasa
                    ,[AFFechaAlta] AS purchase_date
                    ,[AFValorMN] AS gross_purchase_account
                    ,[AFValorMN] AS total_asset_cost
                    ,[AFDeprecAcumMN] AS opening_accumulated_depreciation
                    ,[NivelCodigoAF] AS codLocation
		    ,[NomgrupDescripcion]  AS nameGroup                   
                    ,[NomSubgDescripcion] AS nameSubGroup
                    ,[AFNumInv] AS id
                    ,SubGroup.tipo_depresiacion AS tipo_depresiacion
                    ,[AFFechaApertura]
                    ,[AFFechaNoDeprecia]
                    ,[AFFechaDepreTotal]
                    ,[AFDeprecApertura]
                    FROM [{name}].[dbo].[SAFAPERTURA] INNER JOIN (SELECT SubGroup.[NomGrupCodigo] AS codeGroup
                          ,[NomSubgCodigo] AS codeSubGroup  
                          ,[NomgrupDescripcion]                        
                          ,[NomSubgDescripcion]
                          ,[NomSubgTasaEmpresa] AS tasa                         
                          ,[NomSubgTipoDepreciacion] AS tipo_depresiacion
                          FROM [{name}].[dbo].[SAFSubgruposAFT] AS SubGroup INNER JOIN [{name}].[dbo].[SAFGruposAFT] AS GGroup 
                          ON [SubGroup].[NomGrupCodigo] = [GGroup].[NomGrupCodigo]) AS SubGroup
                    ON [{name}].[dbo].[SAFAPERTURA].SubGrupoCodigo = SubGroup.codeSubGroup AND [{name}].[dbo].[SAFAPERTURA].NomGrupCodigo = SubGroup.codeGroup
                    WHERE  [{name}].[dbo].[SAFAPERTURA].AFActivo = '') AS Apertura 
                    INNER JOIN (SELECT [NivelCodigo], [NivelDescrip] FROM [{name}].[dbo].[SAFNIVELRESPAREASUBAREA]) AS Location 
                    ON [Apertura].[codLocation] = [Location].[NivelCodigo]"""

        query2 = f"""SELECT [AFNumInv] as id
                        ,[AFNumComp]
                        ,[DescripcionComp]
                        ,[CantidadComp]
                        ,[ValorMLCComp]
                        ,[ValorMNComp]
                        ,[DepAcumMNComp]
                        ,[DepAcumMLCComp]
                        ,[AFNumInvCompConversion]
                        ,[UnificadoComp]
                        ,[AFAper1CRC]
                        ,[ValorMEXComp]
                        FROM [{name}].[dbo].[SAFAPERTURA1]"""
        
        query3= f"""SELECT [AFNumInv] AS id, [{name}].[dbo].[SMGAREASUBAREA].[AreaDescrip] AS Area
  FROM [{name}].[dbo].[SAFAPERTURA] INNER JOIN [{name}].[dbo].[SMGAREASUBAREA] 
  ON [SAFAPERTURA].[AreaCodigo] = [SMGAREASUBAREA].AreaCodigo"""

        df = runSQLQuery(query, conn)

        df1 = runSQLQuery(query2, conn)

        dfDep = runSQLQuery(query3, conn)       
                
        #Inserto el codigo del activo fijo
        df.insert(0, "Identificador", "")
        df["Identificador"] = [row['id'] for index, row in df.iterrows()] 
        
        df.insert(1, "Empresa", "")
        df["Empresa"] = ["Tecnomatica" for i in range(0,len(df))]
        
        df.insert(2, "Código del Producto", "")
        df["Código del Producto"]=[f'9999{row['codeGroup']}{row['codeSubGroup']}' for index, row in df.iterrows()]
        
        df.insert(3, "Nombre de Activo", "")
        df["Nombre de Activo"] = df["descrip"]
        
        df.insert(4, "Ubicación", "")
        df["Ubicación"] = [row['NivelDescrip'] for index, row in df.iterrows()]
        
        df.insert(5,"Categoría de activos","")
        df["Categoría de activos"] =  [f'{row['codeGroup']}{row['codeSubGroup']} {row['nameSubGroup']}' for index, row in df.iterrows()]
        
        df.insert(6,"Nombre del producto", "")
        df["Nombre del producto"]= [f'Apertura {row['codeGroup']}{row['codeSubGroup']}' for index, row in df.iterrows()]
        
        df.insert(7,"Total Asset Cost","")
        df["Total Asset Cost"]= df["total_asset_cost"]
        
        df.insert(8,"Número total de amortizaciones", "")
        df["Número total de amortizaciones"] = 100/(df["tasa"] * 12)
        
        df.insert(9, "Método de depreciación", "")
        df["Método de depreciación"] = ["Straight Line" if row["tipo_depresiacion"] == 1 else "Manual" for index, row in df.iterrows()]
        
        df.insert(10, "Propietario del Activo", "")
        df["Propietario del Activo"] = ["Company" for i in range(0,len(df))]
        
        df.insert(11, "Compañia Dueña del Activo", "")
        df["Compañia Dueña del Activo"] = [df["Empresa"] for i in range(0,len(df))]
        df.insert(12, "Es Activo Existente", "")
        df["Es Activo Existente"] = [1 for i in range(0, len(df))]
        df.insert(13, "Is Composite Asset", "")
        df["Is Composite Asset"] = [row["id"] in df1["id"] for index, row in df.iterrows()]
        df.insert(14, "Cantidad de Activos", "")
        df["Cantidad de Activos"] = [1 for i in range(0, len(df))]
        df.insert(15, "Proveedor", "")        

        df.insert(16, "Cliente", "")

        df.insert(17, "Imagen", "")

        df.insert(18, "Entrada de diario para desguace", "")

        df.insert(19, "Secuencias e identificadores", "")

        df.insert(20, "Dividido de, separado de", "")

        df.insert(21, "Custodio", "")
        df["Custodio"] = [row["NivelDescrip"] for index, row in df.iterrows()]
        df.insert(22, "Departamento", "")
        
        df_indexed = dfDep.set_index('id')
        dictDep = df_indexed.to_dict('index')
        df["Department"] = [dictDep[row["id"]]["Area"] for index, row in df.iterrows()]

        df.insert(23, "Fecha de eliminación", "")

        df.insert(24, "Centro de costos", "")

        df.insert(25, "Expense Element", "")

        df.insert(26, "Recibo de compra", "")

        df.insert(27, "Factura de compra", "")

        df.insert(28, "Fecha disponible para usar", "")
        

        df.insert(29, "Additional Asset Cost", "")

        df.insert(30, "Importe Bruto de Compra", "")

        df.insert(31, "Cantidad de activos", "")

        df.insert(32, "Calcular Depreciación", "")
        df["Calcular Depreciación"] = [ 1 if row['AFFechaDepreTotal']==None and row["AFFechaNoDeprecia"]==None else 0 for index, row in df.iterrows()]        
        df["Número total de amortizaciones"] = [str((datetime.now().year - row["purchase_date"].year)*12 + datetime.now().month - row["purchase_date"].month) if row['Calcular Depreciación'] == 1 else "" for index, row in df.iterrows()]
        
        df.insert(33, "Fecha de compra", "")
        
        df.insert(34, "Apertura de la depreciación acumulada", "")

        df.insert(35, "Opening Number of Booked Depreciations", "")

        df.insert(36, "Is Fully Depreciated", "")

        df.insert(37, "Valor después de Depreciación", "")

        df.insert(38, "Frecuencia de Depreciación (Meses)", "")

        df.insert(39, "Siguiente Fecha de Depreciación", "")

        df.insert(40, "Número de Póliza", "")

        df.insert(41, "Asegurador", "")

        df.insert(42, "Valor Asegurado", "")

        df.insert(43, "Fecha de inicio del seguro", "")

        df.insert(44, "Fecha de Finalización del Seguro", "")

        df.insert(45, "Seguro a Todo Riesgo", "")

        df.insert(46, "Requiere Mantenimiento", "")

        df.insert(47, "Estado", "")

        df.insert(48, "Activo Fijo Reservado", "")

        df.insert(49, "Monto de la compra", "")

        df.insert(50, "Libro de Finanzas Predeterminado", "")

        df.insert(51, "Estado de registro de entrada de depreciación", "")

        df.insert(52, "Modificado Desde", "")

        df.insert(53, "Identificador (Libros de Finanzas)", "")

        df.insert(54, "Frecuencia de Depreciación (Meses) (Libros de Finanzas)", "")

        df.insert(55, "Número total de amortizaciones (Libros de Finanzas)", "")

        df.insert(56, "Método de depreciación (Libros de Finanzas)", "")

        df.insert(57, "Depreciate based on daily pro-rata (Libros de Finanzas)", "")

        df.insert(58, "Depreciate based on shifts (Libros de Finanzas)", "")

        df.insert(59, "Fecha de contabilización de la depreciación (Libros de Finanzas)", "")

        df.insert(60, "Libro de finanzas (Libros de Finanzas)", "")

        df.insert(61, "Salvage Value Percentage (Libros de Finanzas)", "")

        df.insert(62, "Tasa de depreciación (Libros de Finanzas)", "")

        df.insert(63, "Total Number of Booked Depreciations  (Libros de Finanzas)", "")

        df.insert(64, "Valor después de Depreciación (Libros de Finanzas)", "")

        df.insert(65, "Valor esperado después de la Vida Útil (Libros de Finanzas)", "")
       
        df.drop(["codeGroup", "codeSubGroup", "descrip", "tasa", 
                 "purchase_date", "gross_purchase_account", "total_asset_cost",
                 "opening_accumulated_depreciation", "codLocation", "nameGroup", 
                 "nameSubGroup", "AFFechaApertura", "AFFechaNoDeprecia", "AFFechaDepreTotal",
                 "AFDeprecApertura", "NivelCodigo", "NivelDescrip"],  axis=1, inplace=True)
                
        return df

def getCategoryAF(conn: Connection, name:str):
        """
        Este metodo retorna las categorias de activos fijos obtenidos desde los Grupos y Subgrupos de SISCONT5
        """
                
        queryChilds = f"""SELECT SubGroup.NomGrupCodigo AS codeGroup
                          ,[NomSubgCodigo] AS codeSubGroup
                          ,CONCAT(SubGroup.NomGrupCodigo,[NomSubgCodigo]) AS erpcode
                          ,[NomgrupDescripcion] AS titleGroup                         
                          ,[NomSubgDescripcion] AS title
                          ,[NomSubgTasaEmpresa] AS tasa                         
                          ,[NomSubgTipoDepreciacion] AS tipo_depresiacion
                          FROM [{name}].[dbo].[SAFSubgruposAFT] AS SubGroup INNER JOIN [{name}].[dbo].[SAFGruposAFT] AS GGroup 
                          ON SubGroup.NomGrupCodigo = GGroup.NomGrupCodigo"""
        
        queryDeprec=f"""SELECT CONCAT(SubGroup.NomGrupCodigo, SubGroup.NomSubgCodigo) AS erpcode, 
                                (SELECT TOP (1) CONCAT([ClCuCuenta],'-', [ClCuSubcuenta])     
                                FROM [{name}].[dbo].[SAFAPERTURA] AS AF INNER JOIN [{name}].[dbo].[SCGCLASIFICADORDECUENTAS] AS CTAS
                                ON AF.ClCuIdCuentaDeprec = CTAS.ClcuIDCuenta 
                                WHERE AF.NomGrupCodigo= SubGroup.NomGrupCodigo AND AF.SubGrupoCodigo=SubGroup.NomSubgCodigo) AS CTADEPREC
                        FROM [{name}].[dbo].[SAFSUBGRUPOSAFT] AS SubGroup
                        """
        queryAFT = f"""SELECT CONCAT(SubGroup.NomGrupCodigo, SubGroup.NomSubgCodigo) AS erpcode, 
                                (SELECT TOP (1) CONCAT([ClCuCuenta], '-' ,[ClCuSubcuenta])        
                                FROM [{name}].[dbo].[SAFAPERTURA] AS AF INNER JOIN [{name}].[dbo].[SCGCLASIFICADORDECUENTAS] AS CTAS
                                ON AF.ClCuIdCuentaAF = CTAS.ClcuIDCuenta 
                                WHERE AF.NomGrupCodigo= SubGroup.NomGrupCodigo AND AF.SubGrupoCodigo=SubGroup.NomSubgCodigo) AS CTAAFT
                        FROM [{name}].[dbo].[SAFSUBGRUPOSAFT] AS SubGroup
                        """
        
        
        result = OrderedDict()
        result["doctype"] = "Asset Category"        
        
        df= runSQLQuery(queryChilds, conn)

        #Inserto la columna number en el Dataframe
        df.insert(0, "asset_category_name", "")
        #Genero los numeros de los subelementos
        df["asset_category_name"] = [f'{row['codeGroup']}{row['codeSubGroup']} {row['title']}' for index, row in df.iterrows()]
        
        df.insert(1, "finance_books", "")
        df["finance_books"] = [[{ "finance_book": f'{row['codeGroup']}{row['codeSubGroup']} {row['title']}',
                                 "depreciation_method": f'{"Straight Line" if row["tipo_depresiacion"]==1 else "Manual"}',
                                   "total_number_of_depreciations": int(1 if row["tipo_depresiacion"]==2 else (100/row["tasa"] if row['tasa']>0 else 0 )* 12), #Esta version le pone 1 cuando la depresiacion es Manual
                                   "frequency_of_depreciation" : 1,
                                   "depreciation_start_date": f'{datetime(datetime.today().year, 2, 28,0,0,0).date()}' }] for index, row in df.iterrows()]
       
        #Inserto las cuentas asociadas a esta categoria
        dfAFT=runSQLQuery(queryAFT, conn)
        dfAFT.dropna(subset=['CTAAFT'], inplace=True)
        dfDeprec=runSQLQuery(queryDeprec, conn)
        dfDeprec.dropna(subset=['CTADEPREC'], inplace=True)
        
        dictCTAS_AFT = {f"{int(row["erpcode"])}": int(f'{str(row["CTAAFT"]).split('-')[0]}{str(row["CTAAFT"]).split('-')[1] if str(row["CTAAFT"]).split('-')[1] == 0 else ""}') for i, row in dfAFT.iterrows()}
        dictCTAS_DEPREC = {f"{int(row["erpcode"])}": int(f'{str(row["CTADEPREC"]).split('-')[0]}{str(row["CTADEPREC"]).split('-')[1] if str(row["CTADEPREC"]).split('-')[1] == 0 else ""}') for i, row in dfDeprec.iterrows() }
        
        
        #Elimino las categorias que no tienen elementos y por lo tanto no tienen cuentas fijas o depresiacion
        df=df[df.apply(lambda row: str(row['erpcode']) in dictCTAS_AFT.keys(), axis=1)]

        
        df.insert(2, "accounts", "")
        df["accounts"] = [[{"fixed_asset_account": dictCTAS_AFT.get(f'{row['erpcode']}', 0), 
                                "accumulated_depreciation_account": dictCTAS_DEPREC.get(f'{row['erpcode']}', 0)}] for index, row in df.iterrows()]
        
        
        #Elimino las columnas code_init y code_last
        df.drop(['codeGroup', 'codeSubGroup', 'title', 'titleGroup', 'tasa', 'tipo_depresiacion', 'erpcode'], axis=1, inplace=True)

        result["data"] = df.to_dict(orient="records")
            
        return result
 
    
def getFinanceBook(conn: Connection, name:str):
        """
        Este metodo exporta los libros de contabilidad necesarios para los activos fijos
        y son creados a partir de los grupos de Siscont
        """
        query = f"""SELECT [NomGrupCodigo] AS CodeGroup
                                ,[NomSubgCodigo] AS CodeSubGroup
                                ,[NomSubgDescripcion] AS Description
                                FROM [{name}].[dbo].[SAFSubgruposAFT]"""
        
        df = runSQLQuery(query, conn)

        #Inserto la columna number en el Dataframe
        df.insert(0, "finance_book_name", "")
        #Genero los numeros de los subelementos
        df["finance_book_name"] = [f'{row['CodeGroup']}{row['CodeSubGroup'] if int(row['CodeSubGroup']) != 0 else ""} {row["Description"]}' for index, row in df.iterrows()]
        #Elimino las columnas code_init y code_last
        df.drop(['CodeGroup', 'CodeSubGroup', 'Description'], axis=1, inplace=True)

        result = OrderedDict()      
        result["doctype"] = "Finance Book"        
        result["data"] =  df.to_dict(orient="records")

        return result
    
def getDepartment(conn: Connection, name:str):
        
        query=f"""SELECT [AreaCodigo] AS codeArea
                ,[AreaDescrip] AS nameArea            
                ,[AreaDesactivada] AS desactivada
                FROM [{name}].[dbo].[SMGAREASUBAREA]"""
        
        querySub=f"""
                SELECT Area.[AreaCodigo] AS codeArea
                    ,Area.[AreaDescrip] AS nameArea
                    ,SubArea.SareaCodigo AS codeSubArea
                    ,SubArea.SareaDescrip AS nameSubArea
                    ,SubArea.[SAreaDesactivada] AS desactivada
                FROM [{name}].[dbo].[SMGAREASUBAREA] AS Area INNER JOIN [{name}].[dbo].[SMGAREASUBAREA1] AS SubArea ON Area.AreaCodigo=SubArea.AreaCodigo
                """

        df = runSQLQuery(query, conn)

        #Inserto la columna number en el Dataframe
        df.insert(0, "department_name", "")
        #Genero los numeros de los subelementos
        df["department_name"] = [f'{row['codeArea']}-{row['nameArea']}' for index, row in df.iterrows()]
        #Elimino las columnas code_init y code_last
        df.drop(['codeArea', 'nameArea'], axis=1, inplace=True)
                
        result={"doctype": "Department"}        
        result["data"] =  df.to_dict(orient="records")

        df = runSQLQuery(querySub, conn)
        #Inserto la columna number en el Dataframe
        df.insert(0, "department_name", "")
        #Genero los numeros de los subelementos
        df["department_name"] = [f'{row['codeSubArea']}-{row['nameSubArea']}' for index, row in df.iterrows()]
        df.insert(1, "parent_department", "")
        #Genero los numeros de los subelementos
        df["parent_department"] = [f'{row['codeArea']}-{row['nameArea']}' for index, row in df.iterrows()]
        #Elimino las columnas code_init y code_last
        df.drop(['codeArea', 'nameArea', 'codeSubArea', 'nameSubArea'], axis=1, inplace=True)

        result["data"].append(df.to_dict(orient="records"))

        return result

def getLocation(conn: Connection, name:str):
        query = f"""SELECT [NivelDescrip] AS location_name
                           FROM [{name}].[dbo].[SAFNivelRespAreaSubarea]"""

        return createJSON(query, conn, doctype="Location")


def getSAFConfiguracionGeneral(conn: Connection):
        query= """SELECT [AFCGId]
                          ,[AFCGMesUltimaDepreciacion]
                          ,[AFCGAnoUltimaDepreciacion]
                          ,[AFCGUI]
                          ,[AFCGFechaModif]
                          ,[AFCGNivelResp]
                          ,[AFFechacierreApertura]
                          ,[AFNumeroDocumentoAutomatico]
                          ,[AFCGDeprecia]
                          FROM [S5Principal].[dbo].[SAFConfiguracionGeneral]
                          """
        return createJSON(query,conn, doctype="Asset")
    

def getSAFConfiguraCtas(conn: Connection):
        query="""SELECT [AFCuentas]
                        ,[ClCuIdCuenta]
                        ,[TipoCuenta]
                        ,[AFFlagClaveOcioso]
                        ,[AFAnoActiva]
                        ,[AFMesActica]
                        ,[AFRecalculado]
                        FROM [S5Principal].[dbo].[SAFConfiguraCtas] """
        
        return createJSON(query,conn, doctype="Asset")
    
def getSAFTiposOperacion(conn: Connection):
        query = """SELECT [IdInternoTipoOperacion]
                          ,[NClMACodigoTipoOperacion]
                          ,[NClMADescripcionTipoOperacion]
                          FROM [S5Principal].[dbo].[SAFTIPOSOPERACION]"""
        
        return createJSON(query,conn, doctype="Asset")
    
def getSAFNomClavOperacion(conn: Connection):
        query = """SELECT [NCIMACodigoTipoOperacion]
                          ,[NCIMASubCodigoTipoOperacion]
                          ,[NCIMADescripcionClaveOperacion]
                          ,[NCIMAActivo]
                          ,[NCIMAUserIDClaveOperacion]
                          ,[NCIMAFechaModifClaveOperacion]
                          FROM [S5Principal].[dbo].[SAFNomClavOperacion]"""
        
        return createJSON(query,conn, doctype="Asset")
    
def getSAFRelacionClaveTratamiento(conn: Connection):
        query="""SELECT [NCIMACodigoTipoOperacion]
                        ,[NClMASubCodigoTipoOperacion]
                        ,[TratamientoId]
                        FROM [S5Principal].[dbo].[SAFRelacionClaveTratamiento]"""
        
        return createJSON(query,conn, doctype="Asset")

    
def getSAFAsocAreaObjCtoSubelem(conn: Connection):
        query = """SELECT [AFAsocId]
                           ,[AFAreacodigo]
                           ,[AFSareaCodigo]
                           ,[OCostcodigo]
                           ,[CICuIsCuenta]
                           ,[SubelId]
                           ,[AFAsoUI]
                           ,[AFAsoFechaModif]
                           ,[AFAsocActiva]
                           ,[AFAsocDeprecio]
                           FROM [S5Principal].[dbo].[SAFAsocAreaObjCtoSubelem]"""
        
        return createJSON(query,conn, doctype="Asset")
    
def getSAFDocMovClienteFactura(conn: Connection):
        query="""SELECT [DocMovAFNro]
                        ,[NCIMACodigoTipoOperacion]
                        ,[Id-compra]
                        ,[DocAnoOper]
                        ,[CliCodigo]
                        ,[DocNumFactura]
                        ,[DocFechafactura]
                        FROM [S5Principal].[dbo].[SAFDocMovClienteFactura]"""
        
        return createJSON(query,conn, doctype="Asset")
    
def getSAFDocumentoMovimiento(conn: Connection):
        query="""SELECT [DocMovAFNro]
                        ,[NClMACodigoTipoOperacion]
                        ,[DocAnoOper]
                        ,[NClMASubCodigoTipoOperacion]
                        ,[DocMes]
                        ,[DocEstado]
                        ,[DocActContabiliza]
                        ,[DocContabilizado]
                        ,[BajaDocMovAF]
                        ,[DocConComprob]
                        ,[DocMovAFFecha]
                        ,[DocMovFechaAvaluo]
                        ,[DocValorMLC]
                        ,[DocValorMN]
                        ,[DocValorTotal]
                        ,[DocDeprecMLC]
                        ,[DocDeprecMN]
                        ,[DocTasaMoneda]
                        ,[DocCodMoneda]
                        ,[DocValorMEX]
                        ,[DocNoTransaccion]
                        ,[DocUBMCOperac]
                        ,[DocMovCRC]
                        ,[SRHPersonasId]
                        ,[SRHPersDireccionOficial]
                        ,[SRHPersonasDireccionFecha]
                        ,[DocDatosInteres]
                        ,[DocFechaPrestamo]
                        ,[DocFechaPrestamoRegreso]
                        FROM [S5Principal].[dbo].[SAFDocumentoMovimiento]"""
        
        return createJSON(query,conn, doctype="Asset")
    
def getSAFDocMovAltaBaja(conn: Connection):
        query = """SELECT [DocMovAFNro]
                          ,[NClMACodigoTipoOperacion]
                          ,[DocAnoOper]
                          ,[ConsAltaBaja]
                          ,[AFNumInv]
                          ,[AFFechaConfirm]
                          ,[AFConfirm]
                          ,[AFDeprecAcumConfirm]
                          ,[AFValorMNac]
                          ,[AFValorMLConver]
                          ,[AFValorMEXac]
                          ,[AFDeprecMN]
                          ,[AFDeprecMLC]
                          FROM [S5Principal].[dbo].[SAFDocMovAltaBaja]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFDocMovActualizaSC(conn: Connection):
        query = """SELECT [SafActSCNumInv]
                          ,[DocMovAFNro]
                          ,[SAFActSCAnoOper]
                          ,[SAFActSCMesOper]
                          ,[SAFActSCIdCuentaAF]
                          ,[SAFActSCIdCuentaAFAntes]
                          ,[SAFActSCIdCuentaDeprec]
                          ,[SAFActSCIdCuentaDeprecAntes]
                          ,[SAFActSCValorMEX]
                          ,[SAFActSCValorMEXAntes]
                          ,[SAFActSCValorMN]
                          ,[SAFActSCValorMNAntes]
                          ,[SAFActSCValorMLC]
                          ,[SAFActSCValorMLCAntes]
                          ,[SAFActSCDeprecMN]
                          ,[SAFActSCDeprecMNAntes]
                          ,[SAFActSCDeprecMLC]
                          ,[SAFActSCDeprecMLCAntes]
                          ,[SAFActSCSubTipoMov]
                          FROM [S5Principal].[dbo].[SAFDocMovActualizaSC]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFDocMovTrasladoAreaSubANivelR(conn: Connection):
        
        query = """SELECT [DocMovAFNro]
                          ,[NClMACodigoTipoOperacion]
                          ,[DocAnoOper]
                          ,[ConsTraslado]
                          ,[AFNumInv]
                          ,[AreaOrigen]
                          ,[AreaDestino]
                          ,[SubAreaOrigen]
                          ,[SubAreaDestino]
                          ,[NivelResponOrigen]
                          ,[NivelResponDestino]
                          ,[ObjcostoOrigen]
                          ,[ObjCostoDestino]
                          ,[TraslDeprecio]
                          ,[IdcuentaAFOrigen]
                          ,[IdCuentaAFDestino]
                          ,[TraslValorAF]
                          FROM [S5Principal].[dbo].[SAFDocMovTrasladoAreaSubANivelR]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFDocMovTrasladoReparAlquiler(conn: Connection):
        
        query = """SELECT [DocMovAFNro]
                          ,[NClMACodigoTipoOperacion]
                          ,[DocAnoOper]
                          ,[ConsRepAlq]
                          ,[AFNumInv]
                          ,[FechaTraslado]
                          ,[TrabajadorPrestamo]
                          FROM [S5Principal].[dbo].[SAFDocMovTrasladoReparAlquiler]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFPartidaDocuMovi(conn: Connection):
        
        query = """SELECT [SafPartNumIv]
                          ,[DocMovAFNro]
                          ,[NClMACodigoTipoOperacion]
                          ,[SafPartAnoOper]
                          ,[SafPartValorFinal]
                          ,[SafPartMesOper]
                          ,[SafPartValorIni]
                          ,[SafPartDepreIni]
                          ,[SafPartDepreFinal]
                          ,[SafPartTasaDepre]
                          ,[SafPartTasaDepreAntes]
                          ,[SafPartTasaDepreUni]
                          ,[SafPartTasaDepreUniAntes]
                          ,[SafPartGrupo]
                          ,[SafPartGrupoAntes]
                          ,[SafPartSubGrupo]
                          ,[SafPartSubGrupoAntes]
                          ,[SafPartDesc]
                          ,[SafPartDescAntes]
                          FROM [S5Principal].[dbo].[SAFPartidaDocuMovi]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFContabilizacion(conn: Connection):
        
        query = """SELECT [Contad]
                          ,[DocMovAFNro]
                          ,[ContIdComp]
                          ,[ContAnoOp]
                          ,[ContTipoTrat]
                          ,[ContTipoMov]
                          ,[ContClaveOperacion]
                          ,[ContTipoContab]
                          ,[ContActivo]
                          ,[ContUI]
                          ,[ContFechaModif]
                          ,[ContIdCompPatrimonial]
                          FROM [S5Principal].[dbo].[SAFContabilizacion]"""
        
        return createJSON(query, conn, doctype="Asset")
    

def getSAFContabilizacion1(conn: Connection):
        
        query = """SELECT [Contad]
                          ,[ContPartId]
                          ,[ClcuIDCuenta]
                          ,[ContPartGasto]
                          ,[ContPartPropia]
                          ,[ContPartImp]
                          ,[ContPartNat]
                          ,[OCostCodigo]
                          ,[OCostCentroB]
                          ,[ContPartUI]
                          ,[ContPartFechModif]
                          FROM [S5Principal].[dbo].[SAFContabilizacion1]"""
        
        return createJSON(query, conn, doctype="Asset")

def getSAFGruposAFT(conn: Connection):
        
        query = """SELECT [NomGrupCodigo]
                          ,[NomgrupDescripcion]
                          ,[NomGrupActivo]
                          ,[NomGrupAgrupacion]
                          ,[NomGrupDeprecia]
                          ,[NomGrupTasaEmpresa]
                          ,[NomGrupTasaUp]
                          ,[NomGrupTipoDepreciacion]
                          ,[NomGrupUsaSubgrupo]
                          FROM [S5Principal].[dbo].[SAFGruposAFT]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFSubgruposAFT(conn: Connection):
        
        query = """SELECT [NomGrupCodigo]
                          ,[NomSubgCodigo]
                          ,[NomSubgActivo]
                          ,[NomSubgDeprecia]
                          ,[NomSubgDescripcion]
                          ,[NomSubgTasaEmpresa]
                          ,[NomSubgTasaUP]
                          ,[NomSubgTipoDepreciacion]
                          FROM [S5Principal].[dbo].[SAFSubgruposAFT]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFSubmayorDepre(conn: Connection):
        
        query = """SELECT [SafNumInv]
                           ,[SafAnoOper]
                           ,[SafFechaActual]
                           ,[SafTipoMetodo]
                           ,[SafDepreInicial]
                           ,[SafDepreAcumulada]
                           ,[SafMesDepre1]
                           ,[SafMesDepre2]
                           ,[SafMesDepre3]
                           ,[SafMesDepre4]
                           ,[SafMesDepre5]
                           ,[SafMesDepre6]
                           ,[SafMesDepre7]
                           ,[SafMesDepre8]
                           ,[SafMesDepre9]
                           ,[SafMesDepre10]
                           ,[SafMesDepre11]
                           ,[SafMesDepre12]
                           ,[SafTasaMesDepre1]
                           ,[SafTasaMesDepre2]
                           ,[SafTasaMesDepre3]
                           ,[SafTasaMesDepre4]
                           ,[SafTasaMesDepre5]
                           ,[SafTasaMesDepre6]
                           ,[SafTasaMesDepre7]
                           ,[SafTasaMesDepre8]
                           ,[SafTasaMesDepre9]
                           ,[SafTasaMesDepre10]
                           ,[SafTasaMesDepre11]
                           ,[SafTasaMesDepre12]
                           ,[SafIdUsuario]
                           ,[SafFechaModif]
                           ,[SafAreaCodigo]
                           ,[SafSubAreaCodigo]
                           ,[SafObjetoCosto]
                           ,[SafClCuIdCuentaGasto]
                           ,[SafClCuIdCuentaDeprec]
                           ,[SafSubElementoCodigo]
                           ,[SafSubDepreCRC]
                           FROM [S5Principal].[dbo].[SAFSubmayorDepre]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFSubTasaMayor(conn: Connection):
        
        query = """SELECT [SafTasaNumInv]
                          ,[SafTasaAnoOper]
                          ,[SafTasaTipoMetodo]
                          ,[SafTasaAcumulado]
                          ,[SafTasaInicial]
                          ,[SafTasaFechaActual]
                          ,[SafTasaFechaModif]
                          ,[SafTasaIdUsuario]
                          ,[SafMesDepre1TasaMayor]
                          ,[SafMesDepre2TasaMayor]
                          ,[SafMesDepre3TasaMayor]
                          ,[SafMesDepre4TasaMayor]
                          ,[SafMesDepre5TasaMayor]
                          ,[SafMesDepre6TasaMayor]
                          ,[SafMesDepre7TasaMayor]
                          ,[SafMesDepre8TasaMayor]
                          ,[SafMesDepre9TasaMayor]
                          ,[SafMesDepre10TasaMayor]
                          ,[SafMesDepre11TasaMayor]
                          ,[SafMesDepre12TasaMayor]
                          ,[SafTasaArea]
                          ,[SafTasasubarea]
                          ,[SafTasaObjetoCosto]
                          ,[SafTasaIdCuentaGasto]
                          ,[SafTasaIdCuentaDepre]
                          ,[SafTasaSubelCodigo]
                          FROM [S5Principal].[dbo].[SAFSubTasaMayor]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFContabilidadDepreciacion(conn: Connection):
        
        query = """SELECT [ContabDepreId]
                          ,[ContabDepreTipoCompro]
                          ,[ContabDepreIdComp]
                          ,[ContabDepreMesOper]
                          ,[ContabDepreAnoOper]
                          ,[ContabDepreTerminado]
                          ,[ContabDepreActivo]
                          ,[ContabDepreUI]
                          ,[ContabDepreFechaMod]
                          FROM [S5Principal].[dbo].[SAFContabilidadDepreciacion]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFContabilidadDepreciacion1(conn: Connection):
        
        query = """SELECT [ContabDepreId]
                          ,[ContDeprecId]
                          ,[ClcuIDCuenta]
                          ,[ContDeprecPartId]
                          ,[ContDeprecGasto]
                          ,[ContDeprecImporte]
                          ,[ContDeprecNaturaleza]
                          ,[ContDeprecCeBe]
                          ,[OCostcodigo]
                          ,[ContDeprecPropia]
                          ,[ContDeprecUI]
                          ,[ContDeprecFechaMod]
                          FROM [S5Principal].[dbo].[SAFContabilidadDepreciacion1]"""
        
        return createJSON(query, conn, doctype="Asset")

def getSAFSaldosCuentas(conn: Connection):
        
        query = """SELECT [AFSaldoId]
                          ,[ClcuIDCuenta]
                          ,[AFSaldoInicial]
                          ,[AFSaldoEnero]
                          ,[AFSaldofebrero]
                          ,[AFSaldomarzo]
                          ,[AFSaldoAbril]
                          ,[AFSaldoMayo]
                          ,[AFSaldoJunio]
                          ,[AFSaldoJulio]
                          ,[AFSaldoAgosto]
                          ,[AFSaldoseptiembre]
                          ,[AFSaldoOctubre]
                          ,[AFSaldoNoviembre]
                          ,[AFSaldoDiciembre]
                          ,[AFSaldoUI]
                          ,[AFSaldoFechaModif]
                          ,[AFSaldoAnio]
                          FROM [S5Principal].[dbo].[SAFSaldosCuentas]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFSEGCHK(conn: Connection):
        
        query = """SELECT [AFEntiId]
                          ,[AFTabla0]
                          ,[AFTabla1]
                          ,[AFTabla2]
                          ,[AFTabla3]
                          ,[AFTabla4]
                          ,[AFTabla5]
                          ,[AFTabla6]
                          ,[AFTabla7]
                          ,[AFTabla8]
                          ,[AFTabla9]
                          FROM [S5Principal].[dbo].[SAFSEGCHK]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFSaldoInicial(conn: Connection):
        
        query = """SELECT [SIAfNumInv]
                          ,[SIAnoOperacion]
                          ,[SIIdcuentaAF]
                          ,[SIIdcuentaDep]
                          ,[SIValorAFMN]
                          ,[SIValorAFMLC]
                          ,[SIValorDepTotal]
                          FROM [S5Principal].[dbo].[SAFSaldoInicial]"""

        return createJSON(query, conn, doctype="Asset")
    
def getSAFRecalculo(conn: Connection):
        
        query = """SELECT [RecalId]
                          ,[ClCuIdcuenta]
                          ,[AFNumInv]
                          ,[RecalAnio]
                          ,[RecalMes]
                          ,[RecalTasa]
                          ,[RecalCoeficiente]
                          ,[RecalPorciento]
                          ,[RecalVariacion]
                          ,[RecalTipo]
                          ,[RecalAFValorAnterior]
                          ,[RecalAFValorNoRecalculado]
                          ,[RecalAFValorRecalculado]
                          ,[RecalAFVariacion]
                          ,[RecalAFValorFinal]
                          ,[RecalAFValorDepAnt]
                          ,[RecalAFValorDepNoRecal]
                          ,[RecalAFValorDepRecal]
                          ,[RecalAFValorDepVar]
                          ,[RecalAFValorDeprec]
                          FROM [S5Principal].[dbo].[SAFRecalculo]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFCuentasTemporal(conn: Connection):
        
        query = """SELECT [clcuidCuentaTmp]
                          ,[clCuCuentaTmp]
                          ,[clCuSubcuentaTmp]
                          ,[clCuSubcontrolTmp]
                          ,[clCuAnalisisTmp]
                          ,[DescripcionCuentaTmp]
                          FROM [S5Principal].[dbo].[SAFCuentasTemporal]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFTrnActivosFijosTemporales(conn: Connection):
        
        query = """SELECT [AFTemporalNumInv]
                          ,[AFNumInvTMP]
                          FROM [S5Principal].[dbo].[SAFTrnActivosFijosTemporales]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFTrnComponenteTemporales(conn: Connection):
        
        query = """SELECT [AFNumInvTMP]
                           ,[AFNumCompTemp]
                           ,[DescripcionCompTMP]
                           ,[CantidadCompTMP]
                           ,[ValorMLCCompTmp]
                           ,[ValorMnComptmp]
                           ,[ValortotalCompTMP]
                           ,[DepAcumMLCCompTMP]
                           ,[DepAcumMNCompTMP]
                           ,[DepAcumTotalCompTMP]
                           ,[AFNumImvConversion]
                           ,[UnificadoTMP]
                           ,[ValorMEXCompTmp]
                           FROM [S5Principal].[dbo].[SAFTrnComponenteTemporales]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFAFComponentesTemporal(conn: Connection):
        
        query = """SELECT [NumInvTmp]
                          ,[DocMovAFNroTMP]
                          ,[AFValorMNTMP]
                          ,[AFValorMLCTMP]
                          ,[AFDeprecMNTMP]
                          ,[AFDeprecMLCTMP]
                          ,[AFValorTotalTMP]
                          ,[AFDeprecTotalTMP]
                          FROM [S5Principal].[dbo].[SAFAFComponentesTemporal]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFTrnGrupoSubgrupoTemporal(conn: Connection):
        
        query = """SELECT [NomGrupCodigoTmp]
                          ,[NomSubgCodigoTemp]
                          ,[DescripcionGrupoSubgrupo]
                          FROM [S5Principal].[dbo].[SAFTrnGrupoSubgrupoTemporal]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFCompActualizaSC(conn: Connection):
        
        query = """SELECT [AFNumcomp]
                          ,[SafPartNumIv]
                          ,[DocMovAFNro]
                          ,[NClMACodigoTipoOperacion]
                          ,[CantidadCompAntes]
                          ,[CantidadCompfinal]
                          ,[ValorMNCompAntes]
                          ,[ValorMNCompFinal]
                          ,[ValorMLCCompAntes]
                          ,[ValorMLCCompfinal]
                          ,[ValorMEXCompAntes]
                          ,[ValorMEXCompFinal]
                          ,[DescripcionCompBaja]
                          ,[AfNumInvConversionBaja]
                          ,[UnificadoCompBaja]
                          ,[DeprecAcumMLCAntes]
                          ,[DeprecAcumMNAntes]
                          ,[DeprecAcumTotalAntes]
                          FROM [S5Principal].[dbo].[SAFTEAFCompRechazado]"""
        
        return createJSON(query, conn, doctype="Asset")
    
def getSAFConfigPartidasGub(conn: Connection):
        
        query = """SELECT [ConfPGId]
                          ,[ConfPGUI]
                          ,[ConfPGFechaModif]
                          ,[PartGCodigo]
                          FROM [S5Principal].[dbo].[SAFConfigPartidasGub]"""
        
        return createJSON(query, conn, doctype="Asset")
    

def getSAFRelacAreaGub(conn: Connection):
        
        query = """SELECT [AreaCodigo]
                          ,[ClcuIDCuenta]
                          ,[RelacCuenta]
                          ,[RelacSubcuenta]
                          ,[RelacSubctrol]
                          ,[RelacUI]
                          ,[RelacActivo]
                          ,[RelacFechaModif]
                          FROM [S5Principal].[dbo].[SAFRelacAreaGub]"""
        
        return createJSON(query, conn, doctype="Asset")


    