from fastapi import APIRouter, HTTPException
from db.db_connection import create_db_managerAlchemy
from db.db_manager import ConexionParams
from collections import OrderedDict
from db import db_activos_fijos as activos_fijos
from fastapi.responses import JSONResponse, StreamingResponse
from io import StringIO, BytesIO
import json
import io

import logging

router = APIRouter()

@router.post("/apertura",
    summary="Submayor de los Medios Básicos",
    description="Muestra listado del Submayor de los Medios Básicos",
    tags=["Activos Fijos"],
)
async def get_af_apertura(params: ConexionParams):    
    try:        
        with create_db_managerAlchemy(params) as db:           
            data= activos_fijos.getSAFApertura(db, params.database)
            return JSONResponse(content=data)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener SAFApertura: {e}",
        )
        

@router.post("/apertura_csv", 
    summary="Submayor de los Medios Básicos",
    description="Muestra listado del Submayor de los Medios Básicos",
    tags=["Activos Fijos"],
    )
async def get_af_apertura_csv(params: ConexionParams):
    try:
        with create_db_managerAlchemy(params) as db:
            
            # Obtener datos como lista de dicts
            df = activos_fijos.getSAFAperturaCSV(db, params.database)
            
            # Usar BytesIO para mejor performance
            stream = BytesIO()
            df.to_csv(stream, index=False, encoding='utf-8')
            stream.seek(0)  # Volver al inicio del stream

            return StreamingResponse(
                stream,
                media_type="text/csv",
                headers={
                   "Content-Disposition": "attachment; filename=datos.csv",                    
                }
            )
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SCPPAGODOCUMENTO: {str(e)}",
        )


@router.post("/categorias",
    summary="Categorias de los Activos Fijos",
    description="Muestra de las Categorias de los Activos Fijos",
    tags=["Activos Fijos"],
)
async def get_af_categoria(params: ConexionParams):   
    print("Categorias") 
    try:
        with create_db_managerAlchemy(params) as db:
            data= activos_fijos.getCategoryAF(db, params.database)
            return JSONResponse(content=data)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener SAFGruposAFT o SAFSubgruposAFT: {e}",
        )
        

@router.post("/libro_finanzas",
    summary="Libros de Finanzas",
    description="Muestra los libros de finanzas",
    tags=["Activos Fijos"],
)
async def get_af_libros_finanzas(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data= activos_fijos.getFinanceBook(db, params.database)            
            return JSONResponse(content=data)
    except Exception as e:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener SAFGruposAFT o SAFSubgruposAFT: {e}",
        )
        
        
@router.post("/location",
    summary="Las posibles localizaciones de los activos fijos.",
    description="Muestra el relacionador del Area/Subarea con Nivel de Responsabilidad.",
    tags=["Activos Fijos"],
)
async def getLocation(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getLocation(db, params.database)
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFNivelRespAreaSubarea",
        )
    
 
@router.post("/department",
    summary="Departamentos",
    description="Muestra los Departamentos",
    tags=["Activos Fijos"],
)
async def get_department(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getDepartment(db, params.database)
            return JSONResponse(content=data)
    except Exception as e:
         logging.error(f"Error al obtener SAFGruposAFT o SAFSubgruposAFT: {e}")
         raise Exception(f"Error al obtener datos de SAFGruposAFT o SAFSubgruposAFT: {str(e)}")
 

@router.post("/configuracion_general",
    summary="Configuracion General del subsistema de Activos Fijos",
    description="Muestra la configuracion general del subsistema de Activos Fijos",
    tags=["Activos Fijos"],
)
async def get_af_configuracion_general(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFConfiguracionGeneral(db)
            return JSONResponse(content=data)
    except Exception as e:
         logging.error(f"Error al obtener SAFConfiguracionGeneral: {e}")
         raise Exception(f"Error al obtener datos de SAFConfiguracionGeneral: {str(e)}")
    

@router.post("/configuracion_cuentas",
    summary="Configuración General de Cuentas de Activo Fijo y Depreciación.",
    description="Muestra la configuración General de Cuentas de Activo Fijo y Depreciación.",
    tags=["Activos Fijos"],
)
async def get_af_configuracion_cuentas(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFConfiguraCtas(db)
            return JSONResponse(content=data)
    except Exception as e:
         logging.error(f"Error al obtener SAFConfiguraCtas: {e}")
         raise Exception(f"Error al obtener datos de SAFConfiguraCtas: {str(e)}")
     

        
    
@router.post("/tipos_operacion",
    summary="Nomenclador de los tipos de operación en los movimientos.",
    description="Muestra los nomencladores de los tipos de operación en los movimientos.",
    tags=["Activos Fijos"],
)
async def get_af_tipos_operacion(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFTiposOperacion(db)            
            return JSONResponse(content=data)
    except Exception as e:
         logging.error(f"Error al obtener SAFTiposOperacion: {e}")
         raise Exception(f"Error al obtener datos de SAFTiposOperacion: {str(e)}")
     

    

@router.post("/nom_clav_operacion",
    summary="Nomenclador de las Claves de Operación",
    description="Muestra los nomencladores de las Claves de Operación",
    tags=["Activos Fijos"],
)
async def get_af_nom_clav_operacion(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFNomClavOperacion(db)
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFNomClavOperacion",
        )
    

@router.post("/rel_clave_trat",
    summary="Tratamiento para las Claves de Operación que contabilizan.",
    description="Muestra el tratamiento para las Claves de Operación que contabilizan.",
    tags=["Activos Fijos"],
)
async def get_af_rel_clave_trat(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFRelacionClaveTratamiento()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFNomClavOperacion",
        )
    


@router.post("/asoc_area_obj_costo",
    summary="Asociación del Area/Subarea con el Objeto de Costo.",
    description="Muestra la asociación del Area/Subarea con el Objeto de Costo",
    tags=["Activos Fijos"],
)
async def get_af_asoc_area_obj_costo(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFAsocAreaObjCtoSubelem()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFAsocAreaObjCtoSubelem",
        )
    

@router.post("/doc_mov_cliente_factura",
    summary="Datos de la factura en movimiento de Alta, Baja o Actualización.",
    description="Muestra los datos de la factura en movimiento de Alta, Baja o Actualización.",
    tags=["Activos Fijos"],
)
async def get_af_doc_mov_cliente_factura(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFDocMovClienteFactura()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFDocMovClienteFactura",
        )
    
@router.post("/documento_movimiento",
    summary="Movimientos para todo Tipo de Operación",
    description="Muestra los movimientos para todo Tipo de Operación",
    tags=["Activos Fijos"],
)
async def get_af_doc_mov_cliente_factura(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFDocumentoMovimiento()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFDocumentoMovimiento",
        )
    

@router.post("/doc_mov_alta_baja",
    summary="Movimientos de las Altas y Bajas por Activo Fijo",
    description="Muestra los movimientos de las Altas y Bajas por Activo Fijo",
    tags=["Activos Fijos"],
)
async def get_af_doc_mov_doc(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFDocMovAltaBaja()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFDocMovAltaBaja",
        )
    

@router.post("/doc_mov_actualiza_sc",
    summary="Movimiento de Actualizaciones de cambio de cuenta o valor por Activo Fijo.",
    description="Muestra los movimientos de Actualizaciones de cambio de cuenta o valor por Activo Fijo.",
    tags=["Activos Fijos"],
)
async def get_af_doc_mov_actualiza_sc(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFDocMovActualizaSC()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFDocMovActualizaSC",
        )
    

@router.post("/doc_mov_traslado",
    summary="Movimiento de Traslado de Cambio de Area/Subarea/Nivel  por AF, o traslado de contabiliza para las compras.",
    description="Muestra los movimientos de Traslado de Cambio de Area/Subarea/Nivel  por AF, o traslado de contabiliza para las compras.",
    tags=["Activos Fijos"],
)
async def get_af_doc_mov_traslado(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFDocMovTrasladoAreaSubANivelR()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFDocMovTrasladoAreaSubANivelR",
        )
    

@router.post("/doc_mov_traslado_repair",
    summary="Movimiento de Traslado por Reparación o Alquiler del AF ",
    description="Muestra los movimientos de Traslado por Reparación o Alquiler del AF ",
    tags=["Activos Fijos"],
)
async def get_af_doc_mov_repair(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFDocMovTrasladoReparAlquiler()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFDocMovTrasladoReparAlquiler",
        )
    

@router.post("/partida_doc_mov",
    summary="Movimiento de Cambio de valor por Avalúo o Actualizaciones que no contabilizan por cambio de grupo/subgrupo,  tasa o descripción.",
    description="Muestra los movimientos de Cambio de valor por Avalúo o Actualizaciones que no contabilizan por cambio de grupo/subgrupo,  tasa o descripción.",
    tags=["Activos Fijos"],
)
async def get_af_doc_partida_doc_mov(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFPartidaDocuMovi()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFPartidaDocuMovi",
        )
    
@router.post("/contabilizacion",
    summary="Contabilización de los Movimientos de Alta, Baja, Traslado para las compras, Avalúo  y Actualizaciones de cambio de cuenta o valor.",
    description="Muestra la contabilización de los Movimientos de Alta, Baja, Traslado para las compras, Avalúo  y Actualizaciones de cambio de cuenta o valor.",
    tags=["Activos Fijos"],
)
async def get_af_contabilizacion(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFContabilizacion()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFContabilizacion",
        )
    

@router.post("/grupo_aft",
    summary="Nomenclador de Grupos de Activos Fijos.",
    description="Muestra los nomencladores de Grupos de Activos Fijos.",
    tags=["Activos Fijos"],
)
async def get_af_contabilizacion(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFGruposAFT()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFGruposAFT",
        )
   

@router.post("/subgrupo_aft",
    summary="Nomenclador de Subgrupos.",
    description="Muestra los nomencladores de Subgrupos",
    tags=["Activos Fijos"],
)
async def get_af_contabilizacion(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFSubgruposAFT()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFSubgruposAFT",
        )

@router.post("/submayor_depre",
    summary="Submayor por mes y año del cálculo de la Depreciación.",
    description="Muestra el submayor por mes y año del cálculo de la Depreciación.",
    tags=["Activos Fijos"],
)
async def get_af_contabilizacion(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFSubmayorDepre()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFSubmayorDepre",
        )
    

@router.post("/subtasa_mayor",
    summary="Submayor por mes y año del cálculo de la Depreciación cuando la tasa es mayor que la permisible.",
    description="Muestra el submayor por mes y año del cálculo de la Depreciación cuando la tasa es mayor que la permisible.",
    tags=["Activos Fijos"],
)
async def get_af_subtasa_mayor(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFSubTasaMayor()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFSubTasaMayor",
        )
    
@router.post("/contab_depre",
    summary="Contabilización del cálculo de la Depreciación.",
    description="Muestra la contabilización del cálculo de la Depreciación.",
    tags=["Activos Fijos"],
)
async def get_af_contab_depre(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFContabilidadDepreciacion()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFContabilidadDepreciacion",
        )
    

@router.post("/contab_depre1",
    summary="Contabilización del cálculo de la Depreciación.",
    description="Muestra la contabilización del cálculo de la Depreciación.",
    tags=["Activos Fijos"],
)
async def get_af_contab_depre1(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFContabilidadDepreciacion1()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFContabilidadDepreciacion1",
        )



@router.post("/saldos_cuentas",
    summary="Saldos de las cuentas parametrizadas al Cierre de Mes.",
    description="Muestra de saldos de las cuentas parametrizadas al Cierre de Mes.",
    tags=["Activos Fijos"],
)
async def get_af_saldos_cuentas(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFSaldosCuentas()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFSaldosCuentas",
        )
    
@router.post("/checksum",
    summary="Se utiliza para guardar chequeos de ChekSum",
    description="Se utiliza para guardar chequeos de ChekSum",
    tags=["Activos Fijos"],
)
async def get_af_saldos_cuentas(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFSEGCHK()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFSaldosCuentas",
        )
    

@router.post("/saldo_incial",
    summary="Saldo inicial de los Activos Fijos al cierre de la apertura y al cierre del año.",
    description="Saldo inicial de los Activos Fijos al cierre de la apertura y al cierre del año.",
    tags=["Activos Fijos"],
)
async def get_af_saldos_cuentas(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFSaldoInicial()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFSaldoInicial",
        )
    

@router.post("/recalculo",
    summary="Saldos calculados por Activos  en el mes y año que se efectúa el recálculo por opción o por única vez en la unificación monetaria.",
    description="Saldos calculados por Activos  en el mes y año que se efectúa el recálculo por opción o por única vez en la unificación monetaria.",
    tags=["Activos Fijos"],
)
async def get_af_saldos_cuentas(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFRecalculo()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFRecalculo",
        )
    

@router.post("/cuentas_temporal",
    summary="Tabla temporal de cuentas parametrizadas.",
    description="Tabla temporal de cuentas parametrizadas.",
    tags=["Activos Fijos"],
)
async def get_af_saldos_cuentas(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFCuentasTemporal()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFCuentasTemporal",
        )


@router.post("/activos_fijos",
    summary="Tabla temporal de cuentas parametrizadas.",
    description="Tabla temporal de cuentas parametrizadas.",
    tags=["Activos Fijos"],
)
async def get_af_activos_temporales(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFTrnActivosFijosTemporales()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFTrnActivosFijosTemporales",
        )
    

@router.post("/componentes_temp",
    summary="Tabla temporal para la unificación de componentes.",
    description="Tabla temporal para la unificación de componentes.",
    tags=["Activos Fijos"],
)
async def get_af_activos_temporales(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFTrnComponenteTemporales()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFTrnComponenteTemporales",
        )
    
@router.post("/componentes_temp",
    summary="Tabla temporal en la creación de las componentes de Unidad Básica o Modulo de Control.",
    description="Tabla temporal en la creación de las componentes de Unidad Básica o Modulo de Control.",
    tags=["Activos Fijos"],
)
async def get_af_componentes_temporales(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFAFComponentesTemporal()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFAFComponentesTemporal",
        )
    

@router.post("/grupo_temporal",
    summary="Tabla temporal para los Grupos/subgrupos.",
    description="Tabla temporal para los Grupos/subgrupos.",
    tags=["Activos Fijos"],
)
async def get_af_componentes_temporales(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFTrnGrupoSubgrupoTemporal()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFTrnGrupoSubgrupoTemporal",
        )
    

@router.post("/comp_actualiza_sc",
    summary="Actualización en Cambio de cuenta, Cambio de valor o Avalúo de las componentes de Unidad Básica o Modulo de Control.",
    description="Actualización en Cambio de cuenta, Cambio de valor o Avalúo de las componentes de Unidad Básica o Modulo de Control.",
    tags=["Activos Fijos"],
)
async def get_af_comp_actualiza(params: ConexionParams):    
    try:
        with create_db_managerAlchemy(params) as db:
            data=activos_fijos.getSAFCompActualizaSC()
            return JSONResponse(content=data)
    except:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFCompActualizaSC",
        )
    
@router.post("/comp_rechazado",
    summary="Cuando se rechaza un comprobante verifica si el Activo fijo se encuentra en otro movimiento posterior y lo guarda en la tabla temporal para imprimir.",
    description="Cuando se rechaza un comprobante verifica si el Activo fijo se encuentra en otro movimiento posterior y lo guarda en la tabla temporal para imprimir.",
    tags=["Activos Fijos"],
)
async def get_af_comp_rechazado(params: ConexionParams):    
    if not activos_fijos.connection is None:
        data=activos_fijos.getSAFTEAFCompRechazado()
        return JSONResponse(content=data)
    else:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFTEAFCompRechazado",
        )
    

@router.post("/conf_partida",
    summary="Configuración de la Partida gubernamental para la Depreciación cuando es Unidad Presupuestada.",
    description="Configuración de la Partida gubernamental para la Depreciación cuando es Unidad Presupuestada.",
    tags=["Activos Fijos"],
)
async def get_af_conf_partida(params: ConexionParams):    
    if not activos_fijos.connection is None:
        data=activos_fijos.getSAFConfigPartidasGub()
        return JSONResponse(content=data)
    else:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFConfigPartidasGub",
        )
    

@router.post("/relac_area",
    summary="Relacionador Area/Nivel de Actividad/Grupo Presupuestario para UP.",
    description="Relacionador Area/Nivel de Actividad/Grupo Presupuestario para UP.",
    tags=["Activos Fijos"],
)
async def get_af_relac_partida(params: ConexionParams):    
    if not activos_fijos.connection is None:
        data=activos_fijos.getSAFRelacAreaGub()
        return JSONResponse(content=data)
    else:
         raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos de SAFRelacAreaGub",
        )