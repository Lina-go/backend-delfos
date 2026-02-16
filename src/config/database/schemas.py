"""Database table schema definitions."""

from src.config.constants import ColumnType
from src.config.database.types import ColumnInfo, TableInfo

DATABASE_TABLES: dict[str, TableInfo] = {
    # =========================================================================
    # DIMENSION TABLES
    # =========================================================================
    "gold.banco": TableInfo(
        table_name="gold.banco",
        table_description="Dimension de entidades financieras colombianas. Contiene datos maestros de bancos, corporaciones, companias de financiamiento y cooperativas. Usar en JOINs via ID_ENTIDAD.",
        table_columns=[
            ColumnInfo(
                column_name="TIPO_ENTIDAD",
                column_type=ColumnType.STRING,
                column_description="Codigo del tipo de entidad (ej: '1', '4', '22', '32'). Es varchar, NO int.",
            ),
            ColumnInfo(
                column_name="CODIGO_ENTIDAD",
                column_type=ColumnType.INTEGER,
                column_description="Codigo numerico unico de la entidad financiera.",
            ),
            ColumnInfo(
                column_name="NOMBRE_ENTIDAD",
                column_type=ColumnType.STRING,
                column_description="Nombre de la entidad financiera. Nombres INCONSISTENTES: algunos con 'S.A.' (ej: 'Banco de Bogota S.A.'), otros sin (ej: 'AV Villas', 'Banco Popular'). SIEMPRE usar get_distinct_values para verificar nombres exactos.",
            ),
            ColumnInfo(
                column_name="ID_ENTIDAD",
                column_type=ColumnType.STRING,
                column_description="PK. Identificador compuesto tipo+codigo (ej: '139', '4128'). FK en tablas de hechos.",
            ),
            ColumnInfo(
                column_name="NOMBRE_TIPO_ENTIDAD",
                column_type=ColumnType.STRING,
                column_description="Nombre del tipo de entidad: 'ESTABLECIMIENTOS BANCARIOS', 'CORPORACIONES FINANCIERAS', 'COMPANIAS DE FINANCIAMIENTO', 'INSTITUCIONES OFICIALES ESPECIALES', 'ENTIDADES COOPERATIVAS DE CARACTER FINANCIERO'.",
            ),
        ],
    ),
    "gold.fecha": TableInfo(
        table_name="gold.fecha",
        table_description="Dimension temporal. Contiene todas las fechas con atributos de calendario. Usar en JOINs via FECHA_CORTE para obtener nombre_mes, nombre_dia, etc.",
        table_columns=[
            ColumnInfo(
                column_name="date",
                column_type=ColumnType.DATE,
                column_description="Fecha como tipo date nativo.",
            ),
            ColumnInfo(
                column_name="FECHA_CORTE",
                column_type=ColumnType.STRING,
                column_description="PK. Fecha en formato texto 'ddMMyyyy' (ej: '31012026'). FK en tablas de hechos.",
            ),
            ColumnInfo(
                column_name="year",
                column_type=ColumnType.INTEGER,
                column_description="Ano extraido de la fecha.",
            ),
            ColumnInfo(
                column_name="month",
                column_type=ColumnType.INTEGER,
                column_description="Mes extraido de la fecha (1-12).",
            ),
            ColumnInfo(
                column_name="day",
                column_type=ColumnType.INTEGER,
                column_description="Dia del mes.",
            ),
            ColumnInfo(
                column_name="nombre_mes",
                column_type=ColumnType.STRING,
                column_description="Nombre del mes en espanol (enero, febrero, ..., diciembre).",
            ),
            ColumnInfo(
                column_name="nombre_dia",
                column_type=ColumnType.STRING,
                column_description="Nombre del dia de la semana en espanol (lunes, martes, ...).",
            ),
        ],
    ),
    # =========================================================================
    # FACT TABLES
    # =========================================================================
    "gold.tasas_interes_captacion": TableInfo(
        table_name="gold.tasas_interes_captacion",
        table_description="Tasas de captacion reportadas por entidades financieras colombianas a la Superintendencia Financiera. Contiene tasas de interes para CDT, CDAT, cuentas de ahorro y operaciones del mercado monetario. JOIN con gold.banco via ID_ENTIDAD, con gold.fecha via FECHA_CORTE.",
        table_columns=[
            ColumnInfo(
                column_name="ID_ENTIDAD",
                column_type=ColumnType.STRING,
                column_description="FK -> banco.ID_ENTIDAD. Identificador de la entidad financiera.",
            ),
            ColumnInfo(
                column_name="TIPO_ENTIDAD",
                column_type=ColumnType.STRING,
                column_description="Codigo del tipo de entidad financiera (varchar).",
            ),
            ColumnInfo(
                column_name="CODIGO_ENTIDAD",
                column_type=ColumnType.INTEGER,
                column_description="Codigo numerico de la entidad.",
            ),
            ColumnInfo(
                column_name="NOMBRE_ENTIDAD",
                column_type=ColumnType.STRING,
                column_description="Nombre de la entidad financiera. SIEMPRE verificar con get_distinct_values — los nombres son inconsistentes.",
            ),
            ColumnInfo(
                column_name="FECHA_CORTE",
                column_type=ColumnType.STRING,
                column_description="FK -> fecha.FECHA_CORTE. Fecha de reporte en formato 'ddMMyyyy'.",
            ),
            ColumnInfo(
                column_name="CODIGO_CATEGORIA",
                column_type=ColumnType.INTEGER,
                column_description="Codigo de categoria de producto de captacion: 1=CDT, 2=CDAT, 3=Operaciones Mercado Monetario, 4=Interbancarios, 5=Repos, 7=Cuentas de Ahorro, 8=Cuentas Corrientes.",
            ),
            ColumnInfo(
                column_name="DESCRIPCION_CATEGORIA",
                column_type=ColumnType.STRING,
                column_description="Descripcion del producto de captacion. Usar get_distinct_values para ver valores exactos.",
            ),
            ColumnInfo(
                column_name="CODIGO_SUBCUENTA",
                column_type=ColumnType.INTEGER,
                column_description="Codigo de subcategoria del producto (plazo o tipo especifico).",
            ),
            ColumnInfo(
                column_name="DESCRIPCION_SUBCUENTA",
                column_type=ColumnType.STRING,
                column_description="Descripcion detallada del plazo o caracteristica (ej: 'A 90 DIAS', 'A 180 DIAS'). NO incluir en GROUP BY a menos que se pida analisis por plazo.",
            ),
            ColumnInfo(
                column_name="TASA",
                column_type=ColumnType.FLOAT,
                column_description="Tasa de interes reportada (valor decimal, ej: 9.69 = 9.69%). Usar AVG(TASA) para promedios.",
            ),
            ColumnInfo(
                column_name="MONTO",
                column_type=ColumnType.FLOAT,
                column_description="Monto en pesos colombianos asociado a la tasa. Usar SUM(MONTO) para totales.",
            ),
            ColumnInfo(
                column_name="year",
                column_type=ColumnType.INTEGER,
                column_description="Ano extraido de la fecha de corte. Usar para filtros temporales.",
            ),
            ColumnInfo(
                column_name="month",
                column_type=ColumnType.INTEGER,
                column_description="Mes extraido de la fecha de corte. Usar para filtros temporales.",
            ),
        ],
    ),
    "gold.tasas_interes_credito": TableInfo(
        table_name="gold.tasas_interes_credito",
        table_description="Tasas de interes de credito (colocacion) reportadas a la Superintendencia Financiera. Contiene tasas efectivas, montos desembolsados, numero de creditos por tipo de credito, producto, plazo, tipo de persona y tamano de empresa. JOIN con gold.banco via ID_ENTIDAD, con gold.fecha via FECHA_CORTE.",
        table_columns=[
            ColumnInfo(
                column_name="ID_ENTIDAD",
                column_type=ColumnType.STRING,
                column_description="FK -> banco.ID_ENTIDAD. Identificador de la entidad financiera.",
            ),
            ColumnInfo(
                column_name="TIPO_ENTIDAD",
                column_type=ColumnType.INTEGER,
                column_description="Codigo del tipo de entidad financiera.",
            ),
            ColumnInfo(
                column_name="CODIGO_ENTIDAD",
                column_type=ColumnType.INTEGER,
                column_description="Codigo numerico de la entidad.",
            ),
            ColumnInfo(
                column_name="NOMBRE_ENTIDAD",
                column_type=ColumnType.STRING,
                column_description="Nombre de la entidad financiera. SIEMPRE verificar con get_distinct_values — los nombres son inconsistentes.",
            ),
            ColumnInfo(
                column_name="NOMBRE_TIPO_ENTIDAD",
                column_type=ColumnType.STRING,
                column_description="Nombre del tipo de entidad (ej: 'BC-ESTABLECIMIENTO BANCARIO').",
            ),
            ColumnInfo(
                column_name="FECHA_CORTE",
                column_type=ColumnType.STRING,
                column_description="FK -> fecha.FECHA_CORTE. Fecha de reporte en formato 'ddMMyyyy'.",
            ),
            ColumnInfo(
                column_name="TIPO_DE_CR_DITO",
                column_type=ColumnType.STRING,
                column_description="Tipo de credito: 'Comercial ordinario', 'Comercial especial', 'Comercial tesoreria', 'Comercial preferencial o corporativo', 'Credito productivo', 'Consumo', 'Vivienda'. SIEMPRE verificar con get_distinct_values.",
            ),
            ColumnInfo(
                column_name="PRODUCTO_DE_CR_DITO",
                column_type=ColumnType.STRING,
                column_description="Producto especifico de credito: 'Tarjeta de credito para ingresos hasta 2 SMMLV', 'Libre inversion', 'Libranza otros', 'Vehiculo', 'Corporativo', etc. SIEMPRE verificar con get_distinct_values.",
            ),
            ColumnInfo(
                column_name="PLAZO_DE_CR_DITO",
                column_type=ColumnType.STRING,
                column_description="Plazo del credito: 'Hasta 6 meses', 'Mas de 1 ano y hasta 2 anos', 'Consumos a un mes', etc. SIEMPRE verificar con get_distinct_values.",
            ),
            ColumnInfo(
                column_name="TIPO_DE_PERSONA",
                column_type=ColumnType.STRING,
                column_description="Tipo de persona: 'Natural', 'Juridica'.",
            ),
            ColumnInfo(
                column_name="TAMA_O_DE_EMPRESA",
                column_type=ColumnType.STRING,
                column_description="Tamano de empresa: 'Gran empresa', 'Mediana empresa', 'Pequena empresa', 'Microempresa', 'No aplica'.",
            ),
            ColumnInfo(
                column_name="TIPO_DE_TASA",
                column_type=ColumnType.STRING,
                column_description="Referencia de tasa: 'DTF', 'IBR', 'IPC', 'CEC', 'FS' (fija), 'OTR', etc.",
            ),
            ColumnInfo(
                column_name="TASA_EFECTIVA_PROMEDIO",
                column_type=ColumnType.FLOAT,
                column_description="Tasa efectiva promedio del credito (%). Usar AVG() o ponderado con MONTOS_DESEMBOLSADOS.",
            ),
            ColumnInfo(
                column_name="MONTOS_DESEMBOLSADOS",
                column_type=ColumnType.INTEGER,
                column_description="Montos desembolsados en pesos colombianos. Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="NUMERO_DE_CREDITOS",
                column_type=ColumnType.INTEGER,
                column_description="Numero de creditos otorgados. Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="MARGEN_ADICIONAL_A_LA",
                column_type=ColumnType.FLOAT,
                column_description="Margen adicional sobre la tasa de referencia (spread).",
            ),
            ColumnInfo(
                column_name="year",
                column_type=ColumnType.INTEGER,
                column_description="Ano extraido de la fecha de corte.",
            ),
            ColumnInfo(
                column_name="month",
                column_type=ColumnType.INTEGER,
                column_description="Mes extraido de la fecha de corte.",
            ),
        ],
    ),
    "gold.distribucion_cartera": TableInfo(
        table_name="gold.distribucion_cartera",
        table_description="Distribucion de la cartera de credito del sistema financiero colombiano. Contiene saldos por tipo de cartera (consumo, comercial, vivienda, microcredito) reportados a la Superintendencia Financiera. JOIN con gold.banco via ID_ENTIDAD, con gold.fecha via FECHA_CORTE.",
        table_columns=[
            ColumnInfo(
                column_name="ID_ENTIDAD",
                column_type=ColumnType.STRING,
                column_description="FK -> banco.ID_ENTIDAD. Identificador de la entidad financiera.",
            ),
            ColumnInfo(
                column_name="TIPO_ENTIDAD",
                column_type=ColumnType.INTEGER,
                column_description="Codigo del tipo de entidad financiera.",
            ),
            ColumnInfo(
                column_name="CODIGO_ENTIDAD",
                column_type=ColumnType.INTEGER,
                column_description="Codigo numerico de la entidad.",
            ),
            ColumnInfo(
                column_name="NOMBRE_ENTIDAD",
                column_type=ColumnType.STRING,
                column_description="Nombre de la entidad financiera. SIEMPRE verificar con get_distinct_values — los nombres son inconsistentes.",
            ),
            ColumnInfo(
                column_name="FECHA_CORTE",
                column_type=ColumnType.STRING,
                column_description="FK -> fecha.FECHA_CORTE. Fecha de reporte en formato 'ddMMyyyy'.",
            ),
            ColumnInfo(
                column_name="CODIGO_CATEGORIA_CARTERA",
                column_type=ColumnType.INTEGER,
                column_description="Codigo de categoria de cartera.",
            ),
            ColumnInfo(
                column_name="DESCRIPCION_CATEGORIA_CARTERA",
                column_type=ColumnType.STRING,
                column_description="Tipo de categoria de cartera: 'CARTERA COMERCIAL CORPORATIVO', 'LIBRE INVERSION', 'LIBRANZA', 'TARJETAS DE CREDITO', 'VEHICULO', 'VIVIENDA VIS PESOS', etc. Usar get_distinct_values para ver valores exactos.",
            ),
            ColumnInfo(
                column_name="CODIGO_SUBCATEGORIA_CARTERA",
                column_type=ColumnType.INTEGER,
                column_description="Codigo de subcategoria de cartera.",
            ),
            ColumnInfo(
                column_name="DESCRIPCION_SUBCATEGORIA_CARTERA",
                column_type=ColumnType.STRING,
                column_description="Subcategoria detallada del producto de credito.",
            ),
            ColumnInfo(
                column_name="SALDO_CARTERA_A_FECHA_CORTE",
                column_type=ColumnType.FLOAT,
                column_description="Saldo total de cartera en pesos colombianos a la fecha de corte. Usar SUM() para agregar.",
            ),
            ColumnInfo(
                column_name="SALDO_CARTERA_VIGENTE",
                column_type=ColumnType.FLOAT,
                column_description="Saldo de cartera vigente (al dia) en pesos colombianos. Usar SUM() para agregar.",
            ),
            ColumnInfo(
                column_name="year",
                column_type=ColumnType.INTEGER,
                column_description="Ano extraido de la fecha de corte.",
            ),
            ColumnInfo(
                column_name="month",
                column_type=ColumnType.INTEGER,
                column_description="Mes extraido de la fecha de corte.",
            ),
            ColumnInfo(
                column_name="AGRUPACION",
                column_type=ColumnType.STRING,
                column_description="Agrupacion simplificada de categorias de cartera. Usar para analisis agrupados de alto nivel.",
            ),
            ColumnInfo(
                column_name="SEGMENTO",
                column_type=ColumnType.STRING,
                column_description="Segmento de cartera: 'Comercial', 'Consumo', 'Microcredito', 'Vivienda'. Util para agrupacion de alto nivel.",
            ),
        ],
    ),
    # =========================================================================
    # CLIENT TABLES
    # =========================================================================
    "gold.adl_clientes_pn": TableInfo(
        table_name="gold.adl_clientes_pn",
        table_description="Datos demograficos y de segmentacion de clientes Persona Natural del Grupo Aval. Contiene estado del cliente, genero, departamento, rango de edad, rango de vinculacion y segmentacion. Los campos TIPO_DOCUMENTO y NUMERO_DOCUMENTO estan hasheados. JOIN con gold.banco NO es via ID_ENTIDAD sino con ENTIDAD (codigo corto: BAVV, BBOG, BOCC, BPOP, DALE) e ID_BANCO. JOIN con gold.adl_sica_clientes_pn via LLAVE para datos financieros.",
        table_columns=[
            ColumnInfo(
                column_name="TIPO_DOCUMENTO",
                column_type=ColumnType.STRING,
                column_description="Tipo de documento de identidad del cliente (hasheado). No usar para filtros de negocio.",
            ),
            ColumnInfo(
                column_name="NUMERO_DOCUMENTO",
                column_type=ColumnType.STRING,
                column_description="Numero de documento del cliente (hasheado). No usar para filtros de negocio.",
            ),
            ColumnInfo(
                column_name="ESTADO_CLIENTE",
                column_type=ColumnType.STRING,
                column_description="Estado del cliente: 'A-Cliente Activo', 'I-Cliente Inactivo', 'I-Cliente Potencial', 'Excliente', 'Cliente Fallecido', 'Prospecto'. SIEMPRE verificar con get_distinct_values.",
            ),
            ColumnInfo(
                column_name="GENERO",
                column_type=ColumnType.STRING,
                column_description="Genero del cliente: 'F' (Femenino), 'M' (Masculino), 'T', 'B', 'D', '-', None. SIEMPRE verificar con get_distinct_values.",
            ),
            ColumnInfo(
                column_name="DEPARTAMENTO",
                column_type=ColumnType.STRING,
                column_description="Departamento de Colombia del cliente (ej: 'Bogota, D. C.', 'Antioquia', 'Valle Del Cauca'). SIEMPRE verificar con get_distinct_values por tildes y formatos inconsistentes.",
            ),
            ColumnInfo(
                column_name="SEGMENTACION_AVAL",
                column_type=ColumnType.STRING,
                column_description="Segmentacion comercial del Grupo Aval: 'Comercial', 'Servicio', 'Grupo Aval', 'Confidencial', 'Sin informacion'.",
            ),
            ColumnInfo(
                column_name="RANGO_EDAD",
                column_type=ColumnType.STRING,
                column_description="Rango de edad del cliente: '-18', '18-25', '26-30', '31-40', '41-50', '51-60', '+60', '9999999' (sin info).",
            ),
            ColumnInfo(
                column_name="RANGO_ANIOS_VINCULACION",
                column_type=ColumnType.STRING,
                column_description="Rango de anos de vinculacion: '1', '1-5', '6-10', '11-20', '+20', '9999999' (sin info).",
            ),
            ColumnInfo(
                column_name="DATE_CRUCE",
                column_type=ColumnType.DATETIME,
                column_description="Fecha de cruce/corte de los datos (datetime). Usar year y month para filtros temporales.",
            ),
            ColumnInfo(
                column_name="ENTIDAD",
                column_type=ColumnType.STRING,
                column_description="Codigo corto de la entidad: 'BAVV' (AV Villas), 'BBOG' (Banco de Bogota), 'BOCC' (Banco de Occidente), 'BPOP' (Banco Popular), 'DALE'. NO es ID_ENTIDAD de gold.banco.",
            ),
            ColumnInfo(
                column_name="USERS",
                column_type=ColumnType.STRING,
                column_description="Identificador hasheado del usuario/registro. No usar para filtros de negocio.",
            ),
            ColumnInfo(
                column_name="INGRESOS",
                column_type=ColumnType.FLOAT,
                column_description="Ingresos del cliente en pesos colombianos. Puede ser NULL. Usar AVG() o SUM() para agregados.",
            ),
            ColumnInfo(
                column_name="RANGO_SALARIO",
                column_type=ColumnType.STRING,
                column_description="Rango salarial del cliente. Puede estar vacio (NULL). Verificar con get_distinct_values.",
            ),
            ColumnInfo(
                column_name="year",
                column_type=ColumnType.INTEGER,
                column_description="Ano extraido de DATE_CRUCE. Usar para filtros temporales.",
            ),
            ColumnInfo(
                column_name="month",
                column_type=ColumnType.INTEGER,
                column_description="Mes extraido de DATE_CRUCE (1-12). Usar para filtros temporales.",
            ),
            ColumnInfo(
                column_name="ID_BANCO",
                column_type=ColumnType.INTEGER,
                column_description="Identificador numerico del banco. Usar para JOINs internos entre tablas adl/sica.",
            ),
            ColumnInfo(
                column_name="LLAVE",
                column_type=ColumnType.STRING,
                column_description="Clave compuesta unica del registro (TIPO_DOCUMENTO + NUMERO_DOCUMENTO hasheados). PK de la tabla. JOIN con gold.adl_sica_clientes_pn.LLAVE.",
            ),
        ],
    ),
    "gold.adl_clientes_pj": TableInfo(
        table_name="gold.adl_clientes_pj",
        table_description="Datos de clientes Persona Juridica del Grupo Aval. Contiene estado del cliente, segmento comercial, segmento de ventas, rango de edad de la empresa y vinculacion. Los campos TIPO_DOCUMENTO y NUMERO_DOCUMENTO estan hasheados. JOIN con gold.banco NO es via ID_ENTIDAD sino con ENTIDAD (codigo corto: BAVV, BBOG, BOCC, BPOP) e ID_BANCO. JOIN con gold.adl_sica_clientes_pj via LLAVE para datos financieros.",
        table_columns=[
            ColumnInfo(
                column_name="TIPO_DOCUMENTO",
                column_type=ColumnType.STRING,
                column_description="Tipo de documento de la empresa (hasheado). No usar para filtros de negocio.",
            ),
            ColumnInfo(
                column_name="NUMERO_DOCUMENTO",
                column_type=ColumnType.STRING,
                column_description="Numero de documento de la empresa (hasheado). No usar para filtros de negocio.",
            ),
            ColumnInfo(
                column_name="ESTADO_CLIENTE",
                column_type=ColumnType.STRING,
                column_description="Estado del cliente: 'A-Cliente Activo', 'I-Cliente Inactivo', 'I-Cliente Potencial', 'Excliente', 'Prospecto', 'Disuelto y/o Liquidado', 'Sin informacion'. SIEMPRE verificar con get_distinct_values.",
            ),
            ColumnInfo(
                column_name="SEGMENTO",
                column_type=ColumnType.STRING,
                column_description="Segmento del cliente: 'Comercial'. Puede estar vacio.",
            ),
            ColumnInfo(
                column_name="RANGO_EDAD",
                column_type=ColumnType.STRING,
                column_description="Rango de antiguedad de la empresa: '1', '1-5', '6-10', '11-20', '+20', 'Sin informacion'. Representa anos de existencia de la empresa.",
            ),
            ColumnInfo(
                column_name="RANGO_ANIOS_VINCULACION",
                column_type=ColumnType.STRING,
                column_description="Rango de anos de vinculacion con el banco: '1', '1-5', '6-10', '11-20', '+20', 'Sin informacion'.",
            ),
            ColumnInfo(
                column_name="NUMERO_EMPLEADOS",
                column_type=ColumnType.STRING,
                column_description="Numero de empleados de la empresa (varchar). Valores numericos como texto. Puede ser '0', 'None' o NULL.",
            ),
            ColumnInfo(
                column_name="DATE_CRUCE",
                column_type=ColumnType.DATETIME,
                column_description="Fecha de cruce/corte de los datos (datetime). Usar year y month para filtros temporales.",
            ),
            ColumnInfo(
                column_name="ENTIDAD",
                column_type=ColumnType.STRING,
                column_description="Codigo corto de la entidad: 'BAVV', 'BBOG', 'BOCC', 'BPOP'. NO es ID_ENTIDAD de gold.banco.",
            ),
            ColumnInfo(
                column_name="USERS",
                column_type=ColumnType.STRING,
                column_description="Identificador hasheado del usuario/registro. No usar para filtros de negocio.",
            ),
            ColumnInfo(
                column_name="SEGMENTO_VENTAS",
                column_type=ColumnType.STRING,
                column_description="Segmento por tamano de ventas: 'Micro', 'Pequena', 'Mediana', 'Grande', 'Gobierno', 'Sin informacion'.",
            ),
            ColumnInfo(
                column_name="year",
                column_type=ColumnType.INTEGER,
                column_description="Ano extraido de DATE_CRUCE. Usar para filtros temporales.",
            ),
            ColumnInfo(
                column_name="month",
                column_type=ColumnType.INTEGER,
                column_description="Mes extraido de DATE_CRUCE (1-12). Usar para filtros temporales.",
            ),
            ColumnInfo(
                column_name="ID_BANCO",
                column_type=ColumnType.INTEGER,
                column_description="Identificador numerico del banco. Usar para JOINs internos entre tablas adl/sica.",
            ),
            ColumnInfo(
                column_name="LLAVE",
                column_type=ColumnType.STRING,
                column_description="Clave compuesta unica del registro (TIPO_DOCUMENTO + NUMERO_DOCUMENTO hasheados). PK de la tabla. JOIN con gold.adl_sica_clientes_pj.LLAVE.",
            ),
        ],
    ),
    # =========================================================================
    # SICA CLIENT TABLES (Financial products per client)
    # =========================================================================
    "gold.adl_sica_clientes_pn": TableInfo(
        table_name="gold.adl_sica_clientes_pn",
        table_description="Informacion financiera SICA de clientes Persona Natural del Grupo Aval. Contiene productos pasivos (ahorro, corriente, CDT), productos activos (hipotecario, libranza, libre inversion, tarjeta de credito, otros), calificacion crediticia, dias de mora y flags de tipo de cliente. JOIN con gold.adl_clientes_pn via LLAVE para datos demograficos. Filtrar por entidad (BAVV, BBOG, BOCC, BPOP).",
        table_columns=[
            ColumnInfo(
                column_name="tipo_id",
                column_type=ColumnType.STRING,
                column_description="Tipo de identificacion del cliente (hasheado). No usar para filtros de negocio.",
            ),
            ColumnInfo(
                column_name="num_id",
                column_type=ColumnType.STRING,
                column_description="Numero de identificacion del cliente (hasheado). No usar para filtros de negocio.",
            ),
            ColumnInfo(
                column_name="aho_cant_productos",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad de productos de ahorro del cliente. Usar SUM() o AVG() para agregados.",
            ),
            ColumnInfo(
                column_name="aho_saldo_actual",
                column_type=ColumnType.FLOAT,
                column_description="Saldo actual en cuentas de ahorro (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="aho_saldo_promedio",
                column_type=ColumnType.FLOAT,
                column_description="Saldo promedio en cuentas de ahorro (COP).",
            ),
            ColumnInfo(
                column_name="cor_cant_productos",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad de cuentas corrientes del cliente.",
            ),
            ColumnInfo(
                column_name="cor_saldo_actual",
                column_type=ColumnType.FLOAT,
                column_description="Saldo actual en cuentas corrientes (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="cor_saldo_promedio",
                column_type=ColumnType.FLOAT,
                column_description="Saldo promedio en cuentas corrientes (COP).",
            ),
            ColumnInfo(
                column_name="cdt_cant_productos",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad de CDTs del cliente.",
            ),
            ColumnInfo(
                column_name="cdt_saldo_actual",
                column_type=ColumnType.FLOAT,
                column_description="Saldo actual en CDTs (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="act_max_dias_mora",
                column_type=ColumnType.INTEGER,
                column_description="Maximo dias de mora del cliente en productos activos. 0 = al dia.",
            ),
            ColumnInfo(
                column_name="act_calificacion",
                column_type=ColumnType.STRING,
                column_description="Calificacion crediticia del cliente: 'A' (normal), 'B' (aceptable), 'C' (apreciable), 'D' (significativo), 'E' (incobrable). Escala de riesgo de la SFC.",
            ),
            ColumnInfo(
                column_name="act_hipotecario_saldo",
                column_type=ColumnType.FLOAT,
                column_description="Saldo credito hipotecario (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="act_libranza_saldo",
                column_type=ColumnType.FLOAT,
                column_description="Saldo credito de libranza (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="act_libre_saldo",
                column_type=ColumnType.FLOAT,
                column_description="Saldo credito libre inversion (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="act_tc_saldo",
                column_type=ColumnType.FLOAT,
                column_description="Saldo tarjeta de credito (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="act_otros_saldo",
                column_type=ColumnType.FLOAT,
                column_description="Saldo otros creditos (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="act_hipotecario_cant_oblig",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad de obligaciones hipotecarias del cliente.",
            ),
            ColumnInfo(
                column_name="act_libranza_cant_oblig",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad de obligaciones de libranza.",
            ),
            ColumnInfo(
                column_name="act_libre_cant_oblig",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad de obligaciones de libre inversion.",
            ),
            ColumnInfo(
                column_name="act_tc_cant_oblig",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad de obligaciones de tarjeta de credito.",
            ),
            ColumnInfo(
                column_name="act_otros_cant_oblig",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad de otras obligaciones.",
            ),
            ColumnInfo(
                column_name="act_hipotecario_valor_inicial",
                column_type=ColumnType.INTEGER,
                column_description="Valor inicial del credito hipotecario (COP).",
            ),
            ColumnInfo(
                column_name="act_libranza_valor_inicial",
                column_type=ColumnType.INTEGER,
                column_description="Valor inicial del credito de libranza (COP).",
            ),
            ColumnInfo(
                column_name="act_libre_valor_inicial",
                column_type=ColumnType.INTEGER,
                column_description="Valor inicial del credito libre inversion (COP).",
            ),
            ColumnInfo(
                column_name="act_tc_valor_inicial",
                column_type=ColumnType.INTEGER,
                column_description="Valor inicial de la tarjeta de credito (COP).",
            ),
            ColumnInfo(
                column_name="act_otros_valor_inicial",
                column_type=ColumnType.INTEGER,
                column_description="Valor inicial de otros creditos (COP).",
            ),
            ColumnInfo(
                column_name="act_hipotecario_valor_cuota",
                column_type=ColumnType.INTEGER,
                column_description="Valor cuota del credito hipotecario (COP).",
            ),
            ColumnInfo(
                column_name="act_libranza_valor_cuota",
                column_type=ColumnType.INTEGER,
                column_description="Valor cuota del credito de libranza (COP).",
            ),
            ColumnInfo(
                column_name="act_libre_valor_cuota",
                column_type=ColumnType.INTEGER,
                column_description="Valor cuota del credito libre inversion (COP).",
            ),
            ColumnInfo(
                column_name="act_tc_valor_cuota",
                column_type=ColumnType.INTEGER,
                column_description="Valor cuota de la tarjeta de credito (COP).",
            ),
            ColumnInfo(
                column_name="act_otros_valor_cuota",
                column_type=ColumnType.INTEGER,
                column_description="Valor cuota de otros creditos (COP).",
            ),
            ColumnInfo(
                column_name="flag_cuenta_nomina",
                column_type=ColumnType.INTEGER,
                column_description="Flag indica si tiene cuenta nomina (1=Si, 0=No).",
            ),
            ColumnInfo(
                column_name="tipo_mono_pasivo",
                column_type=ColumnType.INTEGER,
                column_description="Flag cliente mono-producto pasivo (1=Si, 0=No). Solo tiene productos de deposito.",
            ),
            ColumnInfo(
                column_name="tipo_mono_activo",
                column_type=ColumnType.INTEGER,
                column_description="Flag cliente mono-producto activo (1=Si, 0=No). Solo tiene productos de credito.",
            ),
            ColumnInfo(
                column_name="tipo_multi_pasivo",
                column_type=ColumnType.INTEGER,
                column_description="Flag cliente multi-producto pasivo (1=Si, 0=No). Tiene multiples productos de deposito.",
            ),
            ColumnInfo(
                column_name="tipo_multi_activo",
                column_type=ColumnType.INTEGER,
                column_description="Flag cliente multi-producto activo (1=Si, 0=No). Tiene multiples productos de credito.",
            ),
            ColumnInfo(
                column_name="tipo_mixto",
                column_type=ColumnType.INTEGER,
                column_description="Flag cliente mixto (1=Si, 0=No). Tiene productos activos y pasivos. -1 = sin info.",
            ),
            ColumnInfo(
                column_name="entidad",
                column_type=ColumnType.STRING,
                column_description="Codigo corto de la entidad: 'BAVV', 'BBOG', 'BOCC', 'BPOP'. NOTA: en minuscula a diferencia de adl_clientes_pn.",
            ),
            ColumnInfo(
                column_name="date_cruce",
                column_type=ColumnType.DATETIME,
                column_description="Fecha de cruce/corte de los datos (datetime). Usar year y month para filtros temporales.",
            ),
            ColumnInfo(
                column_name="cant_obligaciones_tot",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad total de obligaciones activas del cliente. Usar SUM() para agregados.",
            ),
            ColumnInfo(
                column_name="saldo_obligaciones_tot",
                column_type=ColumnType.FLOAT,
                column_description="Saldo total de obligaciones activas (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="cant_depositos_tot",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad total de productos de deposito del cliente.",
            ),
            ColumnInfo(
                column_name="saldo_depositos_tot",
                column_type=ColumnType.FLOAT,
                column_description="Saldo total de depositos (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="year",
                column_type=ColumnType.INTEGER,
                column_description="Ano extraido de date_cruce.",
            ),
            ColumnInfo(
                column_name="month",
                column_type=ColumnType.INTEGER,
                column_description="Mes extraido de date_cruce (1-12).",
            ),
            ColumnInfo(
                column_name="ID_BANCO",
                column_type=ColumnType.INTEGER,
                column_description="Identificador numerico del banco. Usar para JOINs internos entre tablas adl/sica.",
            ),
            ColumnInfo(
                column_name="LLAVE",
                column_type=ColumnType.STRING,
                column_description="Clave compuesta unica (tipo_id + num_id). JOIN con gold.adl_clientes_pn.LLAVE para datos demograficos.",
            ),
        ],
    ),
    "gold.adl_sica_clientes_pj": TableInfo(
        table_name="gold.adl_sica_clientes_pj",
        table_description="Informacion financiera SICA de clientes Persona Juridica del Grupo Aval. Contiene productos pasivos (ahorro, corriente, CDT), productos activos (leasing, cartera ordinaria, TDC rotativo, factoring, sobregiro, otros), calificacion crediticia, dias de mora y totales. JOIN con gold.adl_clientes_pj via LLAVE para datos corporativos. Filtrar por entidad (BOCC, BPOP).",
        table_columns=[
            ColumnInfo(
                column_name="tipo_id",
                column_type=ColumnType.STRING,
                column_description="Tipo de identificacion de la empresa (hasheado). No usar para filtros de negocio.",
            ),
            ColumnInfo(
                column_name="num_id",
                column_type=ColumnType.STRING,
                column_description="Numero de identificacion de la empresa (hasheado). No usar para filtros de negocio.",
            ),
            ColumnInfo(
                column_name="flag_gobierno",
                column_type=ColumnType.INTEGER,
                column_description="Flag indica si es entidad del gobierno (1=Si, 0=No).",
            ),
            ColumnInfo(
                column_name="aho_cant_productos",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad de productos de ahorro. Puede ser NULL.",
            ),
            ColumnInfo(
                column_name="aho_saldo_actual",
                column_type=ColumnType.FLOAT,
                column_description="Saldo actual en cuentas de ahorro (COP). Puede ser NULL.",
            ),
            ColumnInfo(
                column_name="aho_saldo_promedio",
                column_type=ColumnType.FLOAT,
                column_description="Saldo promedio en cuentas de ahorro (COP). Puede ser NULL.",
            ),
            ColumnInfo(
                column_name="cor_cant_productos",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad de cuentas corrientes. Puede ser NULL.",
            ),
            ColumnInfo(
                column_name="cor_saldo_actual",
                column_type=ColumnType.FLOAT,
                column_description="Saldo actual en cuentas corrientes (COP). Puede ser NULL.",
            ),
            ColumnInfo(
                column_name="cor_saldo_promedio",
                column_type=ColumnType.FLOAT,
                column_description="Saldo promedio en cuentas corrientes (COP). Puede ser NULL.",
            ),
            ColumnInfo(
                column_name="cdt_cant_productos",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad de CDTs. Puede ser NULL.",
            ),
            ColumnInfo(
                column_name="cdt_saldo_actual",
                column_type=ColumnType.FLOAT,
                column_description="Saldo actual en CDTs (COP). Puede ser NULL.",
            ),
            ColumnInfo(
                column_name="act_otro_valor_inicial",
                column_type=ColumnType.FLOAT,
                column_description="Valor inicial de otros creditos (COP).",
            ),
            ColumnInfo(
                column_name="act_leasing_valor_inicial",
                column_type=ColumnType.FLOAT,
                column_description="Valor inicial del leasing (COP).",
            ),
            ColumnInfo(
                column_name="act_cartera_ord_valor_inicial",
                column_type=ColumnType.FLOAT,
                column_description="Valor inicial de cartera ordinaria (COP).",
            ),
            ColumnInfo(
                column_name="act_tdc_rotativo_valor_inicial",
                column_type=ColumnType.FLOAT,
                column_description="Valor inicial de TDC rotativo/tarjeta de credito (COP).",
            ),
            ColumnInfo(
                column_name="act_factoring_valor_inicial",
                column_type=ColumnType.FLOAT,
                column_description="Valor inicial de factoring (COP).",
            ),
            ColumnInfo(
                column_name="act_sobregiro_valor_inicial",
                column_type=ColumnType.FLOAT,
                column_description="Valor inicial de sobregiro (COP).",
            ),
            ColumnInfo(
                column_name="act_otro_valor_cuota",
                column_type=ColumnType.FLOAT,
                column_description="Valor cuota de otros creditos (COP).",
            ),
            ColumnInfo(
                column_name="act_leasing_valor_cuota",
                column_type=ColumnType.FLOAT,
                column_description="Valor cuota del leasing (COP).",
            ),
            ColumnInfo(
                column_name="act_cartera_ord_valor_cuota",
                column_type=ColumnType.FLOAT,
                column_description="Valor cuota de cartera ordinaria (COP).",
            ),
            ColumnInfo(
                column_name="act_tdc_rotativo_valor_cuota",
                column_type=ColumnType.FLOAT,
                column_description="Valor cuota de TDC rotativo (COP).",
            ),
            ColumnInfo(
                column_name="act_factoring_valor_cuota",
                column_type=ColumnType.FLOAT,
                column_description="Valor cuota de factoring (COP).",
            ),
            ColumnInfo(
                column_name="act_sobregiro_valor_cuota",
                column_type=ColumnType.FLOAT,
                column_description="Valor cuota de sobregiro (COP).",
            ),
            ColumnInfo(
                column_name="act_otro_saldo",
                column_type=ColumnType.FLOAT,
                column_description="Saldo de otros creditos (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="act_leasing_saldo",
                column_type=ColumnType.FLOAT,
                column_description="Saldo de leasing (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="act_cartera_ord_saldo",
                column_type=ColumnType.FLOAT,
                column_description="Saldo de cartera ordinaria (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="act_tdc_rotativo_saldo",
                column_type=ColumnType.FLOAT,
                column_description="Saldo de TDC rotativo (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="act_factoring_saldo",
                column_type=ColumnType.FLOAT,
                column_description="Saldo de factoring (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="act_sobregiro_saldo",
                column_type=ColumnType.FLOAT,
                column_description="Saldo de sobregiro (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="act_otro_cant_oblig",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad de otras obligaciones.",
            ),
            ColumnInfo(
                column_name="act_leasing_cant_oblig",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad de obligaciones de leasing.",
            ),
            ColumnInfo(
                column_name="act_cartera_ord_cant_oblig",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad de obligaciones de cartera ordinaria.",
            ),
            ColumnInfo(
                column_name="act_tdc_rotativo_cant_oblig",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad de obligaciones TDC rotativo.",
            ),
            ColumnInfo(
                column_name="act_factoring_cant_oblig",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad de obligaciones de factoring.",
            ),
            ColumnInfo(
                column_name="act_sobregiro_cant_oblig",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad de obligaciones de sobregiro.",
            ),
            ColumnInfo(
                column_name="act_max_dias_mora",
                column_type=ColumnType.INTEGER,
                column_description="Maximo dias de mora del cliente en productos activos. 0 = al dia.",
            ),
            ColumnInfo(
                column_name="act_calificacion",
                column_type=ColumnType.STRING,
                column_description="Calificacion crediticia: 'A' (normal), 'B' (aceptable), 'C' (apreciable), 'D' (significativo), 'E' (incobrable), 'No_Registra'.",
            ),
            ColumnInfo(
                column_name="act_tot_valor_inicial",
                column_type=ColumnType.FLOAT,
                column_description="Valor inicial total de todos los creditos activos (COP).",
            ),
            ColumnInfo(
                column_name="act_tot_valor_cuota",
                column_type=ColumnType.FLOAT,
                column_description="Valor cuota total de todos los creditos activos (COP).",
            ),
            ColumnInfo(
                column_name="act_tot_saldo",
                column_type=ColumnType.FLOAT,
                column_description="Saldo total de todos los creditos activos (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="act_tot_cant_oblig",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad total de obligaciones activas.",
            ),
            ColumnInfo(
                column_name="entidad",
                column_type=ColumnType.STRING,
                column_description="Codigo corto de la entidad: 'BOCC', 'BPOP'. Solo 2 entidades disponibles en esta tabla.",
            ),
            ColumnInfo(
                column_name="date_cruce",
                column_type=ColumnType.DATETIME,
                column_description="Fecha de cruce/corte de los datos (datetime). Usar year y month para filtros temporales.",
            ),
            ColumnInfo(
                column_name="cant_obligaciones_tot",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad total de obligaciones activas del cliente.",
            ),
            ColumnInfo(
                column_name="saldo_obligaciones_tot",
                column_type=ColumnType.FLOAT,
                column_description="Saldo total de obligaciones activas (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="cant_depositos_tot",
                column_type=ColumnType.INTEGER,
                column_description="Cantidad total de productos de deposito.",
            ),
            ColumnInfo(
                column_name="saldo_depositos_tot",
                column_type=ColumnType.FLOAT,
                column_description="Saldo total de depositos (COP). Usar SUM() para totales.",
            ),
            ColumnInfo(
                column_name="year",
                column_type=ColumnType.INTEGER,
                column_description="Ano extraido de date_cruce.",
            ),
            ColumnInfo(
                column_name="month",
                column_type=ColumnType.INTEGER,
                column_description="Mes extraido de date_cruce (1-12).",
            ),
            ColumnInfo(
                column_name="ID_BANCO",
                column_type=ColumnType.INTEGER,
                column_description="Identificador numerico del banco. Usar para JOINs internos entre tablas adl/sica.",
            ),
            ColumnInfo(
                column_name="LLAVE",
                column_type=ColumnType.STRING,
                column_description="Clave compuesta unica (tipo_id + num_id). JOIN con gold.adl_clientes_pj.LLAVE para datos corporativos.",
            ),
        ],
    ),
}
