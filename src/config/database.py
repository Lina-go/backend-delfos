"""
Database configuration for SuperDB.
"""

from dataclasses import dataclass

from src.config.constants import ColumnType


@dataclass(frozen=True)
class ColumnInfo:
    """Information about a database column."""

    column_name: str
    column_type: ColumnType
    column_description: str


@dataclass(frozen=True)
class TableInfo:
    """Information about a database table."""

    table_name: str
    table_description: str
    table_columns: list[ColumnInfo]


DATABASE_TABLES: dict[str, TableInfo] = {
    "dbo.Tasas_Captacion": TableInfo(
        table_name="dbo.Tasas_Captacion",
        table_description="Tasas de captación reportadas por entidades financieras colombianas a la Superintendencia Financiera. Contiene tasas de interés para CDT, CDAT, cuentas de ahorro y operaciones del mercado monetario.",
        table_columns=[
            ColumnInfo(
                column_name="TIPO_ENTIDAD",
                column_type=ColumnType.INTEGER,
                column_description="Código del tipo de entidad financiera: 1=Bancos Comerciales, 2=Corporaciones Financieras, 4=Compañías de Financiamiento, 22=Bancos de Segundo Piso, 32=Cooperativas Financieras. Usar para agrupar por TIPO de entidad, NO por entidad individual.",
            ),
            ColumnInfo(
                column_name="CODIGO_ENTIDAD",
                column_type=ColumnType.INTEGER,
                column_description="Código único de la entidad financiera individual. Usar para identificar o filtrar una entidad específica.",
            ),
            ColumnInfo(
                column_name="NOMBRE_ENTIDAD",
                column_type=ColumnType.STRING,
                column_description="Nombre de la entidad financiera individual (ej: 'Banco Davivienda', 'Banco Popular', 'Banco W S.A.', 'Bancolombia'). Usar solo cuando la pregunta pide información de un banco específico, NO para agrupar por tipo de entidad.",
            ),
            ColumnInfo(
                column_name="FECHA_CORTE",
                column_type=ColumnType.STRING,
                column_description="Fecha de reporte en formato texto 'ddMMyyyy' (ej: '30012026'). Para filtrar por año usar la columna 'year', para mes usar 'month'. Para evolución temporal agrupar por year, month.",
            ),
            ColumnInfo(
                column_name="CODIGO_CATEGORIA",
                column_type=ColumnType.INTEGER,
                column_description="Código de categoría de producto de captación: 1=CDT, 2=CDAT, 3=Operaciones Mercado Monetario, 4=Interbancarios, 5=Repos, 7=Cuentas de Ahorro, 8=Cuentas Corrientes.",
            ),
            ColumnInfo(
                column_name="DESCRIPCION_CATEGORIA",
                column_type=ColumnType.STRING,
                column_description="Descripción del producto de captación. Valores: 'EMISIONES PUNTUALES Y RANGOS DE EMISION DE CDT', 'EMISIONES PUNTUALES Y RANGOS DE EMISION DE CDAT', 'SALDO DE LOS DEPOSITOS DE AHORRO Y LAS CUENTAS DE AHORRO', 'OPERACIONES DEL MERCADO MONETARIO', 'SALDO DE CTAS CTES CDT Y CDAT', etc. Usar para filtrar por tipo de producto.",
            ),
            ColumnInfo(
                column_name="CODIGO_SUBCUENTA",
                column_type=ColumnType.INTEGER,
                column_description="Código de subcategoría del producto. Subcategoría por plazo o tipo específico.",
            ),
            ColumnInfo(
                column_name="DESCRIPCION_SUBCUENTA",
                column_type=ColumnType.STRING,
                column_description="Descripción detallada del plazo o característica (ej: 'A 90 DIAS', 'A 180 DIAS', 'ENTRE 31 Y 90 DIAS', 'SALDO EN DEPOSITOS DE AHORRO ACTIVOS PERSONA NATURAL'). NO incluir en GROUP BY a menos que se pida análisis por plazo.",
            ),
            ColumnInfo(
                column_name="TASA",
                column_type=ColumnType.FLOAT,
                column_description="Tasa de interés reportada (valor decimal, ej: 9.69 = 9.69%). Usar AVG(TASA) para promedios.",
            ),
            ColumnInfo(
                column_name="MONTO",
                column_type=ColumnType.FLOAT,
                column_description="Monto en pesos colombianos asociado a la tasa. Usar SUM(MONTO) para totales.",
            ),
            ColumnInfo(
                column_name="year",
                column_type=ColumnType.INTEGER,
                column_description="Año extraído de la fecha de corte. Usar para filtros y agrupaciones temporales por año.",
            ),
            ColumnInfo(
                column_name="month",
                column_type=ColumnType.INTEGER,
                column_description="Mes extraído de la fecha de corte. Usar para filtros y agrupaciones temporales por mes.",
            ),
        ],
    ),
    "dbo.Distribucion_Cartera": TableInfo(
        table_name="dbo.Distribucion_Cartera",
        table_description="Distribución de la cartera de crédito del sistema financiero colombiano. Contiene saldos por tipo de cartera (consumo, comercial, vivienda, microcrédito) reportados a la Superintendencia Financiera.",
        table_columns=[
            ColumnInfo(
                column_name="TIPO_ENTIDAD",
                column_type=ColumnType.INTEGER,
                column_description="Código del tipo de entidad financiera: 1=Bancos Comerciales, 2=Corporaciones Financieras, 4=Compañías de Financiamiento, 22=Bancos de Segundo Piso, 32=Cooperativas Financieras. Usar para agrupar por TIPO de entidad, NO por entidad individual.",
            ),
            ColumnInfo(
                column_name="CODIGO_ENTIDAD",
                column_type=ColumnType.INTEGER,
                column_description="Código único de la entidad financiera individual. Usar para identificar o filtrar una entidad específica.",
            ),
            ColumnInfo(
                column_name="NOMBRE_ENTIDAD",
                column_type=ColumnType.STRING,
                column_description="Nombre de la entidad financiera individual (ej: 'Bancolombia', 'Banco de Occidente', 'Banco Davivienda'). Usar solo cuando la pregunta pide información de un banco específico, NO para agrupar por tipo de entidad.",
            ),
            ColumnInfo(
                column_name="FECHA_CORTE",
                column_type=ColumnType.STRING,
                column_description="Fecha de reporte en formato texto 'ddMMyyyy' (ej: '31102019'). Para filtrar por año usar la columna 'year', para mes usar 'month'. Para evolución temporal agrupar por year, month.",
            ),
            ColumnInfo(
                column_name="CODIGO_CATEGORIA_CARTERA",
                column_type=ColumnType.INTEGER,
                column_description="Código de categoría de cartera. Identifica el tipo principal de producto de crédito.",
            ),
            ColumnInfo(
                column_name="DESCRIPCION_CATEGORIA_CARTERA",
                column_type=ColumnType.STRING,
                column_description="Tipo de categoría principal de la cartera: 'CARTERA COMERCIAL CORPORATIVO', 'CARTERA COMERCIAL PYMES', 'LIBRE INVERSION', 'LIBRANZA', 'TARJETAS DE CREDITO', 'VEHICULO', 'VIVIENDA VIS PESOS', 'VIVIENDA NO VIS PESOS', 'MICROCREDITOS...', 'CREDITO ROTATIVO', etc. Usar para agrupar por tipo de producto de crédito.",
            ),
            ColumnInfo(
                column_name="CODIGO_SUBCATEGORIA_CARTERA",
                column_type=ColumnType.INTEGER,
                column_description="Código de subcategoría de cartera.",
            ),
            ColumnInfo(
                column_name="DESCRIPCION_SUBCATEGORIA_CARTERA",
                column_type=ColumnType.STRING,
                column_description="Subcategoría detallada del producto de crédito. Usar para análisis más granular dentro de una categoría.",
            ),
            ColumnInfo(
                column_name="SALDO_CARTERA_A_FECHA_CORTE",
                column_type=ColumnType.FLOAT,
                column_description="Saldo total de cartera en pesos colombianos a la fecha de corte. Usar SUM() para agregar.",
            ),
            ColumnInfo(
                column_name="SALDO_CARTERA_VIGENTE",
                column_type=ColumnType.FLOAT,
                column_description="Saldo de cartera vigente (al día) en pesos colombianos. Usar SUM() para agregar.",
            ),
            ColumnInfo(
                column_name="year",
                column_type=ColumnType.INTEGER,
                column_description="Año extraído de la fecha de corte. Usar para filtros y agrupaciones temporales por año.",
            ),
            ColumnInfo(
                column_name="month",
                column_type=ColumnType.INTEGER,
                column_description="Mes extraído de la fecha de corte. Usar para filtros y agrupaciones temporales por mes.",
            ),
            ColumnInfo(
                column_name="AGRUPACION",
                column_type=ColumnType.STRING,
                column_description="Agrupación simplificada de categorías de cartera: 'CARTERA COMERCIAL CORPORATIVO', 'CARTERA COMERCIAL PYMES', 'LIBRE INVERSION', 'LIBRANZA', 'TARJETAS DE CREDITO', 'VIVIENDA_VIS', 'VIVIENDA_NO_VIS', 'CONSUMO_OTROS', 'CARTERA_COMERCIAL_OTROS', 'CARTERA_COMERCIAL_MICROCREDITOS', 'CREDITO ROTATIVO', 'VEHICULO', etc. Usar para análisis agrupados de alto nivel.",
            ),
        ],
    ),
}

# =============================================================================
# Business Concept to Tables Mapping
# =============================================================================

CONCEPT_TO_TABLES: dict[str, list[str]] = {
    # Spanish terms
    "tasas de captacion": ["dbo.Tasas_Captacion"],
    "tasa de captacion": ["dbo.Tasas_Captacion"],
    "tasa captacion": ["dbo.Tasas_Captacion"],
    "captacion": ["dbo.Tasas_Captacion"],
    "tasas de interes": ["dbo.Tasas_Captacion"],
    "tasa de interes": ["dbo.Tasas_Captacion"],
    "cdt": ["dbo.Tasas_Captacion"],
    "cdat": ["dbo.Tasas_Captacion"],
    "cuenta de ahorro": ["dbo.Tasas_Captacion"],
    "cuentas de ahorro": ["dbo.Tasas_Captacion"],
    "distribucion de cartera": ["dbo.Distribucion_Cartera"],
    "distribucion cartera": ["dbo.Distribucion_Cartera"],
    "cartera": ["dbo.Distribucion_Cartera"],
    "portafolio crediticio": ["dbo.Distribucion_Cartera"],
    "credito": ["dbo.Distribucion_Cartera"],
    "cartera comercial": ["dbo.Distribucion_Cartera"],
    "cartera de consumo": ["dbo.Distribucion_Cartera"],
    "vivienda": ["dbo.Distribucion_Cartera"],
    "microcredito": ["dbo.Distribucion_Cartera"],
    "libranza": ["dbo.Distribucion_Cartera"],
    "tarjeta de credito": ["dbo.Distribucion_Cartera"],
    "tarjetas de credito": ["dbo.Distribucion_Cartera"],
    # English terms
    "interest rate": ["dbo.Tasas_Captacion"],
    "interest rates": ["dbo.Tasas_Captacion"],
    "credit portfolio": ["dbo.Distribucion_Cartera"],
}


############################################################
# Helper functions
############################################################


def is_valid_table(table_name: str) -> bool:
    """Check if a table name is valid."""
    return table_name in DATABASE_TABLES


def is_valid_column(table_name: str, column_name: str) -> bool:
    """Check if a column name is valid for a table."""
    columns = get_table_columns(table_name)
    return any(col.column_name == column_name for col in columns)


def get_tables_for_query(query: str) -> set[str] | None:
    """Get all tables that are referenced in a query."""
    query_lower = query.lower()
    result = set()

    for concept, table_list in CONCEPT_TO_TABLES.items():
        if concept in query_lower:
            result.update(table_list)

    return result if result else None


def get_table_info(table_name: str) -> TableInfo | None:
    """Get table information by table name."""
    return DATABASE_TABLES.get(table_name)


def get_all_table_names() -> list[str]:
    """Get all table names."""
    return list(DATABASE_TABLES.keys())


def get_table_columns(table_name: str) -> list[ColumnInfo]:
    """Get columns for a specific table."""
    table_info = get_table_info(table_name)
    if table_info:
        return table_info.table_columns
    return []


def get_column_info(table_name: str, column_name: str) -> ColumnInfo | None:
    """Get column information for a specific column in a table."""
    columns = get_table_columns(table_name)
    for col in columns:
        if col.column_name == column_name:
            return col
    return None