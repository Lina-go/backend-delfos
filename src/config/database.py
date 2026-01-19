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
                column_name="TIPOENTIDAD",
                column_type=ColumnType.INTEGER,
                column_description="Código del tipo de entidad financiera: 1=Bancos Comerciales, 2=Corporaciones Financieras, 4=Compañías de Financiamiento, 22=Bancos de Segundo Piso (Bancoldex, Finagro, Findeter), 32=Cooperativas Financieras. Usar para agrupar por TIPO de entidad, NO por entidad individual.",
            ),
            ColumnInfo(
                column_name="CODIGOENTIDAD",
                column_type=ColumnType.INTEGER,
                column_description="Código único de la entidad financiera individual. Usar para identificar o filtrar una entidad específica.",
            ),
            ColumnInfo(
                column_name="NOMBREENTIDAD",
                column_type=ColumnType.STRING,
                column_description="Nombre de la entidad financiera individual (ej: 'Bancolombia', 'Davivienda', 'BBVA Colombia'). Usar solo cuando la pregunta pide información de un banco específico, NO para agrupar por tipo de entidad.",
            ),
            ColumnInfo(
                column_name="FECHACORTE",
                column_type=ColumnType.DATE,
                column_description="Fecha de reporte de la tasa (datos diarios). Para evolución temporal, considerar agrupar por mes: DATEPART(YEAR, FECHACORTE), DATEPART(MONTH, FECHACORTE).",
            ),
            ColumnInfo(
                column_name="UCA",
                column_type=ColumnType.INTEGER,
                column_description="Código de categoría de producto de captación: 1=CDT, 2=CDAT, 3=Operaciones Mercado Monetario, 4=Interbancarios, 5=Repos, 7=Cuentas de Ahorro, 8=Cuentas Corrientes.",
            ),
            ColumnInfo(
                column_name="NOMBRE_UNIDAD_DE_CAPTURA",
                column_type=ColumnType.STRING,
                column_description="Descripción del producto de captación (CDT, CDAT, Cuentas de Ahorro, etc.). Usar para filtrar por tipo de producto.",
            ),
            ColumnInfo(
                column_name="SUBCUENTA",
                column_type=ColumnType.INTEGER,
                column_description="Subcategoría del producto (plazo, tipo específico).",
            ),
            ColumnInfo(
                column_name="DESCRIPCION",
                column_type=ColumnType.STRING,
                column_description="Descripción detallada del plazo o característica (ej: '90 días', '180 días'). NO incluir en GROUP BY a menos que se pida análisis por plazo.",
            ),
            ColumnInfo(
                column_name="TASA",
                column_type=ColumnType.FLOAT,
                column_description="Tasa de interés reportada (valor decimal, ej: 0.05 = 5%). Usar AVG(TASA) para promedios.",
            ),
            ColumnInfo(
                column_name="MONTO",
                column_type=ColumnType.FLOAT,
                column_description="Monto en pesos colombianos asociado a la tasa. Usar SUM(MONTO) para totales.",
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
                column_name="NOMBREENTIDAD",
                column_type=ColumnType.STRING,
                column_description="Nombre de la entidad financiera individual (ej: 'Bancolombia', 'Davivienda'). Usar solo cuando la pregunta pide información de un banco específico, NO para agrupar por tipo de entidad.",
            ),
            ColumnInfo(
                column_name="FECHA_CORTE",
                column_type=ColumnType.DATE,
                column_description="Fecha de reporte del saldo de cartera. Para evolución temporal, considerar agrupar por mes.",
            ),
            ColumnInfo(
                column_name="UNICAP",
                column_type=ColumnType.INTEGER,
                column_description="Código de categoría de cartera (comercial, consumo, vivienda, microcrédito).",
            ),
            ColumnInfo(
                column_name="DESCRIP_UC",
                column_type=ColumnType.STRING,
                column_description="Tipo de categoría principal de la cartera: 'CARTERA COMERCIAL...', 'CONSUMO...', 'VIVIENDA...', 'MICROCREDITOS...', 'LIBRANZA', 'TARJETAS DE CREDITO', 'VEHICULO'. Usar para agrupar por tipo de producto de crédito.",
            ),
            ColumnInfo(
                column_name="RENGLON",
                column_type=ColumnType.INTEGER,
                column_description="Código de subcategoría de cartera.",
            ),
            ColumnInfo(
                column_name="DESC_RENGLON",
                column_type=ColumnType.STRING,
                column_description="Subcategoría detallada: 'LIBRE INVERSION', 'TARJETA DE CREDITO TOTAL', 'VEHICULO', 'EMPRESARIAL', 'PYMES', etc. Usar para análisis más granular dentro de una categoría.",
            ),
            ColumnInfo(
                column_name="SALDO_CARTERA_A_FECHA_DE_CORTE",
                column_type=ColumnType.FLOAT,
                column_description="Saldo total de cartera en pesos colombianos a la fecha de corte. Usar SUM() para agregar.",
            ),
            ColumnInfo(
                column_name="VIGENTE",
                column_type=ColumnType.FLOAT,
                column_description="Saldo de cartera vigente (al día) en pesos colombianos. Usar SUM() para agregar.",
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
    "distribucion de cartera": ["dbo.Distribucion_Cartera"],
    "distribucion cartera": ["dbo.Distribucion_Cartera"],
    "cartera": ["dbo.Distribucion_Cartera"],
    "portafolio crediticio": ["dbo.Distribucion_Cartera"],
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
