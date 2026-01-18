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
    "dbo.People": TableInfo(
        table_name="dbo.People",
        table_description="People table containing personal information",
        table_columns=[
            ColumnInfo(
                column_name="id",
                column_type=ColumnType.INTEGER,
                column_description="Person ID (primary key)",
            ),
            ColumnInfo(
                column_name="firstName",
                column_type=ColumnType.STRING,
                column_description="First name",
            ),
            ColumnInfo(
                column_name="lastName",
                column_type=ColumnType.STRING,
                column_description="Last name",
            ),
            ColumnInfo(
                column_name="DateOfBirth",
                column_type=ColumnType.DATE,
                column_description="Date of birth",
            ),
            ColumnInfo(
                column_name="PhoneNumber",
                column_type=ColumnType.STRING,
                column_description="Phone number",
            ),
            ColumnInfo(
                column_name="Email",
                column_type=ColumnType.STRING,
                column_description="Email address",
            ),
            ColumnInfo(
                column_name="Address",
                column_type=ColumnType.STRING,
                column_description="Physical address",
            ),
        ],
    ),
    "dbo.Tasas_Captacion": TableInfo(
        table_name="dbo.Tasas_Captacion",
        table_description="Tasas captacion table containing interest rate observations",
        table_columns=[
            ColumnInfo(
                column_name="TIPOENTIDAD",
                column_type=ColumnType.INTEGER,
                column_description="Financial entity classification",
            ),
            ColumnInfo(
                column_name="CODIGOENTIDAD",
                column_type=ColumnType.INTEGER,
                column_description="Unique entity identifier",
            ),
            ColumnInfo(
                column_name="NOMBREENTIDAD",
                column_type=ColumnType.STRING,
                column_description="Official entity name",
            ),
            ColumnInfo(
                column_name="FECHACORTE",
                column_type=ColumnType.DATE,
                column_description="Observation cutoff date",
            ),
            ColumnInfo(
                column_name="UCA",
                column_type=ColumnType.INTEGER,
                column_description="Analytical classification unit for rate category",
            ),
            ColumnInfo(
                column_name="NOMBRE_UNIDAD_DE_CAPTURA",
                column_type=ColumnType.STRING,
                column_description="Description of the UCA rate category",
            ),
            ColumnInfo(
                column_name="SUBCUENTA",
                column_type=ColumnType.INTEGER,
                column_description="Subaccount code for additional classification",
            ),
            ColumnInfo(
                column_name="DESCRIPCION",
                column_type=ColumnType.STRING,
                column_description="Description of the reporting concept",
            ),
            ColumnInfo(
                column_name="TASA",
                column_type=ColumnType.FLOAT,
                column_description="Reported interest rate",
            ),
            ColumnInfo(
                column_name="MONTO",
                column_type=ColumnType.FLOAT,
                column_description="Associated monetary amount",
            ),
        ],
    ),
    "dbo.Distribucion_Cartera": TableInfo(
        table_name="dbo.Distribucion_Cartera",
        table_description="Distribucion cartera table containing credit portfolio balances",
        table_columns=[
            ColumnInfo(
                column_name="TIPO_ENTIDAD",
                column_type=ColumnType.INTEGER,
                column_description="Financial entity classification",
            ),
            ColumnInfo(
                column_name="CODIGO_ENTIDAD",
                column_type=ColumnType.INTEGER,
                column_description="Unique entity identifier",
            ),
            ColumnInfo(
                column_name="NOMBREENTIDAD",
                column_type=ColumnType.STRING,
                column_description="Official entity name",
            ),
            ColumnInfo(
                column_name="FECHA_CORTE",
                column_type=ColumnType.DATE,
                column_description="Portfolio reporting cutoff date",
            ),
            ColumnInfo(
                column_name="UNICAP",
                column_type=ColumnType.INTEGER,
                column_description="Credit product classification unit",
            ),
            ColumnInfo(
                column_name="DESCRIP_UC",
                column_type=ColumnType.STRING,
                column_description="Credit product description",
            ),
            ColumnInfo(
                column_name="RENGLON",
                column_type=ColumnType.INTEGER,
                column_description="Additional portfolio classification",
            ),
            ColumnInfo(
                column_name="DESC_RENGLON",
                column_type=ColumnType.STRING,
                column_description="Portfolio classification description",
            ),
            ColumnInfo(
                column_name="SALDO_CARTERA_A_FECHA_DE_CORTE",
                column_type=ColumnType.FLOAT,
                column_description="Portfolio balance at cutoff date",
            ),
            ColumnInfo(
                column_name="VIGENTE",
                column_type=ColumnType.FLOAT,
                column_description="Current portfolio balance",
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
