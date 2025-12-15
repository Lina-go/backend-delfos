"""
Database configuration for FinancialDB.
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
    "dbo.Branches": TableInfo(
        table_name="dbo.Branches",
        table_description="Branches table containing branch information",
        table_columns=[
            ColumnInfo(
                column_name="id",
                column_type=ColumnType.INTEGER,
                column_description="Branch ID (primary key)",
            ),
            ColumnInfo(
                column_name="branchName",
                column_type=ColumnType.STRING,
                column_description="Branch name",
            ),
            ColumnInfo(
                column_name="branchCode",
                column_type=ColumnType.STRING,
                column_description="Branch code",
            ),
            ColumnInfo(
                column_name="Address",
                column_type=ColumnType.STRING,
                column_description="Branch address",
            ),
            ColumnInfo(
                column_name="PhoneNumber",
                column_type=ColumnType.STRING,
                column_description="Branch phone number",
            ),
        ],
    ),
    "dbo.Employees": TableInfo(
        table_name="dbo.Employees",
        table_description="Employees table linking people to branches",
        table_columns=[
            ColumnInfo(
                column_name="id",
                column_type=ColumnType.INTEGER,
                column_description="Employee ID (primary key)",
            ),
            ColumnInfo(
                column_name="personId",
                column_type=ColumnType.INTEGER,
                column_description="Foreign key to People.id",
            ),
            ColumnInfo(
                column_name="branchId",
                column_type=ColumnType.INTEGER,
                column_description="Foreign key to Branches.id",
            ),
            ColumnInfo(
                column_name="position",
                column_type=ColumnType.STRING,
                column_description="Employee position",
            ),
        ],
    ),
    "dbo.Customers": TableInfo(
        table_name="dbo.Customers",
        table_description="Customers table linking people to customer type",
        table_columns=[
            ColumnInfo(
                column_name="id",
                column_type=ColumnType.INTEGER,
                column_description="Customer ID (primary key)",
            ),
            ColumnInfo(
                column_name="personId",
                column_type=ColumnType.INTEGER,
                column_description="Foreign key to People.id",
            ),
            ColumnInfo(
                column_name="customerType",
                column_type=ColumnType.STRING,
                column_description="Customer type (natural/juridical)",
            ),
        ],
    ),
    "dbo.Accounts": TableInfo(
        table_name="dbo.Accounts",
        table_description="Accounts table containing account information",
        table_columns=[
            ColumnInfo(
                column_name="id",
                column_type=ColumnType.INTEGER,
                column_description="Account ID (primary key)",
            ),
            ColumnInfo(
                column_name="branchId",
                column_type=ColumnType.INTEGER,
                column_description="Foreign key to Branches.id",
            ),
            ColumnInfo(
                column_name="accountType",
                column_type=ColumnType.STRING,
                column_description="Account type (savings/checking)",
            ),
            ColumnInfo(
                column_name="accountNumber",
                column_type=ColumnType.STRING,
                column_description="Account number",
            ),
            ColumnInfo(
                column_name="currentBalance",
                column_type=ColumnType.FLOAT,
                column_description="Current account balance",
            ),
            ColumnInfo(
                column_name="createdAt",
                column_type=ColumnType.DATETIME,
                column_description="Account creation date",
            ),
            ColumnInfo(
                column_name="closedAt",
                column_type=ColumnType.DATETIME,
                column_description="Account closure date (nullable)",
            ),
            ColumnInfo(
                column_name="accountStatus",
                column_type=ColumnType.STRING,
                column_description="Account status (active/closed)",
            ),
        ],
    ),
    "dbo.AccountOwnerships": TableInfo(
        table_name="dbo.AccountOwnerships",
        table_description="Account ownerships table linking accounts to customers",
        table_columns=[
            ColumnInfo(
                column_name="id",
                column_type=ColumnType.INTEGER,
                column_description="Ownership ID (primary key)",
            ),
            ColumnInfo(
                column_name="accountId",
                column_type=ColumnType.INTEGER,
                column_description="Foreign key to Accounts.id",
            ),
            ColumnInfo(
                column_name="ownerId",
                column_type=ColumnType.INTEGER,
                column_description="Foreign key to Customers.id",
            ),
        ],
    ),
    "dbo.Loans": TableInfo(
        table_name="dbo.Loans",
        table_description="Loans table containing loan information",
        table_columns=[
            ColumnInfo(
                column_name="id",
                column_type=ColumnType.INTEGER,
                column_description="Loan ID (primary key)",
            ),
            ColumnInfo(
                column_name="customerId",
                column_type=ColumnType.INTEGER,
                column_description="Foreign key to Customers.id",
            ),
            ColumnInfo(
                column_name="loanType",
                column_type=ColumnType.STRING,
                column_description="Loan type",
            ),
            ColumnInfo(
                column_name="loanAmount",
                column_type=ColumnType.FLOAT,
                column_description="Loan amount",
            ),
            ColumnInfo(
                column_name="interestRate",
                column_type=ColumnType.FLOAT,
                column_description="Interest rate",
            ),
            ColumnInfo(
                column_name="term",
                column_type=ColumnType.INTEGER,
                column_description="Loan term (in months)",
            ),
            ColumnInfo(
                column_name="startDate",
                column_type=ColumnType.DATE,
                column_description="Loan start date",
            ),
            ColumnInfo(
                column_name="endDate",
                column_type=ColumnType.DATE,
                column_description="Loan end date",
            ),
            ColumnInfo(
                column_name="status",
                column_type=ColumnType.STRING,
                column_description="Loan status (active/closed)",
            ),
        ],
    ),
    "dbo.LoanPayments": TableInfo(
        table_name="dbo.LoanPayments",
        table_description="Loan payments table containing payment information",
        table_columns=[
            ColumnInfo(
                column_name="id",
                column_type=ColumnType.INTEGER,
                column_description="Payment ID (primary key)",
            ),
            ColumnInfo(
                column_name="loanId",
                column_type=ColumnType.INTEGER,
                column_description="Foreign key to Loans.id",
            ),
            ColumnInfo(
                column_name="scheduledPaymentDate",
                column_type=ColumnType.DATE,
                column_description="Scheduled payment date",
            ),
            ColumnInfo(
                column_name="paymentAmount",
                column_type=ColumnType.FLOAT,
                column_description="Total payment amount",
            ),
            ColumnInfo(
                column_name="principalAmount",
                column_type=ColumnType.FLOAT,
                column_description="Principal portion of payment",
            ),
            ColumnInfo(
                column_name="interestAmount",
                column_type=ColumnType.FLOAT,
                column_description="Interest portion of payment",
            ),
            ColumnInfo(
                column_name="paidAmount",
                column_type=ColumnType.FLOAT,
                column_description="Amount actually paid",
            ),
            ColumnInfo(
                column_name="paidDate",
                column_type=ColumnType.DATE,
                column_description="Date payment was made (nullable)",
            ),
        ],
    ),
    "dbo.Transactions": TableInfo(
        table_name="dbo.Transactions",
        table_description="Transactions table containing transaction information",
        table_columns=[
            ColumnInfo(
                column_name="id",
                column_type=ColumnType.INTEGER,
                column_description="Transaction ID (primary key)",
            ),
            ColumnInfo(
                column_name="accountId",
                column_type=ColumnType.INTEGER,
                column_description="Foreign key to Accounts.id",
            ),
            ColumnInfo(
                column_name="transactionType",
                column_type=ColumnType.STRING,
                column_description="Transaction type (deposit/withdrawal)",
            ),
            ColumnInfo(
                column_name="amount",
                column_type=ColumnType.FLOAT,
                column_description="Transaction amount",
            ),
            ColumnInfo(
                column_name="transactionDate",
                column_type=ColumnType.DATETIME,
                column_description="Transaction date",
            ),
        ],
    ),
    "dbo.Transfers": TableInfo(
        table_name="dbo.Transfers",
        table_description="Transfers table containing transfer information between accounts",
        table_columns=[
            ColumnInfo(
                column_name="id",
                column_type=ColumnType.INTEGER,
                column_description="Transfer ID (primary key)",
            ),
            ColumnInfo(
                column_name="originAccountId",
                column_type=ColumnType.INTEGER,
                column_description="Foreign key to Accounts.id (origin)",
            ),
            ColumnInfo(
                column_name="destinationAccountId",
                column_type=ColumnType.INTEGER,
                column_description="Foreign key to Accounts.id (destination)",
            ),
            ColumnInfo(
                column_name="amount",
                column_type=ColumnType.FLOAT,
                column_description="Transfer amount",
            ),
            ColumnInfo(
                column_name="occurenceTime",
                column_type=ColumnType.DATETIME,
                column_description="Transfer occurrence time",
            ),
        ],
    ),
}

# =============================================================================
# Business Concept to Tables Mapping
# =============================================================================

CONCEPT_TO_TABLES: dict[str, list[str]] = {
    # Spanish terms
    "clientes": ["dbo.Customers", "dbo.People"],
    "cliente": ["dbo.Customers", "dbo.People"],
    "personas": ["dbo.People"],
    "persona": ["dbo.People"],
    "cuentas": ["dbo.Accounts", "dbo.AccountOwnerships"],
    "cuenta": ["dbo.Accounts", "dbo.AccountOwnerships"],
    "saldo": ["dbo.Accounts"],
    "préstamos": ["dbo.Loans", "dbo.LoanPayments"],
    "prestamos": ["dbo.Loans", "dbo.LoanPayments"],
    "préstamo": ["dbo.Loans", "dbo.LoanPayments"],
    "prestamo": ["dbo.Loans", "dbo.LoanPayments"],
    "pagos": ["dbo.LoanPayments", "dbo.Transactions"],
    "pago": ["dbo.LoanPayments", "dbo.Transactions"],
    "transacciones": ["dbo.Transactions"],
    "transaccion": ["dbo.Transactions"],
    "transferencias": ["dbo.Transfers"],
    "transferencia": ["dbo.Transfers"],
    "empleados": ["dbo.Employees", "dbo.People"],
    "empleado": ["dbo.Employees", "dbo.People"],
    "sucursales": ["dbo.Branches"],
    "sucursal": ["dbo.Branches"],
    "tipo de cuenta": ["dbo.Accounts"],
    "tipo cuenta": ["dbo.Accounts"],
    "tipo de préstamo": ["dbo.Loans"],
    "tipo prestamo": ["dbo.Loans"],
    "tipo de cliente": ["dbo.Customers"],
    "tipo cliente": ["dbo.Customers"],
    # English terms
    "customers": ["dbo.Customers", "dbo.People"],
    "customer": ["dbo.Customers", "dbo.People"],
    "people": ["dbo.People"],
    "person": ["dbo.People"],
    "accounts": ["dbo.Accounts", "dbo.AccountOwnerships"],
    "account": ["dbo.Accounts", "dbo.AccountOwnerships"],
    "balance": ["dbo.Accounts"],
    "loans": ["dbo.Loans", "dbo.LoanPayments"],
    "loan": ["dbo.Loans", "dbo.LoanPayments"],
    "payments": ["dbo.LoanPayments", "dbo.Transactions"],
    "payment": ["dbo.LoanPayments", "dbo.Transactions"],
    "transactions": ["dbo.Transactions"],
    "transaction": ["dbo.Transactions"],
    "transfers": ["dbo.Transfers"],
    "transfer": ["dbo.Transfers"],
    "employees": ["dbo.Employees", "dbo.People"],
    "employee": ["dbo.Employees", "dbo.People"],
    "branches": ["dbo.Branches"],
    "branch": ["dbo.Branches"],
    "account type": ["dbo.Accounts"],
    "loan type": ["dbo.Loans"],
    "customer type": ["dbo.Customers"],
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
