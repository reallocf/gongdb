//! AST nodes for GongDB's SQL parser.
//!
//! These types are stable, data-only representations of parsed SQL. They are
//! primarily produced by `parser::parse_statement` and consumed by the engine.
//!
//! # Examples
//! ```no_run
//! use gongdb::ast::{Ident, ObjectName, Statement};
//! use gongdb::parser::parse_statement;
//!
//! let stmt = parse_statement("CREATE TABLE t(id INTEGER)").unwrap();
//! match stmt {
//!     Statement::CreateTable(create) => {
//!         assert_eq!(create.name, ObjectName::new([Ident::new("t")]));
//!     }
//!     _ => unreachable!("expected create table"),
//! }
//! ```

#[derive(Debug, Clone, PartialEq)]
/// Top-level SQL statement variants.
pub enum Statement {
    /// CREATE TABLE statement.
    CreateTable(CreateTable),
    /// DROP TABLE statement.
    DropTable(DropTable),
    /// CREATE INDEX statement.
    CreateIndex(CreateIndex),
    /// DROP INDEX statement.
    DropIndex(DropIndex),
    /// REINDEX statement.
    Reindex(Reindex),
    /// CREATE VIEW statement.
    CreateView(CreateView),
    /// DROP VIEW statement.
    DropView(DropView),
    /// CREATE TRIGGER statement.
    CreateTrigger(CreateTrigger),
    /// DROP TRIGGER statement.
    DropTrigger(DropTrigger),
    /// INSERT statement.
    Insert(Insert),
    /// UPDATE statement.
    Update(Update),
    /// DELETE statement.
    Delete(Delete),
    /// SELECT statement.
    Select(Select),
    /// BEGIN transaction statement.
    BeginTransaction(BeginTransaction),
    /// COMMIT statement.
    Commit,
    /// ROLLBACK statement.
    Rollback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Transaction isolation level.
pub enum IsolationLevel {
    /// Read uncommitted isolation.
    ReadUncommitted,
    /// Read committed isolation.
    ReadCommitted,
    /// Repeatable read isolation.
    RepeatableRead,
    /// Serializable isolation.
    Serializable,
}

#[derive(Debug, Clone, PartialEq)]
/// BEGIN TRANSACTION statement.
pub struct BeginTransaction {
    /// Optional isolation level requested by the statement.
    pub isolation: Option<IsolationLevel>,
}

#[derive(Debug, Clone, PartialEq)]
/// Common table expression (WITH clause) wrapper.
pub struct With {
    /// Whether this is a recursive WITH clause.
    pub recursive: bool,
    /// The CTE definitions.
    pub ctes: Vec<Cte>,
}

#[derive(Debug, Clone, PartialEq)]
/// Single common table expression.
pub struct Cte {
    /// CTE name.
    pub name: Ident,
    /// Optional column list.
    pub columns: Vec<Ident>,
    /// CTE query.
    pub query: Box<Select>,
}

#[derive(Debug, Clone, PartialEq)]
/// CREATE TABLE statement.
pub struct CreateTable {
    /// Whether IF NOT EXISTS was specified.
    pub if_not_exists: bool,
    /// Table name.
    pub name: ObjectName,
    /// Column definitions.
    pub columns: Vec<ColumnDef>,
    /// Table-level constraints.
    pub constraints: Vec<TableConstraint>,
    /// Whether WITHOUT ROWID was specified.
    pub without_rowid: bool,
}

#[derive(Debug, Clone, PartialEq)]
/// DROP TABLE statement.
pub struct DropTable {
    /// Whether IF EXISTS was specified.
    pub if_exists: bool,
    /// Table name.
    pub name: ObjectName,
}

#[derive(Debug, Clone, PartialEq)]
/// CREATE INDEX statement.
pub struct CreateIndex {
    /// Whether IF NOT EXISTS was specified.
    pub if_not_exists: bool,
    /// Index name.
    pub name: ObjectName,
    /// Table the index targets.
    pub table: ObjectName,
    /// Indexed columns and sort order.
    pub columns: Vec<IndexedColumn>,
    /// Whether the index is unique.
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq)]
/// DROP INDEX statement.
pub struct DropIndex {
    /// Whether IF EXISTS was specified.
    pub if_exists: bool,
    /// Index name.
    pub name: ObjectName,
}

#[derive(Debug, Clone, PartialEq)]
/// REINDEX statement.
pub struct Reindex {
    /// Optional index or table name.
    pub name: Option<ObjectName>,
}

#[derive(Debug, Clone, PartialEq)]
/// CREATE VIEW statement.
pub struct CreateView {
    /// Whether IF NOT EXISTS was specified.
    pub if_not_exists: bool,
    /// View name.
    pub name: ObjectName,
    /// Optional column list.
    pub columns: Vec<Ident>,
    /// View query.
    pub query: Select,
}

#[derive(Debug, Clone, PartialEq)]
/// DROP VIEW statement.
pub struct DropView {
    /// Whether IF EXISTS was specified.
    pub if_exists: bool,
    /// View name.
    pub name: ObjectName,
}

#[derive(Debug, Clone, PartialEq)]
/// CREATE TRIGGER statement.
pub struct CreateTrigger {
    /// Whether IF NOT EXISTS was specified.
    pub if_not_exists: bool,
    /// Trigger name.
    pub name: ObjectName,
    /// Table the trigger targets.
    pub table: ObjectName,
}

#[derive(Debug, Clone, PartialEq)]
/// DROP TRIGGER statement.
pub struct DropTrigger {
    /// Whether IF EXISTS was specified.
    pub if_exists: bool,
    /// Trigger name.
    pub name: ObjectName,
}

#[derive(Debug, Clone, PartialEq)]
/// INSERT statement.
pub struct Insert {
    /// Target table name.
    pub table: ObjectName,
    /// Target columns list.
    pub columns: Vec<Ident>,
    /// Source for inserted rows.
    pub source: InsertSource,
    /// Conflict handling mode.
    pub on_conflict: InsertConflict,
}

#[derive(Debug, Clone, PartialEq)]
/// Source of data for INSERT.
pub enum InsertSource {
    /// VALUES clause.
    Values(Vec<Vec<Expr>>),
    /// INSERT ... SELECT.
    Select(Box<Select>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Conflict handling mode for INSERT.
pub enum InsertConflict {
    /// Abort on conflict (default).
    Abort,
    /// Replace on conflict.
    Replace,
}

#[derive(Debug, Clone, PartialEq)]
/// UPDATE statement.
pub struct Update {
    /// Target table name.
    pub table: ObjectName,
    /// Column assignments.
    pub assignments: Vec<Assignment>,
    /// Optional WHERE clause.
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
/// DELETE statement.
pub struct Delete {
    /// Target table name.
    pub table: ObjectName,
    /// Optional WHERE clause.
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
/// Column assignment in UPDATE.
pub struct Assignment {
    /// Column being assigned.
    pub column: Ident,
    /// Expression assigned to the column.
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq)]
/// SELECT statement.
pub struct Select {
    /// Optional WITH clause.
    pub with: Option<With>,
    /// Whether DISTINCT was specified.
    pub distinct: bool,
    /// Projection list.
    pub projection: Vec<SelectItem>,
    /// FROM clause.
    pub from: Vec<TableRef>,
    /// Optional WHERE clause.
    pub selection: Option<Expr>,
    /// GROUP BY clause expressions.
    pub group_by: Vec<Expr>,
    /// Optional HAVING clause.
    pub having: Option<Expr>,
    /// ORDER BY expressions.
    pub order_by: Vec<OrderByExpr>,
    /// Optional LIMIT expression.
    pub limit: Option<Expr>,
    /// Optional OFFSET expression.
    pub offset: Option<Expr>,
    /// Compound SELECTs (UNION/INTERSECT/EXCEPT).
    pub compounds: Vec<CompoundSelect>,
}

#[derive(Debug, Clone, PartialEq)]
/// Item in SELECT projection.
pub enum SelectItem {
    /// Expression with an optional alias.
    Expr { expr: Expr, alias: Option<Ident> },
    /// Wildcard projection (`*`).
    Wildcard,
    /// Qualified wildcard (`table.*`).
    QualifiedWildcard(ObjectName),
}

#[derive(Debug, Clone, PartialEq)]
/// Compound operator between SELECTs.
pub enum CompoundOperator {
    /// UNION operator.
    Union,
    /// UNION ALL operator.
    UnionAll,
    /// INTERSECT operator.
    Intersect,
    /// EXCEPT operator.
    Except,
}

#[derive(Debug, Clone, PartialEq)]
/// Compound SELECT entry.
pub struct CompoundSelect {
    /// Operator between SELECTs.
    pub operator: CompoundOperator,
    /// SELECT being combined.
    pub select: Box<Select>,
}

#[derive(Debug, Clone, PartialEq)]
/// Table reference in FROM clause.
pub enum TableRef {
    /// Named table reference.
    Named { name: ObjectName, alias: Option<Ident> },
    /// Subquery reference.
    Subquery { subquery: Box<Select>, alias: Option<Ident> },
    /// Join reference.
    Join {
        left: Box<TableRef>,
        right: Box<TableRef>,
        operator: JoinOperator,
        constraint: Option<JoinConstraint>,
    },
}

#[derive(Debug, Clone, PartialEq)]
/// Join operator type.
pub enum JoinOperator {
    /// INNER JOIN.
    Inner,
    /// LEFT JOIN.
    Left,
    /// RIGHT JOIN.
    Right,
    /// FULL JOIN.
    Full,
    /// CROSS JOIN.
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
/// Join constraint clause.
pub enum JoinConstraint {
    /// ON `expr`.
    On(Expr),
    /// USING (col, ...).
    Using(Vec<Ident>),
}

#[derive(Debug, Clone, PartialEq)]
/// ORDER BY expression with optional ordering.
pub struct OrderByExpr {
    /// ORDER BY expression.
    pub expr: Expr,
    /// Optional ascending/descending indicator.
    pub asc: Option<bool>,
    /// Optional NULLS FIRST/LAST.
    pub nulls: Option<NullsOrder>,
}

#[derive(Debug, Clone, PartialEq)]
/// NULL ordering directive.
pub enum NullsOrder {
    /// NULLS FIRST.
    First,
    /// NULLS LAST.
    Last,
}

#[derive(Debug, Clone, PartialEq)]
/// Expression node.
pub enum Expr {
    /// Unqualified identifier.
    Identifier(Ident),
    /// Compound identifier (`schema.table`).
    CompoundIdentifier(Vec<Ident>),
    /// Literal value.
    Literal(Literal),
    /// Binary operator expression.
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    /// Unary operator expression.
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    /// Function call.
    Function {
        name: Ident,
        args: Vec<Expr>,
        distinct: bool,
    },
    /// CASE expression.
    Case {
        operand: Option<Box<Expr>>,
        when_then: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
    /// BETWEEN expression.
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    /// IN (list) expression.
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// IN (subquery) expression.
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Select>,
        negated: bool,
    },
    /// EXISTS (subquery).
    Exists(Box<Select>),
    /// Nested SELECT used as expression.
    Subquery(Box<Select>),
    /// IS NULL / IS NOT NULL.
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    /// CAST expression.
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    /// Parenthesized expression.
    Nested(Box<Expr>),
    /// Wildcard placeholder in projections.
    Wildcard,
}

#[derive(Debug, Clone, PartialEq)]
/// Binary operator.
pub enum BinaryOperator {
    /// Addition.
    Plus,
    /// Subtraction.
    Minus,
    /// Multiplication.
    Multiply,
    /// Division.
    Divide,
    /// Modulo.
    Modulo,
    /// Logical AND.
    And,
    /// Logical OR.
    Or,
    /// Equals.
    Eq,
    /// Not equals.
    NotEq,
    /// Less-than.
    Lt,
    /// Less-than or equal.
    LtEq,
    /// Greater-than.
    Gt,
    /// Greater-than or equal.
    GtEq,
    /// LIKE operator.
    Like,
    /// NOT LIKE operator.
    NotLike,
    /// IS operator.
    Is,
    /// IS NOT operator.
    IsNot,
    /// String concatenation.
    Concat,
}

#[derive(Debug, Clone, PartialEq)]
/// Unary operator.
pub enum UnaryOperator {
    /// Unary plus.
    Plus,
    /// Unary minus.
    Minus,
    /// Logical NOT.
    Not,
}

#[derive(Debug, Clone, PartialEq)]
/// Literal value.
pub enum Literal {
    /// NULL literal.
    Null,
    /// Integer literal.
    Integer(i64),
    /// Floating-point literal.
    Float(f64),
    /// String literal.
    String(String),
    /// Boolean literal.
    Boolean(bool),
    /// Blob literal.
    Blob(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Identifier with optional quoting metadata.
pub struct Ident {
    /// Raw identifier text.
    pub value: String,
    /// Whether the identifier was quoted.
    pub quoted: bool,
}

impl Ident {
    /// Create an unquoted identifier from a string-like value.
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            quoted: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Qualified object name (e.g. `schema.table`).
pub struct ObjectName(pub Vec<Ident>);

impl ObjectName {
    /// Build an object name from an iterator of identifiers.
    pub fn new(parts: impl IntoIterator<Item = Ident>) -> Self {
        Self(parts.into_iter().collect())
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Column definition within CREATE TABLE.
pub struct ColumnDef {
    /// Column name.
    pub name: Ident,
    /// Optional data type.
    pub data_type: Option<DataType>,
    /// Column constraints.
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone, PartialEq)]
/// Column-level constraint.
pub enum ColumnConstraint {
    /// NOT NULL.
    NotNull,
    /// NULL.
    Null,
    /// PRIMARY KEY.
    PrimaryKey,
    /// UNIQUE.
    Unique,
    /// DEFAULT expression.
    Default(Expr),
}

#[derive(Debug, Clone, PartialEq)]
/// Table-level constraint.
pub enum TableConstraint {
    /// PRIMARY KEY constraint.
    PrimaryKey(Vec<Ident>),
    /// UNIQUE constraint.
    Unique(Vec<Ident>),
    /// CHECK expression.
    Check(Expr),
    /// FOREIGN KEY constraint.
    ForeignKey {
        columns: Vec<Ident>,
        foreign_table: ObjectName,
        referred_columns: Vec<Ident>,
    },
}

#[derive(Debug, Clone, PartialEq)]
/// SQLite-style type affinity used in parsing.
pub enum DataType {
    /// INTEGER affinity.
    Integer,
    /// REAL affinity.
    Real,
    /// TEXT affinity.
    Text,
    /// BLOB affinity.
    Blob,
    /// NUMERIC affinity.
    Numeric,
    /// Custom or unknown type name.
    Custom(String),
}

#[derive(Debug, Clone, PartialEq)]
/// Indexed column with optional sort order.
pub struct IndexedColumn {
    /// Column name.
    pub name: Ident,
    /// Optional sort order.
    pub order: Option<SortOrder>,
}

#[derive(Debug, Clone, PartialEq)]
/// Sort order for index columns and ORDER BY.
pub enum SortOrder {
    /// Ascending order.
    Asc,
    /// Descending order.
    Desc,
}
