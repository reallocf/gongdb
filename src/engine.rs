use crate::ast::{
    BinaryOperator, ColumnDef, DataType, Expr, Ident, InsertSource, Select, SelectItem, Statement,
};
use crate::parser;
use crate::storage::{Column, StorageEngine, StorageError, TableMeta, Value};
use async_trait::async_trait;
use sqllogictest::{DBOutput, DefaultColumnType};
use std::collections::HashMap;

#[derive(Debug)]
pub struct GongDBError {
    message: String,
}

impl GongDBError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for GongDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for GongDBError {}

impl From<StorageError> for GongDBError {
    fn from(err: StorageError) -> Self {
        GongDBError::new(err.to_string())
    }
}

pub struct GongDB {
    storage: StorageEngine,
}

impl GongDB {
    pub fn new_in_memory() -> Result<Self, GongDBError> {
        Ok(Self {
            storage: StorageEngine::new_in_memory()?,
        })
    }

    pub fn new_on_disk(path: &str) -> Result<Self, GongDBError> {
        Ok(Self {
            storage: StorageEngine::new_on_disk(path)?,
        })
    }

    pub fn run_statement(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, GongDBError> {
        let stmt = parser::parse_statement(sql).map_err(|e| GongDBError::new(e.message))?;
        match stmt {
            Statement::CreateTable(create) => {
                let columns = create
                    .columns
                    .into_iter()
                    .map(column_from_def)
                    .collect::<Vec<_>>();
                let name = object_name(&create.name);
                if self.storage.get_table(&name).is_some() && !create.if_not_exists {
                    return Err(GongDBError::new(format!(
                        "table already exists: {}",
                        name
                    )));
                }
                if self.storage.get_table(&name).is_none() {
                    let first_page = self.storage.allocate_data_page()?;
                    let meta = TableMeta {
                        name: name.clone(),
                        columns,
                        first_page,
                        last_page: first_page,
                    };
                    self.storage.create_table(meta)?;
                }
                Ok(DBOutput::StatementComplete(0))
            }
            Statement::DropTable(drop) => {
                let name = object_name(&drop.name);
                if self.storage.get_table(&name).is_none() && !drop.if_exists {
                    return Err(GongDBError::new(format!(
                        "table not found: {}",
                        name
                    )));
                }
                if self.storage.get_table(&name).is_some() {
                    self.storage.drop_table(&name)?;
                }
                Ok(DBOutput::StatementComplete(0))
            }
            Statement::Insert(insert) => {
                let table_name = object_name(&insert.table);
                let table = self
                    .storage
                    .get_table(&table_name)
                    .ok_or_else(|| GongDBError::new(format!("table not found: {}", table_name)))?
                    .clone();
                let rows = match insert.source {
                    InsertSource::Values(values) => values,
                    _ => {
                        return Err(GongDBError::new(
                            "only VALUES insert is supported in phase 2",
                        ))
                    }
                };
                for exprs in rows {
                    let row = build_insert_row(&table, &insert.columns, &exprs)?;
                    self.storage.insert_row(&table_name, &row)?;
                }
                Ok(DBOutput::StatementComplete(0))
            }
            Statement::Select(select) => self.run_select(&select),
            _ => Err(GongDBError::new(
                "statement not supported in phase 2",
            )),
        }
    }

    fn run_select(&self, select: &Select) -> Result<DBOutput<DefaultColumnType>, GongDBError> {
        let (table_name, columns) = single_table(&self.storage, select)?;
        let rows = self.storage.scan_table(&table_name)?;

        let mut filtered = Vec::new();
        for row in rows {
            if let Some(predicate) = &select.selection {
                let value = eval_expr(predicate, &columns, &row)?;
                if !value_to_bool(&value) {
                    continue;
                }
            }
            filtered.push(row);
        }

        if is_count_star(select) {
            let count = filtered.len() as i64;
            let output_rows = vec![vec![value_to_string(&Value::Integer(count))]];
            return Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text],
                rows: output_rows,
            });
        }

        if !select.order_by.is_empty() {
            let order_by = select.order_by.clone();
            filtered.sort_by(|a, b| compare_order_by(&order_by, &columns, a, b));
        }

        let mut projected = Vec::new();
        for row in filtered {
            let mut output = Vec::new();
            for item in &select.projection {
                match item {
                    SelectItem::Wildcard => {
                        for value in &row {
                            output.push(value_to_string(value));
                        }
                    }
                    SelectItem::Expr { expr, .. } => {
                        let value = eval_expr(expr, &columns, &row)?;
                        output.push(value_to_string(&value));
                    }
                    _ => {
                        return Err(GongDBError::new(
                            "qualified wildcard not supported in phase 2",
                        ))
                    }
                }
            }
            projected.push(output);
        }

        let column_count = projected.first().map(|row| row.len()).unwrap_or(0);
        Ok(DBOutput::Rows {
            types: vec![DefaultColumnType::Text; column_count],
            rows: projected,
        })
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for GongDB {
    type Error = GongDBError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        self.run_statement(sql)
    }

    async fn shutdown(&mut self) {}
}

fn single_table(
    storage: &StorageEngine,
    select: &Select,
) -> Result<(String, Vec<Column>), GongDBError> {
    if select.from.len() != 1 {
        return Err(GongDBError::new(
            "only single-table queries are supported in phase 2",
        ));
    }
    match &select.from[0] {
        crate::ast::TableRef::Named { name, .. } => {
            let table_name = object_name(name);
            let table = storage
                .get_table(&table_name)
                .ok_or_else(|| GongDBError::new(format!("table not found: {}", table_name)))?;
            Ok((table_name, table.columns.clone()))
        }
        _ => Err(GongDBError::new(
            "only simple table references are supported in phase 2",
        )),
    }
}

fn column_from_def(def: ColumnDef) -> Column {
    Column {
        name: def.name.value,
        data_type: def.data_type.unwrap_or(DataType::Text),
    }
}

fn object_name(name: &crate::ast::ObjectName) -> String {
    name.0
        .iter()
        .map(|part| part.value.clone())
        .collect::<Vec<_>>()
        .join(".")
}

fn build_insert_row(
    table: &TableMeta,
    columns: &[Ident],
    values: &[Expr],
) -> Result<Vec<Value>, GongDBError> {
    let mut row = vec![Value::Null; table.columns.len()];
    if columns.is_empty() {
        if values.len() != table.columns.len() {
            return Err(GongDBError::new("column count mismatch"));
        }
        for (idx, expr) in values.iter().enumerate() {
            row[idx] = eval_literal(expr)?;
        }
        return Ok(row);
    }
    if values.len() != columns.len() {
        return Err(GongDBError::new("column count mismatch"));
    }
    let mut index_by_name = HashMap::new();
    for (idx, col) in table.columns.iter().enumerate() {
        index_by_name.insert(col.name.to_lowercase(), idx);
    }
    for (col_ident, expr) in columns.iter().zip(values.iter()) {
        let key = col_ident.value.to_lowercase();
        let idx = *index_by_name
            .get(&key)
            .ok_or_else(|| GongDBError::new(format!("unknown column {}", col_ident.value)))?;
        row[idx] = eval_literal(expr)?;
    }
    Ok(row)
}

fn eval_literal(expr: &Expr) -> Result<Value, GongDBError> {
    match expr {
        Expr::Literal(lit) => match lit {
            crate::ast::Literal::Null => Ok(Value::Null),
            crate::ast::Literal::Integer(v) => Ok(Value::Integer(*v)),
            crate::ast::Literal::Float(v) => Ok(Value::Real(*v)),
            crate::ast::Literal::String(s) => Ok(Value::Text(s.clone())),
            _ => Err(GongDBError::new("unsupported literal in phase 2")),
        },
        Expr::UnaryOp { op, expr } => {
            let value = eval_literal(expr)?;
            match op {
                crate::ast::UnaryOperator::Plus => Ok(value),
                crate::ast::UnaryOperator::Minus => match value {
                    Value::Integer(v) => Ok(Value::Integer(-v)),
                    Value::Real(v) => Ok(Value::Real(-v)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(GongDBError::new("invalid unary minus")),
                },
                crate::ast::UnaryOperator::Not => Ok(Value::Integer((!value_to_bool(&value)) as i64)),
            }
        }
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Concat => {
            let left = eval_literal(left)?;
            let right = eval_literal(right)?;
            Ok(concat_values(&left, &right))
        }
        _ => Err(GongDBError::new("unsupported expression in INSERT")),
    }
}

fn eval_expr(expr: &Expr, columns: &[Column], row: &[Value]) -> Result<Value, GongDBError> {
    match expr {
        Expr::Literal(_) => eval_literal(expr),
        Expr::Identifier(ident) => {
            let idx = columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                .ok_or_else(|| GongDBError::new(format!("unknown column {}", ident.value)))?;
            Ok(row[idx].clone())
        }
        Expr::BinaryOp { left, op, right } => {
            let left_val = eval_expr(left, columns, row)?;
            let right_val = eval_expr(right, columns, row)?;
            apply_binary_op(op, left_val, right_val)
        }
        Expr::UnaryOp { op, expr } => {
            let value = eval_expr(expr, columns, row)?;
            match op {
                crate::ast::UnaryOperator::Plus => Ok(value),
                crate::ast::UnaryOperator::Minus => match value {
                    Value::Integer(v) => Ok(Value::Integer(-v)),
                    Value::Real(v) => Ok(Value::Real(-v)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(GongDBError::new("invalid unary minus")),
                },
                crate::ast::UnaryOperator::Not => Ok(Value::Integer((!value_to_bool(&value)) as i64)),
            }
        }
        _ => Err(GongDBError::new(
            "unsupported expression in phase 2",
        )),
    }
}

fn apply_binary_op(
    op: &BinaryOperator,
    left: Value,
    right: Value,
) -> Result<Value, GongDBError> {
    match op {
        BinaryOperator::Plus
        | BinaryOperator::Minus
        | BinaryOperator::Multiply
        | BinaryOperator::Divide
        | BinaryOperator::Modulo => apply_numeric_op(op, left, right),
        BinaryOperator::Concat => Ok(concat_values(&left, &right)),
        BinaryOperator::Eq
        | BinaryOperator::NotEq
        | BinaryOperator::Lt
        | BinaryOperator::LtEq
        | BinaryOperator::Gt
        | BinaryOperator::GtEq => Ok(compare_values(op, &left, &right)),
        BinaryOperator::And => Ok(Value::Integer((value_to_bool(&left) && value_to_bool(&right)) as i64)),
        BinaryOperator::Or => Ok(Value::Integer((value_to_bool(&left) || value_to_bool(&right)) as i64)),
        _ => Err(GongDBError::new("unsupported operator in phase 2")),
    }
}

fn apply_numeric_op(
    op: &BinaryOperator,
    left: Value,
    right: Value,
) -> Result<Value, GongDBError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let (left_num, right_num, is_real) = match (left, right) {
        (Value::Integer(l), Value::Integer(r)) => (l as f64, r as f64, false),
        (Value::Integer(l), Value::Real(r)) => (l as f64, r, true),
        (Value::Real(l), Value::Integer(r)) => (l, r as f64, true),
        (Value::Real(l), Value::Real(r)) => (l, r, true),
        _ => return Err(GongDBError::new("non-numeric operand")),
    };
    let result = match op {
        BinaryOperator::Plus => left_num + right_num,
        BinaryOperator::Minus => left_num - right_num,
        BinaryOperator::Multiply => left_num * right_num,
        BinaryOperator::Divide => left_num / right_num,
        BinaryOperator::Modulo => left_num % right_num,
        _ => return Err(GongDBError::new("invalid numeric op")),
    };
    if is_real {
        Ok(Value::Real(result))
    } else {
        Ok(Value::Integer(result as i64))
    }
}

fn compare_values(op: &BinaryOperator, left: &Value, right: &Value) -> Value {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Value::Null;
    }
    let ordering = match (left, right) {
        (Value::Integer(l), Value::Integer(r)) => l.cmp(r),
        (Value::Real(l), Value::Real(r)) => l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Integer(l), Value::Real(r)) => (*l as f64).partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Real(l), Value::Integer(r)) => l.partial_cmp(&(*r as f64)).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Text(l), Value::Text(r)) => l.cmp(r),
        _ => std::cmp::Ordering::Equal,
    };
    let result = match op {
        BinaryOperator::Eq => ordering == std::cmp::Ordering::Equal,
        BinaryOperator::NotEq => ordering != std::cmp::Ordering::Equal,
        BinaryOperator::Lt => ordering == std::cmp::Ordering::Less,
        BinaryOperator::LtEq => ordering != std::cmp::Ordering::Greater,
        BinaryOperator::Gt => ordering == std::cmp::Ordering::Greater,
        BinaryOperator::GtEq => ordering != std::cmp::Ordering::Less,
        _ => false,
    };
    Value::Integer(result as i64)
}

fn concat_values(left: &Value, right: &Value) -> Value {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Value::Null,
        (Value::Text(l), Value::Text(r)) => Value::Text(format!("{}{}", l, r)),
        (Value::Text(l), Value::Integer(r)) => Value::Text(format!("{}{}", l, r)),
        (Value::Integer(l), Value::Text(r)) => Value::Text(format!("{}{}", l, r)),
        (Value::Text(l), Value::Real(r)) => Value::Text(format!("{}{}", l, r)),
        (Value::Real(l), Value::Text(r)) => Value::Text(format!("{}{}", l, r)),
        _ => Value::Null,
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(v) => v.to_string(),
        Value::Real(v) => format!("{:.3}", v),
        Value::Text(s) => s.clone(),
    }
}

fn value_to_bool(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Integer(v) => *v != 0,
        Value::Real(v) => *v != 0.0,
        Value::Text(s) => !s.is_empty(),
    }
}

fn compare_order_by(
    order_by: &[crate::ast::OrderByExpr],
    columns: &[Column],
    a: &[Value],
    b: &[Value],
) -> std::cmp::Ordering {
    for order in order_by {
        let asc = order.asc.unwrap_or(true);
        let left = eval_expr(&order.expr, columns, a).unwrap_or(Value::Null);
        let right = eval_expr(&order.expr, columns, b).unwrap_or(Value::Null);
        let ord = compare_order_values(&left, &right);
        if ord != std::cmp::Ordering::Equal {
            return if asc { ord } else { ord.reverse() };
        }
    }
    std::cmp::Ordering::Equal
}

fn compare_order_values(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        (Value::Integer(l), Value::Integer(r)) => l.cmp(r),
        (Value::Real(l), Value::Real(r)) => l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Integer(l), Value::Real(r)) => (*l as f64)
            .partial_cmp(r)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::Real(l), Value::Integer(r)) => l
            .partial_cmp(&(*r as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::Text(l), Value::Text(r)) => l.cmp(r),
        (Value::Text(_), _) => std::cmp::Ordering::Greater,
        (_, Value::Text(_)) => std::cmp::Ordering::Less,
    }
}

fn is_count_star(select: &Select) -> bool {
    if select.projection.len() != 1 {
        return false;
    }
    match &select.projection[0] {
        SelectItem::Expr { expr, .. } => match expr {
            Expr::Function { name, args, .. } => {
                name.value.eq_ignore_ascii_case("count")
                    && (args.is_empty() || args.iter().all(|arg| matches!(arg, Expr::Wildcard)))
            }
            _ => false,
        },
        _ => false,
    }
}

pub fn format_query_rows(rows: Vec<Vec<Value>>) -> Vec<Vec<String>> {
    rows.into_iter()
        .map(|row| row.into_iter().map(|v| value_to_string(&v)).collect())
        .collect()
}
