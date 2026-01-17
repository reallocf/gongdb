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
                        constraints: create.constraints,
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

        if let Some(aggregates) = aggregate_projections(&select.projection)? {
            let values = compute_aggregates(&aggregates, &columns, &filtered)?;
            let output_row = values.into_iter().map(|v| value_to_string(&v)).collect();
            return Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text; select.projection.len()],
                rows: vec![output_row],
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
        constraints: def.constraints,
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
            let value = eval_literal(expr)?;
            row[idx] = apply_affinity(value, &table.columns[idx].data_type);
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
        let value = eval_literal(expr)?;
        row[idx] = apply_affinity(value, &table.columns[idx].data_type);
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
            crate::ast::Literal::Boolean(v) => Ok(Value::Integer(if *v { 1 } else { 0 })),
            crate::ast::Literal::Blob(bytes) => Ok(Value::Blob(bytes.clone())),
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
        Expr::IsNull { expr, negated } => {
            let value = eval_expr(expr, columns, row)?;
            let is_null = matches!(value, Value::Null);
            Ok(Value::Integer((if *negated { !is_null } else { is_null }) as i64))
        }
        Expr::Cast { expr, data_type } => {
            let value = eval_expr(expr, columns, row)?;
            Ok(cast_value(value, data_type))
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
        BinaryOperator::Is => Ok(Value::Integer(values_equal(&left, &right) as i64)),
        BinaryOperator::IsNot => Ok(Value::Integer((!values_equal(&left, &right)) as i64)),
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
    let left_num = numeric_value(&left).ok_or_else(|| GongDBError::new("non-numeric operand"))?;
    let right_num = numeric_value(&right).ok_or_else(|| GongDBError::new("non-numeric operand"))?;
    let (left_num, left_real) = numeric_to_f64(left_num);
    let (right_num, right_real) = numeric_to_f64(right_num);
    let is_real = left_real || right_real;
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
    let ordering = if let (Some(left_num), Some(right_num)) =
        (numeric_value(left), numeric_value(right))
    {
        let (left_num, _) = numeric_to_f64(left_num);
        let (right_num, _) = numeric_to_f64(right_num);
        left_num
            .partial_cmp(&right_num)
            .unwrap_or(std::cmp::Ordering::Equal)
    } else {
        let left_text = value_to_text(left);
        let right_text = value_to_text(right);
        left_text.cmp(&right_text)
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
        (Value::Text(l), Value::Blob(_)) => Value::Text(l.clone()),
        (Value::Blob(_), Value::Text(r)) => Value::Text(r.clone()),
        _ => Value::Null,
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(v) => v.to_string(),
        Value::Real(v) => format!("{:.3}", v),
        Value::Text(s) => s.clone(),
        Value::Blob(_) => "NULL".to_string(),
    }
}

fn value_to_bool(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Integer(v) => *v != 0,
        Value::Real(v) => *v != 0.0,
        Value::Text(s) => !s.is_empty(),
        Value::Blob(bytes) => !bytes.is_empty(),
    }
}

#[derive(Clone, Copy)]
enum TypeAffinity {
    Integer,
    Real,
    Text,
    Numeric,
    Blob,
}

#[derive(Clone, Copy)]
enum NumericValue {
    Integer(i64),
    Real(f64),
}

fn apply_affinity(value: Value, data_type: &DataType) -> Value {
    match type_affinity(data_type) {
        TypeAffinity::Text => match value {
            Value::Null => Value::Null,
            Value::Text(text) => Value::Text(text),
            Value::Integer(v) => Value::Text(v.to_string()),
            Value::Real(v) => Value::Text(v.to_string()),
            Value::Blob(bytes) => Value::Blob(bytes),
        },
        TypeAffinity::Integer => apply_integer_affinity(value),
        TypeAffinity::Real => apply_real_affinity(value),
        TypeAffinity::Numeric => apply_numeric_affinity(value),
        TypeAffinity::Blob => value,
    }
}

fn apply_integer_affinity(value: Value) -> Value {
    match value {
        Value::Null => Value::Null,
        Value::Integer(v) => Value::Integer(v),
        Value::Real(v) => {
            if v.fract() == 0.0 {
                Value::Integer(v as i64)
            } else {
                Value::Real(v)
            }
        }
        Value::Text(text) => match parse_numeric_text(&text) {
            Some(NumericValue::Integer(v)) => Value::Integer(v),
            Some(NumericValue::Real(v)) => {
                if v.fract() == 0.0 {
                    Value::Integer(v as i64)
                } else {
                    Value::Real(v)
                }
            }
            None => Value::Text(text),
        },
        Value::Blob(bytes) => Value::Blob(bytes),
    }
}

fn apply_real_affinity(value: Value) -> Value {
    match value {
        Value::Null => Value::Null,
        Value::Integer(v) => Value::Real(v as f64),
        Value::Real(v) => Value::Real(v),
        Value::Text(text) => match parse_numeric_text(&text) {
            Some(NumericValue::Integer(v)) => Value::Real(v as f64),
            Some(NumericValue::Real(v)) => Value::Real(v),
            None => Value::Text(text),
        },
        Value::Blob(bytes) => Value::Blob(bytes),
    }
}

fn apply_numeric_affinity(value: Value) -> Value {
    match value {
        Value::Null => Value::Null,
        Value::Integer(v) => Value::Integer(v),
        Value::Real(v) => {
            if v.fract() == 0.0 {
                Value::Integer(v as i64)
            } else {
                Value::Real(v)
            }
        }
        Value::Text(text) => match parse_numeric_text(&text) {
            Some(NumericValue::Integer(v)) => Value::Integer(v),
            Some(NumericValue::Real(v)) => {
                if v.fract() == 0.0 {
                    Value::Integer(v as i64)
                } else {
                    Value::Real(v)
                }
            }
            None => Value::Text(text),
        },
        Value::Blob(bytes) => Value::Blob(bytes),
    }
}

fn cast_value(value: Value, data_type: &DataType) -> Value {
    match type_affinity(data_type) {
        TypeAffinity::Text => match value {
            Value::Null => Value::Null,
            Value::Text(text) => Value::Text(text),
            Value::Integer(v) => Value::Text(v.to_string()),
            Value::Real(v) => Value::Text(v.to_string()),
            Value::Blob(bytes) => Value::Text(String::from_utf8_lossy(&bytes).to_string()),
        },
        TypeAffinity::Integer => match value {
            Value::Null => Value::Null,
            Value::Integer(v) => Value::Integer(v),
            Value::Real(v) => Value::Integer(v as i64),
            Value::Text(text) => match parse_numeric_text(&text) {
                Some(NumericValue::Integer(v)) => Value::Integer(v),
                Some(NumericValue::Real(v)) => Value::Integer(v as i64),
                None => Value::Integer(0),
            },
            Value::Blob(_) => Value::Integer(0),
        },
        TypeAffinity::Real => match value {
            Value::Null => Value::Null,
            Value::Integer(v) => Value::Real(v as f64),
            Value::Real(v) => Value::Real(v),
            Value::Text(text) => match parse_numeric_text(&text) {
                Some(NumericValue::Integer(v)) => Value::Real(v as f64),
                Some(NumericValue::Real(v)) => Value::Real(v),
                None => Value::Real(0.0),
            },
            Value::Blob(_) => Value::Real(0.0),
        },
        TypeAffinity::Numeric => apply_numeric_affinity(value),
        TypeAffinity::Blob => match value {
            Value::Blob(bytes) => Value::Blob(bytes),
            Value::Text(text) => Value::Blob(text.into_bytes()),
            Value::Null => Value::Null,
            Value::Integer(v) => Value::Blob(v.to_le_bytes().to_vec()),
            Value::Real(v) => Value::Blob(v.to_le_bytes().to_vec()),
        },
    }
}

fn type_affinity(data_type: &DataType) -> TypeAffinity {
    match data_type {
        DataType::Integer => TypeAffinity::Integer,
        DataType::Real => TypeAffinity::Real,
        DataType::Text => TypeAffinity::Text,
        DataType::Blob => TypeAffinity::Blob,
        DataType::Numeric => TypeAffinity::Numeric,
        DataType::Custom(name) => type_affinity_from_name(name),
    }
}

fn type_affinity_from_name(name: &str) -> TypeAffinity {
    let upper = name.to_ascii_uppercase();
    if upper.contains("INT") {
        TypeAffinity::Integer
    } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        TypeAffinity::Text
    } else if upper.contains("BLOB") {
        TypeAffinity::Blob
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        TypeAffinity::Real
    } else {
        TypeAffinity::Numeric
    }
}

fn parse_numeric_text(text: &str) -> Option<NumericValue> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if !trimmed.contains(['.', 'e', 'E']) {
        if let Ok(value) = trimmed.parse::<i64>() {
            return Some(NumericValue::Integer(value));
        }
    }
    if let Ok(value) = trimmed.parse::<f64>() {
        if value.is_finite() {
            return Some(NumericValue::Real(value));
        }
    }
    None
}

fn numeric_value(value: &Value) -> Option<NumericValue> {
    match value {
        Value::Integer(v) => Some(NumericValue::Integer(*v)),
        Value::Real(v) => Some(NumericValue::Real(*v)),
        Value::Text(text) => parse_numeric_text(text),
        _ => None,
    }
}

fn numeric_to_f64(value: NumericValue) -> (f64, bool) {
    match value {
        NumericValue::Integer(v) => (v as f64, false),
        NumericValue::Real(v) => (v, true),
    }
}

fn value_to_text(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(v) => v.to_string(),
        Value::Real(v) => v.to_string(),
        Value::Text(text) => text.clone(),
        Value::Blob(_) => String::new(),
    }
}

fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => true,
        (Value::Null, _) | (_, Value::Null) => false,
        _ => {
            if let (Some(left_num), Some(right_num)) = (numeric_value(left), numeric_value(right)) {
                let (left_num, _) = numeric_to_f64(left_num);
                let (right_num, _) = numeric_to_f64(right_num);
                (left_num - right_num).abs() == 0.0
            } else {
                value_to_text(left) == value_to_text(right)
            }
        }
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
        (Value::Blob(_), Value::Blob(_)) => std::cmp::Ordering::Equal,
        (Value::Text(_), _) => std::cmp::Ordering::Greater,
        (_, Value::Text(_)) => std::cmp::Ordering::Less,
        (Value::Blob(_), _) => std::cmp::Ordering::Greater,
        (_, Value::Blob(_)) => std::cmp::Ordering::Less,
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

#[derive(Clone)]
struct AggregateExpr {
    kind: AggregateKind,
    expr: Option<Expr>,
}

#[derive(Clone)]
enum AggregateKind {
    Sum,
    Count,
}

fn aggregate_projections(
    projection: &[SelectItem],
) -> Result<Option<Vec<AggregateExpr>>, GongDBError> {
    let mut aggregates = Vec::new();
    let mut saw_aggregate = false;
    for item in projection {
        match item {
            SelectItem::Expr { expr, .. } => match expr {
                Expr::Function { name, args, .. } => {
                    let func_name = name.value.to_ascii_lowercase();
                    match func_name.as_str() {
                        "sum" => {
                            saw_aggregate = true;
                            if args.len() != 1 {
                                return Err(GongDBError::new("SUM expects one argument"));
                            }
                            aggregates.push(AggregateExpr {
                                kind: AggregateKind::Sum,
                                expr: Some(args[0].clone()),
                            });
                        }
                        "count" => {
                            saw_aggregate = true;
                            if args.is_empty()
                                || args.iter().all(|arg| matches!(arg, Expr::Wildcard))
                            {
                                aggregates.push(AggregateExpr {
                                    kind: AggregateKind::Count,
                                    expr: None,
                                });
                            } else if args.len() == 1 {
                                aggregates.push(AggregateExpr {
                                    kind: AggregateKind::Count,
                                    expr: Some(args[0].clone()),
                                });
                            } else {
                                return Err(GongDBError::new("COUNT expects at most one argument"));
                            }
                        }
                        _ => return Err(GongDBError::new("unsupported aggregate function")),
                    }
                }
                _ => {
                    if saw_aggregate {
                        return Err(GongDBError::new(
                            "cannot mix aggregate and non-aggregate expressions",
                        ));
                    }
                    return Ok(None);
                }
            },
            _ => {
                if saw_aggregate {
                    return Err(GongDBError::new(
                        "cannot mix aggregate and non-aggregate expressions",
                    ));
                }
                return Ok(None);
            }
        }
    }
    if saw_aggregate {
        Ok(Some(aggregates))
    } else {
        Ok(None)
    }
}

fn compute_aggregates(
    aggregates: &[AggregateExpr],
    columns: &[Column],
    rows: &[Vec<Value>],
) -> Result<Vec<Value>, GongDBError> {
    let mut results = Vec::with_capacity(aggregates.len());
    for agg in aggregates {
        match agg.kind {
            AggregateKind::Count => {
                let count = if let Some(expr) = &agg.expr {
                    let mut tally = 0i64;
                    for row in rows {
                        let value = eval_expr(expr, columns, row)?;
                        if !matches!(value, Value::Null) {
                            tally += 1;
                        }
                    }
                    tally
                } else {
                    rows.len() as i64
                };
                results.push(Value::Integer(count));
            }
            AggregateKind::Sum => {
                let Some(expr) = &agg.expr else {
                    return Err(GongDBError::new("SUM requires an expression"));
                };
                let mut sum_int = 0i64;
                let mut sum_real = 0.0;
                let mut any_real = false;
                let mut has_value = false;
                for row in rows {
                    let value = eval_expr(expr, columns, row)?;
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    if let Some(num) = numeric_value(&value) {
                        has_value = true;
                        match num {
                            NumericValue::Integer(v) => {
                                if any_real {
                                    sum_real += v as f64;
                                } else {
                                    sum_int += v;
                                }
                            }
                            NumericValue::Real(v) => {
                                if !any_real {
                                    any_real = true;
                                    sum_real = sum_int as f64;
                                }
                                sum_real += v;
                            }
                        }
                    }
                }
                if !has_value {
                    results.push(Value::Null);
                } else if any_real {
                    results.push(Value::Real(sum_real));
                } else {
                    results.push(Value::Integer(sum_int));
                }
            }
        }
    }
    Ok(results)
}

pub fn format_query_rows(rows: Vec<Vec<Value>>) -> Vec<Vec<String>> {
    rows.into_iter()
        .map(|row| row.into_iter().map(|v| value_to_string(&v)).collect())
        .collect()
}
