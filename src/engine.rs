use crate::ast::{
    BinaryOperator, ColumnConstraint, ColumnDef, CreateTable, DataType, Expr, Ident, IndexedColumn,
    InsertSource, Select, SelectItem, Statement, TableConstraint,
};
use crate::parser;
use crate::storage::{Column, IndexMeta, StorageEngine, StorageError, TableMeta, Value, ViewMeta};
use async_trait::async_trait;
use sqllogictest::{DBOutput, DefaultColumnType};
use std::collections::{HashMap, HashSet};

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
                let name = object_name(&create.name);
                if self.storage.get_table(&name).is_some() {
                    if !create.if_not_exists {
                        return Err(GongDBError::new(format!(
                            "table already exists: {}",
                            name
                        )));
                    }
                    return Ok(DBOutput::StatementComplete(0));
                }
                if self.storage.get_view(&name).is_some() {
                    return Err(GongDBError::new(format!(
                        "view already exists: {}",
                        name
                    )));
                }

                let plan = build_create_table_plan(create)?;
                let first_page = self.storage.allocate_data_page()?;
                let meta = TableMeta {
                    name: name.clone(),
                    columns: plan.columns,
                    constraints: plan.constraints,
                    first_page,
                    last_page: first_page,
                };
                self.storage.create_table(meta)?;

                let mut counter = 1;
                let mut used_names = HashSet::new();
                for spec in plan.auto_indexes {
                    let index_name =
                        next_auto_index_name(&self.storage, &name, &mut counter, &mut used_names);
                    let first_page = self.storage.allocate_data_page()?;
                    let meta = IndexMeta {
                        name: index_name,
                        table: name.clone(),
                        columns: spec.columns,
                        unique: spec.unique,
                        first_page,
                        last_page: first_page,
                    };
                    self.storage.create_index(meta)?;
                }
                Ok(DBOutput::StatementComplete(0))
            }
            Statement::CreateIndex(create) => {
                let index_name = object_name(&create.name);
                if self.storage.get_index(&index_name).is_some() && !create.if_not_exists {
                    return Err(GongDBError::new(format!(
                        "index already exists: {}",
                        index_name
                    )));
                }
                let table_name = object_name(&create.table);
                let table = self
                    .storage
                    .get_table(&table_name)
                    .ok_or_else(|| GongDBError::new(format!("table not found: {}", table_name)))?
                    .clone();
                for column in &create.columns {
                    let exists = table.columns.iter().any(|c| {
                        c.name
                            .eq_ignore_ascii_case(&column.name.value)
                    });
                    if !exists {
                        return Err(GongDBError::new(format!(
                            "unknown column {}",
                            column.name.value
                        )));
                    }
                }
                if self.storage.get_index(&index_name).is_none() {
                    let first_page = self.storage.allocate_data_page()?;
                    let meta = IndexMeta {
                        name: index_name.clone(),
                        table: table_name,
                        columns: create.columns,
                        unique: create.unique,
                        first_page,
                        last_page: first_page,
                    };
                    self.storage.create_index(meta)?;
                }
                Ok(DBOutput::StatementComplete(0))
            }
            Statement::DropIndex(drop) => {
                let name = object_name(&drop.name);
                if self.storage.get_index(&name).is_none() && !drop.if_exists {
                    return Err(GongDBError::new(format!(
                        "index not found: {}",
                        name
                    )));
                }
                if self.storage.get_index(&name).is_some() {
                    self.storage.drop_index(&name)?;
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
            Statement::CreateView(create) => {
                let name = object_name(&create.name);
                if self.storage.get_table(&name).is_some() {
                    return Err(GongDBError::new(format!(
                        "table already exists: {}",
                        name
                    )));
                }
                if self.storage.get_view(&name).is_some() {
                    if !create.if_not_exists {
                        return Err(GongDBError::new(format!(
                            "view already exists: {}",
                            name
                        )));
                    }
                    return Ok(DBOutput::StatementComplete(0));
                }
                let output_columns = self.evaluate_select_values(&create.query)?.columns;
                if !create.columns.is_empty() && create.columns.len() != output_columns.len() {
                    return Err(GongDBError::new(format!(
                        "view column count mismatch: {}",
                        name
                    )));
                }
                ensure_unique_idents(&create.columns)?;
                let view = ViewMeta {
                    name: name.clone(),
                    columns: create.columns,
                    query: create.query,
                };
                self.storage.create_view(view)?;
                Ok(DBOutput::StatementComplete(0))
            }
            Statement::DropView(drop) => {
                let name = object_name(&drop.name);
                if self.storage.get_view(&name).is_none() && !drop.if_exists {
                    return Err(GongDBError::new(format!(
                        "view not found: {}",
                        name
                    )));
                }
                if self.storage.get_view(&name).is_some() {
                    self.storage.drop_view(&name)?;
                }
                Ok(DBOutput::StatementComplete(0))
            }
            Statement::Insert(insert) => {
                let table_name = object_name(&insert.table);
                if self.storage.get_view(&table_name).is_some() {
                    return Err(GongDBError::new(format!(
                        "cannot modify view {}",
                        table_name
                    )));
                }
                let table = self
                    .storage
                    .get_table(&table_name)
                    .ok_or_else(|| GongDBError::new(format!("table not found: {}", table_name)))?
                    .clone();
                let mut inserted = 0u64;
                match insert.source {
                    InsertSource::Values(values) => {
                        for exprs in values {
                            let row =
                                build_insert_row(self, &table, &insert.columns, &exprs)?;
                            let _ = self.storage.insert_row(&table_name, &row)?;
                            inserted += 1;
                        }
                    }
                    InsertSource::Select(select) => {
                        let result = self.evaluate_select_values(&select)?;
                        if insert.columns.is_empty() {
                            if result.columns.len() != table.columns.len() {
                                return Err(GongDBError::new("column count mismatch"));
                            }
                        } else if result.columns.len() != insert.columns.len() {
                            return Err(GongDBError::new("column count mismatch"));
                        }
                        for values in result.rows {
                            let row = build_insert_row_from_values(
                                self,
                                &table,
                                &insert.columns,
                                &values,
                            )?;
                            let _ = self.storage.insert_row(&table_name, &row)?;
                            inserted += 1;
                        }
                    }
                }
                Ok(DBOutput::StatementComplete(inserted))
            }
            Statement::Update(update) => {
                let table_name = object_name(&update.table);
                if self.storage.get_view(&table_name).is_some() {
                    return Err(GongDBError::new(format!(
                        "cannot modify view {}",
                        table_name
                    )));
                }
                let table = self
                    .storage
                    .get_table(&table_name)
                    .ok_or_else(|| GongDBError::new(format!("table not found: {}", table_name)))?
                    .clone();
                let rows = self.storage.scan_table(&table_name)?;
                let table_scope = TableScope {
                    table_name: Some(table_name.clone()),
                    table_alias: None,
                };
                let mut updated_rows = Vec::with_capacity(rows.len());
                for row in rows {
                    let scope = EvalScope {
                        columns: &table.columns,
                        row: &row,
                        table_scope: &table_scope,
                    };
                    if let Some(predicate) = &update.selection {
                        let value = eval_expr(self, predicate, &scope, None)?;
                        if !value_to_bool(&value) {
                            updated_rows.push(row);
                            continue;
                        }
                    }
                    let mut new_row = row.clone();
                    for assignment in &update.assignments {
                        let idx = resolve_column_index(&assignment.column.value, &table.columns)
                            .ok_or_else(|| {
                                GongDBError::new(format!(
                                    "unknown column {}",
                                    assignment.column.value
                                ))
                            })?;
                        let value = eval_expr(self, &assignment.value, &scope, None)?;
                        new_row[idx] = apply_affinity(value, &table.columns[idx].data_type);
                    }
                    updated_rows.push(new_row);
                }
                self.storage.replace_table_rows(&table_name, &updated_rows)?;
                Ok(DBOutput::StatementComplete(0))
            }
            Statement::Delete(delete) => {
                let table_name = object_name(&delete.table);
                if self.storage.get_view(&table_name).is_some() {
                    return Err(GongDBError::new(format!(
                        "cannot modify view {}",
                        table_name
                    )));
                }
                let table = self
                    .storage
                    .get_table(&table_name)
                    .ok_or_else(|| GongDBError::new(format!("table not found: {}", table_name)))?
                    .clone();
                let rows = self.storage.scan_table(&table_name)?;
                let table_scope = TableScope {
                    table_name: Some(table_name.clone()),
                    table_alias: None,
                };
                let mut remaining_rows = Vec::with_capacity(rows.len());
                for row in rows {
                    let scope = EvalScope {
                        columns: &table.columns,
                        row: &row,
                        table_scope: &table_scope,
                    };
                    if let Some(predicate) = &delete.selection {
                        let value = eval_expr(self, predicate, &scope, None)?;
                        if value_to_bool(&value) {
                            continue;
                        }
                    } else {
                        continue;
                    }
                    remaining_rows.push(row);
                }
                self.storage
                    .replace_table_rows(&table_name, &remaining_rows)?;
                Ok(DBOutput::StatementComplete(0))
            }
            Statement::Select(select) => self.run_select(&select),
        }
    }

    fn run_select(&self, select: &Select) -> Result<DBOutput<DefaultColumnType>, GongDBError> {
        let result = self.evaluate_select_values(select)?;
        let output_rows = result
            .rows
            .into_iter()
            .map(|row| row.into_iter().map(|v| value_to_string(&v)).collect())
            .collect();
        Ok(DBOutput::Rows {
            types: vec![DefaultColumnType::Text; result.columns.len()],
            rows: output_rows,
        })
    }

    fn evaluate_select_values(&self, select: &Select) -> Result<QueryResult, GongDBError> {
        let mut view_stack = Vec::new();
        self.evaluate_select_values_with_views(select, &mut view_stack, None)
    }

    fn evaluate_select_values_with_views(
        &self,
        select: &Select,
        view_stack: &mut Vec<String>,
        outer: Option<&EvalScope<'_>>,
    ) -> Result<QueryResult, GongDBError> {
        let source = self.resolve_source(select, view_stack)?;
        let QuerySource {
            columns,
            rows,
            table_scope,
        } = source;
        let mut filtered = Vec::new();
        for row in rows {
            let scope = EvalScope {
                columns: &columns,
                row: &row,
                table_scope: &table_scope,
            };
            if let Some(predicate) = &select.selection {
                let value = eval_expr(self, predicate, &scope, outer)?;
                if !value_to_bool(&value) {
                    continue;
                }
            }
            filtered.push(row);
        }

        let output_columns = projection_columns(&select.projection, &columns)?;

        if is_count_star(select) {
            let count = filtered.len() as i64;
            return Ok(QueryResult {
                columns: output_columns,
                rows: vec![vec![Value::Integer(count)]],
            });
        }

        if let Some(aggregates) = aggregate_projections(&select.projection)? {
            let values =
                compute_aggregates(self, &aggregates, &columns, &table_scope, &filtered, outer)?;
            return Ok(QueryResult {
                columns: output_columns,
                rows: vec![values],
            });
        }

        if !select.order_by.is_empty() {
            let order_by = select.order_by.clone();
            filtered.sort_by(|a, b| {
                compare_order_by(self, &order_by, &columns, &table_scope, outer, a, b)
            });
        }

        let mut projected = Vec::new();
        for row in filtered {
            let scope = EvalScope {
                columns: &columns,
                row: &row,
                table_scope: &table_scope,
            };
            let mut output = Vec::new();
            for item in &select.projection {
                match item {
                    SelectItem::Wildcard => {
                        for value in &row {
                            output.push(value.clone());
                        }
                    }
                    SelectItem::Expr { expr, .. } => {
                        let value = eval_expr(self, expr, &scope, outer)?;
                        output.push(value);
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

        Ok(QueryResult {
            columns: output_columns,
            rows: projected,
        })
    }

    fn evaluate_select_values_with_outer(
        &self,
        select: &Select,
        outer: Option<&EvalScope<'_>>,
    ) -> Result<QueryResult, GongDBError> {
        let mut view_stack = Vec::new();
        self.evaluate_select_values_with_views(select, &mut view_stack, outer)
    }

    fn resolve_source(
        &self,
        select: &Select,
        view_stack: &mut Vec<String>,
    ) -> Result<QuerySource, GongDBError> {
        if select.from.is_empty() {
            return Ok(QuerySource {
                columns: Vec::new(),
                rows: vec![Vec::new()],
                table_scope: TableScope {
                    table_name: None,
                    table_alias: None,
                },
            });
        }

        let mut sources = Vec::new();
        for table_ref in &select.from {
            sources.push(self.resolve_table_ref(table_ref, view_stack)?);
        }

        if sources.len() == 1 {
            return Ok(sources.remove(0));
        }

        let mut rows = vec![Vec::new()];
        let mut columns = Vec::new();
        for source in sources {
            columns.extend(source.columns);
            let mut next_rows = Vec::new();
            for left_row in &rows {
                for right_row in &source.rows {
                    let mut combined = left_row.clone();
                    combined.extend(right_row.clone());
                    next_rows.push(combined);
                }
            }
            rows = next_rows;
        }

        Ok(QuerySource {
            columns,
            rows,
            table_scope: TableScope {
                table_name: None,
                table_alias: None,
            },
        })
    }

    fn resolve_table_ref(
        &self,
        table_ref: &crate::ast::TableRef,
        view_stack: &mut Vec<String>,
    ) -> Result<QuerySource, GongDBError> {
        match table_ref {
            crate::ast::TableRef::Named { name, alias } => {
                let table_name = object_name(name);
                let table_alias = alias.as_ref().map(|ident| ident.value.clone());
                if let Some(table) = self.storage.get_table(&table_name) {
                    let rows = self.storage.scan_table(&table_name)?;
                    return Ok(QuerySource {
                        columns: table.columns.clone(),
                        rows,
                        table_scope: TableScope {
                            table_name: Some(table_name),
                            table_alias,
                        },
                    });
                }
                if let Some(view) = self.storage.get_view(&table_name) {
                    if view_stack
                        .iter()
                        .any(|entry| entry.eq_ignore_ascii_case(&table_name))
                    {
                        return Err(GongDBError::new(format!(
                            "circular view reference: {}",
                            table_name
                        )));
                    }
                    view_stack.push(table_name.clone());
                    let mut result =
                        self.evaluate_select_values_with_views(&view.query, view_stack, None)?;
                    view_stack.pop();
                    if !view.columns.is_empty() {
                        if view.columns.len() != result.columns.len() {
                            return Err(GongDBError::new(format!(
                                "view column count mismatch: {}",
                                table_name
                            )));
                        }
                        result.columns = columns_from_idents(&view.columns);
                    }
                    return Ok(QuerySource {
                        columns: result.columns,
                        rows: result.rows,
                        table_scope: TableScope {
                            table_name: Some(table_name),
                            table_alias,
                        },
                    });
                }
                Err(GongDBError::new(format!(
                    "table not found: {}",
                    table_name
                )))
            }
            _ => Err(GongDBError::new(
                "only simple table references are supported in phase 2",
            )),
        }
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

struct QuerySource {
    columns: Vec<Column>,
    rows: Vec<Vec<Value>>,
    table_scope: TableScope,
}

struct QueryResult {
    columns: Vec<Column>,
    rows: Vec<Vec<Value>>,
}

#[derive(Clone, Debug)]
struct TableScope {
    table_name: Option<String>,
    table_alias: Option<String>,
}

impl TableScope {
    fn matches_qualifier(&self, qualifier: &str) -> bool {
        if let Some(alias) = &self.table_alias {
            return alias.eq_ignore_ascii_case(qualifier);
        }
        if let Some(name) = &self.table_name {
            return name.eq_ignore_ascii_case(qualifier);
        }
        false
    }
}

#[derive(Clone, Copy)]
struct EvalScope<'a> {
    columns: &'a [Column],
    row: &'a [Value],
    table_scope: &'a TableScope,
}

fn column_from_def(def: ColumnDef) -> Column {
    Column {
        name: def.name.value,
        data_type: def.data_type.unwrap_or(DataType::Text),
        constraints: def.constraints,
    }
}

struct AutoIndexSpec {
    columns: Vec<IndexedColumn>,
    unique: bool,
}

struct CreateTablePlan {
    columns: Vec<Column>,
    constraints: Vec<TableConstraint>,
    auto_indexes: Vec<AutoIndexSpec>,
}

fn build_create_table_plan(create: CreateTable) -> Result<CreateTablePlan, GongDBError> {
    if create.without_rowid {
        return Err(GongDBError::new("WITHOUT ROWID is not supported"));
    }
    if create.columns.is_empty() {
        return Err(GongDBError::new("table must have at least one column"));
    }

    let mut column_names = HashSet::new();
    for column in &create.columns {
        let key = column.name.value.to_lowercase();
        if !column_names.insert(key.clone()) {
            return Err(GongDBError::new(format!(
                "duplicate column name: {}",
                column.name.value
            )));
        }
    }

    let mut primary_key: Option<Vec<Ident>> = None;
    let mut unique_specs: Vec<Vec<Ident>> = Vec::new();

    for column in &create.columns {
        let mut has_null = false;
        let mut has_not_null = false;
        for constraint in &column.constraints {
            match constraint {
                ColumnConstraint::NotNull => {
                    if has_null {
                        return Err(GongDBError::new(format!(
                            "conflicting NULL constraints on {}",
                            column.name.value
                        )));
                    }
                    has_not_null = true;
                }
                ColumnConstraint::Null => {
                    if has_not_null {
                        return Err(GongDBError::new(format!(
                            "conflicting NULL constraints on {}",
                            column.name.value
                        )));
                    }
                    has_null = true;
                }
                ColumnConstraint::PrimaryKey => {
                    if primary_key.is_some() {
                        return Err(GongDBError::new("multiple primary keys are not allowed"));
                    }
                    primary_key = Some(vec![column.name.clone()]);
                }
                ColumnConstraint::Unique => {
                    unique_specs.push(vec![column.name.clone()]);
                }
                ColumnConstraint::Default(_) => {}
            }
        }
    }

    for constraint in &create.constraints {
        match constraint {
            TableConstraint::PrimaryKey(columns) => {
                validate_constraint_columns(&column_names, columns)?;
                if primary_key.is_some() {
                    return Err(GongDBError::new("multiple primary keys are not allowed"));
                }
                primary_key = Some(columns.clone());
            }
            TableConstraint::Unique(columns) => {
                validate_constraint_columns(&column_names, columns)?;
                unique_specs.push(columns.clone());
            }
            TableConstraint::Check(_) => {}
            TableConstraint::ForeignKey {
                columns,
                referred_columns,
                ..
            } => {
                validate_constraint_columns(&column_names, columns)?;
                if columns.len() != referred_columns.len() {
                    return Err(GongDBError::new(
                        "foreign key column count does not match referenced columns",
                    ));
                }
            }
        }
    }

    let mut auto_indexes = Vec::new();
    let mut seen_index_keys = HashSet::new();
    if let Some(columns) = primary_key {
        let key = normalized_column_key(&columns);
        if seen_index_keys.insert(key) {
            auto_indexes.push(AutoIndexSpec {
                columns: indexed_columns(&columns),
                unique: true,
            });
        }
    }
    for columns in unique_specs {
        let key = normalized_column_key(&columns);
        if seen_index_keys.insert(key) {
            auto_indexes.push(AutoIndexSpec {
                columns: indexed_columns(&columns),
                unique: true,
            });
        }
    }

    let columns = create.columns.into_iter().map(column_from_def).collect();
    Ok(CreateTablePlan {
        columns,
        constraints: create.constraints,
        auto_indexes,
    })
}

fn validate_constraint_columns(
    column_names: &HashSet<String>,
    columns: &[Ident],
) -> Result<(), GongDBError> {
    let mut seen = HashSet::new();
    for column in columns {
        let key = column.value.to_lowercase();
        if !column_names.contains(&key) {
            return Err(GongDBError::new(format!(
                "unknown column in constraint: {}",
                column.value
            )));
        }
        if !seen.insert(key) {
            return Err(GongDBError::new(format!(
                "duplicate column in constraint: {}",
                column.value
            )));
        }
    }
    Ok(())
}

fn normalized_column_key(columns: &[Ident]) -> String {
    columns
        .iter()
        .map(|col| col.value.to_lowercase())
        .collect::<Vec<_>>()
        .join(",")
}

fn indexed_columns(columns: &[Ident]) -> Vec<IndexedColumn> {
    columns
        .iter()
        .cloned()
        .map(|name| IndexedColumn { name, order: None })
        .collect()
}

fn next_auto_index_name(
    storage: &StorageEngine,
    table_name: &str,
    counter: &mut usize,
    used: &mut HashSet<String>,
) -> String {
    loop {
        let name = format!("__gongdb_autoindex_{}_{}", table_name, *counter);
        *counter += 1;
        if storage.get_index(&name).is_none() && used.insert(name.clone()) {
            return name;
        }
    }
}

fn object_name(name: &crate::ast::ObjectName) -> String {
    name.0
        .iter()
        .map(|part| part.value.clone())
        .collect::<Vec<_>>()
        .join(".")
}

fn projection_columns(
    projection: &[SelectItem],
    source_columns: &[Column],
) -> Result<Vec<Column>, GongDBError> {
    let mut columns = Vec::new();
    for (idx, item) in projection.iter().enumerate() {
        match item {
            SelectItem::Wildcard => {
                columns.extend(source_columns.iter().cloned());
            }
            SelectItem::Expr { expr, alias } => {
                let name = if let Some(alias) = alias {
                    alias.value.clone()
                } else {
                    match expr {
                        Expr::Identifier(ident) => ident.value.clone(),
                        Expr::CompoundIdentifier(idents) => idents
                            .last()
                            .map(|ident| ident.value.clone())
                            .unwrap_or_else(|| format!("expr{}", idx + 1)),
                        _ => format!("expr{}", idx + 1),
                    }
                };
                columns.push(Column {
                    name,
                    data_type: DataType::Text,
                    constraints: Vec::new(),
                });
            }
            SelectItem::QualifiedWildcard(_) => {
                return Err(GongDBError::new(
                    "qualified wildcard not supported in phase 2",
                ))
            }
        }
    }
    Ok(columns)
}

fn columns_from_idents(idents: &[Ident]) -> Vec<Column> {
    idents
        .iter()
        .map(|ident| Column {
            name: ident.value.clone(),
            data_type: DataType::Text,
            constraints: Vec::new(),
        })
        .collect()
}

fn ensure_unique_idents(idents: &[Ident]) -> Result<(), GongDBError> {
    let mut seen = HashSet::new();
    for ident in idents {
        let key = ident.value.to_lowercase();
        if !seen.insert(key) {
            return Err(GongDBError::new(format!(
                "duplicate column name: {}",
                ident.value
            )));
        }
    }
    Ok(())
}

fn build_insert_row(
    db: &GongDB,
    table: &TableMeta,
    columns: &[Ident],
    values: &[Expr],
) -> Result<Vec<Value>, GongDBError> {
    let mut row = build_default_row(db, table)?;
    if columns.is_empty() {
        if values.len() != table.columns.len() {
            return Err(GongDBError::new("column count mismatch"));
        }
        for (idx, expr) in values.iter().enumerate() {
            let value = eval_insert_expr(db, expr)?;
            row[idx] = apply_affinity(value, &table.columns[idx].data_type);
        }
        validate_insert_row(db, table, &row)?;
        return Ok(row);
    }
    if values.len() != columns.len() {
        return Err(GongDBError::new("column count mismatch"));
    }
    let mut index_by_name = HashMap::new();
    for (idx, col) in table.columns.iter().enumerate() {
        index_by_name.insert(col.name.to_lowercase(), idx);
    }
    let mut seen = HashSet::new();
    for (col_ident, expr) in columns.iter().zip(values.iter()) {
        let key = col_ident.value.to_lowercase();
        if !seen.insert(key.clone()) {
            return Err(GongDBError::new(format!(
                "duplicate column {}",
                col_ident.value
            )));
        }
        let idx = *index_by_name
            .get(&key)
            .ok_or_else(|| GongDBError::new(format!("unknown column {}", col_ident.value)))?;
        let value = eval_insert_expr(db, expr)?;
        row[idx] = apply_affinity(value, &table.columns[idx].data_type);
    }
    validate_insert_row(db, table, &row)?;
    Ok(row)
}

fn build_insert_row_from_values(
    db: &GongDB,
    table: &TableMeta,
    columns: &[Ident],
    values: &[Value],
) -> Result<Vec<Value>, GongDBError> {
    let mut row = build_default_row(db, table)?;
    if columns.is_empty() {
        if values.len() != table.columns.len() {
            return Err(GongDBError::new("column count mismatch"));
        }
        for (idx, value) in values.iter().enumerate() {
            row[idx] = apply_affinity(value.clone(), &table.columns[idx].data_type);
        }
        validate_insert_row(db, table, &row)?;
        return Ok(row);
    }
    if values.len() != columns.len() {
        return Err(GongDBError::new("column count mismatch"));
    }
    let mut index_by_name = HashMap::new();
    for (idx, col) in table.columns.iter().enumerate() {
        index_by_name.insert(col.name.to_lowercase(), idx);
    }
    let mut seen = HashSet::new();
    for (col_ident, value) in columns.iter().zip(values.iter()) {
        let key = col_ident.value.to_lowercase();
        if !seen.insert(key.clone()) {
            return Err(GongDBError::new(format!(
                "duplicate column {}",
                col_ident.value
            )));
        }
        let idx = *index_by_name
            .get(&key)
            .ok_or_else(|| GongDBError::new(format!("unknown column {}", col_ident.value)))?;
        row[idx] = apply_affinity(value.clone(), &table.columns[idx].data_type);
    }
    validate_insert_row(db, table, &row)?;
    Ok(row)
}

fn build_default_row(db: &GongDB, table: &TableMeta) -> Result<Vec<Value>, GongDBError> {
    let mut row = Vec::with_capacity(table.columns.len());
    for column in &table.columns {
        let mut value = Value::Null;
        for constraint in &column.constraints {
            if let ColumnConstraint::Default(expr) = constraint {
                value = eval_insert_expr(db, expr)?;
                break;
            }
        }
        row.push(apply_affinity(value, &column.data_type));
    }
    Ok(row)
}

fn eval_insert_expr(db: &GongDB, expr: &Expr) -> Result<Value, GongDBError> {
    let table_scope = TableScope {
        table_name: None,
        table_alias: None,
    };
    let scope = EvalScope {
        columns: &[],
        row: &[],
        table_scope: &table_scope,
    };
    eval_expr(db, expr, &scope, None)
}

fn validate_insert_row(
    db: &GongDB,
    table: &TableMeta,
    row: &[Value],
) -> Result<(), GongDBError> {
    let mut pk_columns = HashSet::new();
    for constraint in &table.constraints {
        if let TableConstraint::PrimaryKey(columns) = constraint {
            for column in columns {
                pk_columns.insert(column.value.to_lowercase());
            }
        }
    }

    for (idx, column) in table.columns.iter().enumerate() {
        let mut not_null = false;
        for constraint in &column.constraints {
            match constraint {
                ColumnConstraint::NotNull | ColumnConstraint::PrimaryKey => {
                    not_null = true;
                }
                _ => {}
            }
        }
        if pk_columns.contains(&column.name.to_lowercase()) {
            not_null = true;
        }
        if not_null && matches!(row[idx], Value::Null) {
            return Err(GongDBError::new(format!(
                "NOT NULL constraint failed: {}.{}",
                table.name, column.name
            )));
        }
    }

    if !table.constraints.is_empty() {
        let table_scope = TableScope {
            table_name: Some(table.name.clone()),
            table_alias: None,
        };
        let scope = EvalScope {
            columns: &table.columns,
            row,
            table_scope: &table_scope,
        };
        for constraint in &table.constraints {
            if let TableConstraint::Check(expr) = constraint {
                let value = eval_expr(db, expr, &scope, None)?;
                if !value_to_bool(&value) {
                    return Err(GongDBError::new("CHECK constraint failed"));
                }
            }
        }
    }
    Ok(())
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
                crate::ast::UnaryOperator::Not => Ok(apply_logical_not(value)),
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

fn resolve_column_index(name: &str, columns: &[Column]) -> Option<usize> {
    columns
        .iter()
        .position(|col| col.name.eq_ignore_ascii_case(name))
}

fn split_qualified_identifier(idents: &[Ident]) -> Result<(&str, &str), GongDBError> {
    if idents.len() < 2 {
        return Err(GongDBError::new("invalid qualified identifier"));
    }
    let column = &idents[idents.len() - 1].value;
    let qualifier = &idents[idents.len() - 2].value;
    Ok((qualifier.as_str(), column.as_str()))
}

fn is_aggregate_function_call(name: &str, args: &[Expr]) -> bool {
    match name {
        "sum" | "avg" | "count" | "total" | "group_concat" => true,
        "min" | "max" => args.len() == 1,
        _ => false,
    }
}

fn eval_expr<'a, 'b>(
    db: &GongDB,
    expr: &Expr,
    scope: &EvalScope<'a>,
    outer: Option<&EvalScope<'b>>,
) -> Result<Value, GongDBError> {
    match expr {
        Expr::Literal(_) => eval_literal(expr),
        Expr::Identifier(ident) => {
            if let Some(idx) = resolve_column_index(&ident.value, scope.columns) {
                return Ok(scope.row[idx].clone());
            }
            if let Some(outer_scope) = outer {
                if let Some(idx) = resolve_column_index(&ident.value, outer_scope.columns) {
                    return Ok(outer_scope.row[idx].clone());
                }
            }
            Err(GongDBError::new(format!(
                "unknown column {}",
                ident.value
            )))
        }
        Expr::CompoundIdentifier(idents) => {
            let (qualifier, column) = split_qualified_identifier(idents)?;
            if scope.table_scope.matches_qualifier(qualifier) {
                if let Some(idx) = resolve_column_index(column, scope.columns) {
                    return Ok(scope.row[idx].clone());
                }
            }
            if let Some(outer_scope) = outer {
                if outer_scope.table_scope.matches_qualifier(qualifier) {
                    if let Some(idx) = resolve_column_index(column, outer_scope.columns) {
                        return Ok(outer_scope.row[idx].clone());
                    }
                }
            }
            Err(GongDBError::new(format!(
                "unknown column {}",
                column
            )))
        }
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            let func_name = name.value.to_ascii_lowercase();
            if is_aggregate_function_call(func_name.as_str(), args) {
                return Err(GongDBError::new(
                    "aggregate function is not allowed in this context",
                ));
            }
            if *distinct {
                return Err(GongDBError::new(
                    "DISTINCT is only valid for aggregate functions",
                ));
            }
            match func_name.as_str() {
                "abs" => {
                    if args.len() != 1 {
                        return Err(GongDBError::new("ABS expects one argument"));
                    }
                    let value = eval_expr(db, &args[0], scope, outer)?;
                    match value {
                        Value::Null => Ok(Value::Null),
                        _ => match numeric_value_or_zero(&value) {
                            Some(NumericValue::Integer(v)) => Ok(Value::Integer(v.abs())),
                            Some(NumericValue::Real(v)) => Ok(Value::Real(v.abs())),
                            None => Ok(Value::Null),
                        },
                    }
                }
                "min" | "max" => {
                    if args.len() < 2 {
                        return Err(GongDBError::new(
                            "MIN and MAX require at least two arguments in scalar context",
                        ));
                    }
                    let mut current: Option<Value> = None;
                    for arg in args {
                        let value = eval_expr(db, arg, scope, outer)?;
                        if matches!(value, Value::Null) {
                            continue;
                        }
                        match &current {
                            None => current = Some(value),
                            Some(existing) => {
                                let ord = compare_order_values(&value, existing);
                                let replace = match func_name.as_str() {
                                    "min" => ord == std::cmp::Ordering::Less,
                                    "max" => ord == std::cmp::Ordering::Greater,
                                    _ => false,
                                };
                                if replace {
                                    current = Some(value);
                                }
                            }
                        }
                    }
                    Ok(current.unwrap_or(Value::Null))
                }
                _ => Err(GongDBError::new("unsupported function")),
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let left_val = eval_expr(db, left, scope, outer)?;
            let right_val = eval_expr(db, right, scope, outer)?;
            apply_binary_op(op, left_val, right_val)
        }
        Expr::UnaryOp { op, expr } => {
            let value = eval_expr(db, expr, scope, outer)?;
            match op {
                crate::ast::UnaryOperator::Plus => Ok(value),
                crate::ast::UnaryOperator::Minus => match value {
                    Value::Integer(v) => Ok(Value::Integer(-v)),
                    Value::Real(v) => Ok(Value::Real(-v)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(GongDBError::new("invalid unary minus")),
                },
                crate::ast::UnaryOperator::Not => Ok(apply_logical_not(value)),
            }
        }
        Expr::IsNull { expr, negated } => {
            let value = eval_expr(db, expr, scope, outer)?;
            let is_null = matches!(value, Value::Null);
            Ok(Value::Integer((if *negated { !is_null } else { is_null }) as i64))
        }
        Expr::Cast { expr, data_type } => {
            let value = eval_expr(db, expr, scope, outer)?;
            Ok(cast_value(value, data_type))
        }
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            let operand_value = match operand {
                Some(expr) => Some(eval_expr(db, expr, scope, outer)?),
                None => None,
            };
            for (when_expr, then_expr) in when_then {
                let is_match = if let Some(ref value) = operand_value {
                    let when_value = eval_expr(db, when_expr, scope, outer)?;
                    let comparison = compare_values(&BinaryOperator::Eq, value, &when_value);
                    value_to_bool(&comparison)
                } else {
                    let condition = eval_expr(db, when_expr, scope, outer)?;
                    value_to_bool(&condition)
                };
                if is_match {
                    return eval_expr(db, then_expr, scope, outer);
                }
            }
            if let Some(expr) = else_result {
                eval_expr(db, expr, scope, outer)
            } else {
                Ok(Value::Null)
            }
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let expr_val = eval_expr(db, expr, scope, outer)?;
            let low_val = eval_expr(db, low, scope, outer)?;
            let high_val = eval_expr(db, high, scope, outer)?;
            let lower_cmp = compare_values(&BinaryOperator::GtEq, &expr_val, &low_val);
            let upper_cmp = compare_values(&BinaryOperator::LtEq, &expr_val, &high_val);
            let between = apply_logical_and(&lower_cmp, &upper_cmp);
            if *negated {
                Ok(apply_logical_not(between))
            } else {
                Ok(between)
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let expr_val = eval_expr(db, expr, scope, outer)?;
            let result = eval_in_list(db, &expr_val, list, scope, outer)?;
            if *negated {
                Ok(apply_logical_not(result))
            } else {
                Ok(result)
            }
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let expr_val = eval_expr(db, expr, scope, outer)?;
            let result = eval_in_subquery(db, &expr_val, subquery, scope)?;
            if *negated {
                Ok(apply_logical_not(result))
            } else {
                Ok(result)
            }
        }
        Expr::Exists(subquery) => {
            let result = db.evaluate_select_values_with_outer(subquery, Some(scope))?;
            Ok(Value::Integer((!result.rows.is_empty()) as i64))
        }
        Expr::Nested(expr) => eval_expr(db, expr, scope, outer),
        _ => Err(GongDBError::new(
            "unsupported expression in phase 2",
        )),
    }
}

fn eval_in_list<'a, 'b>(
    db: &GongDB,
    expr_val: &Value,
    list: &[Expr],
    scope: &EvalScope<'a>,
    outer: Option<&EvalScope<'b>>,
) -> Result<Value, GongDBError> {
    if list.is_empty() {
        return Ok(Value::Integer(0));
    }
    let mut saw_null = false;
    for item in list {
        let item_val = eval_expr(db, item, scope, outer)?;
        match compare_values(&BinaryOperator::Eq, expr_val, &item_val) {
            Value::Integer(1) => return Ok(Value::Integer(1)),
            Value::Null => saw_null = true,
            _ => {}
        }
    }
    if saw_null {
        Ok(Value::Null)
    } else {
        Ok(Value::Integer(0))
    }
}

fn eval_in_subquery<'a>(
    db: &GongDB,
    expr_val: &Value,
    subquery: &Select,
    scope: &EvalScope<'a>,
) -> Result<Value, GongDBError> {
    let result = db.evaluate_select_values_with_outer(subquery, Some(scope))?;
    if result.columns.len() != 1 {
        return Err(GongDBError::new(
            "subquery returned more than one column",
        ));
    }
    if result.rows.is_empty() {
        return Ok(Value::Integer(0));
    }
    let mut saw_null = false;
    for row in result.rows {
        let item_val = row
            .get(0)
            .cloned()
            .unwrap_or(Value::Null);
        match compare_values(&BinaryOperator::Eq, expr_val, &item_val) {
            Value::Integer(1) => return Ok(Value::Integer(1)),
            Value::Null => saw_null = true,
            _ => {}
        }
    }
    if saw_null {
        Ok(Value::Null)
    } else {
        Ok(Value::Integer(0))
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
        BinaryOperator::And => Ok(apply_logical_and(&left, &right)),
        BinaryOperator::Or => Ok(apply_logical_or(&left, &right)),
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
        BinaryOperator::Divide => {
            if right_num == 0.0 {
                return Ok(Value::Null);
            }
            left_num / right_num
        }
        BinaryOperator::Modulo => {
            if right_num == 0.0 {
                return Ok(Value::Null);
            }
            left_num % right_num
        }
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
        Value::Text(text) => match parse_numeric_text(text) {
            Some(num) => numeric_to_f64(num).0 != 0.0,
            None => false,
        },
        Value::Blob(_) => false,
    }
}

fn value_to_truth_value(value: &Value) -> Option<bool> {
    match value {
        Value::Null => None,
        _ => Some(value_to_bool(value)),
    }
}

fn apply_logical_not(value: Value) -> Value {
    match value_to_truth_value(&value) {
        None => Value::Null,
        Some(result) => Value::Integer((!result) as i64),
    }
}

fn apply_logical_and(left: &Value, right: &Value) -> Value {
    match (value_to_truth_value(left), value_to_truth_value(right)) {
        (Some(false), _) | (_, Some(false)) => Value::Integer(0),
        (Some(true), Some(true)) => Value::Integer(1),
        _ => Value::Null,
    }
}

fn apply_logical_or(left: &Value, right: &Value) -> Value {
    match (value_to_truth_value(left), value_to_truth_value(right)) {
        (Some(true), _) | (_, Some(true)) => Value::Integer(1),
        (Some(false), Some(false)) => Value::Integer(0),
        _ => Value::Null,
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

fn numeric_value_or_zero(value: &Value) -> Option<NumericValue> {
    match value {
        Value::Null => None,
        _ => Some(numeric_value(value).unwrap_or(NumericValue::Integer(0))),
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
    db: &GongDB,
    order_by: &[crate::ast::OrderByExpr],
    columns: &[Column],
    table_scope: &TableScope,
    outer: Option<&EvalScope<'_>>,
    a: &[Value],
    b: &[Value],
) -> std::cmp::Ordering {
    for order in order_by {
        let asc = order.asc.unwrap_or(true);
        let left_scope = EvalScope {
            columns,
            row: a,
            table_scope,
        };
        let right_scope = EvalScope {
            columns,
            row: b,
            table_scope,
        };
        let left = eval_expr(db, &order.expr, &left_scope, outer).unwrap_or(Value::Null);
        let right = eval_expr(db, &order.expr, &right_scope, outer).unwrap_or(Value::Null);
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
            Expr::Function {
                name,
                args,
                distinct,
            } => {
                if *distinct {
                    return false;
                }
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
    distinct: bool,
    separator: Option<Expr>,
}

#[derive(Clone)]
enum AggregateKind {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    Total,
    GroupConcat,
}

fn aggregate_projections(
    projection: &[SelectItem],
) -> Result<Option<Vec<AggregateExpr>>, GongDBError> {
    let mut aggregates = Vec::new();
    let mut saw_aggregate = false;
    for item in projection {
        match item {
            SelectItem::Expr { expr, .. } => match expr {
                Expr::Function {
                    name,
                    args,
                    distinct,
                } => {
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
                                distinct: *distinct,
                                separator: None,
                            });
                        }
                        "count" => {
                            saw_aggregate = true;
                            if args.is_empty()
                                || args.iter().all(|arg| matches!(arg, Expr::Wildcard))
                            {
                                if *distinct {
                                    return Err(GongDBError::new(
                                        "COUNT DISTINCT does not support *",
                                    ));
                                }
                                aggregates.push(AggregateExpr {
                                    kind: AggregateKind::Count,
                                    expr: None,
                                    distinct: false,
                                    separator: None,
                                });
                            } else if args.len() == 1 {
                                if matches!(args[0], Expr::Wildcard) {
                                    if *distinct {
                                        return Err(GongDBError::new(
                                            "COUNT DISTINCT does not support *",
                                        ));
                                    }
                                    aggregates.push(AggregateExpr {
                                        kind: AggregateKind::Count,
                                        expr: None,
                                        distinct: false,
                                        separator: None,
                                    });
                                } else {
                                aggregates.push(AggregateExpr {
                                    kind: AggregateKind::Count,
                                    expr: Some(args[0].clone()),
                                    distinct: *distinct,
                                    separator: None,
                                });
                                }
                            } else {
                                return Err(GongDBError::new("COUNT expects at most one argument"));
                            }
                        }
                        "avg" => {
                            saw_aggregate = true;
                            if args.len() != 1 {
                                return Err(GongDBError::new("AVG expects one argument"));
                            }
                            aggregates.push(AggregateExpr {
                                kind: AggregateKind::Avg,
                                expr: Some(args[0].clone()),
                                distinct: *distinct,
                                separator: None,
                            });
                        }
                        "min" => {
                            if args.len() == 1 {
                                saw_aggregate = true;
                                aggregates.push(AggregateExpr {
                                    kind: AggregateKind::Min,
                                    expr: Some(args[0].clone()),
                                    distinct: *distinct,
                                    separator: None,
                                });
                            } else if *distinct {
                                return Err(GongDBError::new(
                                    "DISTINCT is only valid for single-argument aggregates",
                                ));
                            } else if saw_aggregate {
                                return Err(GongDBError::new(
                                    "cannot mix aggregate and non-aggregate expressions",
                                ));
                            } else {
                                return Ok(None);
                            }
                        }
                        "max" => {
                            if args.len() == 1 {
                                saw_aggregate = true;
                                aggregates.push(AggregateExpr {
                                    kind: AggregateKind::Max,
                                    expr: Some(args[0].clone()),
                                    distinct: *distinct,
                                    separator: None,
                                });
                            } else if *distinct {
                                return Err(GongDBError::new(
                                    "DISTINCT is only valid for single-argument aggregates",
                                ));
                            } else if saw_aggregate {
                                return Err(GongDBError::new(
                                    "cannot mix aggregate and non-aggregate expressions",
                                ));
                            } else {
                                return Ok(None);
                            }
                        }
                        "total" => {
                            saw_aggregate = true;
                            if args.len() != 1 {
                                return Err(GongDBError::new("TOTAL expects one argument"));
                            }
                            aggregates.push(AggregateExpr {
                                kind: AggregateKind::Total,
                                expr: Some(args[0].clone()),
                                distinct: *distinct,
                                separator: None,
                            });
                        }
                        "group_concat" => {
                            saw_aggregate = true;
                            if args.is_empty() || args.len() > 2 {
                                return Err(GongDBError::new(
                                    "GROUP_CONCAT expects one or two arguments",
                                ));
                            }
                            if *distinct && args.len() != 1 {
                                return Err(GongDBError::new(
                                    "DISTINCT is only valid for single-argument aggregates",
                                ));
                            }
                            aggregates.push(AggregateExpr {
                                kind: AggregateKind::GroupConcat,
                                expr: Some(args[0].clone()),
                                distinct: *distinct,
                                separator: if args.len() == 2 {
                                    Some(args[1].clone())
                                } else {
                                    None
                                },
                            });
                        }
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
    db: &GongDB,
    aggregates: &[AggregateExpr],
    columns: &[Column],
    table_scope: &TableScope,
    rows: &[Vec<Value>],
    outer: Option<&EvalScope<'_>>,
) -> Result<Vec<Value>, GongDBError> {
    let mut results = Vec::with_capacity(aggregates.len());
    for agg in aggregates {
        match agg.kind {
            AggregateKind::Count => {
                let count = if let Some(expr) = &agg.expr {
                    let mut tally = 0i64;
                    let mut seen = Vec::new();
                    for row in rows {
                        let scope = EvalScope {
                            columns,
                            row,
                            table_scope,
                        };
                        let value = eval_expr(db, expr, &scope, outer)?;
                        if !matches!(value, Value::Null) {
                            if agg.distinct {
                                if seen.iter().any(|v| values_equal(v, &value)) {
                                    continue;
                                }
                                seen.push(value);
                                tally += 1;
                            } else {
                            tally += 1;
                            }
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
                let mut seen = Vec::new();
                for row in rows {
                    let scope = EvalScope {
                        columns,
                        row,
                        table_scope,
                    };
                    let value = eval_expr(db, expr, &scope, outer)?;
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    if agg.distinct {
                        if seen.iter().any(|v| values_equal(v, &value)) {
                            continue;
                        }
                        seen.push(value.clone());
                    }
                    if let Some(num) = numeric_value_or_zero(&value) {
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
            AggregateKind::Avg => {
                let Some(expr) = &agg.expr else {
                    return Err(GongDBError::new("AVG requires an expression"));
                };
                let mut sum = 0.0;
                let mut count = 0i64;
                let mut seen = Vec::new();
                for row in rows {
                    let scope = EvalScope {
                        columns,
                        row,
                        table_scope,
                    };
                    let value = eval_expr(db, expr, &scope, outer)?;
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    if agg.distinct {
                        if seen.iter().any(|v| values_equal(v, &value)) {
                            continue;
                        }
                        seen.push(value.clone());
                    }
                    if let Some(num) = numeric_value_or_zero(&value) {
                        sum += numeric_to_f64(num).0;
                        count += 1;
                    }
                }
                if count == 0 {
                    results.push(Value::Null);
                } else {
                    results.push(Value::Real(sum / count as f64));
                }
            }
            AggregateKind::Min => {
                let Some(expr) = &agg.expr else {
                    return Err(GongDBError::new("MIN requires an expression"));
                };
                let mut current: Option<NumericValue> = None;
                let mut seen = Vec::new();
                for row in rows {
                    let scope = EvalScope {
                        columns,
                        row,
                        table_scope,
                    };
                    let value = eval_expr(db, expr, &scope, outer)?;
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    if agg.distinct {
                        if seen.iter().any(|v| values_equal(v, &value)) {
                            continue;
                        }
                        seen.push(value.clone());
                    }
                    let Some(num) = numeric_value_or_zero(&value) else {
                        continue;
                    };
                    match current {
                        None => current = Some(num),
                        Some(existing) => {
                            let left = numeric_to_f64(num).0;
                            let right = numeric_to_f64(existing).0;
                            if left < right {
                                current = Some(num);
                            }
                        }
                    }
                }
                results.push(match current {
                    Some(NumericValue::Integer(v)) => Value::Integer(v),
                    Some(NumericValue::Real(v)) => Value::Real(v),
                    None => Value::Null,
                });
            }
            AggregateKind::Max => {
                let Some(expr) = &agg.expr else {
                    return Err(GongDBError::new("MAX requires an expression"));
                };
                let mut current: Option<NumericValue> = None;
                let mut seen = Vec::new();
                for row in rows {
                    let scope = EvalScope {
                        columns,
                        row,
                        table_scope,
                    };
                    let value = eval_expr(db, expr, &scope, outer)?;
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    if agg.distinct {
                        if seen.iter().any(|v| values_equal(v, &value)) {
                            continue;
                        }
                        seen.push(value.clone());
                    }
                    let Some(num) = numeric_value_or_zero(&value) else {
                        continue;
                    };
                    match current {
                        None => current = Some(num),
                        Some(existing) => {
                            let left = numeric_to_f64(num).0;
                            let right = numeric_to_f64(existing).0;
                            if left > right {
                                current = Some(num);
                            }
                        }
                    }
                }
                results.push(match current {
                    Some(NumericValue::Integer(v)) => Value::Integer(v),
                    Some(NumericValue::Real(v)) => Value::Real(v),
                    None => Value::Null,
                });
            }
            AggregateKind::Total => {
                let Some(expr) = &agg.expr else {
                    return Err(GongDBError::new("TOTAL requires an expression"));
                };
                let mut sum = 0.0;
                let mut seen = Vec::new();
                let mut saw_value = false;
                for row in rows {
                    let scope = EvalScope {
                        columns,
                        row,
                        table_scope,
                    };
                    let value = eval_expr(db, expr, &scope, outer)?;
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    if agg.distinct {
                        if seen.iter().any(|v| values_equal(v, &value)) {
                            continue;
                        }
                        seen.push(value.clone());
                    }
                    if let Some(num) = numeric_value_or_zero(&value) {
                        sum += numeric_to_f64(num).0;
                        saw_value = true;
                    }
                }
                if saw_value {
                    results.push(Value::Real(sum));
                } else {
                    results.push(Value::Real(0.0));
                }
            }
            AggregateKind::GroupConcat => {
                let Some(expr) = &agg.expr else {
                    return Err(GongDBError::new("GROUP_CONCAT requires an expression"));
                };
                let mut result = String::new();
                let mut first = true;
                let mut seen = Vec::new();
                for row in rows {
                    let scope = EvalScope {
                        columns,
                        row,
                        table_scope,
                    };
                    let value = eval_expr(db, expr, &scope, outer)?;
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    if agg.distinct {
                        if seen.iter().any(|v| values_equal(v, &value)) {
                            continue;
                        }
                        seen.push(value.clone());
                    }
                    let separator = if let Some(separator_expr) = &agg.separator {
                        let sep_value = eval_expr(db, separator_expr, &scope, outer)?;
                        if matches!(sep_value, Value::Null) {
                            String::new()
                        } else {
                            value_to_text(&sep_value)
                        }
                    } else {
                        ",".to_string()
                    };
                    if first {
                        result.push_str(&value_to_text(&value));
                        first = false;
                    } else {
                        result.push_str(&separator);
                        result.push_str(&value_to_text(&value));
                    }
                }
                if first {
                    results.push(Value::Null);
                } else {
                    results.push(Value::Text(result));
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
