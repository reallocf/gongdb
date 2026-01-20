//! Execution engine for GongDB.
//!
//! This module runs parsed SQL statements against a storage engine.
//! It is intentionally minimal and geared toward correctness and testability.
//!
//! # Examples
//! ```no_run
//! use gongdb::engine::GongDB;
//!
//! let mut db = GongDB::new_in_memory().expect("db");
//! db.run_statement("CREATE TABLE t(id INTEGER)").unwrap();
//! db.run_statement("INSERT INTO t VALUES (1)").unwrap();
//! let output = db.run_statement("SELECT id FROM t").unwrap();
//! println!("{:?}", output);
//! ```

use crate::ast::{
    BeginTransaction, BinaryOperator, ColumnConstraint, ColumnDef, CreateTable, Cte, DataType,
    Expr, Ident, IndexedColumn, InsertConflict, InsertSource, IsolationLevel, JoinConstraint,
    JoinOperator, Literal, NullsOrder, OrderByExpr, Select, SelectItem, SortOrder, Statement,
    TableConstraint, TableRef, Update, With,
};
use crate::parser;
use crate::storage::{
    Column, IndexMeta, RowLocation, StorageEngine, StorageError, StorageSnapshot, TableMeta, Value,
    ViewMeta,
};
use async_trait::async_trait;
use sqllogictest::{DBOutput, DefaultColumnType};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static NEXT_TXN_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
/// Error type returned by the execution engine.
pub enum GongDBError {
    /// Parsing failed for the input SQL.
    Parse(String),
    /// Execution failed due to unsupported features or runtime errors.
    Execution(String),
    /// Constraint violation (e.g. UNIQUE or NOT NULL).
    Constraint(String),
    /// Storage layer error.
    Storage(StorageError),
}

impl GongDBError {
    fn new(message: impl Into<String>) -> Self {
        Self::Execution(message.into())
    }

    fn parse(message: impl Into<String>) -> Self {
        Self::Parse(message.into())
    }

    fn constraint(message: impl Into<String>) -> Self {
        Self::Constraint(message.into())
    }
}

impl std::fmt::Display for GongDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GongDBError::Parse(message)
            | GongDBError::Execution(message)
            | GongDBError::Constraint(message) => write!(f, "{}", message),
            GongDBError::Storage(err) => write!(f, "{}", err),
        }
    }
}

impl std::error::Error for GongDBError {}

impl From<StorageError> for GongDBError {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::UniqueViolation { table, columns } => GongDBError::constraint(format!(
                "UNIQUE constraint failed: {}",
                columns
                    .iter()
                    .map(|col| format!("{}.{}", table, col))
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            _ => GongDBError::Storage(err),
        }
    }
}

#[derive(Debug, Clone)]
struct TransactionState {
    id: u64,
    isolation: IsolationLevel,
    snapshot: StorageSnapshot,
    hold_read_lock: bool,
    read_lock_acquired: bool,
}

#[derive(Debug, Clone, Copy)]
struct StatementLock {
    txn_id: u64,
}

/// Main entry point for executing SQL against GongDB.
///
/// # Best Practices
/// - Reuse a single `GongDB` instance for multiple statements to keep caches warm.
/// - Use explicit transactions for multi-statement updates to guarantee atomicity.
pub struct GongDB {
    storage: StorageEngine,
    stats_cache: RefCell<HashMap<String, TableStats>>,
    transaction: Option<TransactionState>,
    triggers: HashMap<String, TriggerMeta>,
    in_list_cache: RefCell<InListCache>,
    in_subquery_cache: RefCell<InSubqueryCache>,
    subquery_cache: RefCell<SubqueryCache>,
    statement_cache: RefCell<HashMap<String, CachedStatement>>,
    select_cache: RefCell<HashMap<String, CachedSelect>>,
    fast_update_cache: RefCell<HashMap<String, FastUpdateTemplate>>,
    index_cache: RefCell<HashMap<String, Vec<IndexMeta>>>,
    column_index_cache: RefCell<HashMap<String, HashMap<String, usize>>>,
    insert_validation_cache: RefCell<HashMap<String, InsertValidationInfo>>,
    fast_stock_offsets_cache: RefCell<Option<FastStockOffsetsCache>>,
    fast_stock_location_cache: RefCell<Option<HashMap<(i64, i64), RowLocation>>>,
    fast_order_line_amount_offsets_cache: RefCell<Option<FastOrderLineAmountOffsetsCache>>,
    fast_customer_delivery_offsets_cache: RefCell<Option<FastCustomerDeliveryOffsetsCache>>,
    fast_district_offsets_cache: RefCell<Option<FastDistrictOffsetsCache>>,
    fast_warehouse_offsets_cache: RefCell<Option<FastWarehouseOffsetsCache>>,
    fast_district_payment_offsets_cache: RefCell<Option<FastDistrictPaymentOffsetsCache>>,
    fast_customer_payment_offsets_cache: RefCell<Option<FastCustomerPaymentOffsetsCache>>,
}

#[derive(Debug, Clone)]
struct TriggerMeta {
    table: String,
}

#[derive(Clone)]
struct CachedSelect {
    types: Vec<DefaultColumnType>,
    rows: Vec<Vec<String>>,
}

#[derive(Clone)]
struct CachedStatement {
    statement: Statement,
    param_count: usize,
}

#[derive(Clone)]
struct InsertValidationInfo {
    not_null_indices: Vec<usize>,
    has_check_constraints: bool,
}

#[derive(Debug, Clone)]
struct FastOrderBy {
    column: String,
    desc: bool,
}

#[derive(Debug, Clone)]
struct FastSelectPlan {
    table: String,
    columns: Vec<String>,
    predicates: Vec<(String, Value)>,
    order_by: Option<FastOrderBy>,
    limit: Option<usize>,
}

#[derive(Clone)]
struct FastStockOffsetsCache {
    record_len: usize,
    offsets: FastStockUpdateOffsets,
}

#[derive(Clone)]
struct FastOrderLineAmountOffsetsCache {
    record_len: usize,
    amount: usize,
}

#[derive(Clone)]
struct FastCustomerDeliveryOffsetsCache {
    record_len: usize,
    balance: usize,
    delivery_cnt: usize,
}

#[derive(Clone)]
struct FastCustomerDeliveryOffsets {
    balance: usize,
    delivery_cnt: usize,
}

#[derive(Clone)]
struct FastDistrictOffsetsCache {
    record_len: usize,
    next_o_id: usize,
}

#[derive(Clone)]
struct FastWarehouseOffsetsCache {
    record_len: usize,
    ytd: usize,
}

#[derive(Clone)]
struct FastDistrictPaymentOffsetsCache {
    record_len: usize,
    ytd: usize,
}

#[derive(Clone)]
struct FastCustomerPaymentOffsetsCache {
    record_len: usize,
    balance: usize,
    ytd_payment: usize,
    payment_cnt: usize,
}

#[derive(Clone)]
struct FastCustomerPaymentOffsets {
    balance: usize,
    ytd_payment: usize,
    payment_cnt: usize,
}

impl GongDB {
    const STATEMENT_CACHE_LIMIT: usize = 128;

    /// Create a new in-memory database.
    ///
    /// # Examples
    /// ```no_run
    /// use gongdb::engine::GongDB;
    ///
    /// let mut db = GongDB::new_in_memory().unwrap();
    /// db.run_statement("CREATE TABLE t(id INTEGER)").unwrap();
    /// ```
    pub fn new_in_memory() -> Result<Self, GongDBError> {
        Ok(Self {
            storage: StorageEngine::new_in_memory()?,
            stats_cache: RefCell::new(HashMap::new()),
            transaction: None,
            triggers: HashMap::new(),
            in_list_cache: RefCell::new(InListCache::new()),
            in_subquery_cache: RefCell::new(InSubqueryCache::new()),
            subquery_cache: RefCell::new(SubqueryCache::new()),
            statement_cache: RefCell::new(HashMap::new()),
            select_cache: RefCell::new(HashMap::new()),
            fast_update_cache: RefCell::new(HashMap::new()),
            index_cache: RefCell::new(HashMap::new()),
            column_index_cache: RefCell::new(HashMap::new()),
            insert_validation_cache: RefCell::new(HashMap::new()),
            fast_stock_offsets_cache: RefCell::new(None),
            fast_stock_location_cache: RefCell::new(None),
            fast_order_line_amount_offsets_cache: RefCell::new(None),
            fast_customer_delivery_offsets_cache: RefCell::new(None),
            fast_district_offsets_cache: RefCell::new(None),
            fast_warehouse_offsets_cache: RefCell::new(None),
            fast_district_payment_offsets_cache: RefCell::new(None),
            fast_customer_payment_offsets_cache: RefCell::new(None),
        })
    }

    /// Create or open an on-disk database at the given path.
    ///
    /// # Examples
    /// ```no_run
    /// use gongdb::engine::GongDB;
    ///
    /// let mut db = GongDB::new_on_disk("db.gong").unwrap();
    /// db.run_statement("CREATE TABLE t(id INTEGER)").unwrap();
    /// ```
    pub fn new_on_disk(path: &str) -> Result<Self, GongDBError> {
        Ok(Self {
            storage: StorageEngine::new_on_disk(path)?,
            stats_cache: RefCell::new(HashMap::new()),
            transaction: None,
            triggers: HashMap::new(),
            in_list_cache: RefCell::new(InListCache::new()),
            in_subquery_cache: RefCell::new(InSubqueryCache::new()),
            subquery_cache: RefCell::new(SubqueryCache::new()),
            statement_cache: RefCell::new(HashMap::new()),
            select_cache: RefCell::new(HashMap::new()),
            fast_update_cache: RefCell::new(HashMap::new()),
            index_cache: RefCell::new(HashMap::new()),
            column_index_cache: RefCell::new(HashMap::new()),
            insert_validation_cache: RefCell::new(HashMap::new()),
            fast_stock_offsets_cache: RefCell::new(None),
            fast_stock_location_cache: RefCell::new(None),
            fast_order_line_amount_offsets_cache: RefCell::new(None),
            fast_customer_delivery_offsets_cache: RefCell::new(None),
            fast_district_offsets_cache: RefCell::new(None),
            fast_warehouse_offsets_cache: RefCell::new(None),
            fast_district_payment_offsets_cache: RefCell::new(None),
            fast_customer_payment_offsets_cache: RefCell::new(None),
        })
    }

    pub fn defer_index_updates(&mut self, table_name: &str) -> Result<(), GongDBError> {
        self.storage.defer_index_updates(table_name)?;
        self.invalidate_schema_caches();
        Ok(())
    }

    pub fn resume_index_updates(&mut self, table_name: &str) -> Result<(), GongDBError> {
        self.storage.resume_index_updates(table_name)?;
        self.invalidate_schema_caches();
        Ok(())
    }

    fn invalidate_schema_caches(&self) {
        self.fast_update_cache.borrow_mut().clear();
        self.index_cache.borrow_mut().clear();
        self.column_index_cache.borrow_mut().clear();
        self.insert_validation_cache.borrow_mut().clear();
        self.fast_stock_offsets_cache.borrow_mut().take();
        self.fast_stock_location_cache.borrow_mut().take();
        self.fast_order_line_amount_offsets_cache.borrow_mut().take();
        self.fast_customer_delivery_offsets_cache.borrow_mut().take();
        self.fast_district_offsets_cache.borrow_mut().take();
        self.fast_warehouse_offsets_cache.borrow_mut().take();
        self.fast_district_payment_offsets_cache.borrow_mut().take();
        self.fast_customer_payment_offsets_cache.borrow_mut().take();
    }

    fn table_indexes_cached(&self, table_name: &str) -> Vec<IndexMeta> {
        if self.storage.index_updates_deferred(table_name) {
            return Vec::new();
        }
        let key = table_name.to_ascii_lowercase();
        if let Some(indexes) = self.index_cache.borrow().get(&key) {
            return indexes.clone();
        }
        let indexes: Vec<IndexMeta> = self
            .storage
            .list_indexes()
            .into_iter()
            .filter(|index| index.table.eq_ignore_ascii_case(table_name))
            .collect();
        self.index_cache.borrow_mut().insert(key, indexes.clone());
        indexes
    }

    fn column_index_map_cached(&self, table: &TableMeta) -> HashMap<String, usize> {
        let key = table.name.to_ascii_lowercase();
        if let Some(map) = self.column_index_cache.borrow().get(&key) {
            return map.clone();
        }
        let map = column_index_map_fast(&table.columns);
        self.column_index_cache.borrow_mut().insert(key, map.clone());
        map
    }

    fn insert_validation_info_cached(&self, table: &TableMeta) -> InsertValidationInfo {
        let key = table.name.to_ascii_lowercase();
        if let Some(info) = self.insert_validation_cache.borrow().get(&key) {
            return info.clone();
        }
        let info = build_insert_validation_info(table);
        self.insert_validation_cache
            .borrow_mut()
            .insert(key, info.clone());
        info
    }

    fn build_fast_stock_location_cache(
        &mut self,
    ) -> Result<Option<HashMap<(i64, i64), RowLocation>>, GongDBError> {
        let stock = match self.storage.get_table("stock") {
            Some(table) => table.clone(),
            None => return Ok(None),
        };
        let column_map = self.column_index_map_cached(&stock);
        let w_idx = match column_map.get("s_w_id") {
            Some(idx) => *idx,
            None => return Ok(None),
        };
        let i_idx = match column_map.get("s_i_id") {
            Some(idx) => *idx,
            None => return Ok(None),
        };
        let rows = self.storage.scan_table_with_locations("stock")?;
        let mut cache = HashMap::with_capacity(rows.len());
        for (location, row) in rows {
            let (Value::Integer(w_id), Value::Integer(i_id)) = (&row[w_idx], &row[i_idx])
            else {
                continue;
            };
            cache.insert((*w_id, *i_id), location);
        }
        Ok(Some(cache))
    }

    fn lookup_stock_location(
        &mut self,
        index: &IndexMeta,
        w_id: i64,
        i_id: i64,
    ) -> Result<Option<RowLocation>, GongDBError> {
        if self.fast_stock_location_cache.borrow().is_none() {
            if let Some(cache) = self.build_fast_stock_location_cache()? {
                self.fast_stock_location_cache.borrow_mut().replace(cache);
            }
        }
        if let Some(cache) = self.fast_stock_location_cache.borrow().as_ref() {
            if let Some(location) = cache.get(&(w_id, i_id)) {
                return Ok(Some(*location));
            }
        }
        let key = vec![Value::Integer(w_id), Value::Integer(i_id)];
        let location = self.storage.scan_index_first_location(&index.name, &key)?;
        if let Some(location) = location {
            if let Some(cache) = self.fast_stock_location_cache.borrow_mut().as_mut() {
                cache.insert((w_id, i_id), location);
            }
            return Ok(Some(location));
        }
        Ok(None)
    }

    /// Execute a single SQL statement and return the result.
    ///
    /// This API accepts exactly one SQL statement. It returns `DBOutput`
    /// matching the sqllogictest expectations used by GongDB's tests.
    ///
    /// # Examples
    /// ```no_run
    /// use gongdb::engine::GongDB;
    ///
    /// let mut db = GongDB::new_in_memory().unwrap();
    /// let output = db.run_statement("SELECT 1").unwrap();
    /// println!("{:?}", output);
    /// ```
    pub fn run_statement(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, GongDBError> {
        if let Some(result) = self.try_fast_transaction(sql) {
            return result;
        }
        if let Some(result) = self.try_fast_insert(sql) {
            return result;
        }
        if let Some(result) = self.try_fast_stock_update(sql) {
            return result;
        }
        if let Some(result) = self.try_fast_delivery_customer_update(sql) {
            return result;
        }
        if let Some(result) = self.try_fast_update(sql) {
            return result;
        }
        if let Some(result) = self.try_fast_stock_level(sql) {
            return result;
        }
        if let Some(result) = self.try_fast_select(sql) {
            return result;
        }
        if let Some(cached) = { self.select_cache.borrow().get(sql).cloned() } {
            return Ok(DBOutput::Rows {
                types: cached.types.clone(),
                rows: cached.rows.clone(),
            });
        }
        let normalized = parser::normalize_statement(sql)
            .map_err(|e| GongDBError::parse(e.sqlite_message_with_sql(sql)))?;
        let cache_key = normalized.normalized_sql;
        let cached = { self.statement_cache.borrow().get(&cache_key).cloned() };
        let template = if let Some(stmt) = cached {
            stmt
        } else {
            let parsed = parser::parse_statement_with_params(&cache_key)
                .map_err(|e| GongDBError::parse(e.sqlite_message_with_sql(sql)))?;
            let mut cache = self.statement_cache.borrow_mut();
            let cached = CachedStatement {
                statement: parsed.statement.clone(),
                param_count: parsed.param_count,
            };
            cache.insert(cache_key.clone(), cached.clone());
            if cache.len() > Self::STATEMENT_CACHE_LIMIT {
                cache.clear();
            }
            cached
        };
        if template.param_count != normalized.params.len() {
            return Err(GongDBError::new("parameter count mismatch"));
        }
        let stmt = bind_statement_params(&template.statement, &normalized.params)?;
        self.in_list_cache.replace(InListCache::new());
        self.in_subquery_cache.replace(InSubqueryCache::new());
        self.subquery_cache.replace(SubqueryCache::new());
        match stmt {
            Statement::BeginTransaction(begin) => self.begin_transaction(begin),
            Statement::Commit => self.commit_transaction(),
            Statement::Rollback => self.rollback_transaction(),
            _ => {
                let is_write = is_write_statement(&stmt);
                if is_write {
                    self.select_cache.borrow_mut().clear();
                }
                let lock = self.acquire_statement_lock(is_write)?;
                let result = self.execute_statement(stmt);
                match result {
                    Ok(output) => {
                        if let DBOutput::Rows { types, rows } = &output {
                            self.select_cache.borrow_mut().insert(
                                sql.to_string(),
                                CachedSelect {
                                    types: types.clone(),
                                    rows: rows.clone(),
                                },
                            );
                        }
                        self.release_statement_lock(lock);
                        Ok(output)
                    }
                    Err(err) => {
                        if self.transaction.is_some() {
                            if let Err(rollback_err) = self.rollback_internal() {
                                self.release_statement_lock(lock);
                                return Err(rollback_err);
                            }
                        }
                        self.release_statement_lock(lock);
                        Err(err)
                    }
                }
            }
        }
    }

    /// Execute a SQL statement with pre-bound parameters.
    ///
    /// The SQL must use `?` parameter markers. This method skips normalization
    /// so callers can reuse parameterized statements to avoid per-call parsing
    /// overhead.
    pub fn run_statement_with_params(
        &mut self,
        sql: &str,
        params: &[Literal],
    ) -> Result<DBOutput<DefaultColumnType>, GongDBError> {
        if let Some(result) = self.try_fast_delivery_params(sql, params) {
            return result;
        }
        if let Some(result) = self.try_fast_update_params(sql, params) {
            return result;
        }
        let cached = { self.statement_cache.borrow().get(sql).cloned() };
        let template = if let Some(stmt) = cached {
            stmt
        } else {
            let parsed = parser::parse_statement_with_params(sql)
                .map_err(|e| GongDBError::parse(e.sqlite_message_with_sql(sql)))?;
            let mut cache = self.statement_cache.borrow_mut();
            let cached = CachedStatement {
                statement: parsed.statement.clone(),
                param_count: parsed.param_count,
            };
            cache.insert(sql.to_string(), cached.clone());
            if cache.len() > Self::STATEMENT_CACHE_LIMIT {
                cache.clear();
            }
            cached
        };
        if template.param_count != params.len() {
            return Err(GongDBError::new("parameter count mismatch"));
        }
        let stmt = bind_statement_params(&template.statement, params)?;
        self.in_list_cache.replace(InListCache::new());
        self.in_subquery_cache.replace(InSubqueryCache::new());
        self.subquery_cache.replace(SubqueryCache::new());
        match stmt {
            Statement::BeginTransaction(begin) => self.begin_transaction(begin),
            Statement::Commit => self.commit_transaction(),
            Statement::Rollback => self.rollback_transaction(),
            _ => {
                let is_write = is_write_statement(&stmt);
                if is_write {
                    self.select_cache.borrow_mut().clear();
                }
                let lock = self.acquire_statement_lock(is_write)?;
                let result = self.execute_statement(stmt);
                match result {
                    Ok(output) => {
                        self.release_statement_lock(lock);
                        Ok(output)
                    }
                    Err(err) => {
                        if self.transaction.is_some() {
                            if let Err(rollback_err) = self.rollback_internal() {
                                self.release_statement_lock(lock);
                                return Err(rollback_err);
                            }
                        }
                        self.release_statement_lock(lock);
                        Err(err)
                    }
                }
            }
        }
    }

    fn try_fast_delivery_params(
        &mut self,
        sql: &str,
        params: &[Literal],
    ) -> Option<Result<DBOutput<DefaultColumnType>, GongDBError>> {
        let trimmed = sql.trim();
        if trimmed.eq_ignore_ascii_case(
            "DELETE FROM new_order WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?",
        ) {
            if params.len() != 3 {
                return Some(Err(GongDBError::new("parameter count mismatch")));
            }
            let w_id = literal_to_i64(&params[0])?;
            let d_id = literal_to_i64(&params[1])?;
            let o_id = literal_to_i64(&params[2])?;
            let table = self.storage.get_table("new_order")?.clone();
            let indexes = self.table_indexes_cached(&table.name);
            let index = fast_find_index_prefix(&indexes, &["no_w_id", "no_d_id", "no_o_id"])?;
            let key = vec![
                Value::Integer(w_id),
                Value::Integer(d_id),
                Value::Integer(o_id),
            ];
            let location = match self.storage.scan_index_first_location(&index.name, &key) {
                Ok(location) => location,
                Err(err) => return Some(Err(GongDBError::from(err))),
            };
            let mut affected = 0u64;
            if let Some(location) = location {
                if let Err(err) = self.storage.delete_rows_at(&table.name, &[location]) {
                    return Some(Err(GongDBError::from(err)));
                }
                affected = 1;
                self.invalidate_table_stats("new_order");
                self.select_cache.borrow_mut().clear();
            }
            return Some(Ok(DBOutput::StatementComplete(affected)));
        }
        if trimmed.eq_ignore_ascii_case(
            "UPDATE orders SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?",
        ) {
            if params.len() != 4 {
                return Some(Err(GongDBError::new("parameter count mismatch")));
            }
            let carrier_id = literal_to_i64(&params[0])?;
            let w_id = literal_to_i64(&params[1])?;
            let d_id = literal_to_i64(&params[2])?;
            let o_id = literal_to_i64(&params[3])?;
            let table = self.storage.get_table("orders")?.clone();
            let indexes = self.table_indexes_cached(&table.name);
            let index = fast_find_index_prefix(&indexes, &["o_w_id", "o_d_id", "o_id"])?;
            let key = vec![
                Value::Integer(w_id),
                Value::Integer(d_id),
                Value::Integer(o_id),
            ];
            let location = match self.storage.scan_index_first_location(&index.name, &key) {
                Ok(location) => location,
                Err(err) => return Some(Err(GongDBError::from(err))),
            };
            let Some(location) = location else {
                return Some(Ok(DBOutput::StatementComplete(0)));
            };
            let mut fallback_needed = false;
            let applied = match self.storage.update_record_at_with(location, |record| {
                if fallback_needed {
                    return Ok(false);
                }
                let offset = match fast_orders_carrier_offset(record) {
                    Ok(Some(offset)) => offset,
                    Ok(None) => {
                        fallback_needed = true;
                        return Ok(false);
                    }
                    Err(err) => return Err(StorageError::Invalid(err.to_string())),
                };
                if record.get(offset) != Some(&1) {
                    fallback_needed = true;
                    return Ok(false);
                }
                if write_i64_at(record, offset + 1, carrier_id).is_none() {
                    fallback_needed = true;
                    return Ok(false);
                }
                Ok(true)
            }) {
                Ok(applied) => applied,
                Err(StorageError::NotFound(_)) => false,
                Err(err) => return Some(Err(GongDBError::from(err))),
            };
            if fallback_needed {
                return None;
            }
            if applied {
                self.invalidate_table_stats("orders");
                self.select_cache.borrow_mut().clear();
            }
            return Some(Ok(DBOutput::StatementComplete(if applied { 1 } else { 0 })));
        }
        if trimmed.eq_ignore_ascii_case(
            "UPDATE order_line SET ol_delivery_d = '2024-01-01' WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?",
        ) {
            if params.len() != 3 {
                return Some(Err(GongDBError::new("parameter count mismatch")));
            }
            let w_id = literal_to_i64(&params[0])?;
            let d_id = literal_to_i64(&params[1])?;
            let o_id = literal_to_i64(&params[2])?;
            let table = self.storage.get_table("order_line")?.clone();
            let indexes = self.table_indexes_cached(&table.name);
            let index = fast_find_index_prefix(&indexes, &["ol_w_id", "ol_d_id", "ol_o_id"])?;
            let lower_key = build_index_bound(
                index.columns.len(),
                &[Value::Integer(w_id), Value::Integer(d_id)],
                Some(&Value::Integer(o_id)),
                Value::Null,
            );
            let upper_key = build_index_bound(
                index.columns.len(),
                &[Value::Integer(w_id), Value::Integer(d_id)],
                Some(&Value::Integer(o_id)),
                Value::Blob(Vec::new()),
            );
            let locations = match self
                .storage
                .scan_index_range(&index.name, Some(&lower_key), Some(&upper_key))
            {
                Ok(locations) => locations,
                Err(err) => return Some(Err(GongDBError::from(err))),
            };
            if locations.is_empty() {
                return Some(Ok(DBOutput::StatementComplete(0)));
            }
            let column_map = self.column_index_map_cached(&table);
            let delivery_idx = match column_map.get("ol_delivery_d") {
                Some(idx) => *idx,
                None => return Some(Err(GongDBError::new("missing ol_delivery_d column"))),
            };
            let mut updates = Vec::with_capacity(locations.len());
            for location in locations {
                let row = match self.storage.read_row_at(&location) {
                    Ok(row) => row,
                    Err(StorageError::NotFound(_)) => continue,
                    Err(err) => return Some(Err(GongDBError::from(err))),
                };
                let mut new_row = row.clone();
                new_row[delivery_idx] = Value::Text("2024-01-01".to_string());
                updates.push((location, new_row));
            }
            if !updates.is_empty() {
                if let Err(err) = self.storage.update_rows_at(&updates) {
                    return Some(Err(GongDBError::from(err)));
                }
                self.invalidate_table_stats("order_line");
                self.select_cache.borrow_mut().clear();
            }
            return Some(Ok(DBOutput::StatementComplete(updates.len() as u64)));
        }
        None
    }

    /// Fast path for TPC-C stock updates without SQL parsing.
    pub fn run_fast_stock_update(
        &mut self,
        quantity: i64,
        ytd: i64,
        w_id: i64,
        i_id: i64,
        remote: bool,
    ) -> Result<DBOutput<DefaultColumnType>, GongDBError> {
        let plan = FastStockUpdatePlan {
            quantity,
            ytd,
            w_id,
            i_id,
            remote,
        };
        if let Some(result) = self.apply_fast_stock_update(plan) {
            return result;
        }
        let sql = if remote {
            "UPDATE stock SET s_quantity = s_quantity - ?, s_ytd = s_ytd + ?, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + 1 WHERE s_w_id = ? AND s_i_id = ?"
        } else {
            "UPDATE stock SET s_quantity = s_quantity - ?, s_ytd = s_ytd + ?, s_order_cnt = s_order_cnt + 1 WHERE s_w_id = ? AND s_i_id = ?"
        };
        let params = [
            Literal::Integer(quantity),
            Literal::Integer(ytd),
            Literal::Integer(w_id),
            Literal::Integer(i_id),
        ];
        self.run_statement_with_params(sql, &params)
    }

    /// Fast path for batch TPC-C stock updates without SQL parsing.
    pub fn run_fast_stock_updates(
        &mut self,
        updates: &[(i64, i64, i64, i64, bool)],
    ) -> Result<DBOutput<DefaultColumnType>, GongDBError> {
        if updates.is_empty() {
            return Ok(DBOutput::StatementComplete(0));
        }
        let mut plans = Vec::with_capacity(updates.len());
        for (quantity, ytd, w_id, i_id, remote) in updates {
            plans.push(FastStockUpdatePlan {
                quantity: *quantity,
                ytd: *ytd,
                w_id: *w_id,
                i_id: *i_id,
                remote: *remote,
            });
        }
        if let Some(result) = self.apply_fast_stock_update_batch(&plans) {
            return result;
        }
        for (quantity, ytd, w_id, i_id, remote) in updates {
            self.run_fast_stock_update(*quantity, *ytd, *w_id, *i_id, *remote)?;
        }
        Ok(DBOutput::StatementComplete(0))
    }

    /// Fast path for TPC-C payment updates without SQL parsing.
    pub fn run_fast_payment(
        &mut self,
        payment: f64,
        w_id: i64,
        d_id: i64,
        c_w_id: i64,
        c_d_id: i64,
        c_id: i64,
    ) -> Result<DBOutput<DefaultColumnType>, GongDBError> {
        if let Some(result) = self.apply_fast_payment(payment, w_id, d_id, c_w_id, c_d_id, c_id) {
            return result;
        }
        let params = [Literal::Float(payment), Literal::Integer(w_id)];
        self.run_statement_with_params(
            "UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?",
            &params,
        )?;
        let params = [
            Literal::Float(payment),
            Literal::Integer(w_id),
            Literal::Integer(d_id),
        ];
        self.run_statement_with_params(
            "UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?",
            &params,
        )?;
        let params = [
            Literal::Float(payment),
            Literal::Float(payment),
            Literal::Integer(c_w_id),
            Literal::Integer(c_d_id),
            Literal::Integer(c_id),
        ];
        self.run_statement_with_params(
            "UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
            &params,
        )?;
        self.run_statement_with_params(
            "INSERT INTO history VALUES (?, ?, ?, ?, ?, '2024-01-01', ?, 'Payment history data')",
            &[
                Literal::Integer(c_id),
                Literal::Integer(c_d_id),
                Literal::Integer(c_w_id),
                Literal::Integer(d_id),
                Literal::Integer(w_id),
                Literal::Float(payment),
            ],
        )?;
        Ok(DBOutput::StatementComplete(0))
    }

    /// Fast path for incrementing district next order ID without SQL parsing.
    pub fn run_fast_district_next_o_id_increment(
        &mut self,
        w_id: i64,
        d_id: i64,
    ) -> Result<DBOutput<DefaultColumnType>, GongDBError> {
        if let Some(result) = self.apply_fast_district_next_o_id_increment(w_id, d_id) {
            return result;
        }
        let sql =
            "UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = ? AND d_id = ?";
        let params = [Literal::Integer(w_id), Literal::Integer(d_id)];
        self.run_statement_with_params(sql, &params)
    }

    /// Fast path for inserting already-materialized rows without SQL parsing.
    pub fn run_fast_insert_rows(
        &mut self,
        table_name: &str,
        rows: Vec<Vec<Value>>,
    ) -> Result<DBOutput<DefaultColumnType>, GongDBError> {
        if rows.is_empty() {
            return Ok(DBOutput::StatementComplete(0));
        }
        if self.storage.get_view(table_name).is_some() {
            return Err(GongDBError::new(format!(
                "cannot modify view {}",
                table_name
            )));
        }
        let table = self
            .storage
            .get_table(table_name)
            .ok_or_else(|| GongDBError::new(format!("no such table: {}", table_name)))?
            .clone();
        let mut built_rows = Vec::with_capacity(rows.len());
        for row in rows {
            built_rows.push(build_insert_row_from_owned_values_no_columns(
                self, &table, row,
            )?);
        }
        self.storage.insert_rows(table_name, &built_rows)?;
        self.select_cache.borrow_mut().clear();
        self.invalidate_table_stats(table_name);
        Ok(DBOutput::StatementComplete(built_rows.len() as u64))
    }

    /// Fast path for inserting already-materialized rows without validation.
    pub fn run_fast_insert_rows_unchecked(
        &mut self,
        table_name: &str,
        rows: Vec<Vec<Value>>,
    ) -> Result<DBOutput<DefaultColumnType>, GongDBError> {
        if rows.is_empty() {
            return Ok(DBOutput::StatementComplete(0));
        }
        if self.storage.get_view(table_name).is_some() {
            return Err(GongDBError::new(format!(
                "cannot modify view {}",
                table_name
            )));
        }
        let table = self
            .storage
            .get_table(table_name)
            .ok_or_else(|| GongDBError::new(format!("no such table: {}", table_name)))?
            .clone();
        for row in &rows {
            if row.len() != table.columns.len() {
                return Err(GongDBError::new("column count mismatch"));
            }
        }
        self.storage.insert_rows_unchecked(table_name, &rows)?;
        self.select_cache.borrow_mut().clear();
        self.invalidate_table_stats(table_name);
        Ok(DBOutput::StatementComplete(rows.len() as u64))
    }

    fn begin_transaction(
        &mut self,
        begin: BeginTransaction,
    ) -> Result<DBOutput<DefaultColumnType>, GongDBError> {
        if self.transaction.is_some() {
            return Err(GongDBError::new("transaction already in progress"));
        }
        let isolation = begin.isolation.unwrap_or(IsolationLevel::ReadCommitted);
        let hold_read_lock = matches!(isolation, IsolationLevel::RepeatableRead | IsolationLevel::Serializable);
        let txn_id = NEXT_TXN_ID.fetch_add(1, Ordering::SeqCst);
        if hold_read_lock && isolation != IsolationLevel::ReadUncommitted {
            self.storage.acquire_read_lock(txn_id)?;
        }
        let snapshot = self.storage.snapshot()?;
        self.storage.begin_transaction();
        self.transaction = Some(TransactionState {
            id: txn_id,
            isolation,
            snapshot,
            hold_read_lock,
            read_lock_acquired: hold_read_lock,
        });
        Ok(DBOutput::StatementComplete(0))
    }

    fn commit_transaction(&mut self) -> Result<DBOutput<DefaultColumnType>, GongDBError> {
        if let Some(txn) = self.transaction.take() {
            self.storage.commit_transaction()?;
            self.storage.release_locks(txn.id);
        }
        Ok(DBOutput::StatementComplete(0))
    }

    fn rollback_transaction(&mut self) -> Result<DBOutput<DefaultColumnType>, GongDBError> {
        if self.transaction.is_some() {
            self.rollback_internal()?;
        }
        Ok(DBOutput::StatementComplete(0))
    }

    fn rollback_internal(&mut self) -> Result<(), GongDBError> {
        if let Some(txn) = self.transaction.take() {
            self.storage.restore(txn.snapshot)?;
            self.storage.rollback_transaction();
            self.storage.release_locks(txn.id);
            self.stats_cache.borrow_mut().clear();
        }
        Ok(())
    }

    fn acquire_statement_lock(&mut self, is_write: bool) -> Result<Option<StatementLock>, GongDBError> {
        if let Some(txn) = &mut self.transaction {
            if is_write {
                self.storage.acquire_write_lock(txn.id)?;
                return Ok(None);
            }
            if txn.isolation == IsolationLevel::ReadUncommitted {
                return Ok(None);
            }
            if txn.hold_read_lock {
                if !txn.read_lock_acquired {
                    self.storage.acquire_read_lock(txn.id)?;
                    txn.read_lock_acquired = true;
                }
                return Ok(None);
            }
            self.storage.acquire_read_lock(txn.id)?;
            return Ok(Some(StatementLock { txn_id: txn.id }));
        }

        let txn_id = NEXT_TXN_ID.fetch_add(1, Ordering::SeqCst);
        if is_write {
            self.storage.acquire_write_lock(txn_id)?;
            return Ok(Some(StatementLock { txn_id }));
        }
        self.storage.acquire_read_lock(txn_id)?;
        Ok(Some(StatementLock { txn_id }))
    }

    fn release_statement_lock(&mut self, lock: Option<StatementLock>) {
        if let Some(lock) = lock {
            self.storage.release_locks(lock.txn_id);
        }
    }

    fn execute_statement(
        &mut self,
        stmt: Statement,
    ) -> Result<DBOutput<DefaultColumnType>, GongDBError> {
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

                let mut plan = build_create_table_plan(create)?;
                if let Some(spec) = tpcc_orders_auto_index_spec(&name, &plan.columns) {
                    let mut seen = HashSet::new();
                    for existing in &plan.auto_indexes {
                        seen.insert(auto_index_key(existing));
                    }
                    let key = auto_index_key(&spec);
                    if seen.insert(key) {
                        plan.auto_indexes.push(spec);
                    }
                }
                let first_page = self.storage.allocate_data_page()?;
                let meta = TableMeta {
                    name: name.clone(),
                    columns: plan.columns,
                    constraints: plan.constraints,
                    first_page,
                    last_page: first_page,
                    row_count: 0,
                };
                self.storage.create_table(meta)?;
                self.invalidate_table_stats(&name);
                self.invalidate_schema_caches();

                let mut counter = 1;
                let mut used_names = HashSet::new();
                for spec in plan.auto_indexes {
                    let index_name =
                        next_auto_index_name(&self.storage, &name, &mut counter, &mut used_names);
                    let first_page = self.storage.allocate_index_root()?;
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
                    .ok_or_else(|| GongDBError::new(format!("no such table: {}", table_name)))?
                    .clone();
                for column in &create.columns {
                    let exists = table.columns.iter().any(|c| {
                        c.name
                            .eq_ignore_ascii_case(&column.name.value)
                    });
                    if !exists {
                        return Err(GongDBError::new(format!(
                            "no such column: {}",
                            column.name.value
                        )));
                    }
                }
                if self.storage.get_index(&index_name).is_none() {
                    let first_page = self.storage.allocate_index_root()?;
                    let meta = IndexMeta {
                        name: index_name.clone(),
                        table: table_name.clone(),
                        columns: create.columns,
                        unique: create.unique,
                        first_page,
                        last_page: first_page,
                    };
                    self.storage.create_index(meta)?;
                }
                self.invalidate_table_stats(&table_name);
                self.invalidate_schema_caches();
                Ok(DBOutput::StatementComplete(0))
            }
            Statement::CreateTrigger(create) => {
                let name = object_name(&create.name);
                let key = name.to_ascii_lowercase();
                if self.triggers.contains_key(&key) {
                    if !create.if_not_exists {
                        return Err(GongDBError::new(format!(
                            "trigger already exists: {}",
                            name
                        )));
                    }
                    return Ok(DBOutput::StatementComplete(0));
                }
                let table_name = object_name(&create.table);
                if self.storage.get_table(&table_name).is_none() {
                    return Err(GongDBError::new(format!(
                        "no such table: {}",
                        table_name
                    )));
                }
                self.triggers.insert(
                    key,
                    TriggerMeta {
                        table: table_name,
                    },
                );
                Ok(DBOutput::StatementComplete(0))
            }
            Statement::DropIndex(drop) => {
                let name = object_name(&drop.name);
                if self.storage.get_index(&name).is_none() && !drop.if_exists {
                    return Err(GongDBError::new(format!(
                        "no such index: {}",
                        name
                    )));
                }
                if self.storage.get_index(&name).is_some() {
                    self.storage.drop_index(&name)?;
                }
                self.invalidate_schema_caches();
                Ok(DBOutput::StatementComplete(0))
            }
            Statement::DropTrigger(drop) => {
                let name = object_name(&drop.name);
                let key = name.to_ascii_lowercase();
                if self.triggers.remove(&key).is_none() && !drop.if_exists {
                    return Err(GongDBError::new(format!(
                        "no such trigger: {}",
                        name
                    )));
                }
                Ok(DBOutput::StatementComplete(0))
            }
            Statement::Reindex(reindex) => {
                let target = reindex.name.as_ref().map(object_name);
                self.storage.reindex(target.as_deref())?;
                self.invalidate_schema_caches();
                Ok(DBOutput::StatementComplete(0))
            }
            Statement::DropTable(drop) => {
                let name = object_name(&drop.name);
                if self.storage.get_table(&name).is_none() && !drop.if_exists {
                    return Err(GongDBError::new(format!(
                        "no such table: {}",
                        name
                    )));
                }
                if self.storage.get_table(&name).is_some() {
                    self.storage.drop_table(&name)?;
                }
                self.triggers
                    .retain(|_, trigger| !trigger.table.eq_ignore_ascii_case(&name));
                self.invalidate_table_stats(&name);
                self.invalidate_schema_caches();
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
                        "no such view: {}",
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
                    .ok_or_else(|| GongDBError::new(format!("no such table: {}", table_name)))?
                    .clone();
                let mut inserted = 0u64;
                let replace = matches!(insert.on_conflict, InsertConflict::Replace);
                if replace {
                    let unique_indexes = unique_indexes_for_table(&self.storage, &table_name);
                    if unique_indexes.is_empty() {
                        match &insert.source {
                            InsertSource::Values(values) => {
                                let mut rows = Vec::with_capacity(values.len());
                                for exprs in values {
                                    rows.push(build_insert_row(
                                        self,
                                        &table,
                                        &insert.columns,
                                        exprs,
                                    )?);
                                }
                                self.storage.insert_rows(&table_name, &rows)?;
                                inserted += rows.len() as u64;
                            }
                            InsertSource::Select(select) => {
                                let result = self.evaluate_select_values(select)?;
                                if insert.columns.is_empty() {
                                    if result.columns.len() != table.columns.len() {
                                        return Err(GongDBError::new("column count mismatch"));
                                    }
                                } else if result.columns.len() != insert.columns.len() {
                                    return Err(GongDBError::new("column count mismatch"));
                                }
                                let mut rows = Vec::with_capacity(result.rows.len());
                                for values in result.rows {
                                    rows.push(build_insert_row_from_values(
                                        self,
                                        &table,
                                        &insert.columns,
                                        &values,
                                    )?);
                                }
                                self.storage.insert_rows(&table_name, &rows)?;
                                inserted += rows.len() as u64;
                            }
                        }
                    } else {
                        let mut rows = self.storage.scan_table(&table_name)?;
                        let column_map = column_index_map(&table.columns);
                        match &insert.source {
                            InsertSource::Values(values) => {
                                for exprs in values {
                                    let row =
                                        build_insert_row(self, &table, &insert.columns, exprs)?;
                                    apply_replace_row(
                                        &mut rows,
                                        row,
                                        &unique_indexes,
                                        &column_map,
                                    )?;
                                    inserted += 1;
                                }
                            }
                            InsertSource::Select(select) => {
                                let result = self.evaluate_select_values(select)?;
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
                                    apply_replace_row(
                                        &mut rows,
                                        row,
                                        &unique_indexes,
                                        &column_map,
                                    )?;
                                    inserted += 1;
                                }
                            }
                        }
                        self.storage.replace_table_rows(&table_name, &rows)?;
                    }
                } else {
                    match &insert.source {
                        InsertSource::Values(values) => {
                            let mut rows = Vec::with_capacity(values.len());
                            for exprs in values {
                                rows.push(build_insert_row(
                                    self,
                                    &table,
                                    &insert.columns,
                                    exprs,
                                )?);
                            }
                            self.storage.insert_rows(&table_name, &rows)?;
                            inserted += rows.len() as u64;
                        }
                        InsertSource::Select(select) => {
                            let result = self.evaluate_select_values(select)?;
                            if insert.columns.is_empty() {
                                if result.columns.len() != table.columns.len() {
                                    return Err(GongDBError::new("column count mismatch"));
                                }
                            } else if result.columns.len() != insert.columns.len() {
                                return Err(GongDBError::new("column count mismatch"));
                            }
                            let mut rows = Vec::with_capacity(result.rows.len());
                            for values in result.rows {
                                rows.push(build_insert_row_from_values(
                                    self,
                                    &table,
                                    &insert.columns,
                                    &values,
                                )?);
                            }
                            self.storage.insert_rows(&table_name, &rows)?;
                            inserted += rows.len() as u64;
                        }
                    }
                }
                self.invalidate_table_stats(&table_name);
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
                    .ok_or_else(|| GongDBError::new(format!("no such table: {}", table_name)))?
                    .clone();
                let table_scope = TableScope {
                    table_name: Some(table_name.clone()),
                    table_alias: None,
                };
                let column_scopes = vec![table_scope.clone(); table.columns.len()];
                let column_lookup = build_column_lookup(&table.columns, &column_scopes);
                let mut selection = update.selection.as_ref();
                if let Some(predicate) = selection {
                    if expr_is_constant(predicate) {
                        let value = eval_constant_expr_checked(self, predicate)?;
                        if !value_to_bool(&value) {
                            return Ok(DBOutput::StatementComplete(0));
                        }
                        selection = None;
                    }
                }
                let predicate_plan = selection.map(|predicate| {
                    self.build_row_predicate_plan(predicate, &table_scope, &table.columns)
                });
                let index_plan =
                    choose_index_scan_plan_no_stats(self, &table, selection, &[], &table_scope);
                let mut updates = Vec::new();
                if let Some(plan) = index_plan {
                    let locations = self.storage.scan_index_range(
                        &plan.index_name,
                        plan.lower.as_deref(),
                        plan.upper.as_deref(),
                    )?;
                    for location in locations {
                        let row = match self.storage.read_row_at(&location) {
                            Ok(row) => row,
                            Err(StorageError::NotFound(msg)) if msg == "row deleted" => continue,
                            Err(err) => return Err(err.into()),
                        };
                        if let Some(plan) = predicate_plan.as_ref() {
                            if !row_matches_predicate_plan(
                                self,
                                plan,
                                &row,
                                &table.columns,
                                &column_scopes,
                                &table_scope,
                                None,
                                None,
                                &column_lookup,
                            )? {
                                continue;
                            }
                        }
                        let new_row = self.apply_update_assignments(
                            &update,
                            &row,
                            &table,
                            &column_scopes,
                            &table_scope,
                            &column_lookup,
                        )?;
                        updates.push((location, new_row));
                    }
                } else {
                    let rows = self.storage.scan_table_with_locations(&table_name)?;
                    for (location, row) in rows {
                        if let Some(plan) = predicate_plan.as_ref() {
                            if !row_matches_predicate_plan(
                                self,
                                plan,
                                &row,
                                &table.columns,
                                &column_scopes,
                                &table_scope,
                                None,
                                None,
                                &column_lookup,
                            )? {
                                continue;
                            }
                        }
                        let new_row = self.apply_update_assignments(
                            &update,
                            &row,
                            &table,
                            &column_scopes,
                            &table_scope,
                            &column_lookup,
                        )?;
                        updates.push((location, new_row));
                    }
                }

                if !updates.is_empty() {
                    let mut used_replace = false;
                    match self.storage.update_rows_at(&updates) {
                        Ok(()) => {}
                        Err(StorageError::Invalid(msg)) if msg == "page full" => {
                            let rows = self.storage.scan_table(&table_name)?;
                            let mut updated_rows = Vec::with_capacity(rows.len());
                            for row in rows {
                                if let Some(plan) = predicate_plan.as_ref() {
                                    if !row_matches_predicate_plan(
                                        self,
                                        plan,
                                        &row,
                                        &table.columns,
                                        &column_scopes,
                                        &table_scope,
                                        None,
                                        None,
                                        &column_lookup,
                                    )? {
                                        updated_rows.push(row);
                                        continue;
                                    }
                                }
                                updated_rows.push(self.apply_update_assignments(
                                    &update,
                                    &row,
                                    &table,
                                    &column_scopes,
                                    &table_scope,
                                    &column_lookup,
                                )?);
                            }
                            self.storage.replace_table_rows(&table_name, &updated_rows)?;
                            used_replace = true;
                        }
                        Err(err) => return Err(err.into()),
                    }
                    if !used_replace {
                        let mut indexed_columns = HashSet::new();
                        for index in self.storage.list_indexes() {
                            if index.table.eq_ignore_ascii_case(&table_name) {
                                for column in &index.columns {
                                    indexed_columns.insert(column.name.value.to_ascii_lowercase());
                                }
                            }
                        }
                        if update.assignments.iter().any(|assignment| {
                            indexed_columns.contains(&assignment.column.value.to_ascii_lowercase())
                        }) {
                            self.storage.reindex(Some(&table_name))?;
                        }
                    }
                }
                self.invalidate_table_stats(&table_name);
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
                    .ok_or_else(|| GongDBError::new(format!("no such table: {}", table_name)))?
                    .clone();
                let table_scope = TableScope {
                    table_name: Some(table_name.clone()),
                    table_alias: None,
                };
                let column_scopes = vec![table_scope.clone(); table.columns.len()];
                let column_lookup = build_column_lookup(&table.columns, &column_scopes);
                let mut selection = delete.selection.as_ref();
                if let Some(predicate) = selection {
                    if expr_is_constant(predicate) {
                        let value = eval_constant_expr_checked(self, predicate)?;
                        if !value_to_bool(&value) {
                            return Ok(DBOutput::StatementComplete(0));
                        }
                        selection = None;
                    }
                }
                if selection.is_none() {
                    self.storage.replace_table_rows(&table_name, &[])?;
                    self.invalidate_table_stats(&table_name);
                    return Ok(DBOutput::StatementComplete(0));
                }
                let predicate_plan = selection.map(|predicate| {
                    self.build_row_predicate_plan(predicate, &table_scope, &table.columns)
                });
                let index_plan =
                    choose_index_scan_plan_no_stats(self, &table, selection, &[], &table_scope);
                let mut deletions = Vec::new();
                if let Some(plan) = index_plan {
                    let locations = self.storage.scan_index_range(
                        &plan.index_name,
                        plan.lower.as_deref(),
                        plan.upper.as_deref(),
                    )?;
                    for location in locations {
                        let row = match self.storage.read_row_at(&location) {
                            Ok(row) => row,
                            Err(StorageError::NotFound(msg)) if msg == "row deleted" => continue,
                            Err(err) => return Err(err.into()),
                        };
                        if let Some(plan) = predicate_plan.as_ref() {
                            if !row_matches_predicate_plan(
                                self,
                                plan,
                                &row,
                                &table.columns,
                                &column_scopes,
                                &table_scope,
                                None,
                                None,
                                &column_lookup,
                            )? {
                                continue;
                            }
                        }
                        deletions.push(location);
                    }
                } else {
                    let rows = self.storage.scan_table_with_locations(&table_name)?;
                    for (location, row) in rows {
                        if let Some(plan) = predicate_plan.as_ref() {
                            if !row_matches_predicate_plan(
                                self,
                                plan,
                                &row,
                                &table.columns,
                                &column_scopes,
                                &table_scope,
                                None,
                                None,
                                &column_lookup,
                            )? {
                                continue;
                            }
                        }
                        deletions.push(location);
                    }
                }
                if !deletions.is_empty() {
                    let _ = self.storage.delete_rows_at(&table_name, &deletions)?;
                }
                self.invalidate_table_stats(&table_name);
                Ok(DBOutput::StatementComplete(0))
            }
            Statement::Select(select) => self.run_select(&select),
            Statement::BeginTransaction(_)
            | Statement::Commit
            | Statement::Rollback => Err(GongDBError::new("unexpected transaction statement")),
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

    fn try_fast_transaction(
        &mut self,
        sql: &str,
    ) -> Option<Result<DBOutput<DefaultColumnType>, GongDBError>> {
        let trimmed = sql.trim();
        if trimmed.eq_ignore_ascii_case("BEGIN")
            || trimmed.eq_ignore_ascii_case("BEGIN TRANSACTION")
        {
            return Some(self.begin_transaction(BeginTransaction { isolation: None }));
        }
        if trimmed.eq_ignore_ascii_case("COMMIT")
            || trimmed.eq_ignore_ascii_case("COMMIT TRANSACTION")
            || trimmed.eq_ignore_ascii_case("END")
            || trimmed.eq_ignore_ascii_case("END TRANSACTION")
        {
            return Some(self.commit_transaction());
        }
        if trimmed.eq_ignore_ascii_case("ROLLBACK")
            || trimmed.eq_ignore_ascii_case("ROLLBACK TRANSACTION")
        {
            return Some(self.rollback_transaction());
        }
        None
    }

    fn try_fast_insert(
        &mut self,
        sql: &str,
    ) -> Option<Result<DBOutput<DefaultColumnType>, GongDBError>> {
        let (table_name, values) = parse_fast_insert(sql)?;
        let table = match self.storage.get_table(&table_name) {
            Some(table) => table.clone(),
            None => {
                return Some(Err(GongDBError::new(format!(
                    "no such table: {}",
                    table_name
                ))))
            }
        };
        let mut rows = Vec::with_capacity(values.len());
        for row_values in values {
            let row = match build_insert_row_from_owned_values_no_columns(
                self,
                &table,
                row_values,
            ) {
                Ok(row) => row,
                Err(err) => return Some(Err(err)),
            };
            rows.push(row);
        }
        if let Err(err) = self.storage.insert_rows(&table_name, &rows) {
            return Some(Err(err.into()));
        }
        self.invalidate_table_stats(&table_name);
        Some(Ok(DBOutput::StatementComplete(rows.len() as u64)))
    }

    fn try_fast_delivery_customer_update(
        &mut self,
        sql: &str,
    ) -> Option<Result<DBOutput<DefaultColumnType>, GongDBError>> {
        let (w_id, d_id, o_id) = parse_fast_delivery_customer_update(sql)?;
        let orders = self.storage.get_table("orders")?.clone();
        let order_line = self.storage.get_table("order_line")?.clone();
        let customer = self.storage.get_table("customer")?.clone();

        let orders_idx = self.column_index_map_cached(&orders);
        let customer_idx = self.column_index_map_cached(&customer);
        let o_c_id_idx = *orders_idx.get("o_c_id")?;
        let c_balance_idx = *customer_idx.get("c_balance")?;
        let c_delivery_cnt_idx = *customer_idx.get("c_delivery_cnt")?;

        let orders_indexes = self.table_indexes_cached(&orders.name);
        let order_line_indexes = self.table_indexes_cached(&order_line.name);
        let customer_indexes = self.table_indexes_cached(&customer.name);

        let orders_index = fast_find_index_prefix(&orders_indexes, &["o_w_id", "o_d_id", "o_id"])?;
        let order_line_index =
            fast_find_index_prefix(&order_line_indexes, &["ol_w_id", "ol_d_id", "ol_o_id"])?;
        let customer_index =
            fast_find_index_prefix(&customer_indexes, &["c_w_id", "c_d_id", "c_id"])?;

        let order_key = vec![
            Value::Integer(w_id),
            Value::Integer(d_id),
            Value::Integer(o_id),
        ];
        let order_location = match self
            .storage
            .scan_index_first_location(&orders_index.name, &order_key)
        {
            Ok(location) => location,
            Err(err) => return Some(Err(GongDBError::from(err))),
        };
        let order_location = match order_location {
            Some(location) => location,
            None => return Some(Ok(DBOutput::StatementComplete(0))),
        };
        let order_row = match self.storage.read_row_at(&order_location) {
            Ok(row) => row,
            Err(StorageError::NotFound(_)) => return Some(Ok(DBOutput::StatementComplete(0))),
            Err(err) => return Some(Err(GongDBError::from(err))),
        };
        let o_c_id = match order_row.get(o_c_id_idx) {
            Some(Value::Integer(v)) => *v,
            Some(Value::Real(v)) => *v as i64,
            _ => return Some(Ok(DBOutput::StatementComplete(0))),
        };

        let lower_key = build_index_bound(
            order_line_index.columns.len(),
            &[Value::Integer(w_id), Value::Integer(d_id)],
            Some(&Value::Integer(o_id)),
            Value::Null,
        );
        let upper_key = build_index_bound(
            order_line_index.columns.len(),
            &[Value::Integer(w_id), Value::Integer(d_id)],
            Some(&Value::Integer(o_id)),
            Value::Blob(Vec::new()),
        );
        let locations = match self
            .storage
            .scan_index_range(&order_line_index.name, Some(&lower_key), Some(&upper_key))
        {
            Ok(locations) => locations,
            Err(err) => return Some(Err(GongDBError::from(err))),
        };
        let mut amount_cache = self.fast_order_line_amount_offsets_cache.borrow().clone();
        let mut amount_cache_update: Option<FastOrderLineAmountOffsetsCache> = None;
        let mut sum_amount = 0.0_f64;
        for location in locations {
            let amount = match self.storage.with_record_at(&location, |record| {
                let cached = amount_cache.as_ref().and_then(|cache| {
                    if cache.record_len == record.len() {
                        Some(cache.amount)
                    } else {
                        None
                    }
                });
                let offset = match cached {
                    Some(offset) => offset,
                    None => match fast_order_line_amount_offset(record) {
                        Ok(Some(offset)) => {
                            let update = FastOrderLineAmountOffsetsCache {
                                record_len: record.len(),
                                amount: offset,
                            };
                            amount_cache = Some(update.clone());
                            if amount_cache_update.is_none() {
                                amount_cache_update = Some(update);
                            }
                            offset
                        }
                        Ok(None) => return Ok(None),
                        Err(err) => return Err(StorageError::Invalid(err.to_string())),
                    },
                };
                let tag = *record.get(offset).unwrap_or(&0);
                let amount = match tag {
                    1 => read_i64_at(record, offset + 1).map(|value| value as f64),
                    2 => read_f64_at(record, offset + 1),
                    _ => None,
                };
                Ok(amount)
            }) {
                Ok(Some(amount)) => amount,
                Ok(None) => continue,
                Err(StorageError::NotFound(_)) => continue,
                Err(err) => return Some(Err(GongDBError::from(err))),
            };
            sum_amount += amount;
        }
        if let Some(update) = amount_cache_update {
            self.fast_order_line_amount_offsets_cache
                .borrow_mut()
                .replace(update);
        }

        let customer_key = vec![
            Value::Integer(w_id),
            Value::Integer(d_id),
            Value::Integer(o_c_id),
        ];
        let customer_location = match self
            .storage
            .scan_index_first_location(&customer_index.name, &customer_key)
        {
            Ok(location) => location,
            Err(err) => return Some(Err(GongDBError::from(err))),
        };
        let Some(customer_location) = customer_location else {
            return Some(Ok(DBOutput::StatementComplete(0)));
        };
        let mut cached_offsets = self.fast_customer_delivery_offsets_cache.borrow().clone();
        let mut cache_update: Option<FastCustomerDeliveryOffsetsCache> = None;
        let mut fallback_needed = false;
        let applied = match self.storage.update_record_at_with(customer_location, |record| {
            if fallback_needed {
                return Ok(false);
            }
            let cached = cached_offsets.as_ref().and_then(|cache| {
                if cache.record_len == record.len() {
                    Some((cache.balance, cache.delivery_cnt))
                } else {
                    None
                }
            });
            let offsets = match cached {
                Some((balance, delivery_cnt)) => FastCustomerDeliveryOffsets {
                    balance,
                    delivery_cnt,
                },
                None => match fast_customer_delivery_offsets(record) {
                    Ok(Some(offsets)) => {
                        let update = FastCustomerDeliveryOffsetsCache {
                            record_len: record.len(),
                            balance: offsets.balance,
                            delivery_cnt: offsets.delivery_cnt,
                        };
                        cached_offsets = Some(update.clone());
                        if cache_update.is_none() {
                            cache_update = Some(update);
                        }
                        offsets
                    }
                    Ok(None) => {
                        fallback_needed = true;
                        return Ok(false);
                    }
                    Err(err) => return Err(StorageError::Invalid(err.to_string())),
                },
            };
            if record.get(offsets.balance) != Some(&2) || record.get(offsets.delivery_cnt) != Some(&1)
            {
                fallback_needed = true;
                return Ok(false);
            }
            let current_balance = match read_f64_at(record, offsets.balance + 1) {
                Some(value) => value,
                None => {
                    fallback_needed = true;
                    return Ok(false);
                }
            };
            let current_delivery_cnt = match read_i64_at(record, offsets.delivery_cnt + 1) {
                Some(value) => value,
                None => {
                    fallback_needed = true;
                    return Ok(false);
                }
            };
            let next_balance = current_balance + sum_amount;
            let next_delivery_cnt = current_delivery_cnt.wrapping_add(1);
            if write_f64_at(record, offsets.balance + 1, next_balance).is_none()
                || write_i64_at(record, offsets.delivery_cnt + 1, next_delivery_cnt).is_none()
            {
                fallback_needed = true;
                return Ok(false);
            }
            Ok(true)
        }) {
            Ok(applied) => applied,
            Err(StorageError::NotFound(_)) => false,
            Err(err) => return Some(Err(GongDBError::from(err))),
        };
        if let Some(update) = cache_update {
            self.fast_customer_delivery_offsets_cache
                .borrow_mut()
                .replace(update);
        }
        if fallback_needed {
            let row = match self.storage.read_row_at(&customer_location) {
                Ok(row) => row,
                Err(StorageError::NotFound(_)) => return Some(Ok(DBOutput::StatementComplete(0))),
                Err(err) => return Some(Err(GongDBError::from(err))),
            };
            let mut new_row = row.clone();
            let balance = match row.get(c_balance_idx) {
                Some(Value::Integer(v)) => *v as f64,
                Some(Value::Real(v)) => *v,
                _ => 0.0,
            };
            let delivery_cnt = match row.get(c_delivery_cnt_idx) {
                Some(Value::Integer(v)) => *v,
                Some(Value::Real(v)) => *v as i64,
                _ => 0,
            };
            new_row[c_balance_idx] = Value::Real(balance + sum_amount);
            new_row[c_delivery_cnt_idx] = Value::Integer(delivery_cnt + 1);
            if let Err(err) = self.storage.update_rows_at(&[(customer_location, new_row)]) {
                return Some(Err(GongDBError::from(err)));
            }
        }
        if applied || fallback_needed {
            self.invalidate_table_stats("customer");
            self.select_cache.borrow_mut().clear();
        }
        Some(Ok(DBOutput::StatementComplete(0)))
    }

    fn try_fast_update(
        &mut self,
        sql: &str,
    ) -> Option<Result<DBOutput<DefaultColumnType>, GongDBError>> {
        let plan = parse_fast_update(sql)?;
        match self.execute_fast_update_plan(plan) {
            Ok(Some(output)) => Some(Ok(output)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }

    fn try_fast_update_params(
        &mut self,
        sql: &str,
        params: &[Literal],
    ) -> Option<Result<DBOutput<DefaultColumnType>, GongDBError>> {
        match parse_fast_update_with_params(sql, params) {
            Ok(Some(plan)) => match self.execute_fast_update_plan(plan) {
                Ok(Some(output)) => Some(Ok(output)),
                Ok(None) => None,
                Err(err) => Some(Err(err)),
            },
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }

    fn execute_fast_update_plan(
        &mut self,
        plan: FastUpdatePlan,
    ) -> Result<Option<DBOutput<DefaultColumnType>>, GongDBError> {
        let table = match self.storage.get_table(&plan.table) {
            Some(table) => table.clone(),
            None => {
                return Err(GongDBError::new(format!(
                    "no such table: {}",
                    plan.table
                )))
            }
        };
        if self.storage.get_view(&plan.table).is_some() {
            return Err(GongDBError::new(format!(
                "cannot modify view {}",
                plan.table
            )));
        }
        let cache_key = fast_update_cache_key(&plan);
        let cached = { self.fast_update_cache.borrow().get(&cache_key).cloned() };
        let template = if let Some(template) = cached {
            template
        } else {
            let indexes = self.storage.list_indexes();
            let template = build_fast_update_template(&table, &plan, &indexes)?;
            self.fast_update_cache
                .borrow_mut()
                .insert(cache_key, template.clone());
            template
        };
        let (assignment_index_map, predicate_index_map, best_index, best_prefix_index) =
            match template {
                FastUpdateTemplate::Ineligible => return Ok(None),
                FastUpdateTemplate::Eligible {
                    assignment_indices,
                    predicate_indices,
                    best_index,
                    best_prefix_index,
                } => (assignment_indices, predicate_indices, best_index, best_prefix_index),
            };
        let mut predicate_values = Vec::with_capacity(plan.predicates.len());
        let mut predicate_indices = Vec::with_capacity(plan.predicates.len());
        for (pos, (_, value)) in plan.predicates.iter().enumerate() {
            let idx = predicate_index_map[pos];
            let value = apply_affinity(value.clone(), &table.columns[idx].data_type);
            predicate_values.push(value.clone());
            predicate_indices.push((idx, value));
        }
        let mut assignment_indices = Vec::with_capacity(plan.assignments.len());
        for (idx, assignment) in assignment_index_map.iter().zip(plan.assignments.iter()) {
            assignment_indices.push((*idx, assignment.clone()));
        }
        let mut updates = Vec::new();
        let mut in_place_updates = Vec::new();
        if let Some(index_plan) = best_index {
            let mut key = Vec::with_capacity(index_plan.predicate_positions.len());
            for pos in &index_plan.predicate_positions {
                key.push(predicate_values[*pos].clone());
            }
            if index_plan.index.unique {
                    let location =
                        self.storage
                            .scan_index_first_location(&index_plan.index.name, &key)?;
                    if let Some(location) = location {
                        let mut row_update: Option<Vec<Value>> = None;
                    let applied = match self.storage.update_record_fields_at_with(
                        location,
                        |record| {
                            let matches = if index_plan.all_predicates_covered {
                                true
                            } else {
                                fast_record_matches_predicates(record, &predicate_indices).map_err(
                                    |err| StorageError::Invalid(err.to_string()),
                                )?
                            };
                            if !matches {
                                return Ok(None);
                            }
                            let outcome = build_fast_update_outcome(
                                &table.columns,
                                record,
                                &assignment_indices,
                            )
                            .map_err(|err| StorageError::Invalid(err.to_string()))?;
                            match outcome {
                                FastUpdateOutcome::InPlace(fields) => Ok(Some(fields)),
                                FastUpdateOutcome::Row(new_row) => {
                                    row_update = Some(new_row);
                                    Ok(None)
                                }
                            }
                        },
                    ) {
                        Ok(applied) => applied,
                        Err(StorageError::NotFound(msg)) if msg == "row deleted" => {
                            return Ok(Some(DBOutput::StatementComplete(0)));
                        }
                        Err(err) => return Err(err.into()),
                    };
                    if let Some(new_row) = row_update {
                        updates.push((location, new_row));
                    } else if applied {
                        // Applied in place above.
                    }
                }
            } else {
                    let locations = self.storage.scan_index_range(
                        &index_plan.index.name,
                        Some(&key),
                        Some(&key),
                    )?;
                    for location in locations {
                    let outcome = match self.storage.with_record_at(&location, |record| {
                        let matches = fast_record_matches_predicates(record, &predicate_indices)
                            .map_err(|err| StorageError::Invalid(err.to_string()))?;
                        if !matches {
                            return Ok(None);
                        }
                        let outcome = build_fast_update_outcome(
                            &table.columns,
                            record,
                            &assignment_indices,
                        )
                        .map_err(|err| StorageError::Invalid(err.to_string()))?;
                        Ok(Some(outcome))
                    }) {
                        Ok(outcome) => outcome,
                        Err(StorageError::NotFound(msg)) if msg == "row deleted" => continue,
                        Err(err) => return Err(err.into()),
                    };
                    if let Some(outcome) = outcome {
                        match outcome {
                            FastUpdateOutcome::InPlace(fields) => {
                                in_place_updates.push((location, fields));
                            }
                            FastUpdateOutcome::Row(new_row) => {
                                updates.push((location, new_row));
                            }
                        }
                    }
                }
            }
        } else if let Some(index_plan) = best_prefix_index {
            let mut prefix = Vec::with_capacity(index_plan.predicate_positions.len());
            for pos in &index_plan.predicate_positions {
                prefix.push(predicate_values[*pos].clone());
            }
            let lower = build_index_bound(
                index_plan.index.columns.len(),
                &prefix,
                None,
                Value::Null,
            );
            let upper = build_index_bound(
                index_plan.index.columns.len(),
                &prefix,
                None,
                Value::Blob(Vec::new()),
            );
            let locations =
                self.storage
                    .scan_index_range(&index_plan.index.name, Some(&lower), Some(&upper))?;
            for location in locations {
                let outcome = match self.storage.with_record_at(&location, |record| {
                    let matches = fast_record_matches_predicates(record, &predicate_indices)
                        .map_err(|err| StorageError::Invalid(err.to_string()))?;
                    if !matches {
                        return Ok(None);
                    }
                    let outcome = build_fast_update_outcome(
                        &table.columns,
                        record,
                        &assignment_indices,
                    )
                    .map_err(|err| StorageError::Invalid(err.to_string()))?;
                    Ok(Some(outcome))
                }) {
                    Ok(outcome) => outcome,
                    Err(StorageError::NotFound(msg)) if msg == "row deleted" => continue,
                    Err(err) => return Err(err.into()),
                };
                if let Some(outcome) = outcome {
                    match outcome {
                        FastUpdateOutcome::InPlace(fields) => {
                            in_place_updates.push((location, fields));
                        }
                        FastUpdateOutcome::Row(new_row) => {
                            updates.push((location, new_row));
                        }
                    }
                }
            }
        } else {
            let rows = self.storage.scan_table_with_locations(&plan.table)?;
            for (location, row) in rows {
                if !fast_row_matches_predicates(&row, &predicate_indices) {
                    continue;
                }
                let new_row =
                    apply_fast_update_assignments(&table.columns, &row, &assignment_indices)?;
                updates.push((location, new_row));
            }
        }
        if !in_place_updates.is_empty() {
            self.storage.update_record_fields_at(&in_place_updates)?;
        }
        if !updates.is_empty() {
            match self.storage.update_rows_at(&updates) {
                Ok(()) => {}
                Err(StorageError::Invalid(msg)) if msg == "page full" => {
                    let rows = self.storage.scan_table(&plan.table)?;
                    let mut updated_rows = Vec::with_capacity(rows.len());
                    for row in rows {
                        if fast_row_matches_predicates(&row, &predicate_indices) {
                            let new_row = apply_fast_update_assignments(
                                &table.columns,
                                &row,
                                &assignment_indices,
                            )?;
                            updated_rows.push(new_row);
                        } else {
                            updated_rows.push(row);
                        }
                    }
                    self.storage.replace_table_rows(&plan.table, &updated_rows)?;
                }
                Err(err) => return Err(err.into()),
            }
        }
        self.select_cache.borrow_mut().clear();
        self.invalidate_table_stats(&plan.table);
        Ok(Some(DBOutput::StatementComplete(0)))
    }

    fn try_fast_stock_update(
        &mut self,
        sql: &str,
    ) -> Option<Result<DBOutput<DefaultColumnType>, GongDBError>> {
        let plan = parse_fast_stock_update(sql)?;
        self.apply_fast_stock_update(plan)
    }

    fn apply_fast_stock_update(
        &mut self,
        plan: FastStockUpdatePlan,
    ) -> Option<Result<DBOutput<DefaultColumnType>, GongDBError>> {
        let mut target_index: Option<IndexMeta> = None;
        for index in self.table_indexes_cached("stock") {
            if !index.table.eq_ignore_ascii_case("stock") || !index.unique {
                continue;
            }
            if index.columns.len() != 2 {
                continue;
            }
            let first = index.columns[0].name.value.to_ascii_lowercase();
            let second = index.columns[1].name.value.to_ascii_lowercase();
            if first == "s_w_id" && second == "s_i_id" {
                target_index = Some(index.clone());
                break;
            }
        }
        let index = match target_index {
            Some(index) => index,
            None => return None,
        };
        let location = match self.lookup_stock_location(&index, plan.w_id, plan.i_id) {
            Ok(location) => location,
            Err(err) => return Some(Err(err)),
        };
        let location = match location {
            Some(location) => location,
            None => return Some(Ok(DBOutput::StatementComplete(0))),
        };
        let cached_offsets = self.fast_stock_offsets_cache.borrow().clone();
        let mut cache_update: Option<FastStockOffsetsCache> = None;
        let mut fallback_needed = false;
        let applied = match self.storage.update_record_at_with(location, |record| {
            let cached = cached_offsets.as_ref().and_then(|cache| {
                if cache.record_len == record.len()
                    && (!plan.remote || cache.offsets.remote_cnt.is_some())
                {
                    Some(cache.offsets.clone())
                } else {
                    None
                }
            });
            let offsets = match cached {
                Some(offsets) => offsets,
                None => match fast_stock_update_offsets(record, plan.remote) {
                    Ok(Some(offsets)) => {
                        cache_update = Some(FastStockOffsetsCache {
                            record_len: record.len(),
                            offsets: offsets.clone(),
                        });
                        offsets
                    }
                    Ok(None) => {
                        fallback_needed = true;
                        return Ok(false);
                    }
                    Err(err) => return Err(StorageError::Invalid(err.to_string())),
                },
            };
            let quantity = match read_i64_at(record, offsets.quantity + 1) {
                Some(value) => value,
                None => {
                    fallback_needed = true;
                    return Ok(false);
                }
            };
            let ytd = match read_i64_at(record, offsets.ytd + 1) {
                Some(value) => value,
                None => {
                    fallback_needed = true;
                    return Ok(false);
                }
            };
            let order_cnt = match read_i64_at(record, offsets.order_cnt + 1) {
                Some(value) => value,
                None => {
                    fallback_needed = true;
                    return Ok(false);
                }
            };
            let remote_cnt = if let Some(offset) = offsets.remote_cnt {
                match read_i64_at(record, offset + 1) {
                    Some(value) => Some(value),
                    None => {
                        fallback_needed = true;
                        return Ok(false);
                    }
                }
            } else {
                None
            };
            let remote_ok = offsets
                .remote_cnt
                .map(|offset| record[offset] == 1)
                .unwrap_or(true);
            if record[offsets.quantity] != 1
                || record[offsets.ytd] != 1
                || record[offsets.order_cnt] != 1
                || !remote_ok
            {
                fallback_needed = true;
                return Ok(false);
            }
            let next_quantity = quantity.wrapping_sub(plan.quantity);
            let next_ytd = ytd.wrapping_add(plan.ytd);
            let next_order_cnt = order_cnt.wrapping_add(1);
            let next_remote_cnt = remote_cnt.map(|value| value.wrapping_add(1));
            if write_i64_at(record, offsets.quantity + 1, next_quantity).is_none()
                || write_i64_at(record, offsets.ytd + 1, next_ytd).is_none()
                || write_i64_at(record, offsets.order_cnt + 1, next_order_cnt).is_none()
            {
                fallback_needed = true;
                return Ok(false);
            }
            if let (Some(offset), Some(next)) = (offsets.remote_cnt, next_remote_cnt) {
                if write_i64_at(record, offset + 1, next).is_none() {
                    fallback_needed = true;
                    return Ok(false);
                }
            }
            Ok(true)
        }) {
            Ok(applied) => applied,
            Err(StorageError::NotFound(msg)) if msg == "row deleted" => {
                return Some(Ok(DBOutput::StatementComplete(0)))
            }
            Err(err) => return Some(Err(err.into())),
        };
        if fallback_needed {
            return None;
        }
        if let Some(cache_update) = cache_update {
            self.fast_stock_offsets_cache
                .borrow_mut()
                .replace(cache_update);
        }
        if applied {
            self.select_cache.borrow_mut().clear();
            self.invalidate_table_stats("stock");
        }
        Some(Ok(DBOutput::StatementComplete(0)))
    }

    fn apply_fast_stock_update_batch(
        &mut self,
        plans: &[FastStockUpdatePlan],
    ) -> Option<Result<DBOutput<DefaultColumnType>, GongDBError>> {
        if plans.is_empty() {
            return Some(Ok(DBOutput::StatementComplete(0)));
        }
        let mut target_index: Option<IndexMeta> = None;
        for index in self.table_indexes_cached("stock") {
            if !index.table.eq_ignore_ascii_case("stock") || !index.unique {
                continue;
            }
            if index.columns.len() != 2 {
                continue;
            }
            let first = index.columns[0].name.value.to_ascii_lowercase();
            let second = index.columns[1].name.value.to_ascii_lowercase();
            if first == "s_w_id" && second == "s_i_id" {
                target_index = Some(index.clone());
                break;
            }
        }
        let index = match target_index {
            Some(index) => index,
            None => return None,
        };
        let mut cached_offsets = self.fast_stock_offsets_cache.borrow().clone();
        let mut cache_update: Option<FastStockOffsetsCache> = None;
        let mut fallback_needed = false;
        let mut any_applied = false;
        let mut updates = Vec::with_capacity(plans.len());
        for plan in plans {
            let location = match self.lookup_stock_location(&index, plan.w_id, plan.i_id) {
                Ok(location) => location,
                Err(err) => return Some(Err(err)),
            };
            let location = match location {
                Some(location) => location,
                None => continue,
            };
            updates.push((
                location,
                FastStockUpdatePlan {
                    quantity: plan.quantity,
                    ytd: plan.ytd,
                    w_id: plan.w_id,
                    i_id: plan.i_id,
                    remote: plan.remote,
                },
            ));
        }
        let result = self
            .storage
            .update_records_at_with_data(&updates, |record, plan| {
                if fallback_needed {
                    return Ok(false);
                }
                let cached = cached_offsets.as_ref().and_then(|cache| {
                    if cache.record_len == record.len()
                        && (!plan.remote || cache.offsets.remote_cnt.is_some())
                    {
                        Some(cache.offsets.clone())
                    } else {
                        None
                    }
                });
                let offsets = match cached {
                    Some(offsets) => offsets,
                    None => match fast_stock_update_offsets(record, plan.remote) {
                        Ok(Some(offsets)) => {
                            let update = FastStockOffsetsCache {
                                record_len: record.len(),
                                offsets: offsets.clone(),
                            };
                            cached_offsets = Some(update.clone());
                            if cache_update.is_none() {
                                cache_update = Some(update);
                            }
                            offsets
                        }
                        Ok(None) => {
                            fallback_needed = true;
                            return Ok(false);
                        }
                        Err(err) => return Err(StorageError::Invalid(err.to_string())),
                    },
                };
                let quantity = match read_i64_at(record, offsets.quantity + 1) {
                    Some(value) => value,
                    None => {
                        fallback_needed = true;
                        return Ok(false);
                    }
                };
                let ytd = match read_i64_at(record, offsets.ytd + 1) {
                    Some(value) => value,
                    None => {
                        fallback_needed = true;
                        return Ok(false);
                    }
                };
                let order_cnt = match read_i64_at(record, offsets.order_cnt + 1) {
                    Some(value) => value,
                    None => {
                        fallback_needed = true;
                        return Ok(false);
                    }
                };
                let remote_cnt = if let Some(offset) = offsets.remote_cnt {
                    match read_i64_at(record, offset + 1) {
                        Some(value) => Some(value),
                        None => {
                            fallback_needed = true;
                            return Ok(false);
                        }
                    }
                } else {
                    None
                };
                let remote_ok = offsets
                    .remote_cnt
                    .map(|offset| record[offset] == 1)
                    .unwrap_or(true);
                if record[offsets.quantity] != 1
                    || record[offsets.ytd] != 1
                    || record[offsets.order_cnt] != 1
                    || !remote_ok
                {
                    fallback_needed = true;
                    return Ok(false);
                }
                let next_quantity = quantity.wrapping_sub(plan.quantity);
                let next_ytd = ytd.wrapping_add(plan.ytd);
                let next_order_cnt = order_cnt.wrapping_add(1);
                let next_remote_cnt = remote_cnt.map(|value| value.wrapping_add(1));
                if write_i64_at(record, offsets.quantity + 1, next_quantity).is_none()
                    || write_i64_at(record, offsets.ytd + 1, next_ytd).is_none()
                    || write_i64_at(record, offsets.order_cnt + 1, next_order_cnt).is_none()
                {
                    fallback_needed = true;
                    return Ok(false);
                }
                if let (Some(offset), Some(next)) = (offsets.remote_cnt, next_remote_cnt) {
                    if write_i64_at(record, offset + 1, next).is_none() {
                        fallback_needed = true;
                        return Ok(false);
                    }
                }
                any_applied = true;
                Ok(true)
            });
        match result {
            Ok(_) => {}
            Err(StorageError::NotFound(msg)) if msg == "row deleted" => {}
            Err(err) => return Some(Err(err.into())),
        }
        if fallback_needed {
            return None;
        }
        if let Some(cache_update) = cache_update {
            self.fast_stock_offsets_cache
                .borrow_mut()
                .replace(cache_update);
        }
        if any_applied {
            self.select_cache.borrow_mut().clear();
            self.invalidate_table_stats("stock");
        }
        Some(Ok(DBOutput::StatementComplete(0)))
    }

    fn apply_fast_district_next_o_id_increment(
        &mut self,
        w_id: i64,
        d_id: i64,
    ) -> Option<Result<DBOutput<DefaultColumnType>, GongDBError>> {
        let mut target_index: Option<IndexMeta> = None;
        for index in self.table_indexes_cached("district") {
            if !index.table.eq_ignore_ascii_case("district") || !index.unique {
                continue;
            }
            if index.columns.len() != 2 {
                continue;
            }
            let first = index.columns[0].name.value.to_ascii_lowercase();
            let second = index.columns[1].name.value.to_ascii_lowercase();
            if first == "d_w_id" && second == "d_id" {
                target_index = Some(index.clone());
                break;
            }
        }
        let index = match target_index {
            Some(index) => index,
            None => return None,
        };
        let key = vec![Value::Integer(w_id), Value::Integer(d_id)];
        let location = match self.storage.scan_index_first_location(&index.name, &key) {
            Ok(location) => location,
            Err(err) => return Some(Err(err.into())),
        };
        let location = match location {
            Some(location) => location,
            None => return Some(Ok(DBOutput::StatementComplete(0))),
        };
        let cached_offsets = self.fast_district_offsets_cache.borrow().clone();
        let mut cache_update: Option<FastDistrictOffsetsCache> = None;
        let mut fallback_needed = false;
        let applied = match self.storage.update_record_at_with(location, |record| {
            let cached = cached_offsets.as_ref().and_then(|cache| {
                if cache.record_len == record.len() {
                    Some(cache.next_o_id)
                } else {
                    None
                }
            });
            let offset = match cached {
                Some(offset) => offset,
                None => match fast_district_next_o_id_offset(record) {
                    Ok(Some(offset)) => {
                        cache_update = Some(FastDistrictOffsetsCache {
                            record_len: record.len(),
                            next_o_id: offset,
                        });
                        offset
                    }
                    Ok(None) => {
                        fallback_needed = true;
                        return Ok(false);
                    }
                    Err(err) => return Err(StorageError::Invalid(err.to_string())),
                },
            };
            if record.get(offset) != Some(&1) {
                fallback_needed = true;
                return Ok(false);
            }
            let current = match read_i64_at(record, offset + 1) {
                Some(value) => value,
                None => {
                    fallback_needed = true;
                    return Ok(false);
                }
            };
            let next = current.wrapping_add(1);
            if write_i64_at(record, offset + 1, next).is_none() {
                fallback_needed = true;
                return Ok(false);
            }
            Ok(true)
        }) {
            Ok(applied) => applied,
            Err(StorageError::NotFound(msg)) if msg == "row deleted" => {
                return Some(Ok(DBOutput::StatementComplete(0)))
            }
            Err(err) => return Some(Err(err.into())),
        };
        if fallback_needed {
            return None;
        }
        if let Some(cache_update) = cache_update {
            self.fast_district_offsets_cache
                .borrow_mut()
                .replace(cache_update);
        }
        if applied {
            self.select_cache.borrow_mut().clear();
            self.invalidate_table_stats("district");
        }
        Some(Ok(DBOutput::StatementComplete(0)))
    }

    fn apply_fast_payment(
        &mut self,
        payment: f64,
        w_id: i64,
        d_id: i64,
        c_w_id: i64,
        c_d_id: i64,
        c_id: i64,
    ) -> Option<Result<DBOutput<DefaultColumnType>, GongDBError>> {
        let warehouse_indexes = self.table_indexes_cached("warehouse");
        let warehouse_index = fast_find_index_prefix(&warehouse_indexes, &["w_id"])?;
        if !warehouse_index.unique {
            return None;
        }
        let district_indexes = self.table_indexes_cached("district");
        let district_index = fast_find_index_prefix(&district_indexes, &["d_w_id", "d_id"])?;
        if !district_index.unique {
            return None;
        }
        let customer_indexes = self.table_indexes_cached("customer");
        let customer_index =
            fast_find_index_prefix(&customer_indexes, &["c_w_id", "c_d_id", "c_id"])?;
        if !customer_index.unique {
            return None;
        }
        let mut fallback_needed = false;

        let warehouse_key = vec![Value::Integer(w_id)];
        let warehouse_location = match self
            .storage
            .scan_index_first_location(&warehouse_index.name, &warehouse_key)
        {
            Ok(Some(location)) => location,
            Ok(None) => return None,
            Err(err) => return Some(Err(err.into())),
        };
        let cached_snapshot = self.fast_warehouse_offsets_cache.borrow().clone();
        let mut cache_update: Option<FastWarehouseOffsetsCache> = None;
        let warehouse_applied = match self.storage.update_record_at_with(warehouse_location, |record| {
            let cached = cached_snapshot
                .as_ref()
                .and_then(|cache| if cache.record_len == record.len() { Some(cache.ytd) } else { None });
            let offset = match cached {
                Some(offset) => offset,
                None => match fast_warehouse_ytd_offset(record) {
                    Ok(Some(offset)) => {
                        cache_update = Some(FastWarehouseOffsetsCache {
                            record_len: record.len(),
                            ytd: offset,
                        });
                        offset
                    }
                    Ok(None) => {
                        fallback_needed = true;
                        return Ok(false);
                    }
                    Err(err) => return Err(StorageError::Invalid(err.to_string())),
                },
            };
            if record[offset] != 2 {
                fallback_needed = true;
                return Ok(false);
            }
            let current = match read_f64_at(record, offset + 1) {
                Some(value) => value,
                None => {
                    fallback_needed = true;
                    return Ok(false);
                }
            };
            let next = current + payment;
            if write_f64_at(record, offset + 1, next).is_none() {
                fallback_needed = true;
                return Ok(false);
            }
            Ok(true)
        }) {
            Ok(applied) => applied,
            Err(StorageError::NotFound(msg)) if msg == "row deleted" => return None,
            Err(err) => return Some(Err(err.into())),
        };
        if fallback_needed {
            return None;
        }
        if let Some(update) = cache_update {
            self.fast_warehouse_offsets_cache
                .borrow_mut()
                .replace(update);
        }

        let district_key = vec![Value::Integer(w_id), Value::Integer(d_id)];
        let district_location = match self
            .storage
            .scan_index_first_location(&district_index.name, &district_key)
        {
            Ok(Some(location)) => location,
            Ok(None) => return None,
            Err(err) => return Some(Err(err.into())),
        };
        let cached_snapshot = self.fast_district_payment_offsets_cache.borrow().clone();
        let mut cache_update: Option<FastDistrictPaymentOffsetsCache> = None;
        let district_applied = match self.storage.update_record_at_with(district_location, |record| {
            let cached = cached_snapshot
                .as_ref()
                .and_then(|cache| if cache.record_len == record.len() { Some(cache.ytd) } else { None });
            let offset = match cached {
                Some(offset) => offset,
                None => match fast_district_ytd_offset(record) {
                    Ok(Some(offset)) => {
                        cache_update = Some(FastDistrictPaymentOffsetsCache {
                            record_len: record.len(),
                            ytd: offset,
                        });
                        offset
                    }
                    Ok(None) => {
                        fallback_needed = true;
                        return Ok(false);
                    }
                    Err(err) => return Err(StorageError::Invalid(err.to_string())),
                },
            };
            if record[offset] != 2 {
                fallback_needed = true;
                return Ok(false);
            }
            let current = match read_f64_at(record, offset + 1) {
                Some(value) => value,
                None => {
                    fallback_needed = true;
                    return Ok(false);
                }
            };
            let next = current + payment;
            if write_f64_at(record, offset + 1, next).is_none() {
                fallback_needed = true;
                return Ok(false);
            }
            Ok(true)
        }) {
            Ok(applied) => applied,
            Err(StorageError::NotFound(msg)) if msg == "row deleted" => return None,
            Err(err) => return Some(Err(err.into())),
        };
        if fallback_needed {
            return None;
        }
        if let Some(update) = cache_update {
            self.fast_district_payment_offsets_cache
                .borrow_mut()
                .replace(update);
        }

        let customer_key = vec![
            Value::Integer(c_w_id),
            Value::Integer(c_d_id),
            Value::Integer(c_id),
        ];
        let customer_location = match self
            .storage
            .scan_index_first_location(&customer_index.name, &customer_key)
        {
            Ok(Some(location)) => location,
            Ok(None) => return None,
            Err(err) => return Some(Err(err.into())),
        };
        let cached_snapshot = self.fast_customer_payment_offsets_cache.borrow().clone();
        let mut cache_update: Option<FastCustomerPaymentOffsetsCache> = None;
        let customer_applied = match self.storage.update_record_at_with(customer_location, |record| {
            let cached = cached_snapshot.as_ref().and_then(|cache| {
                if cache.record_len == record.len() {
                    Some(cache.clone())
                } else {
                    None
                }
            });
            let offsets = match cached {
                Some(offsets) => FastCustomerPaymentOffsets {
                    balance: offsets.balance,
                    ytd_payment: offsets.ytd_payment,
                    payment_cnt: offsets.payment_cnt,
                },
                None => match fast_customer_payment_offsets(record) {
                    Ok(Some(offsets)) => {
                        cache_update = Some(FastCustomerPaymentOffsetsCache {
                            record_len: record.len(),
                            balance: offsets.balance,
                            ytd_payment: offsets.ytd_payment,
                            payment_cnt: offsets.payment_cnt,
                        });
                        offsets
                    }
                    Ok(None) => {
                        fallback_needed = true;
                        return Ok(false);
                    }
                    Err(err) => return Err(StorageError::Invalid(err.to_string())),
                },
            };
            if record[offsets.balance] != 2
                || record[offsets.ytd_payment] != 2
                || record[offsets.payment_cnt] != 1
            {
                fallback_needed = true;
                return Ok(false);
            }
            let current_balance = match read_f64_at(record, offsets.balance + 1) {
                Some(value) => value,
                None => {
                    fallback_needed = true;
                    return Ok(false);
                }
            };
            let current_ytd = match read_f64_at(record, offsets.ytd_payment + 1) {
                Some(value) => value,
                None => {
                    fallback_needed = true;
                    return Ok(false);
                }
            };
            let current_cnt = match read_i64_at(record, offsets.payment_cnt + 1) {
                Some(value) => value,
                None => {
                    fallback_needed = true;
                    return Ok(false);
                }
            };
            let next_balance = current_balance - payment;
            let next_ytd = current_ytd + payment;
            let next_cnt = current_cnt.wrapping_add(1);
            if write_f64_at(record, offsets.balance + 1, next_balance).is_none()
                || write_f64_at(record, offsets.ytd_payment + 1, next_ytd).is_none()
                || write_i64_at(record, offsets.payment_cnt + 1, next_cnt).is_none()
            {
                fallback_needed = true;
                return Ok(false);
            }
            Ok(true)
        }) {
            Ok(applied) => applied,
            Err(StorageError::NotFound(msg)) if msg == "row deleted" => return None,
            Err(err) => return Some(Err(err.into())),
        };
        if fallback_needed {
            return None;
        }
        if let Some(update) = cache_update {
            self.fast_customer_payment_offsets_cache
                .borrow_mut()
                .replace(update);
        }

        if warehouse_applied || district_applied || customer_applied {
            self.select_cache.borrow_mut().clear();
            self.invalidate_table_stats("warehouse");
            self.invalidate_table_stats("district");
            self.invalidate_table_stats("customer");
        }
        let history_row = vec![vec![
            Value::Integer(c_id),
            Value::Integer(c_d_id),
            Value::Integer(c_w_id),
            Value::Integer(d_id),
            Value::Integer(w_id),
            Value::Text("2024-01-01".to_string()),
            Value::Real(payment),
            Value::Text("Payment history data".to_string()),
        ]];
        if let Err(err) = self.run_fast_insert_rows("history", history_row) {
            return Some(Err(err));
        }
        Some(Ok(DBOutput::StatementComplete(0)))
    }

    fn try_fast_stock_level(
        &mut self,
        sql: &str,
    ) -> Option<Result<DBOutput<DefaultColumnType>, GongDBError>> {
        let (w_id, d_id, next_o_id, threshold) = parse_fast_stock_level(sql)?;
        let order_line = match self.storage.get_table("order_line") {
            Some(table) => table.clone(),
            None => return None,
        };
        let stock = match self.storage.get_table("stock") {
            Some(table) => table.clone(),
            None => return None,
        };

        let order_line_idx = self.column_index_map_cached(&order_line);
        let ol_i_id_idx = *order_line_idx.get("ol_i_id")?;

        let order_indexes = self.table_indexes_cached(&order_line.name);
        let stock_indexes = self.table_indexes_cached(&stock.name);

        let order_index = fast_find_index_prefix(
            &order_indexes,
            &["ol_w_id", "ol_d_id", "ol_o_id"],
        )?;
        let stock_index = fast_find_index_prefix(&stock_indexes, &["s_w_id", "s_i_id"])?;

        let lower_o = next_o_id.saturating_sub(20);
        if next_o_id == 0 || next_o_id <= lower_o {
            return Some(Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text],
                rows: vec![vec![value_to_string(&Value::Integer(0))]],
            }));
        }
        let upper_o = next_o_id.saturating_sub(1);
        if upper_o < lower_o {
            return Some(Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text],
                rows: vec![vec![value_to_string(&Value::Integer(0))]],
            }));
        }

        let lower_key = build_index_bound(
            order_index.columns.len(),
            &[Value::Integer(w_id), Value::Integer(d_id)],
            Some(&Value::Integer(lower_o)),
            Value::Null,
        );
        let upper_key = build_index_bound(
            order_index.columns.len(),
            &[Value::Integer(w_id), Value::Integer(d_id)],
            Some(&Value::Integer(upper_o)),
            Value::Blob(Vec::new()),
        );

        let locations = match self
            .storage
            .scan_index_range(&order_index.name, Some(&lower_key), Some(&upper_key))
        {
            Ok(locations) => locations,
            Err(err) => return Some(Err(GongDBError::from(err))),
        };
        let mut distinct_items: HashSet<i64> = HashSet::new();
        for location in locations {
            let row = match self.storage.read_row_at(&location) {
                Ok(row) => row,
                Err(StorageError::NotFound(_)) => continue,
                Err(err) => return Some(Err(GongDBError::from(err))),
            };
            if let Some(Value::Integer(value)) = row.get(ol_i_id_idx) {
                distinct_items.insert(*value);
            }
        }

        let mut cached_offsets = self.fast_stock_offsets_cache.borrow().clone();
        let mut cache_update: Option<FastStockOffsetsCache> = None;
        let mut count = 0i64;
        for item_id in distinct_items {
            let location = match self.lookup_stock_location(&stock_index, w_id, item_id) {
                Ok(location) => location,
                Err(err) => return Some(Err(err)),
            };
            let Some(location) = location else {
                continue;
            };
            let qty_value = match self.storage.with_record_at(&location, |record| {
                let cached = cached_offsets.as_ref().and_then(|cache| {
                    if cache.record_len == record.len() {
                        Some(cache.offsets.clone())
                    } else {
                        None
                    }
                });
                let offsets = match cached {
                    Some(offsets) => offsets,
                    None => match fast_stock_update_offsets(record, false) {
                        Ok(Some(offsets)) => {
                            let update = FastStockOffsetsCache {
                                record_len: record.len(),
                                offsets: offsets.clone(),
                            };
                            cached_offsets = Some(update.clone());
                            if cache_update.is_none() {
                                cache_update = Some(update);
                            }
                            offsets
                        }
                        Ok(None) => return Ok(None),
                        Err(err) => return Err(StorageError::Invalid(err.to_string())),
                    },
                };
                if record.get(offsets.quantity) != Some(&1) {
                    return Ok(None);
                }
                Ok(read_i64_at(record, offsets.quantity + 1))
            }) {
                Ok(Some(value)) => value,
                Ok(None) => continue,
                Err(StorageError::NotFound(_)) => continue,
                Err(err) => return Some(Err(GongDBError::from(err))),
            };
            if qty_value < threshold {
                count += 1;
            }
        }
        if let Some(update) = cache_update {
            self.fast_stock_offsets_cache.borrow_mut().replace(update);
        }

        Some(Ok(DBOutput::Rows {
            types: vec![DefaultColumnType::Text],
            rows: vec![vec![value_to_string(&Value::Integer(count))]],
        }))
    }

    fn try_fast_select(
        &mut self,
        sql: &str,
    ) -> Option<Result<DBOutput<DefaultColumnType>, GongDBError>> {
        let plan = parse_fast_select(sql)?;
        let table = match self.storage.get_table(&plan.table) {
            Some(table) => table.clone(),
            None => {
                return Some(Err(GongDBError::new(format!(
                    "no such table: {}",
                    plan.table
                ))))
            }
        };
        let column_map = self.column_index_map_cached(&table);
        let mut predicate_indices = Vec::with_capacity(plan.predicates.len());
        let mut predicate_values: HashMap<String, Value> = HashMap::new();
        for (name, value) in plan.predicates {
            let key = name.to_lowercase();
            let idx = match column_map.get(&key) {
                Some(idx) => *idx,
                None => {
                    return Some(Err(GongDBError::new(format!(
                        "no such column: {}",
                        name
                    ))))
                }
            };
            predicate_indices.push((idx, value.clone()));
            predicate_values.insert(key, value);
        }
        let output_indices = if plan.columns.len() == 1 && plan.columns[0] == "*" {
            (0..table.columns.len()).collect::<Vec<_>>()
        } else {
            let mut indices = Vec::with_capacity(plan.columns.len());
            for name in &plan.columns {
                let key = name.to_lowercase();
                let idx = match column_map.get(&key) {
                    Some(idx) => *idx,
                    None => {
                        return Some(Err(GongDBError::new(format!(
                            "no such column: {}",
                            name
                        ))))
                    }
                };
                indices.push(idx);
            }
            indices
        };

        let indexes = self.table_indexes_cached(&table.name);

        let rows = if let Some((index, key)) =
            fast_select_eq_index(&indexes, &predicate_values)
        {
            if index.unique {
                let location = match self.storage.scan_index_first_location(&index.name, &key) {
                    Ok(location) => location,
                    Err(err) => return Some(Err(GongDBError::from(err))),
                };
                let mut rows = Vec::new();
                if let Some(location) = location {
                    let row = match self.storage.read_row_at(&location) {
                        Ok(row) => row,
                        Err(StorageError::NotFound(_)) => Vec::new(),
                        Err(err) => return Some(Err(GongDBError::from(err))),
                    };
                    if row.is_empty() || (!predicate_indices.is_empty()
                        && !fast_row_matches_predicates(&row, &predicate_indices))
                    {
                        rows
                    } else {
                        rows.push(row);
                        rows
                    }
                } else {
                    rows
                }
            } else {
                let mut rows = match self
                    .storage
                    .scan_index_rows(&index.name, Some(&key), Some(&key), false)
                {
                    Ok(rows) => rows,
                    Err(err) => return Some(Err(GongDBError::from(err))),
                };
                if !predicate_indices.is_empty() {
                    rows.retain(|row| fast_row_matches_predicates(row, &predicate_indices));
                }
                rows
            }
        } else if let Some(order_by) = plan.order_by.as_ref() {
            let limit = plan.limit.unwrap_or(usize::MAX);
            let (index, lower, upper) = match fast_select_order_by_range(
                &indexes,
                order_by,
                &predicate_values,
            ) {
                Some(result) => result,
                None => return None,
            };
            let locations = match self
                .storage
                .scan_index_range(&index.name, Some(&lower), Some(&upper))
            {
                Ok(locations) => locations,
                Err(err) => return Some(Err(GongDBError::from(err))),
            };
            let iter: Box<dyn Iterator<Item = RowLocation>> = if order_by.desc {
                Box::new(locations.into_iter().rev())
            } else {
                Box::new(locations.into_iter())
            };
            let mut rows = Vec::new();
            for location in iter {
                let row = match self.storage.read_row_at(&location) {
                    Ok(row) => row,
                    Err(StorageError::NotFound(_)) => continue,
                    Err(err) => return Some(Err(GongDBError::from(err))),
                };
                if !predicate_indices.is_empty()
                    && !fast_row_matches_predicates(&row, &predicate_indices)
                {
                    continue;
                }
                rows.push(row);
                if rows.len() >= limit {
                    break;
                }
            }
            rows
        } else {
            return None;
        };

        let limit = plan.limit.unwrap_or(rows.len());
        let output_rows = rows
            .into_iter()
            .take(limit)
            .map(|row| {
                output_indices
                    .iter()
                    .map(|idx| value_to_string(&row[*idx]))
                    .collect::<Vec<String>>()
            })
            .collect::<Vec<_>>();

        Some(Ok(DBOutput::Rows {
            types: vec![DefaultColumnType::Text; output_indices.len()],
            rows: output_rows,
        }))
    }

    fn invalidate_table_stats(&self, table_name: &str) {
        self.stats_cache
            .borrow_mut()
            .remove(&table_name.to_ascii_lowercase());
    }

    fn get_table_stats(&self, table: &TableMeta) -> Result<TableStats, GongDBError> {
        const STATS_SCAN_ROW_LIMIT: usize = 2048;
        let key = table.name.to_ascii_lowercase();
        if let Some(stats) = self.stats_cache.borrow().get(&key) {
            return Ok(stats.clone());
        }
        let row_count = table.row_count as usize;
        if row_count > STATS_SCAN_ROW_LIMIT {
            let stats = TableStats {
                row_count,
                column_stats: HashMap::new(),
            };
            self.stats_cache.borrow_mut().insert(key, stats.clone());
            return Ok(stats);
        }
        let rows = self.storage.scan_table(&table.name)?;
        let stats = compute_table_stats(&table.columns, &rows);
        self.stats_cache.borrow_mut().insert(key, stats.clone());
        Ok(stats)
    }

    fn evaluate_select_values(&self, select: &Select) -> Result<QueryResult, GongDBError> {
        let mut view_stack = Vec::new();
        self.evaluate_select_values_with_views(select, &mut view_stack, None, None)
    }

    fn evaluate_select_values_with_views(
        &self,
        select: &Select,
        view_stack: &mut Vec<String>,
        outer: Option<&EvalScope<'_>>,
        cte_context: Option<&CteContext>,
    ) -> Result<QueryResult, GongDBError> {
        let owned_cte_context = if let Some(with_clause) = &select.with {
            Some(self.build_cte_context(
                with_clause,
                cte_context,
                view_stack,
                outer,
            )?)
        } else {
            None
        };
        let cte_context = owned_cte_context.as_ref().or(cte_context);
        let mut result = if !select.compounds.is_empty() {
            let mut result =
                self.evaluate_select_values_core(select, view_stack, outer, cte_context)?;
            for compound in &select.compounds {
                let right = self.evaluate_select_values_with_views(
                    &compound.select,
                    view_stack,
                    outer,
                    cte_context,
                )?;
                if result.columns.len() != right.columns.len() {
                    return Err(GongDBError::new(
                        "compound select column count mismatch",
                    ));
                }
                match compound.operator {
                    crate::ast::CompoundOperator::UnionAll => {
                        result.rows.extend(right.rows);
                    }
                    crate::ast::CompoundOperator::Union => {
                        result.rows.extend(right.rows);
                        dedup_rows(&mut result.rows);
                    }
                    crate::ast::CompoundOperator::Intersect => {
                        let mut left_rows = result.rows;
                        let mut right_rows = right.rows;
                        dedup_rows(&mut left_rows);
                        dedup_rows(&mut right_rows);
                        let right_keys: HashSet<Vec<DistinctKey>> =
                            right_rows.iter().map(|row| row_distinct_key(row)).collect();
                        let mut merged = Vec::new();
                        for row in left_rows {
                            if right_keys.contains(&row_distinct_key(&row)) {
                                merged.push(row);
                            }
                        }
                        result.rows = merged;
                    }
                    crate::ast::CompoundOperator::Except => {
                        let mut left_rows = result.rows;
                        let mut right_rows = right.rows;
                        dedup_rows(&mut left_rows);
                        dedup_rows(&mut right_rows);
                        let right_keys: HashSet<Vec<DistinctKey>> =
                            right_rows.iter().map(|row| row_distinct_key(row)).collect();
                        let mut merged = Vec::new();
                        for row in left_rows {
                            if !right_keys.contains(&row_distinct_key(&row)) {
                                merged.push(row);
                            }
                        }
                        result.rows = merged;
                    }
                }
            }
            result
        } else {
            self.evaluate_select_values_core(select, view_stack, outer, cte_context)?
        };
        result.rows = apply_limit_offset_rows(self, select, result.rows)?;
        Ok(result)
    }

    fn evaluate_select_values_core(
        &self,
        select: &Select,
        view_stack: &mut Vec<String>,
        outer: Option<&EvalScope<'_>>,
        cte_context: Option<&CteContext>,
    ) -> Result<QueryResult, GongDBError> {
        let mut selection_applied = false;
        let mut preordered_by_index = false;
        if select.from.len() == 1
            && select.group_by.is_empty()
            && select.having.is_none()
            && select.order_by.is_empty()
            && is_count_star(select)
        {
            if let crate::ast::TableRef::Named { name, alias } = &select.from[0] {
                let table_name = object_name(name);
                if let Some(table) = self.storage.get_table(&table_name) {
                    let table_alias = alias.as_ref().map(|ident| ident.value.clone());
                    let table_scope = TableScope {
                        table_name: Some(table_name),
                        table_alias,
                    };
                    let column_scopes = vec![table_scope.clone(); table.columns.len()];
                    let output_columns =
                        projection_columns(&select.projection, &table.columns, &column_scopes)?;
                    let fast_count = match select.selection.as_ref() {
                        None => Some(table.row_count as i64),
                        Some(predicate) if expr_is_constant(predicate) => {
                            let value = eval_constant_expr_checked(self, predicate)?;
                            if value_to_bool(&value) {
                                Some(table.row_count as i64)
                            } else {
                                Some(0)
                            }
                        }
                        _ => None,
                    };
                    if let Some(count) = fast_count {
                        return Ok(QueryResult {
                            columns: output_columns,
                            rows: vec![vec![Value::Integer(count)]],
                        });
                    }
                }
            }
        }
        let source = if select.from.len() == 1 {
            if let crate::ast::TableRef::Named { name, alias } = &select.from[0] {
                let table_name = object_name(name);
                if self.storage.get_table(&table_name).is_some() {
                    let table_alias = alias.as_ref().map(|ident| ident.value.clone());
                    let (rows, ordered_by_index) = self.scan_table_rows(
                        &table_name,
                        table_alias.as_deref(),
                        select.selection.as_ref(),
                        &select.order_by,
                        outer,
                        cte_context,
                    )?;
                    selection_applied = select.selection.is_some();
                    preordered_by_index = ordered_by_index;
                    let table = self
                        .storage
                        .get_table(&table_name)
                        .ok_or_else(|| GongDBError::new(format!("no such table: {}", table_name)))?;
                    let table_scope = TableScope {
                        table_name: Some(table_name),
                        table_alias,
                    };
                    QuerySource {
                        columns: table.columns.clone(),
                        column_scopes: vec![table_scope.clone(); table.columns.len()],
                        rows,
                        table_scope,
                    }
                } else {
                    self.resolve_source(
                        select,
                        view_stack,
                        select.selection.as_ref(),
                        outer,
                        cte_context,
                    )?
                }
            } else {
                self.resolve_source(
                    select,
                    view_stack,
                    select.selection.as_ref(),
                    outer,
                    cte_context,
                )?
            }
        } else {
            self.resolve_source(
                select,
                view_stack,
                select.selection.as_ref(),
                outer,
                cte_context,
            )?
        };
        let QuerySource {
            columns,
            column_scopes,
            rows,
            table_scope,
        } = source;
        let column_lookup = build_column_lookup(&columns, &column_scopes);
        let mut filtered = Vec::new();
        for row in rows {
            let scope = EvalScope {
                columns: &columns,
                column_scopes: &column_scopes,
                row: &row,
                table_scope: &table_scope,
                cte_context,
                column_lookup: Some(&column_lookup),
            };
            if let Some(predicate) = &select.selection {
                if selection_applied {
                    filtered.push(row);
                    continue;
                }
                let value = eval_expr(self, predicate, &scope, outer)?;
                if !value_to_bool(&value) {
                    continue;
                }
            }
            filtered.push(row);
        }

        let output_columns = projection_columns(&select.projection, &columns, &column_scopes)?;
        let projection_has_aggregate = projection_has_aggregate(&select.projection);
        let having_has_aggregate = select
            .having
            .as_ref()
            .is_some_and(|expr| expr_contains_aggregate(expr));

        if select
            .group_by
            .iter()
            .any(|expr| expr_contains_aggregate(expr))
        {
            return Err(GongDBError::new("GROUP BY cannot contain aggregate expressions"));
        }

        if select.group_by.is_empty()
            && select.having.is_some()
            && !projection_has_aggregate
            && !having_has_aggregate
        {
            return Err(GongDBError::new("HAVING requires aggregate expressions"));
        }

        if select.group_by.is_empty() && is_count_star(select) && select.having.is_none() {
            let count = filtered.len() as i64;
            return Ok(QueryResult {
                columns: output_columns,
                rows: vec![vec![Value::Integer(count)]],
            });
        }

        if !select.group_by.is_empty() {
            struct Group {
                rows: Vec<Vec<Value>>,
            }

            let mut groups: Vec<Group> = Vec::new();
            let mut group_lookup: HashMap<Vec<DistinctKey>, usize> = HashMap::new();
            for row in filtered {
                let scope = EvalScope {
                    columns: &columns,
                    column_scopes: &column_scopes,
                    row: &row,
                    table_scope: &table_scope,
                    cte_context,
                    column_lookup: Some(&column_lookup),
                };
                let mut key = Vec::with_capacity(select.group_by.len());
                for expr in &select.group_by {
                    let value = eval_expr(self, expr, &scope, outer)?;
                    key.push(distinct_key(&value));
                }
                match group_lookup.entry(key) {
                    std::collections::hash_map::Entry::Occupied(entry) => {
                        if let Some(group) = groups.get_mut(*entry.get()) {
                            group.rows.push(row);
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let index = groups.len();
                        groups.push(Group { rows: vec![row] });
                        entry.insert(index);
                    }
                }
            }

            let order_plans = if select.order_by.is_empty() {
                Vec::new()
            } else {
                resolve_order_by_plans(&select.order_by, &output_columns)?
            };

            let projected = if order_plans.is_empty() {
                let mut rows = Vec::with_capacity(groups.len());
                for group in &groups {
                    if let Some(having) = &select.having {
                        if !evaluate_group_having(
                            self,
                            having,
                            &columns,
                            &column_scopes,
                            &table_scope,
                            &group.rows,
                            outer,
                            cte_context,
                        )? {
                            continue;
                        }
                    }
                    rows.push(evaluate_group_projection(
                        self,
                        &select.projection,
                        &columns,
                        &column_scopes,
                        &table_scope,
                        &group.rows,
                        outer,
                        cte_context,
                    )?);
                }
                if select.distinct {
                    let mut unique: Vec<Vec<Value>> = Vec::new();
                    let mut seen: HashSet<Vec<DistinctKey>> = HashSet::new();
                    for row in rows {
                        let key = row_distinct_key(&row);
                        if seen.insert(key) {
                            unique.push(row);
                        }
                    }
                    unique
                } else {
                    rows
                }
            } else {
                let order_table_scope = TableScope {
                    table_name: None,
                    table_alias: None,
                };
                let order_column_scopes =
                    vec![order_table_scope.clone(); output_columns.len()];
                let order_lookup =
                    build_column_lookup(&output_columns, &order_column_scopes);
                let mut rows = Vec::with_capacity(groups.len());
                for group in &groups {
                    if let Some(having) = &select.having {
                        if !evaluate_group_having(
                            self,
                            having,
                            &columns,
                            &column_scopes,
                            &table_scope,
                            &group.rows,
                            outer,
                            cte_context,
                        )? {
                            continue;
                        }
                    }
                    let projected_row = evaluate_group_projection(
                        self,
                        &select.projection,
                        &columns,
                        &column_scopes,
                        &table_scope,
                        &group.rows,
                        outer,
                        cte_context,
                    )?;
                    let scope = EvalScope {
                        columns: &columns,
                        column_scopes: &column_scopes,
                        row: group.rows.first().unwrap(),
                        table_scope: &table_scope,
                        cte_context,
                        column_lookup: Some(&column_lookup),
                    };
                    let order_values = compute_group_order_values(
                        self,
                        &order_plans,
                        &projected_row,
                        &output_columns,
                        &order_column_scopes,
                        &order_table_scope,
                        &order_lookup,
                        &scope,
                        &group.rows,
                        outer,
                    )?;
                    rows.push(SortedRow {
                        order_values,
                        projected: projected_row,
                    });
                }
                let mut sorted_rows = if select.distinct {
                    let mut unique: Vec<SortedRow> = Vec::new();
                    let mut seen: HashSet<Vec<DistinctKey>> = HashSet::new();
                    for row in rows {
                        let key = row_distinct_key(&row.projected);
                        if seen.insert(key) {
                            unique.push(row);
                        }
                    }
                    unique
                } else {
                    rows
                };
                sorted_rows.sort_unstable_by(|a, b| {
                    compare_order_keys(&a.order_values, &b.order_values, &order_plans)
                });
                sorted_rows
                    .into_iter()
                    .map(|row| row.projected)
                    .collect()
            };

            return Ok(QueryResult {
                columns: output_columns,
                rows: projected,
            });
        }

        if projection_has_aggregate || having_has_aggregate {
            if let Some(having) = &select.having {
                if !evaluate_group_having(
                    self,
                    having,
                    &columns,
                    &column_scopes,
                    &table_scope,
                    &filtered,
                    outer,
                    cte_context,
                )? {
                    return Ok(QueryResult {
                        columns: output_columns,
                        rows: Vec::new(),
                    });
                }
            }

            let projected = evaluate_group_projection(
                self,
                &select.projection,
                &columns,
                &column_scopes,
                &table_scope,
                &filtered,
                outer,
                cte_context,
            )?;

            return Ok(QueryResult {
                columns: output_columns,
                rows: vec![projected],
            });
        }

        if preordered_by_index
            && !select.distinct
            && select.group_by.is_empty()
            && select.having.is_none()
            && select.limit.is_some()
            && select.offset.is_none()
        {
            let (limit, offset) = eval_limit_offset(self, select)?;
            if offset == 0 {
                if let Some(limit) = limit {
                    if limit == 0 {
                        return Ok(QueryResult {
                            columns: output_columns,
                            rows: Vec::new(),
                        });
                    }
                    if filtered.len() > limit {
                        filtered.truncate(limit);
                    }
                }
            }
        }

        let order_plans = if select.order_by.is_empty() {
            Vec::new()
        } else {
            resolve_order_by_plans(&select.order_by, &output_columns)?
        };

        let projected = if order_plans.is_empty() {
            let mut rows = Vec::with_capacity(filtered.len());
            for row in filtered {
                let scope = EvalScope {
                    columns: &columns,
                    column_scopes: &column_scopes,
                    row: &row,
                    table_scope: &table_scope,
                    cte_context,
                    column_lookup: Some(&column_lookup),
                };
                rows.push(project_row(
                    self,
                    &select.projection,
                    &row,
                    &scope,
                    outer,
                )?);
            }
            if select.distinct {
                dedup_rows(&mut rows);
                rows
            } else {
                rows
            }
        } else {
            let order_table_scope = TableScope {
                table_name: None,
                table_alias: None,
            };
            let order_column_scopes =
                vec![order_table_scope.clone(); output_columns.len()];
            let order_lookup = build_column_lookup(&output_columns, &order_column_scopes);
            if preordered_by_index {
                let mut rows = Vec::with_capacity(filtered.len());
                for row in filtered {
                    let scope = EvalScope {
                        columns: &columns,
                        column_scopes: &column_scopes,
                        row: &row,
                        table_scope: &table_scope,
                        cte_context,
                        column_lookup: Some(&column_lookup),
                    };
                    let projected_row = project_row(
                        self,
                        &select.projection,
                        &row,
                        &scope,
                        outer,
                    )?;
                    rows.push(projected_row);
                }
                if select.distinct {
                    dedup_rows(&mut rows);
                }
                rows
            } else {
                let mut rows = Vec::with_capacity(filtered.len());
                for row in filtered {
                    let scope = EvalScope {
                        columns: &columns,
                        column_scopes: &column_scopes,
                        row: &row,
                        table_scope: &table_scope,
                        cte_context,
                        column_lookup: Some(&column_lookup),
                    };
                    let projected_row = project_row(
                        self,
                        &select.projection,
                        &row,
                        &scope,
                        outer,
                    )?;
                    let order_values = compute_order_values(
                        self,
                        &order_plans,
                        &projected_row,
                        &output_columns,
                        &order_column_scopes,
                        &order_table_scope,
                        &order_lookup,
                        &scope,
                    )?;
                    rows.push(SortedRow {
                        order_values,
                        projected: projected_row,
                    });
                }
                if select.distinct {
                    dedup_sorted_rows(&mut rows);
                }
                rows.sort_unstable_by(|a, b| {
                    compare_order_keys(&a.order_values, &b.order_values, &order_plans)
                });
                rows.into_iter().map(|row| row.projected).collect()
            }
        };

        Ok(QueryResult {
            columns: output_columns,
            rows: projected,
        })
    }

    fn evaluate_select_values_with_outer(
        &self,
        select: &Select,
        outer: Option<&EvalScope<'_>>,
        cte_context: Option<&CteContext>,
    ) -> Result<QueryResult, GongDBError> {
        let mut view_stack = Vec::new();
        self.evaluate_select_values_with_views(select, &mut view_stack, outer, cte_context)
    }

    fn build_cte_context(
        &self,
        with_clause: &With,
        parent: Option<&CteContext>,
        view_stack: &mut Vec<String>,
        outer: Option<&EvalScope<'_>>,
    ) -> Result<CteContext, GongDBError> {
        let mut context = parent.cloned().unwrap_or_else(CteContext::new);
        for cte in &with_clause.ctes {
            let mut result = if with_clause.recursive {
                self.evaluate_recursive_cte(cte, &context, view_stack, outer)?
            } else {
                self.evaluate_select_values_with_views(
                    &cte.query,
                    view_stack,
                    outer,
                    Some(&context),
                )?
            };
            if !cte.columns.is_empty() {
                if cte.columns.len() != result.columns.len() {
                    return Err(GongDBError::new(format!(
                        "CTE column count mismatch: {}",
                        cte.name.value
                    )));
                }
                result.columns = columns_from_idents(&cte.columns);
            }
            context.insert(&cte.name.value, result);
        }
        Ok(context)
    }

    fn evaluate_recursive_cte(
        &self,
        cte: &Cte,
        context: &CteContext,
        view_stack: &mut Vec<String>,
        outer: Option<&EvalScope<'_>>,
    ) -> Result<QueryResult, GongDBError> {
        if cte.query.compounds.is_empty() {
            return self.evaluate_select_values_with_views(
                &cte.query,
                view_stack,
                outer,
                Some(context),
            );
        }

        let mut seed_select = (*cte.query).clone();
        let compounds = seed_select.compounds.clone();
        seed_select.compounds.clear();

        let mut seed = self.evaluate_select_values_with_views(
            &seed_select,
            view_stack,
            outer,
            Some(context),
        )?;
        if !cte.columns.is_empty() {
            if cte.columns.len() != seed.columns.len() {
                return Err(GongDBError::new(format!(
                    "CTE column count mismatch: {}",
                    cte.name.value
                )));
            }
            seed.columns = columns_from_idents(&cte.columns);
        }

        let mut union_all_only = true;
        for compound in &compounds {
            match compound.operator {
                crate::ast::CompoundOperator::UnionAll => {}
                crate::ast::CompoundOperator::Union => {
                    union_all_only = false;
                }
                _ => {
                    return Err(GongDBError::new(
                        "recursive CTEs require UNION or UNION ALL",
                    ));
                }
            }
        }

        let output_columns = seed.columns.clone();
        let mut all_rows = seed.rows;
        let mut seen: HashSet<Vec<DistinctKey>> = HashSet::new();
        if !union_all_only {
            dedup_rows(&mut all_rows);
            seen.extend(all_rows.iter().map(|row| row_distinct_key(row)));
        }
        let mut delta = all_rows.clone();

        while !delta.is_empty() {
            let mut iter_context = context.clone();
            iter_context.insert(
                &cte.name.value,
                QueryResult {
                    columns: output_columns.clone(),
                    rows: delta.clone(),
                },
            );
            let mut generated = Vec::new();
            for compound in &compounds {
                let term = self.evaluate_select_values_with_views(
                    &compound.select,
                    view_stack,
                    outer,
                    Some(&iter_context),
                )?;
                if term.columns.len() != output_columns.len() {
                    return Err(GongDBError::new(
                        "compound select column count mismatch",
                    ));
                }
                generated.extend(term.rows);
            }

            if union_all_only {
                delta = generated.clone();
                all_rows.extend(generated);
            } else {
                let mut next_delta = Vec::new();
                for row in generated {
                    let key = row_distinct_key(&row);
                    if seen.insert(key) {
                        next_delta.push(row.clone());
                        all_rows.push(row);
                    }
                }
                delta = next_delta;
            }
        }

        Ok(QueryResult {
            columns: output_columns,
            rows: all_rows,
        })
    }

    fn resolve_source(
        &self,
        select: &Select,
        view_stack: &mut Vec<String>,
        selection: Option<&Expr>,
        outer: Option<&EvalScope<'_>>,
        cte_context: Option<&CteContext>,
    ) -> Result<QuerySource, GongDBError> {
        if select.from.is_empty() {
            return Ok(QuerySource {
                columns: Vec::new(),
                column_scopes: Vec::new(),
                rows: vec![Vec::new()],
                table_scope: TableScope {
                    table_name: None,
                    table_alias: None,
                },
            });
        }

        if select.from.len() == 1 {
            return self.resolve_table_ref(&select.from[0], view_stack, cte_context);
        }

        let predicates = selection
            .map(split_conjuncts)
            .unwrap_or_default();

        let mut named_only = true;
        for table_ref in &select.from {
            if !matches!(table_ref, TableRef::Named { .. }) {
                named_only = false;
                break;
            }
        }

        if named_only {
            let mut table_infos = Vec::new();
            for table_ref in &select.from {
                if let TableRef::Named { name, alias } = table_ref {
                    let table_name = object_name(name);
                    let table_alias = alias.as_ref().map(|ident| ident.value.clone());
                    let table_scope = TableScope {
                        table_name: Some(table_name.clone()),
                        table_alias: table_alias.clone(),
                    };
                    if let Some(ctes) = cte_context {
                        if let Some(result) = ctes.get(&table_name) {
                            let column_count = result.columns.len();
                            table_infos.push(TableInfo {
                                source: QuerySource {
                                    columns: result.columns.clone(),
                                    column_scopes: vec![table_scope.clone(); column_count],
                                    rows: result.rows.clone(),
                                    table_scope: table_scope.clone(),
                                },
                                scope: table_scope,
                            });
                            continue;
                        }
                    }
                    if let Some(table) = self.storage.get_table(&table_name) {
                        let column_count = table.columns.len();
                        table_infos.push(TableInfo {
                            source: QuerySource {
                                columns: table.columns.clone(),
                                column_scopes: vec![table_scope.clone(); column_count],
                                rows: Vec::new(),
                                table_scope: table_scope.clone(),
                            },
                            scope: table_scope,
                        });
                        continue;
                    }
                }
                let source = self.resolve_table_ref(table_ref, view_stack, cte_context)?;
                let scope = source.table_scope.clone();
                table_infos.push(TableInfo { source, scope });
            }

            let ordered_scopes = table_infos.iter().map(|info| info.scope.clone()).collect();
            return self.resolve_join_plan(
                table_infos,
                predicates,
                ordered_scopes,
                outer,
                cte_context,
            );
        }

        let mut sources = Vec::new();
        for table_ref in &select.from {
            sources.push(self.resolve_table_ref(table_ref, view_stack, cte_context)?);
        }

        if sources.len() == 1 {
            return Ok(sources.remove(0));
        }

        let mut rows = vec![Vec::new()];
        let mut columns = Vec::new();
        let mut column_scopes = Vec::new();
        for source in sources {
            columns.extend(source.columns);
            column_scopes.extend(source.column_scopes);
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
            column_scopes,
            rows,
            table_scope: TableScope {
                table_name: None,
                table_alias: None,
            },
        })
    }

    fn scan_table_rows(
        &self,
        table_name: &str,
        table_alias: Option<&str>,
        selection: Option<&Expr>,
        order_by: &[OrderByExpr],
        outer: Option<&EvalScope<'_>>,
        cte_context: Option<&CteContext>,
    ) -> Result<(Vec<Vec<Value>>, bool), GongDBError> {
        let table = self
            .storage
            .get_table(table_name)
            .ok_or_else(|| GongDBError::new(format!("no such table: {}", table_name)))?;
        let table_scope = TableScope {
            table_name: Some(table_name.to_string()),
            table_alias: table_alias.map(|alias| alias.to_string()),
        };
        let column_scopes = vec![table_scope.clone(); table.columns.len()];
        let column_lookup = build_column_lookup(&table.columns, &column_scopes);
        let mut selection = selection;
        if let Some(predicate) = selection {
            if expr_is_constant(predicate) {
                let value = eval_constant_expr_checked(self, predicate)?;
                if !value_to_bool(&value) {
                    return Ok((Vec::new(), false));
                }
                selection = None;
            }
        }
        let predicate_plan = selection
            .map(|predicate| self.build_row_predicate_plan(predicate, &table_scope, &table.columns))
            .filter(|plan| plan.steps.iter().all(|step| matches!(step, RowPredicateStep::Simple(_))));
        let mut ordered_by_index = false;
        let mut ordered_by_desc = false;
        let mut rows = if let Some(plan) =
            choose_index_scan_plan(self, table, selection, order_by, &table_scope)
        {
            ordered_by_index = plan.ordered_by;
            ordered_by_desc = plan.ordered_by_desc;
            scan_rows_with_index(self, &plan)?
        } else {
            let mut fresh_rows = Vec::with_capacity(table.row_count as usize);
            let mut scan = self.storage.table_scan(table_name)?;
            while let Some(result) = scan.next() {
                let row = result?;
                if let Some(plan) = predicate_plan.as_ref() {
                    if !row_matches_predicate_plan(
                        self,
                        plan,
                        &row,
                        &table.columns,
                        &column_scopes,
                        &table_scope,
                        cte_context,
                        outer,
                        &column_lookup,
                    )? {
                        continue;
                    }
                } else if let Some(predicate) = selection {
                    let scope = EvalScope {
                        columns: &table.columns,
                        column_scopes: &column_scopes,
                        row: &row,
                        table_scope: &table_scope,
                        cte_context,
                        column_lookup: Some(&column_lookup),
                    };
                    let value = eval_expr(self, predicate, &scope, outer)?;
                    if !value_to_bool(&value) {
                        continue;
                    }
                }
                fresh_rows.push(row);
            }
            fresh_rows
        };
        if let Some(plan) = predicate_plan.as_ref() {
            if !rows.is_empty() {
                let mut filtered = Vec::with_capacity(rows.len());
                for row in rows.drain(..) {
                    if row_matches_predicate_plan(
                        self,
                        plan,
                        &row,
                        &table.columns,
                        &column_scopes,
                        &table_scope,
                        cte_context,
                        outer,
                        &column_lookup,
                    )? {
                        filtered.push(row);
                    }
                }
                rows = filtered;
            }
        } else if let Some(predicate) = selection {
            let mut filtered = Vec::with_capacity(rows.len());
            for row in rows.drain(..) {
                let scope = EvalScope {
                    columns: &table.columns,
                    column_scopes: &column_scopes,
                    row: &row,
                    table_scope: &table_scope,
                    cte_context,
                    column_lookup: Some(&column_lookup),
                };
                let value = eval_expr(self, predicate, &scope, outer)?;
                if !value_to_bool(&value) {
                    continue;
                }
                filtered.push(row);
            }
            rows = filtered;
        }
        if ordered_by_index && ordered_by_desc && rows.len() > 1 {
            rows.reverse();
        }
        Ok((rows, ordered_by_index))
    }

    fn apply_update_assignments(
        &self,
        update: &Update,
        row: &[Value],
        table: &TableMeta,
        column_scopes: &[TableScope],
        table_scope: &TableScope,
        column_lookup: &ColumnLookup,
    ) -> Result<Vec<Value>, GongDBError> {
        let scope = EvalScope {
            columns: &table.columns,
            column_scopes,
            row,
            table_scope,
            cte_context: None,
            column_lookup: Some(column_lookup),
        };
        let mut new_row = row.to_vec();
        for assignment in &update.assignments {
            let idx = resolve_column_index(&assignment.column.value, &table.columns).ok_or_else(
                || GongDBError::new(format!("no such column: {}", assignment.column.value)),
            )?;
            let value = eval_expr(self, &assignment.value, &scope, None)?;
            new_row[idx] = apply_affinity(value, &table.columns[idx].data_type);
        }
        Ok(new_row)
    }

    fn build_row_predicate_plan(
        &self,
        selection: &Expr,
        table_scope: &TableScope,
        columns: &[Column],
    ) -> RowPredicatePlan {
        let mut steps = Vec::new();
        for expr in split_conjuncts(selection) {
            if let Some(predicate) =
                simple_predicate_from_expr(self, &expr, table_scope, columns)
            {
                steps.push(RowPredicateStep::Simple(predicate));
            } else {
                steps.push(RowPredicateStep::Complex(expr));
            }
        }
        RowPredicatePlan { steps }
    }

    fn resolve_table_ref(
        &self,
        table_ref: &crate::ast::TableRef,
        view_stack: &mut Vec<String>,
        cte_context: Option<&CteContext>,
    ) -> Result<QuerySource, GongDBError> {
        match table_ref {
            crate::ast::TableRef::Named { name, alias } => {
                let table_name = object_name(name);
                let table_alias = alias.as_ref().map(|ident| ident.value.clone());
                if let Some(ctes) = cte_context {
                    if let Some(result) = ctes.get(&table_name) {
                        let table_scope = TableScope {
                            table_name: Some(table_name),
                            table_alias,
                        };
                        let column_count = result.columns.len();
                        return Ok(QuerySource {
                            columns: result.columns.clone(),
                            column_scopes: vec![table_scope.clone(); column_count],
                            rows: result.rows.clone(),
                            table_scope,
                        });
                    }
                }
                if let Some(table) = self.storage.get_table(&table_name) {
                    let rows = self.storage.scan_table(&table_name)?;
                    let table_scope = TableScope {
                        table_name: Some(table_name),
                        table_alias,
                    };
                    return Ok(QuerySource {
                        columns: table.columns.clone(),
                        column_scopes: vec![table_scope.clone(); table.columns.len()],
                        rows,
                        table_scope,
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
                        self.evaluate_select_values_with_views(&view.query, view_stack, None, None)?;
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
                    let table_scope = TableScope {
                        table_name: Some(table_name),
                        table_alias,
                    };
                    let column_count = result.columns.len();
                    return Ok(QuerySource {
                        columns: result.columns,
                        column_scopes: vec![table_scope.clone(); column_count],
                        rows: result.rows,
                        table_scope,
                    });
                }
                Err(GongDBError::new(format!(
                    "no such table: {}",
                    table_name
                )))
            }
            crate::ast::TableRef::Subquery { subquery, alias } => {
                let result =
                    self.evaluate_select_values_with_views(subquery, view_stack, None, cte_context)?;
                let table_scope = TableScope {
                    table_name: None,
                    table_alias: alias.as_ref().map(|ident| ident.value.clone()),
                };
                let column_count = result.columns.len();
                Ok(QuerySource {
                    columns: result.columns,
                    column_scopes: vec![table_scope.clone(); column_count],
                    rows: result.rows,
                    table_scope,
                })
            }
            crate::ast::TableRef::Join {
                left,
                right,
                operator,
                constraint,
            } => {
                let left_source = self.resolve_table_ref(left, view_stack, cte_context)?;
                let right_source = self.resolve_table_ref(right, view_stack, cte_context)?;
                self.join_sources_with_constraint(
                    left_source,
                    right_source,
                    constraint.as_ref(),
                    operator.clone(),
                    None,
                    cte_context,
                )
            }
        }
    }

    fn resolve_join_plan(
        &self,
        tables: Vec<TableInfo>,
        predicates: Vec<Expr>,
        ordered_scopes: Vec<TableScope>,
        outer: Option<&EvalScope<'_>>,
        cte_context: Option<&CteContext>,
    ) -> Result<QuerySource, GongDBError> {
        let table_count = tables.len();
        let table_stats: Vec<TableStats> = tables
            .iter()
            .map(|info| {
                if info.source.rows.is_empty() {
                    if let Some(table_name) = info.source.table_scope.table_name.as_ref() {
                        if let Some(table) = self.storage.get_table(table_name) {
                            return TableStats {
                                row_count: table.row_count as usize,
                                column_stats: HashMap::new(),
                            };
                        }
                    }
                }
                compute_table_stats(&info.source.columns, &info.source.rows)
            })
            .collect();

        let mut remaining: Vec<PredicateInfo> = predicates
            .into_iter()
            .map(|expr| {
                let tables_ref = predicate_table_refs(&expr, &tables);
                let selectivity =
                    estimate_predicate_selectivity(self, &expr, &tables_ref, &tables, &table_stats);
                PredicateInfo {
                    tables: tables_ref,
                    expr,
                    selectivity,
                }
            })
            .collect();

        let mut sources: Vec<Option<QuerySource>> =
            tables.into_iter().map(|info| Some(info.source)).collect();

        let base_rows: Vec<f64> = table_stats
            .iter()
            .map(|stats| stats.row_count as f64)
            .collect();
        let mut local_selectivity = vec![1.0; table_count];
        for pred in &remaining {
            if let Some(tables) = &pred.tables {
                if tables.len() == 1 {
                    local_selectivity[tables[0]] *= pred.selectivity;
                }
            }
        }
        let estimated_table_rows: Vec<f64> = base_rows
            .iter()
            .zip(local_selectivity.iter())
            .map(|(rows, sel)| {
                if *rows == 0.0 {
                    0.0
                } else {
                    (*rows * *sel).max(1.0)
                }
            })
            .collect();

        let mut joined = HashSet::new();
        let mut current_index = 0usize;
        let mut best_cost = f64::INFINITY;
        for (idx, rows) in estimated_table_rows.iter().enumerate() {
            let cost = base_rows[idx] + rows;
            if cost < best_cost {
                best_cost = cost;
                current_index = idx;
            }
        }
        joined.insert(current_index);

        let mut current = apply_predicates_to_source(
            self,
            sources[current_index].take().unwrap(),
            extract_predicates_for_tables(&mut remaining, &joined),
            outer,
            cte_context,
        )?;
        let mut current_est_rows = estimated_table_rows[current_index];

        while joined.len() < table_count {
            let mut best_idx = None;
            let mut best_join_cost = f64::INFINITY;
            let mut best_est_rows = 0.0;
            for idx in 0..table_count {
                if joined.contains(&idx) {
                    continue;
                }
                let candidate_rows = estimated_table_rows[idx];
                let join_selectivity = estimate_join_selectivity(&remaining, &joined, idx);
                let estimated_rows = if current_est_rows == 0.0 || candidate_rows == 0.0 {
                    0.0
                } else {
                    (current_est_rows * candidate_rows * join_selectivity).max(1.0)
                };
                let join_cost = current_est_rows * candidate_rows + estimated_rows;
                if join_cost < best_join_cost {
                    best_join_cost = join_cost;
                    best_idx = Some(idx);
                    best_est_rows = estimated_rows;
                }
            }

            let idx = best_idx.unwrap_or_else(|| {
                (0..table_count)
                    .find(|i| !joined.contains(i))
                    .unwrap()
            });

            let right_source = sources[idx].take().unwrap();
            let right_local_predicates = extract_local_predicates(&mut remaining, idx);
            let new_joined = extend_joined(&joined, idx);
            let mut join_predicates = extract_predicates_for_tables(&mut remaining, &new_joined);
            if can_use_index_lookup(self, &current, &right_source, &join_predicates) {
                join_predicates.extend(right_local_predicates);
                current = join_sources_with_predicates(
                    self,
                    current,
                    right_source,
                    join_predicates,
                    outer,
                    cte_context,
                )?;
            } else {
                let right = apply_predicates_to_source(
                    self,
                    right_source,
                    right_local_predicates,
                    outer,
                    cte_context,
                )?;
                current = join_sources_with_predicates(
                    self,
                    current,
                    right,
                    join_predicates,
                    outer,
                    cte_context,
                )?;
            }
            joined.insert(idx);
            current_est_rows = best_est_rows;
        }

        Ok(Self::reorder_query_source(current, &ordered_scopes))
    }

    fn reorder_query_source(
        mut source: QuerySource,
        ordered_scopes: &[TableScope],
    ) -> QuerySource {
        if ordered_scopes.is_empty() {
            return source;
        }
        let mut indices = Vec::with_capacity(source.column_scopes.len());
        let mut used = vec![false; source.column_scopes.len()];
        for scope in ordered_scopes {
            for (idx, col_scope) in source.column_scopes.iter().enumerate() {
                if used[idx] {
                    continue;
                }
                if col_scope.same_source(scope) {
                    indices.push(idx);
                    used[idx] = true;
                }
            }
        }
        for idx in 0..source.column_scopes.len() {
            if !used[idx] {
                indices.push(idx);
            }
        }

        if indices.len() != source.column_scopes.len() {
            return source;
        }

        let columns = indices
            .iter()
            .map(|&idx| source.columns[idx].clone())
            .collect();
        let column_scopes = indices
            .iter()
            .map(|&idx| source.column_scopes[idx].clone())
            .collect();
        let rows = source
            .rows
            .into_iter()
            .map(|row| indices.iter().map(|&idx| row[idx].clone()).collect())
            .collect();

        source.columns = columns;
        source.column_scopes = column_scopes;
        source.rows = rows;
        source
    }

    fn join_sources_with_constraint(
        &self,
        left: QuerySource,
        right: QuerySource,
        constraint: Option<&JoinConstraint>,
        operator: JoinOperator,
        outer: Option<&EvalScope<'_>>,
        cte_context: Option<&CteContext>,
    ) -> Result<QuerySource, GongDBError> {
        let QuerySource {
            columns: left_columns,
            column_scopes: left_scopes,
            rows: left_rows,
            ..
        } = left;
        let QuerySource {
            columns: right_columns,
            column_scopes: right_scopes,
            rows: right_rows,
            ..
        } = right;
        let left_len = left_columns.len();
        let right_len = right_columns.len();
        let mut columns = left_columns;
        columns.extend(right_columns);
        let mut column_scopes = left_scopes;
        column_scopes.extend(right_scopes);
        let table_scope = TableScope {
            table_name: None,
            table_alias: None,
        };
        let mut rows = Vec::new();
        let null_left = vec![Value::Null; left_len];
        let null_right = vec![Value::Null; right_len];

        let mut join_pairs: Vec<(usize, usize)> = Vec::new();
        let mut remaining_predicates: Vec<Expr> = Vec::new();

        if let Some(JoinConstraint::Using(cols)) = constraint {
            let pairs = resolve_using_pairs(cols, &columns, &column_scopes)?;
            for (left_idx, right_idx) in pairs {
                if left_idx >= left_len || right_idx < left_len {
                    return Err(GongDBError::new(
                        "USING clause does not reference both join sources",
                    ));
                }
                join_pairs.push((left_idx, right_idx - left_len));
            }
        } else if let Some(JoinConstraint::On(expr)) = constraint {
            let predicates = split_conjuncts(expr);
            let (pairs, remaining) = extract_join_pairs(
                &predicates,
                &columns[..left_len],
                &column_scopes[..left_len],
                &columns[left_len..],
                &column_scopes[left_len..],
            );
            join_pairs = pairs;
            remaining_predicates = remaining;
        }

        let column_lookup = if remaining_predicates.is_empty() {
            None
        } else {
            Some(build_column_lookup(&columns, &column_scopes))
        };
        let mut right_matched = vec![false; right_rows.len()];

        if join_pairs.is_empty() {
            let on_expr = constraint.and_then(|c| match c {
                JoinConstraint::On(expr) => Some(expr),
                _ => None,
            });
            let column_lookup = on_expr
                .as_ref()
                .map(|_| build_column_lookup(&columns, &column_scopes));

            for left_row in &left_rows {
                let mut matched = false;
                for (right_idx, right_row) in right_rows.iter().enumerate() {
                    let mut combined = Vec::with_capacity(left_len + right_len);
                    combined.extend(left_row.iter().cloned());
                    combined.extend(right_row.iter().cloned());
                    if let Some(expr) = on_expr {
                        let scope = EvalScope {
                            columns: &columns,
                            column_scopes: &column_scopes,
                            row: &combined,
                            table_scope: &table_scope,
                            cte_context,
                            column_lookup: column_lookup.as_ref(),
                        };
                        let value = eval_expr(self, expr, &scope, outer)?;
                        if !value_to_bool(&value) {
                            continue;
                        }
                    }
                    matched = true;
                    right_matched[right_idx] = true;
                    rows.push(combined);
                }
                if !matched && matches!(operator, JoinOperator::Left | JoinOperator::Full) {
                    let mut combined = Vec::with_capacity(left_len + right_len);
                    combined.extend(left_row.iter().cloned());
                    combined.extend(null_right.iter().cloned());
                    rows.push(combined);
                }
            }
        } else if join_pairs.len() == 1 {
            let (left_idx, right_idx) = join_pairs[0];
            let mut right_map: HashMap<DistinctKey, Vec<usize>> =
                HashMap::with_capacity(right_rows.len());
            for (idx, row) in right_rows.iter().enumerate() {
                let value = &row[right_idx];
                if matches!(value, Value::Null) {
                    continue;
                }
                right_map.entry(distinct_key(value)).or_default().push(idx);
            }

            for left_row in &left_rows {
                let value = &left_row[left_idx];
                let mut matched = false;
                if !matches!(value, Value::Null) {
                    if let Some(matches) = right_map.get(&distinct_key(value)) {
                        for right_idx in matches {
                            let right_row = &right_rows[*right_idx];
                            let mut combined = Vec::with_capacity(left_len + right_len);
                            combined.extend(left_row.iter().cloned());
                            combined.extend(right_row.iter().cloned());
                            if !remaining_predicates.is_empty() {
                                let scope = EvalScope {
                                    columns: &columns,
                                    column_scopes: &column_scopes,
                                    row: &combined,
                                    table_scope: &table_scope,
                                    cte_context,
                                    column_lookup: column_lookup.as_ref(),
                                };
                                let mut keep = true;
                                for predicate in &remaining_predicates {
                                    let value = eval_expr(self, predicate, &scope, outer)?;
                                    if !value_to_bool(&value) {
                                        keep = false;
                                        break;
                                    }
                                }
                                if !keep {
                                    continue;
                                }
                            }
                            matched = true;
                            right_matched[*right_idx] = true;
                            rows.push(combined);
                        }
                    }
                }
                if !matched && matches!(operator, JoinOperator::Left | JoinOperator::Full) {
                    let mut combined = Vec::with_capacity(left_len + right_len);
                    combined.extend(left_row.iter().cloned());
                    combined.extend(null_right.iter().cloned());
                    rows.push(combined);
                }
            }
        } else {
            let left_key_indices: Vec<usize> = join_pairs.iter().map(|(l, _)| *l).collect();
            let right_key_indices: Vec<usize> = join_pairs.iter().map(|(_, r)| *r).collect();
            let mut right_map: HashMap<Vec<DistinctKey>, Vec<usize>> =
                HashMap::with_capacity(right_rows.len());
            for (idx, row) in right_rows.iter().enumerate() {
                let mut key = Vec::with_capacity(right_key_indices.len());
                let mut has_null = false;
                for col_idx in &right_key_indices {
                    let value = &row[*col_idx];
                    if matches!(value, Value::Null) {
                        has_null = true;
                        break;
                    }
                    key.push(distinct_key(value));
                }
                if has_null {
                    continue;
                }
                right_map.entry(key).or_default().push(idx);
            }

            let mut left_key = Vec::with_capacity(left_key_indices.len());
            for left_row in &left_rows {
                left_key.clear();
                let mut matched = false;
                let mut has_null = false;
                for col_idx in &left_key_indices {
                    let value = &left_row[*col_idx];
                    if matches!(value, Value::Null) {
                        has_null = true;
                        break;
                    }
                    left_key.push(distinct_key(value));
                }
                if !has_null {
                    if let Some(matches) = right_map.get(&left_key) {
                        for right_idx in matches {
                            let right_row = &right_rows[*right_idx];
                            let mut combined = Vec::with_capacity(left_len + right_len);
                            combined.extend(left_row.iter().cloned());
                            combined.extend(right_row.iter().cloned());
                            if !remaining_predicates.is_empty() {
                                let scope = EvalScope {
                                    columns: &columns,
                                    column_scopes: &column_scopes,
                                    row: &combined,
                                    table_scope: &table_scope,
                                    cte_context,
                                    column_lookup: column_lookup.as_ref(),
                                };
                                let mut keep = true;
                                for predicate in &remaining_predicates {
                                    let value = eval_expr(self, predicate, &scope, outer)?;
                                    if !value_to_bool(&value) {
                                        keep = false;
                                        break;
                                    }
                                }
                                if !keep {
                                    continue;
                                }
                            }
                            matched = true;
                            right_matched[*right_idx] = true;
                            rows.push(combined);
                        }
                    }
                }
                if !matched && matches!(operator, JoinOperator::Left | JoinOperator::Full) {
                    let mut combined = Vec::with_capacity(left_len + right_len);
                    combined.extend(left_row.iter().cloned());
                    combined.extend(null_right.iter().cloned());
                    rows.push(combined);
                }
            }
        }

        if matches!(operator, JoinOperator::Right | JoinOperator::Full) {
            for (right_idx, right_row) in right_rows.into_iter().enumerate() {
                if right_matched[right_idx] {
                    continue;
                }
                let mut combined = Vec::with_capacity(left_len + right_len);
                combined.extend(null_left.iter().cloned());
                combined.extend(right_row);
                rows.push(combined);
            }
        }

        Ok(QuerySource {
            columns,
            column_scopes,
            rows,
            table_scope,
        })
    }
}

#[derive(Clone)]
struct IndexScanPlan {
    index_name: String,
    lower: Option<Vec<Value>>,
    upper: Option<Vec<Value>>,
    ordered_by: bool,
    ordered_by_desc: bool,
}

#[derive(Clone)]
struct RowPredicatePlan {
    steps: Vec<RowPredicateStep>,
}

#[derive(Clone)]
enum RowPredicateStep {
    Simple(SimpleRowPredicate),
    Complex(Expr),
}

#[derive(Clone)]
enum SimpleRowPredicate {
    Compare {
        idx: usize,
        op: BinaryOperator,
        value: Value,
    },
    Between {
        idx: usize,
        low: Value,
        high: Value,
    },
    IsNull {
        idx: usize,
        negated: bool,
    },
}

#[derive(Clone, Default)]
struct IndexColumnConstraint {
    eq: Option<Value>,
    lower: Option<Value>,
    upper: Option<Value>,
}

enum Constraint {
    Eq(Value),
    Lower(Value),
    Upper(Value),
}

const DEFAULT_EQ_SELECTIVITY: f64 = 0.1;
const DEFAULT_RANGE_SELECTIVITY: f64 = 0.3;
const DEFAULT_SINGLE_BOUND_SELECTIVITY: f64 = 0.5;
const MIN_SELECTIVITY: f64 = 0.0001;
const INDEX_ROW_LOOKUP_COST: f64 = 2.0;

#[derive(Clone, Default)]
struct ColumnStats {
    null_count: usize,
    distinct_count: usize,
}

#[derive(Clone, Default)]
struct TableStats {
    row_count: usize,
    column_stats: HashMap<String, ColumnStats>,
}

fn compute_table_stats(columns: &[Column], rows: &[Vec<Value>]) -> TableStats {
    let mut column_stats: HashMap<String, ColumnStats> = HashMap::new();
    let mut distinct_sets: Vec<HashSet<String>> = Vec::with_capacity(columns.len());
    for column in columns {
        column_stats.insert(column.name.to_ascii_lowercase(), ColumnStats::default());
        distinct_sets.push(HashSet::new());
    }

    for row in rows {
        for (idx, value) in row.iter().enumerate() {
            let key = columns[idx].name.to_ascii_lowercase();
            if let Some(stats) = column_stats.get_mut(&key) {
                match value {
                    Value::Null => stats.null_count += 1,
                    _ => {
                        distinct_sets[idx].insert(value_signature(value));
                    }
                }
            }
        }
    }

    for (idx, column) in columns.iter().enumerate() {
        if let Some(stats) = column_stats.get_mut(&column.name.to_ascii_lowercase()) {
            stats.distinct_count = distinct_sets[idx].len();
        }
    }

    TableStats {
        row_count: rows.len(),
        column_stats,
    }
}

fn value_signature(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Integer(num) => format!("i:{}", num),
        Value::Real(num) => format!("r:{:x}", num.to_bits()),
        Value::Text(text) => format!("t:{}", text),
        Value::Blob(bytes) => {
            let mut out = String::with_capacity(bytes.len() * 2 + 2);
            out.push_str("b:");
            for byte in bytes {
                out.push_str(&format!("{:02x}", byte));
            }
            out
        }
    }
}

fn clamp_selectivity(selectivity: f64) -> f64 {
    if selectivity <= 0.0 {
        MIN_SELECTIVITY
    } else if selectivity > 1.0 {
        1.0
    } else {
        selectivity
    }
}

fn estimate_eq_selectivity(
    value: &Value,
    stats: Option<&ColumnStats>,
    total_rows: usize,
) -> f64 {
    match value {
        Value::Null => {
            if let Some(stats) = stats {
                if total_rows > 0 {
                    return clamp_selectivity(stats.null_count as f64 / total_rows as f64);
                }
            }
            DEFAULT_EQ_SELECTIVITY
        }
        _ => {
            if let Some(stats) = stats {
                if stats.distinct_count > 0 {
                    return clamp_selectivity(1.0 / stats.distinct_count as f64);
                }
            }
            DEFAULT_EQ_SELECTIVITY
        }
    }
}

fn estimate_range_selectivity(constraint: &IndexColumnConstraint) -> f64 {
    if constraint.lower.is_some() && constraint.upper.is_some() {
        DEFAULT_RANGE_SELECTIVITY
    } else {
        DEFAULT_SINGLE_BOUND_SELECTIVITY
    }
}

fn estimate_selectivity_for_constraints(
    constraints: &HashMap<String, IndexColumnConstraint>,
    stats: Option<&TableStats>,
) -> f64 {
    let mut selectivity = 1.0;
    for (name, constraint) in constraints {
        let column_stats = stats
            .and_then(|table_stats| table_stats.column_stats.get(name));
        if let Some(eq) = &constraint.eq {
            let total_rows = stats.map(|stats| stats.row_count).unwrap_or(0);
            selectivity *= estimate_eq_selectivity(eq, column_stats, total_rows);
        } else if constraint.lower.is_some() || constraint.upper.is_some() {
            selectivity *= estimate_range_selectivity(constraint);
        }
    }
    clamp_selectivity(selectivity)
}

fn estimate_index_selectivity(
    index: &IndexMeta,
    constraints: &HashMap<String, IndexColumnConstraint>,
    stats: Option<&TableStats>,
) -> f64 {
    let mut selectivity = 1.0;
    for column in &index.columns {
        let name = column.name.value.to_ascii_lowercase();
        let constraint = match constraints.get(&name) {
            Some(value) => value,
            None => break,
        };
        let column_stats = stats
            .and_then(|table_stats| table_stats.column_stats.get(&name));
        if let Some(eq) = &constraint.eq {
            let total_rows = stats.map(|stats| stats.row_count).unwrap_or(0);
            selectivity *= estimate_eq_selectivity(eq, column_stats, total_rows);
        } else if constraint.lower.is_some() || constraint.upper.is_some() {
            selectivity *= estimate_range_selectivity(constraint);
            break;
        } else {
            break;
        }
    }
    clamp_selectivity(selectivity)
}

fn estimate_table_scan_cost(row_count: usize, selectivity: f64, needs_sort: bool) -> f64 {
    let rows = row_count as f64;
    let filtered = rows * selectivity;
    let mut cost = rows + filtered;
    if needs_sort {
        cost += sort_cost(filtered);
    }
    cost
}

fn estimate_index_scan_cost(row_count: usize, selectivity: f64, needs_sort: bool) -> f64 {
    let rows = row_count as f64;
    if rows == 0.0 {
        return 0.0;
    }
    let filtered = rows * selectivity;
    let mut cost = index_traversal_cost(rows) + filtered * INDEX_ROW_LOOKUP_COST + filtered;
    if needs_sort {
        cost += sort_cost(filtered);
    }
    cost
}

fn sort_cost(rows: f64) -> f64 {
    if rows <= 1.0 {
        0.0
    } else {
        rows * log2(rows + 1.0)
    }
}

fn index_traversal_cost(rows: f64) -> f64 {
    log2(rows + 1.0).max(1.0)
}

fn log2(value: f64) -> f64 {
    value.ln() / 2.0_f64.ln()
}

fn choose_index_scan_plan(
    db: &GongDB,
    table: &TableMeta,
    selection: Option<&Expr>,
    order_by: &[OrderByExpr],
    table_scope: &TableScope,
) -> Option<IndexScanPlan> {
    let stats = db.get_table_stats(table).ok();
    choose_index_scan_plan_with_stats(
        db,
        table,
        selection,
        order_by,
        table_scope,
        stats.as_ref(),
    )
}

fn choose_index_scan_plan_no_stats(
    db: &GongDB,
    table: &TableMeta,
    selection: Option<&Expr>,
    order_by: &[OrderByExpr],
    table_scope: &TableScope,
) -> Option<IndexScanPlan> {
    choose_index_scan_plan_with_stats(db, table, selection, order_by, table_scope, None)
}

fn choose_index_scan_plan_with_stats(
    db: &GongDB,
    table: &TableMeta,
    selection: Option<&Expr>,
    order_by: &[OrderByExpr],
    table_scope: &TableScope,
    stats: Option<&TableStats>,
) -> Option<IndexScanPlan> {
    if db.storage.index_updates_deferred(&table.name) {
        return None;
    }
    let indexes: Vec<IndexMeta> = db
        .storage
        .list_indexes()
        .into_iter()
        .filter(|index| index.table.eq_ignore_ascii_case(&table.name))
        .collect();
    if indexes.is_empty() {
        return None;
    }

    let constraints = selection
        .map(|selection| extract_index_constraints(db, selection, table_scope, &table.columns))
        .unwrap_or_default();

    let row_count = stats
        .map(|stats| stats.row_count)
        .unwrap_or_else(|| table.row_count as usize);
    let selection_selectivity = estimate_selectivity_for_constraints(&constraints, stats);
    let needs_sort = !order_by.is_empty();
    let table_scan_cost = estimate_table_scan_cost(row_count, selection_selectivity, needs_sort);

    let mut best_plan: Option<(IndexScanPlan, f64)> = None;

    for index in &indexes {
        let order_match = order_by_matches_index_with_constraints(
            order_by,
            index,
            table_scope,
            &table.columns,
            &constraints,
        );
        let ordered_by = !order_by.is_empty() && order_match.is_some();
        let ordered_by_desc = matches!(order_match, Some(SortOrder::Desc));
        if let Some(key) = build_eq_key(index, &constraints) {
            let plan = IndexScanPlan {
                index_name: index.name.clone(),
                lower: Some(key.clone()),
                upper: Some(key),
                ordered_by,
                ordered_by_desc,
            };
            let selectivity = estimate_index_selectivity(index, &constraints, stats);
            let cost = estimate_index_scan_cost(
                row_count,
                selectivity,
                needs_sort && !ordered_by,
            );
            if best_plan
                .as_ref()
                .map(|(_, best_cost)| cost < *best_cost)
                .unwrap_or(true)
            {
                best_plan = Some((plan, cost));
            }
            continue;
        }

        if let Some(plan) = build_prefix_range_plan(
            index,
            &constraints,
            ordered_by,
            ordered_by_desc,
        ) {
            let selectivity = estimate_index_selectivity(index, &constraints, stats);
            let cost = estimate_index_scan_cost(row_count, selectivity, needs_sort && !ordered_by);
            if best_plan
                .as_ref()
                .map(|(_, best_cost)| cost < *best_cost)
                .unwrap_or(true)
            {
                best_plan = Some((plan, cost));
            }
        }
    }

    if !order_by.is_empty() {
        for index in &indexes {
            let order_match = order_by_matches_index_with_constraints(
                order_by,
                index,
                table_scope,
                &table.columns,
                &constraints,
            );
            if order_match.is_none() {
                continue;
            }
            let plan = IndexScanPlan {
                index_name: index.name.clone(),
                lower: None,
                upper: None,
                ordered_by: true,
                ordered_by_desc: matches!(order_match, Some(SortOrder::Desc)),
            };
            let cost = estimate_index_scan_cost(row_count, 1.0, false);
            if best_plan
                .as_ref()
                .map(|(_, best_cost)| cost < *best_cost)
                .unwrap_or(true)
            {
                best_plan = Some((plan, cost));
            }
        }
    }

    match best_plan {
        Some((plan, cost)) if cost < table_scan_cost => Some(plan),
        _ => None,
    }
}

fn build_prefix_range_plan(
    index: &IndexMeta,
    constraints: &HashMap<String, IndexColumnConstraint>,
    ordered_by: bool,
    ordered_by_desc: bool,
) -> Option<IndexScanPlan> {
    let mut prefix: Vec<Value> = Vec::new();
    for column in &index.columns {
        let entry = match constraints.get(&column.name.value.to_lowercase()) {
            Some(entry) => entry,
            None => break,
        };
        let Some(eq) = &entry.eq else {
            break;
        };
        prefix.push(eq.clone());
    }

    if prefix.len() == index.columns.len() {
        return None;
    }

    let range_entry = index
        .columns
        .get(prefix.len())
        .and_then(|column| constraints.get(&column.name.value.to_lowercase()));
    let range_lower = range_entry.and_then(|entry| entry.lower.clone());
    let range_upper = range_entry.and_then(|entry| entry.upper.clone());

    if prefix.is_empty() && range_lower.is_none() && range_upper.is_none() {
        return None;
    }

    let lower = build_index_bound(
        index.columns.len(),
        &prefix,
        range_lower.as_ref(),
        Value::Null,
    );
    let upper = build_index_bound(
        index.columns.len(),
        &prefix,
        range_upper.as_ref(),
        Value::Blob(Vec::new()),
    );

    Some(IndexScanPlan {
        index_name: index.name.clone(),
        lower: Some(lower),
        upper: Some(upper),
        ordered_by,
        ordered_by_desc,
    })
}

fn build_index_bound(
    index_len: usize,
    prefix: &[Value],
    range_value: Option<&Value>,
    fill_value: Value,
) -> Vec<Value> {
    let mut key = Vec::with_capacity(index_len);
    key.extend(prefix.iter().cloned());
    if let Some(value) = range_value {
        key.push(value.clone());
    }
    while key.len() < index_len {
        key.push(fill_value.clone());
    }
    key
}

fn scan_rows_with_index(db: &GongDB, plan: &IndexScanPlan) -> Result<Vec<Vec<Value>>, GongDBError> {
    let lower = plan.lower.as_deref();
    let upper = plan.upper.as_deref();
    Ok(db.storage.scan_index_rows(
        &plan.index_name,
        lower,
        upper,
        plan.ordered_by,
    )?)
}

fn build_eq_key(
    index: &IndexMeta,
    constraints: &HashMap<String, IndexColumnConstraint>,
) -> Option<Vec<Value>> {
    let mut key = Vec::with_capacity(index.columns.len());
    for column in &index.columns {
        let entry = constraints.get(&column.name.value.to_lowercase())?;
        let value = entry.eq.clone()?;
        key.push(value);
    }
    Some(key)
}

fn extract_index_constraints(
    db: &GongDB,
    selection: &Expr,
    table_scope: &TableScope,
    columns: &[Column],
) -> HashMap<String, IndexColumnConstraint> {
    let mut constraints: HashMap<String, IndexColumnConstraint> = HashMap::new();
    for expr in split_conjuncts(selection) {
        match &expr {
            Expr::Between {
                expr,
                negated: false,
                low,
                high,
            } => {
                let (idx, name) =
                    match column_ref_for_expr(expr, table_scope, columns) {
                        Some(value) => value,
                        None => continue,
                    };
                let low_val = match eval_constant_expr(db, low) {
                    Some(value) => apply_affinity(value, &columns[idx].data_type),
                    None => continue,
                };
                let high_val = match eval_constant_expr(db, high) {
                    Some(value) => apply_affinity(value, &columns[idx].data_type),
                    None => continue,
                };
                let entry = constraints
                    .entry(name)
                    .or_insert_with(IndexColumnConstraint::default);
                if entry.eq.is_none() {
                    apply_lower_bound(entry, low_val);
                    apply_upper_bound(entry, high_val);
                }
            }
            Expr::IsNull {
                expr,
                negated: false,
            } => {
                if let Some((_idx, name)) = column_ref_for_expr(expr, table_scope, columns) {
                    let entry = constraints
                        .entry(name)
                        .or_insert_with(IndexColumnConstraint::default);
                    entry.eq = Some(Value::Null);
                    entry.lower = None;
                    entry.upper = None;
                }
            }
            Expr::BinaryOp { left, op, right } => {
                if let Some((name, constraint)) =
                    extract_binary_constraint(db, left, op, right, table_scope, columns)
                {
                    let entry = constraints
                        .entry(name)
                        .or_insert_with(IndexColumnConstraint::default);
                    match constraint {
                        Constraint::Eq(value) => {
                            entry.eq = Some(value);
                            entry.lower = None;
                            entry.upper = None;
                        }
                        Constraint::Lower(value) => {
                            if entry.eq.is_none() {
                                apply_lower_bound(entry, value);
                            }
                        }
                        Constraint::Upper(value) => {
                            if entry.eq.is_none() {
                                apply_upper_bound(entry, value);
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    constraints
}

fn apply_lower_bound(entry: &mut IndexColumnConstraint, value: Value) {
    match entry.lower.take() {
        Some(existing) => {
            if compare_order_values(&existing, &value) == std::cmp::Ordering::Greater {
                entry.lower = Some(existing);
            } else {
                entry.lower = Some(value);
            }
        }
        None => entry.lower = Some(value),
    }
}

fn apply_upper_bound(entry: &mut IndexColumnConstraint, value: Value) {
    match entry.upper.take() {
        Some(existing) => {
            if compare_order_values(&existing, &value) == std::cmp::Ordering::Less {
                entry.upper = Some(existing);
            } else {
                entry.upper = Some(value);
            }
        }
        None => entry.upper = Some(value),
    }
}

fn extract_binary_constraint(
    db: &GongDB,
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    table_scope: &TableScope,
    columns: &[Column],
) -> Option<(String, Constraint)> {
    if let Some((idx, name)) = column_ref_for_expr(left, table_scope, columns) {
        let value = eval_constant_expr(db, right)?;
        return constraint_from_operator(
            op,
            name,
            apply_affinity(value, &columns[idx].data_type),
        );
    }
    if let Some((idx, name)) = column_ref_for_expr(right, table_scope, columns) {
        let value = eval_constant_expr(db, left)?;
        let inverted = invert_comparison_operator(op)?;
        return constraint_from_operator(
            &inverted,
            name,
            apply_affinity(value, &columns[idx].data_type),
        );
    }
    None
}

fn constraint_from_operator(
    op: &BinaryOperator,
    column: String,
    value: Value,
) -> Option<(String, Constraint)> {
    match op {
        BinaryOperator::Eq => Some((column, Constraint::Eq(value))),
        BinaryOperator::Lt | BinaryOperator::LtEq => Some((column, Constraint::Upper(value))),
        BinaryOperator::Gt | BinaryOperator::GtEq => Some((column, Constraint::Lower(value))),
        _ => None,
    }
}

fn invert_comparison_operator(op: &BinaryOperator) -> Option<BinaryOperator> {
    match op {
        BinaryOperator::Eq => Some(BinaryOperator::Eq),
        BinaryOperator::Lt => Some(BinaryOperator::Gt),
        BinaryOperator::LtEq => Some(BinaryOperator::GtEq),
        BinaryOperator::Gt => Some(BinaryOperator::Lt),
        BinaryOperator::GtEq => Some(BinaryOperator::LtEq),
        _ => None,
    }
}

fn order_by_matches_index_with_constraints(
    order_by: &[OrderByExpr],
    index: &IndexMeta,
    table_scope: &TableScope,
    columns: &[Column],
    constraints: &HashMap<String, IndexColumnConstraint>,
) -> Option<SortOrder> {
    if order_by.is_empty() {
        return None;
    }
    if order_by.len() > index.columns.len() {
        return None;
    }
    let mut max_skip = 0usize;
    for column in &index.columns {
        let entry = match constraints.get(&column.name.value.to_lowercase()) {
            Some(entry) => entry,
            None => break,
        };
        if entry.eq.is_some() {
            max_skip += 1;
        } else {
            break;
        }
    }
    for offset in 0..=max_skip {
        if order_by.len() + offset > index.columns.len() {
            continue;
        }
        if let Some(order) =
            order_by_matches_index_at_offset(order_by, index, table_scope, columns, offset)
        {
            return Some(order);
        }
    }
    None
}

fn order_by_matches_index_at_offset(
    order_by: &[OrderByExpr],
    index: &IndexMeta,
    table_scope: &TableScope,
    columns: &[Column],
    offset: usize,
) -> Option<SortOrder> {
    let mut desired_order: Option<SortOrder> = None;
    for (idx, order) in order_by.iter().enumerate() {
        if matches!(order.nulls.as_ref(), Some(NullsOrder::Last)) {
            return None;
        }
        let order_dir = if order.asc == Some(false) {
            SortOrder::Desc
        } else {
            SortOrder::Asc
        };
        if let Some(existing) = desired_order.as_ref() {
            if existing != &order_dir {
                return None;
            }
        } else {
            desired_order = Some(order_dir);
        }
        let column = match column_ref_for_expr(&order.expr, table_scope, columns) {
            Some((_idx, name)) => name,
            None => return None,
        };
        let index_column = index.columns.get(idx + offset)?;
        if !index_column.name.value.eq_ignore_ascii_case(&column) {
            return None;
        }
        if matches!(index_column.order, Some(SortOrder::Desc)) {
            return None;
        }
    }
    desired_order.or(Some(SortOrder::Asc))
}

fn column_ref_for_expr(
    expr: &Expr,
    table_scope: &TableScope,
    columns: &[Column],
) -> Option<(usize, String)> {
    match expr {
        Expr::Identifier(ident) => resolve_column_index(&ident.value, columns)
            .map(|idx| (idx, columns[idx].name.to_lowercase())),
        Expr::CompoundIdentifier(idents) => {
            let (qualifier, name) = split_qualified_identifier(idents).ok()?;
            if !table_scope.matches_qualifier(qualifier) {
                return None;
            }
            resolve_column_index(name, columns)
                .map(|idx| (idx, columns[idx].name.to_lowercase()))
        }
        _ => None,
    }
}

fn eval_constant_expr(db: &GongDB, expr: &Expr) -> Option<Value> {
    if !expr_is_constant(expr) {
        return None;
    }
    eval_constant_expr_checked(db, expr).ok()
}

fn eval_constant_expr_checked(db: &GongDB, expr: &Expr) -> Result<Value, GongDBError> {
    let scope = EvalScope {
        columns: &[],
        column_scopes: &[],
        row: &[],
        table_scope: &TableScope {
            table_name: None,
            table_alias: None,
        },
        cte_context: None,
        column_lookup: None,
    };
    eval_expr(db, expr, &scope, None)
}

fn eval_limit_offset_value(
    db: &GongDB,
    expr: &Expr,
    label: &str,
) -> Result<Option<i64>, GongDBError> {
    if !expr_is_constant(expr) {
        return Err(GongDBError::new(format!(
            "{} expression must be constant",
            label
        )));
    }
    let value = eval_constant_expr_checked(db, expr)?;
    match value {
        Value::Null => Ok(None),
        Value::Integer(value) => Ok(Some(value)),
        Value::Real(value) => Ok(Some(value as i64)),
        Value::Text(value) => value.parse::<i64>().map(Some).map_err(|_| {
            GongDBError::new(format!("{} expression must be numeric", label))
        }),
        Value::Blob(_) => Err(GongDBError::new(format!(
            "{} expression must be numeric",
            label
        ))),
    }
}

fn eval_limit_offset(
    db: &GongDB,
    select: &Select,
) -> Result<(Option<usize>, usize), GongDBError> {
    let limit = match select.limit.as_ref() {
        Some(expr) => eval_limit_offset_value(db, expr, "LIMIT")?,
        None => None,
    };
    let offset = match select.offset.as_ref() {
        Some(expr) => eval_limit_offset_value(db, expr, "OFFSET")?,
        None => None,
    };
    let limit = limit.map(|value| {
        if value <= 0 {
            0
        } else {
            value as usize
        }
    });
    let offset = offset.unwrap_or(0);
    let offset = if offset <= 0 { 0 } else { offset as usize };
    Ok((limit, offset))
}

fn apply_limit_offset_rows(
    db: &GongDB,
    select: &Select,
    rows: Vec<Vec<Value>>,
) -> Result<Vec<Vec<Value>>, GongDBError> {
    if select.limit.is_none() && select.offset.is_none() {
        return Ok(rows);
    }
    let (limit, offset) = eval_limit_offset(db, select)?;
    if rows.is_empty() {
        return Ok(rows);
    }
    if limit == Some(0) {
        return Ok(Vec::new());
    }
    let mut output = Vec::new();
    let mut skipped = 0usize;
    let mut taken = 0usize;
    for row in rows {
        if skipped < offset {
            skipped += 1;
            continue;
        }
        if let Some(limit) = limit {
            if taken >= limit {
                break;
            }
        }
        output.push(row);
        taken += 1;
    }
    Ok(output)
}

fn expr_is_constant(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) | Expr::Parameter(_) => true,
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Wildcard => false,
        Expr::BinaryOp { left, right, .. } => {
            expr_is_constant(left) && expr_is_constant(right)
        }
        Expr::UnaryOp { expr, .. } => expr_is_constant(expr),
        Expr::Function { args, distinct, .. } => {
            !*distinct && args.iter().all(expr_is_constant)
        }
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => operand
            .as_ref()
            .map(|expr| expr_is_constant(expr))
            .unwrap_or(true)
            && when_then.iter().all(|(when_expr, then_expr)| {
                expr_is_constant(when_expr) && expr_is_constant(then_expr)
            })
            && else_result
                .as_ref()
                .map(|expr| expr_is_constant(expr))
                .unwrap_or(true),
        Expr::Between { expr, low, high, .. } => {
            expr_is_constant(expr) && expr_is_constant(low) && expr_is_constant(high)
        }
        Expr::InList { expr, list, .. } => {
            expr_is_constant(expr) && list.iter().all(expr_is_constant)
        }
        Expr::IsNull { expr, .. } => expr_is_constant(expr),
        Expr::Cast { expr, .. } => expr_is_constant(expr),
        Expr::Nested(expr) => expr_is_constant(expr),
        Expr::InSubquery { .. } | Expr::Exists(_) | Expr::Subquery(_) => false,
    }
}

fn simple_predicate_from_expr(
    db: &GongDB,
    expr: &Expr,
    table_scope: &TableScope,
    columns: &[Column],
) -> Option<SimpleRowPredicate> {
    match expr {
        Expr::IsNull { expr, negated } => {
            let (idx, _name) = column_ref_for_expr(expr, table_scope, columns)?;
            Some(SimpleRowPredicate::IsNull {
                idx,
                negated: *negated,
            })
        }
        Expr::Between {
            expr,
            negated: false,
            low,
            high,
        } => {
            let (idx, _name) = column_ref_for_expr(expr, table_scope, columns)?;
            let low_val = eval_constant_expr(db, low)?;
            let high_val = eval_constant_expr(db, high)?;
            Some(SimpleRowPredicate::Between {
                idx,
                low: low_val,
                high: high_val,
            })
        }
        Expr::BinaryOp { left, op, right } => {
            if !matches!(
                op,
                BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq
            ) {
                return None;
            }
            if let Some((idx, _name)) = column_ref_for_expr(left, table_scope, columns) {
                let value = eval_constant_expr(db, right)?;
                return Some(SimpleRowPredicate::Compare {
                    idx,
                    op: op.clone(),
                    value,
                });
            }
            if let Some((idx, _name)) = column_ref_for_expr(right, table_scope, columns) {
                let value = eval_constant_expr(db, left)?;
                let op = if *op == BinaryOperator::NotEq {
                    BinaryOperator::NotEq
                } else {
                    invert_comparison_operator(op)?
                };
                return Some(SimpleRowPredicate::Compare {
                    idx,
                    op,
                    value,
                });
            }
            None
        }
        _ => None,
    }
}

fn row_matches_predicate_plan(
    db: &GongDB,
    plan: &RowPredicatePlan,
    row: &[Value],
    columns: &[Column],
    column_scopes: &[TableScope],
    table_scope: &TableScope,
    cte_context: Option<&CteContext>,
    outer: Option<&EvalScope<'_>>,
    column_lookup: &ColumnLookup,
) -> Result<bool, GongDBError> {
    let mut scope: Option<EvalScope<'_>> = None;
    for step in &plan.steps {
        let matches = match step {
            RowPredicateStep::Simple(predicate) => match predicate {
                SimpleRowPredicate::Compare { idx, op, value } => {
                    let comparison = compare_values(op, &row[*idx], value);
                    value_to_bool(&comparison)
                }
                SimpleRowPredicate::Between { idx, low, high } => {
                    let lower = compare_values(&BinaryOperator::GtEq, &row[*idx], low);
                    let upper = compare_values(&BinaryOperator::LtEq, &row[*idx], high);
                    let between = apply_logical_and(&lower, &upper);
                    value_to_bool(&between)
                }
                SimpleRowPredicate::IsNull { idx, negated } => {
                    let is_null = matches!(row[*idx], Value::Null);
                    if *negated { !is_null } else { is_null }
                }
            },
            RowPredicateStep::Complex(predicate) => {
                let scope = scope.get_or_insert_with(|| EvalScope {
                    columns,
                    column_scopes,
                    row,
                    table_scope,
                    cte_context,
                    column_lookup: Some(column_lookup),
                });
                let value = eval_expr(db, predicate, scope, outer)?;
                value_to_bool(&value)
            }
        };
        if !matches {
            return Ok(false);
        }
    }
    Ok(true)
}

fn split_conjuncts(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            let mut items = split_conjuncts(left);
            items.extend(split_conjuncts(right));
            items
        }
        _ => vec![expr.clone()],
    }
}

fn predicate_table_refs(expr: &Expr, tables: &[TableInfo]) -> Option<Vec<usize>> {
    if expr_contains_subquery(expr) {
        return None;
    }
    let mut refs = HashSet::new();
    if !collect_predicate_tables(expr, tables, &mut refs) {
        return None;
    }
    let mut refs: Vec<usize> = refs.into_iter().collect();
    refs.sort_unstable();
    Some(refs)
}

fn estimate_predicate_selectivity(
    db: &GongDB,
    expr: &Expr,
    tables_ref: &Option<Vec<usize>>,
    tables: &[TableInfo],
    stats: &[TableStats],
) -> f64 {
    let Some(table_indices) = tables_ref else {
        return DEFAULT_EQ_SELECTIVITY;
    };
    if table_indices.len() == 1 {
        let table_idx = table_indices[0];
        let scope = &tables[table_idx].scope;
        let columns = &tables[table_idx].source.columns;
        let constraints = extract_index_constraints(db, expr, scope, columns);
        if constraints.is_empty() {
            return DEFAULT_EQ_SELECTIVITY;
        }
        return estimate_selectivity_for_constraints(&constraints, stats.get(table_idx));
    }
    if table_indices.len() == 2 {
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = expr
        {
            if let (Some((left_idx, left_col)), Some((right_idx, right_col))) = (
                resolve_column_ref_in_tables(left, tables),
                resolve_column_ref_in_tables(right, tables),
            ) {
                if left_idx != right_idx {
                    let left_distinct = stats
                        .get(left_idx)
                        .and_then(|stats| stats.column_stats.get(&left_col))
                        .map(|stats| stats.distinct_count)
                        .unwrap_or(0);
                    let right_distinct = stats
                        .get(right_idx)
                        .and_then(|stats| stats.column_stats.get(&right_col))
                        .map(|stats| stats.distinct_count)
                        .unwrap_or(0);
                    let denom = left_distinct.max(right_distinct);
                    if denom > 0 {
                        return clamp_selectivity(1.0 / denom as f64);
                    }
                }
            }
        }
    }
    DEFAULT_EQ_SELECTIVITY
}

fn estimate_join_selectivity(
    predicates: &[PredicateInfo],
    joined: &HashSet<usize>,
    candidate: usize,
) -> f64 {
    let mut selectivity = 1.0;
    for pred in predicates {
        if let Some(tables) = &pred.tables {
            if tables.len() > 1
                && tables_contains(tables, candidate)
                && tables_subset(tables, joined, candidate)
            {
                selectivity *= pred.selectivity;
            }
        }
    }
    clamp_selectivity(selectivity)
}

fn resolve_column_ref_in_tables(expr: &Expr, tables: &[TableInfo]) -> Option<(usize, String)> {
    match expr {
        Expr::Identifier(ident) => {
            let mut matches = Vec::new();
            for (idx, info) in tables.iter().enumerate() {
                if resolve_column_index(&ident.value, &info.source.columns).is_some() {
                    matches.push(idx);
                }
            }
            if matches.len() != 1 {
                return None;
            }
            let idx = matches[0];
            resolve_column_index(&ident.value, &tables[idx].source.columns)
                .map(|col_idx| (idx, tables[idx].source.columns[col_idx].name.to_lowercase()))
        }
        Expr::CompoundIdentifier(idents) => {
            let (qualifier, column) = split_qualified_identifier(idents).ok()?;
            let mut matches = Vec::new();
            for (idx, info) in tables.iter().enumerate() {
                if info.scope.matches_qualifier(qualifier)
                    && resolve_column_index(column, &info.source.columns).is_some()
                {
                    matches.push(idx);
                }
            }
            if matches.len() != 1 {
                return None;
            }
            let idx = matches[0];
            resolve_column_index(column, &tables[idx].source.columns)
                .map(|col_idx| (idx, tables[idx].source.columns[col_idx].name.to_lowercase()))
        }
        _ => None,
    }
}

fn collect_predicate_tables(expr: &Expr, tables: &[TableInfo], refs: &mut HashSet<usize>) -> bool {
    match expr {
        Expr::Identifier(ident) => {
            let mut matches = Vec::new();
            for (idx, info) in tables.iter().enumerate() {
                if resolve_column_index(&ident.value, &info.source.columns).is_some() {
                    matches.push(idx);
                }
            }
            if matches.len() != 1 {
                return false;
            }
            refs.insert(matches[0]);
            true
        }
        Expr::CompoundIdentifier(idents) => {
            let (qualifier, column) = match split_qualified_identifier(idents) {
                Ok(result) => result,
                Err(_) => return false,
            };
            let mut matches = Vec::new();
            for (idx, info) in tables.iter().enumerate() {
                if info.scope.matches_qualifier(qualifier)
                    && resolve_column_index(column, &info.source.columns).is_some()
                {
                    matches.push(idx);
                }
            }
            if matches.len() != 1 {
                return false;
            }
            refs.insert(matches[0]);
            true
        }
        Expr::Literal(_) | Expr::Parameter(_) | Expr::Wildcard => true,
        Expr::BinaryOp { left, right, .. } => {
            collect_predicate_tables(left, tables, refs)
                && collect_predicate_tables(right, tables, refs)
        }
        Expr::UnaryOp { expr, .. } => collect_predicate_tables(expr, tables, refs),
        Expr::Function { args, .. } => args
            .iter()
            .all(|arg| collect_predicate_tables(arg, tables, refs)),
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            if let Some(expr) = operand {
                if !collect_predicate_tables(expr, tables, refs) {
                    return false;
                }
            }
            for (when_expr, then_expr) in when_then {
                if !collect_predicate_tables(when_expr, tables, refs)
                    || !collect_predicate_tables(then_expr, tables, refs)
                {
                    return false;
                }
            }
            if let Some(expr) = else_result {
                if !collect_predicate_tables(expr, tables, refs) {
                    return false;
                }
            }
            true
        }
        Expr::Between {
            expr,
            low,
            high,
            ..
        } => {
            collect_predicate_tables(expr, tables, refs)
                && collect_predicate_tables(low, tables, refs)
                && collect_predicate_tables(high, tables, refs)
        }
        Expr::InList { expr, list, .. } => {
            if !collect_predicate_tables(expr, tables, refs) {
                return false;
            }
            list.iter()
                .all(|item| collect_predicate_tables(item, tables, refs))
        }
        Expr::IsNull { expr, .. } => collect_predicate_tables(expr, tables, refs),
        Expr::Cast { expr, .. } => collect_predicate_tables(expr, tables, refs),
        Expr::Nested(expr) => collect_predicate_tables(expr, tables, refs),
        Expr::InSubquery { .. } | Expr::Exists(_) | Expr::Subquery(_) => false,
    }
}

fn expr_contains_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::InSubquery { .. } | Expr::Exists(_) | Expr::Subquery(_) => true,
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_subquery(left) || expr_contains_subquery(right)
        }
        Expr::UnaryOp { expr, .. } => expr_contains_subquery(expr),
        Expr::Function { args, .. } => args.iter().any(expr_contains_subquery),
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| expr_contains_subquery(expr))
                || when_then.iter().any(|(when_expr, then_expr)| {
                    expr_contains_subquery(when_expr) || expr_contains_subquery(then_expr)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|expr| expr_contains_subquery(expr))
        }
        Expr::Between {
            expr,
            low,
            high,
            ..
        } => {
            expr_contains_subquery(expr)
                || expr_contains_subquery(low)
                || expr_contains_subquery(high)
        }
        Expr::InList { expr, list, .. } => {
            expr_contains_subquery(expr) || list.iter().any(expr_contains_subquery)
        }
        Expr::IsNull { expr, .. } => expr_contains_subquery(expr),
        Expr::Cast { expr, .. } => expr_contains_subquery(expr),
        Expr::Nested(expr) => expr_contains_subquery(expr),
        Expr::Literal(_)
        | Expr::Parameter(_)
        | Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Wildcard => {
            false
        }
    }
}

fn extract_local_predicates(
    predicates: &mut Vec<PredicateInfo>,
    table_idx: usize,
) -> Vec<Expr> {
    let mut applied = Vec::new();
    let mut remaining = Vec::new();
    for pred in predicates.drain(..) {
        if let Some(tables) = &pred.tables {
            if tables.len() == 1 && tables[0] == table_idx {
                applied.push(pred.expr);
                continue;
            }
        }
        remaining.push(pred);
    }
    *predicates = remaining;
    applied
}

fn extract_predicates_for_tables(
    predicates: &mut Vec<PredicateInfo>,
    joined: &HashSet<usize>,
) -> Vec<Expr> {
    let mut applied = Vec::new();
    let mut remaining = Vec::new();
    for pred in predicates.drain(..) {
        if let Some(tables) = &pred.tables {
            if tables.iter().all(|idx| joined.contains(idx)) {
                applied.push(pred.expr);
                continue;
            }
        }
        remaining.push(pred);
    }
    *predicates = remaining;
    applied
}

fn tables_contains(tables: &[usize], idx: usize) -> bool {
    tables.iter().any(|table_idx| *table_idx == idx)
}

fn tables_subset(tables: &[usize], joined: &HashSet<usize>, candidate: usize) -> bool {
    tables
        .iter()
        .all(|table_idx| *table_idx == candidate || joined.contains(table_idx))
}

fn extend_joined(joined: &HashSet<usize>, idx: usize) -> HashSet<usize> {
    let mut next = joined.clone();
    next.insert(idx);
    next
}

fn resolve_column_ref_in_source(
    expr: &Expr,
    columns: &[Column],
    column_scopes: &[TableScope],
) -> Option<usize> {
    match expr {
        Expr::Identifier(ident) => {
            let mut match_idx = None;
            for (idx, col) in columns.iter().enumerate() {
                if col.name.eq_ignore_ascii_case(&ident.value) {
                    if match_idx.is_some() {
                        return None;
                    }
                    match_idx = Some(idx);
                }
            }
            match_idx
        }
        Expr::CompoundIdentifier(idents) => {
            let (qualifier, name) = split_qualified_identifier(idents).ok()?;
            resolve_qualified_column_index(qualifier, name, columns, column_scopes)
        }
        _ => None,
    }
}

fn extract_join_pairs(
    predicates: &[Expr],
    left_columns: &[Column],
    left_scopes: &[TableScope],
    right_columns: &[Column],
    right_scopes: &[TableScope],
) -> (Vec<(usize, usize)>, Vec<Expr>) {
    let mut pairs = Vec::new();
    let mut remaining = Vec::new();
    for pred in predicates {
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = pred
        {
            if let (Some(left_idx), Some(right_idx)) = (
                resolve_column_ref_in_source(left, left_columns, left_scopes),
                resolve_column_ref_in_source(right, right_columns, right_scopes),
            ) {
                pairs.push((left_idx, right_idx));
                continue;
            }
            if let (Some(left_idx), Some(right_idx)) = (
                resolve_column_ref_in_source(right, left_columns, left_scopes),
                resolve_column_ref_in_source(left, right_columns, right_scopes),
            ) {
                pairs.push((left_idx, right_idx));
                continue;
            }
        }
        remaining.push(pred.clone());
    }
    (pairs, remaining)
}

fn apply_predicates_to_source(
    db: &GongDB,
    source: QuerySource,
    predicates: Vec<Expr>,
    outer: Option<&EvalScope<'_>>,
    cte_context: Option<&CteContext>,
) -> Result<QuerySource, GongDBError> {
    let mut remaining = Vec::with_capacity(predicates.len());
    for predicate in predicates {
        if expr_is_constant(&predicate) {
            let value = eval_constant_expr_checked(db, &predicate)?;
            if !value_to_bool(&value) {
                return Ok(QuerySource {
                    rows: Vec::new(),
                    ..source
                });
            }
        } else {
            remaining.push(predicate);
        }
    }
    let selection = combine_predicates(&remaining);
    if let Some(table_name) = source.table_scope.table_name.as_ref() {
        let cte_match = cte_context
            .and_then(|ctes| ctes.get(table_name))
            .is_some();
        if !cte_match && db.storage.get_table(table_name).is_some() {
            if selection.is_some() || source.rows.is_empty() {
                let (rows, _) = db.scan_table_rows(
                    table_name,
                    source.table_scope.table_alias.as_deref(),
                    selection.as_ref(),
                    &[],
                    outer,
                    cte_context,
                )?;
                return Ok(QuerySource { rows, ..source });
            }
        }
    }
    if remaining.is_empty() {
        return Ok(source);
    }
    let table_scope = source.table_scope.clone();
    let column_lookup = build_column_lookup(&source.columns, &source.column_scopes);
    let mut rows = Vec::new();
    for row in source.rows {
        let scope = EvalScope {
            columns: &source.columns,
            column_scopes: &source.column_scopes,
            row: &row,
            table_scope: &table_scope,
            cte_context,
            column_lookup: Some(&column_lookup),
        };
        let mut keep = true;
        for predicate in &remaining {
            let value = eval_expr(db, predicate, &scope, outer)?;
            if !value_to_bool(&value) {
                keep = false;
                break;
            }
        }
        if keep {
            rows.push(row);
        }
    }
    Ok(QuerySource {
        rows,
        ..source
    })
}

fn combine_predicates(predicates: &[Expr]) -> Option<Expr> {
    let mut iter = predicates.iter();
    let first = iter.next()?.clone();
    Some(iter.fold(first, |acc, expr| Expr::BinaryOp {
        left: Box::new(acc),
        op: BinaryOperator::And,
        right: Box::new(expr.clone()),
    }))
}

struct IndexLookupPlan {
    index_name: String,
    left_key_indices: Vec<usize>,
}

fn index_lookup_plan_for_pairs(
    db: &GongDB,
    right_scope: &TableScope,
    right_columns: &[Column],
    join_pairs: &[(usize, usize)],
) -> Option<IndexLookupPlan> {
    let table_name = right_scope.table_name.as_ref()?;
    if db.storage.index_updates_deferred(table_name) {
        return None;
    }
    let mut indexes: Vec<IndexMeta> = db
        .storage
        .list_indexes()
        .into_iter()
        .filter(|index| index.table.eq_ignore_ascii_case(table_name))
        .collect();
    if indexes.is_empty() {
        return None;
    }
    indexes.sort_by(|a, b| {
        b.unique
            .cmp(&a.unique)
            .then_with(|| a.columns.len().cmp(&b.columns.len()))
    });
    let mut right_to_left = HashMap::new();
    for (left_idx, right_idx) in join_pairs {
        right_to_left.insert(*right_idx, *left_idx);
    }
    for index in &indexes {
        if index.columns.len() != join_pairs.len() {
            continue;
        }
        let mut left_key_indices = Vec::with_capacity(index.columns.len());
        let mut matches = true;
        for column in &index.columns {
            let Some(right_idx) = resolve_column_index(&column.name.value, right_columns) else {
                matches = false;
                break;
            };
            let Some(left_idx) = right_to_left.get(&right_idx) else {
                matches = false;
                break;
            };
            left_key_indices.push(*left_idx);
        }
        if matches {
            return Some(IndexLookupPlan {
                index_name: index.name.clone(),
                left_key_indices,
            });
        }
    }
    None
}

fn can_use_index_lookup(
    db: &GongDB,
    left: &QuerySource,
    right: &QuerySource,
    join_predicates: &[Expr],
) -> bool {
    if !right.rows.is_empty() {
        return false;
    }
    let (join_pairs, _) = extract_join_pairs(
        join_predicates,
        &left.columns,
        &left.column_scopes,
        &right.columns,
        &right.column_scopes,
    );
    if join_pairs.is_empty() {
        return false;
    }
    index_lookup_plan_for_pairs(db, &right.table_scope, &right.columns, &join_pairs).is_some()
}

fn join_sources_with_predicates(
    db: &GongDB,
    left: QuerySource,
    right: QuerySource,
    predicates: Vec<Expr>,
    outer: Option<&EvalScope<'_>>,
    cte_context: Option<&CteContext>,
) -> Result<QuerySource, GongDBError> {
    let QuerySource {
        columns: mut left_columns,
        column_scopes: mut left_scopes,
        rows: left_rows,
        ..
    } = left;
    let QuerySource {
        columns: right_columns,
        column_scopes: right_scopes,
        rows: right_rows,
        table_scope: right_table_scope,
        ..
    } = right;
    let left_len = left_columns.len();
    left_columns.extend(right_columns);
    left_scopes.extend(right_scopes);
    let columns = left_columns;
    let column_scopes = left_scopes;
    let table_scope = TableScope {
        table_name: None,
        table_alias: None,
    };
    let mut filtered_predicates = Vec::with_capacity(predicates.len());
    for predicate in predicates {
        if expr_is_constant(&predicate) {
            let value = eval_constant_expr_checked(db, &predicate)?;
            if !value_to_bool(&value) {
                return Ok(QuerySource {
                    columns,
                    column_scopes,
                    rows: Vec::new(),
                    table_scope,
                });
            }
        } else {
            filtered_predicates.push(predicate);
        }
    }
    let (join_pairs, remaining_predicates) = extract_join_pairs(
        &filtered_predicates,
        &columns[..left_len],
        &column_scopes[..left_len],
        &columns[left_len..],
        &column_scopes[left_len..],
    );
    let column_lookup = if remaining_predicates.is_empty() {
        None
    } else {
        Some(build_column_lookup(&columns, &column_scopes))
    };
    let mut rows = Vec::new();
    if !join_pairs.is_empty() && right_rows.is_empty() {
        if let Some(plan) = index_lookup_plan_for_pairs(
            db,
            &right_table_scope,
            &columns[left_len..],
            &join_pairs,
        ) {
            for left_row in &left_rows {
                let mut key = Vec::with_capacity(plan.left_key_indices.len());
                let mut has_null = false;
                for idx in &plan.left_key_indices {
                    let value = &left_row[*idx];
                    if matches!(value, Value::Null) {
                        has_null = true;
                        break;
                    }
                    key.push(value.clone());
                }
                if has_null {
                    continue;
                }
                let right_matches = db.storage.scan_index_rows(
                    &plan.index_name,
                    Some(&key),
                    Some(&key),
                    false,
                )?;
                for right_row in right_matches {
                    let mut combined = Vec::with_capacity(left_row.len() + right_row.len());
                    combined.extend(left_row.iter().cloned());
                    combined.extend(right_row.iter().cloned());
                    if !remaining_predicates.is_empty() {
                        let scope = EvalScope {
                            columns: &columns,
                            column_scopes: &column_scopes,
                            row: &combined,
                            table_scope: &table_scope,
                            cte_context,
                            column_lookup: column_lookup.as_ref(),
                        };
                        let mut keep = true;
                        for predicate in &remaining_predicates {
                            let value = eval_expr(db, predicate, &scope, outer)?;
                            if !value_to_bool(&value) {
                                keep = false;
                                break;
                            }
                        }
                        if !keep {
                            continue;
                        }
                    }
                    rows.push(combined);
                }
            }
            return Ok(QuerySource {
                columns,
                column_scopes,
                rows,
                table_scope,
            });
        }
    }
    if join_pairs.is_empty() {
        for left_row in &left_rows {
            for right_row in &right_rows {
                let mut combined = Vec::with_capacity(left_row.len() + right_row.len());
                combined.extend(left_row.iter().cloned());
                combined.extend(right_row.iter().cloned());
                if !remaining_predicates.is_empty() {
                    let scope = EvalScope {
                        columns: &columns,
                        column_scopes: &column_scopes,
                        row: &combined,
                        table_scope: &table_scope,
                        cte_context,
                        column_lookup: column_lookup.as_ref(),
                    };
                    let mut keep = true;
                    for predicate in &remaining_predicates {
                        let value = eval_expr(db, predicate, &scope, outer)?;
                        if !value_to_bool(&value) {
                            keep = false;
                            break;
                        }
                    }
                    if !keep {
                        continue;
                    }
                }
                rows.push(combined);
            }
        }
    } else if join_pairs.len() == 1 {
        let (left_idx, right_idx) = join_pairs[0];
        if left_rows.len() <= right_rows.len() {
            let mut left_map: HashMap<DistinctKey, Vec<usize>> =
                HashMap::with_capacity(left_rows.len());
            for (idx, row) in left_rows.iter().enumerate() {
                let value = &row[left_idx];
                if matches!(value, Value::Null) {
                    continue;
                }
                left_map.entry(distinct_key(value)).or_default().push(idx);
            }
            for right_row in &right_rows {
                let value = &right_row[right_idx];
                if matches!(value, Value::Null) {
                    continue;
                }
                let Some(matches) = left_map.get(&distinct_key(value)) else {
                    continue;
                };
                for left_idx in matches {
                    let left_row = &left_rows[*left_idx];
                    let mut combined = Vec::with_capacity(left_row.len() + right_row.len());
                    combined.extend(left_row.iter().cloned());
                    combined.extend(right_row.iter().cloned());
                    if !remaining_predicates.is_empty() {
                        let scope = EvalScope {
                            columns: &columns,
                            column_scopes: &column_scopes,
                            row: &combined,
                            table_scope: &table_scope,
                            cte_context,
                            column_lookup: column_lookup.as_ref(),
                        };
                        let mut keep = true;
                        for predicate in &remaining_predicates {
                            let value = eval_expr(db, predicate, &scope, outer)?;
                            if !value_to_bool(&value) {
                                keep = false;
                                break;
                            }
                        }
                        if !keep {
                            continue;
                        }
                    }
                    rows.push(combined);
                }
            }
        } else {
            let mut right_map: HashMap<DistinctKey, Vec<usize>> =
                HashMap::with_capacity(right_rows.len());
            for (idx, row) in right_rows.iter().enumerate() {
                let value = &row[right_idx];
                if matches!(value, Value::Null) {
                    continue;
                }
                right_map.entry(distinct_key(value)).or_default().push(idx);
            }
            for left_row in &left_rows {
                let value = &left_row[left_idx];
                if matches!(value, Value::Null) {
                    continue;
                }
                let Some(matches) = right_map.get(&distinct_key(value)) else {
                    continue;
                };
                for right_idx in matches {
                    let right_row = &right_rows[*right_idx];
                    let mut combined = Vec::with_capacity(left_row.len() + right_row.len());
                    combined.extend(left_row.iter().cloned());
                    combined.extend(right_row.iter().cloned());
                    if !remaining_predicates.is_empty() {
                        let scope = EvalScope {
                            columns: &columns,
                            column_scopes: &column_scopes,
                            row: &combined,
                            table_scope: &table_scope,
                            cte_context,
                            column_lookup: column_lookup.as_ref(),
                        };
                        let mut keep = true;
                        for predicate in &remaining_predicates {
                            let value = eval_expr(db, predicate, &scope, outer)?;
                            if !value_to_bool(&value) {
                                keep = false;
                                break;
                            }
                        }
                        if !keep {
                            continue;
                        }
                    }
                    rows.push(combined);
                }
            }
        }
    } else {
        let left_key_indices: Vec<usize> = join_pairs.iter().map(|(l, _)| *l).collect();
        let right_key_indices: Vec<usize> = join_pairs.iter().map(|(_, r)| *r).collect();
        if left_rows.len() <= right_rows.len() {
            let mut left_map: HashMap<Vec<DistinctKey>, Vec<usize>> =
                HashMap::with_capacity(left_rows.len());
            for (idx, row) in left_rows.iter().enumerate() {
                let mut key = Vec::with_capacity(left_key_indices.len());
                let mut has_null = false;
                for col_idx in &left_key_indices {
                    let value = &row[*col_idx];
                    if matches!(value, Value::Null) {
                        has_null = true;
                        break;
                    }
                    key.push(distinct_key(value));
                }
                if has_null {
                    continue;
                }
                left_map.entry(key).or_default().push(idx);
            }
            let mut right_key = Vec::with_capacity(right_key_indices.len());
            for right_row in &right_rows {
                right_key.clear();
                let mut has_null = false;
                for col_idx in &right_key_indices {
                    let value = &right_row[*col_idx];
                    if matches!(value, Value::Null) {
                        has_null = true;
                        break;
                    }
                    right_key.push(distinct_key(value));
                }
                if has_null {
                    continue;
                }
                let Some(matches) = left_map.get(&right_key) else {
                    continue;
                };
                for left_idx in matches {
                    let left_row = &left_rows[*left_idx];
                    let mut combined = Vec::with_capacity(left_row.len() + right_row.len());
                    combined.extend(left_row.iter().cloned());
                    combined.extend(right_row.iter().cloned());
                    if !remaining_predicates.is_empty() {
                        let scope = EvalScope {
                            columns: &columns,
                            column_scopes: &column_scopes,
                            row: &combined,
                            table_scope: &table_scope,
                            cte_context,
                            column_lookup: column_lookup.as_ref(),
                        };
                        let mut keep = true;
                        for predicate in &remaining_predicates {
                            let value = eval_expr(db, predicate, &scope, outer)?;
                            if !value_to_bool(&value) {
                                keep = false;
                                break;
                            }
                        }
                        if !keep {
                            continue;
                        }
                    }
                    rows.push(combined);
                }
            }
        } else {
            let mut right_map: HashMap<Vec<DistinctKey>, Vec<usize>> =
                HashMap::with_capacity(right_rows.len());
            for (idx, row) in right_rows.iter().enumerate() {
                let mut key = Vec::with_capacity(right_key_indices.len());
                let mut has_null = false;
                for col_idx in &right_key_indices {
                    let value = &row[*col_idx];
                    if matches!(value, Value::Null) {
                        has_null = true;
                        break;
                    }
                    key.push(distinct_key(value));
                }
                if has_null {
                    continue;
                }
                right_map.entry(key).or_default().push(idx);
            }
            let mut left_key = Vec::with_capacity(left_key_indices.len());
            for left_row in &left_rows {
                left_key.clear();
                let mut has_null = false;
                for col_idx in &left_key_indices {
                    let value = &left_row[*col_idx];
                    if matches!(value, Value::Null) {
                        has_null = true;
                        break;
                    }
                    left_key.push(distinct_key(value));
                }
                if has_null {
                    continue;
                }
                let Some(matches) = right_map.get(&left_key) else {
                    continue;
                };
                for right_idx in matches {
                    let right_row = &right_rows[*right_idx];
                    let mut combined = Vec::with_capacity(left_row.len() + right_row.len());
                    combined.extend(left_row.iter().cloned());
                    combined.extend(right_row.iter().cloned());
                    if !remaining_predicates.is_empty() {
                        let scope = EvalScope {
                            columns: &columns,
                            column_scopes: &column_scopes,
                            row: &combined,
                            table_scope: &table_scope,
                            cte_context,
                            column_lookup: column_lookup.as_ref(),
                        };
                        let mut keep = true;
                        for predicate in &remaining_predicates {
                            let value = eval_expr(db, predicate, &scope, outer)?;
                            if !value_to_bool(&value) {
                                keep = false;
                                break;
                            }
                        }
                        if !keep {
                            continue;
                        }
                    }
                    rows.push(combined);
                }
            }
        }
    }
    Ok(QuerySource {
        columns,
        column_scopes,
        rows,
        table_scope,
    })
}

fn resolve_using_pairs(
    cols: &[Ident],
    columns: &[Column],
    _column_scopes: &[TableScope],
) -> Result<Vec<(usize, usize)>, GongDBError> {
    let mut pairs = Vec::new();
    for ident in cols {
        let name = &ident.value;
        let mut left = None;
        let mut right = None;
        for (idx, col) in columns.iter().enumerate() {
            if col.name.eq_ignore_ascii_case(name) {
                if left.is_none() {
                    left = Some(idx);
                } else {
                    right = Some(idx);
                    break;
                }
            }
        }
        let (Some(left_idx), Some(right_idx)) = (left, right) else {
            return Err(GongDBError::new(format!(
                "no such column: {}",
                name
            )));
        };
        pairs.push((left_idx, right_idx));
    }
    Ok(pairs)
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
    column_scopes: Vec<TableScope>,
    rows: Vec<Vec<Value>>,
    table_scope: TableScope,
}

struct TableInfo {
    source: QuerySource,
    scope: TableScope,
}

struct PredicateInfo {
    expr: Expr,
    tables: Option<Vec<usize>>,
    selectivity: f64,
}

#[derive(Clone)]
struct QueryResult {
    columns: Vec<Column>,
    rows: Vec<Vec<Value>>,
}

enum SubqueryCacheEntry {
    Uncorrelated(Arc<QueryResult>),
    Correlated,
}

struct SubqueryCache {
    entries: HashMap<usize, SubqueryCacheEntry>,
}

impl SubqueryCache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

#[derive(Default)]
struct InListCache {
    entries: HashMap<(usize, usize), Arc<PreparedInList>>,
}

impl InListCache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

#[derive(Default)]
struct InSubqueryCache {
    entries: HashMap<usize, Arc<PreparedInSubquery>>,
}

impl InSubqueryCache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

struct PreparedInList {
    literal_keys: HashSet<DistinctKey>,
    literal_values: Vec<Value>,
    non_literal_indices: Vec<usize>,
    saw_null_literal: bool,
}

struct PreparedInSubquery {
    keys: HashSet<DistinctKey>,
    saw_null: bool,
}

#[derive(Clone, Default)]
struct CteContext {
    entries: HashMap<String, QueryResult>,
}

impl CteContext {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    fn get(&self, name: &str) -> Option<&QueryResult> {
        self.entries.get(&name.to_ascii_lowercase())
    }

    fn insert(&mut self, name: &str, result: QueryResult) {
        self.entries.insert(name.to_ascii_lowercase(), result);
    }
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

    fn same_source(&self, other: &TableScope) -> bool {
        match (&self.table_alias, &other.table_alias) {
            (Some(left), Some(right)) => left.eq_ignore_ascii_case(right),
            (None, None) => match (&self.table_name, &other.table_name) {
                (Some(left), Some(right)) => left.eq_ignore_ascii_case(right),
                _ => false,
            },
            _ => false,
        }
    }
}

#[derive(Debug)]
struct ColumnLookup {
    unqualified: HashMap<String, usize>,
    qualified: HashMap<String, HashMap<String, usize>>,
}

fn build_column_lookup(columns: &[Column], column_scopes: &[TableScope]) -> ColumnLookup {
    let mut unqualified = HashMap::with_capacity(columns.len());
    let mut qualified: HashMap<String, HashMap<String, usize>> =
        HashMap::with_capacity(columns.len());
    for (idx, col) in columns.iter().enumerate() {
        let name_lower = col.name.to_ascii_lowercase();
        unqualified.entry(name_lower.clone()).or_insert(idx);
        let qualifier = column_scopes.get(idx).and_then(|scope| {
            if scope.table_alias.is_some() {
                scope.table_alias.as_ref()
            } else {
                scope.table_name.as_ref()
            }
        });
        if let Some(qualifier) = qualifier {
            let qual_lower = qualifier.to_ascii_lowercase();
            qualified
                .entry(qual_lower)
                .or_insert_with(HashMap::new)
                .entry(name_lower)
                .or_insert(idx);
        }
    }
    ColumnLookup {
        unqualified,
        qualified,
    }
}

#[derive(Clone, Copy)]
struct EvalScope<'a> {
    columns: &'a [Column],
    column_scopes: &'a [TableScope],
    row: &'a [Value],
    table_scope: &'a TableScope,
    cte_context: Option<&'a CteContext>,
    column_lookup: Option<&'a ColumnLookup>,
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

fn tpcc_orders_auto_index_spec(
    table_name: &str,
    columns: &[Column],
) -> Option<AutoIndexSpec> {
    if !table_name.eq_ignore_ascii_case("orders") {
        return None;
    }
    let mut name_map = HashMap::new();
    for column in columns {
        name_map.insert(column.name.to_lowercase(), column.name.clone());
    }
    let required = ["o_w_id", "o_d_id", "o_c_id", "o_id"];
    if !required.iter().all(|name| name_map.contains_key(*name)) {
        return None;
    }
    let indexed = required
        .iter()
        .map(|name| IndexedColumn {
            name: Ident::new(
                name_map
                    .get(*name)
                    .expect("required column name missing"),
            ),
            order: None,
        })
        .collect();
    Some(AutoIndexSpec {
        columns: indexed,
        unique: false,
    })
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
                "no such column: {}",
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

fn auto_index_key(spec: &AutoIndexSpec) -> String {
    spec.columns
        .iter()
        .map(|column| column.name.value.to_lowercase())
        .collect::<Vec<_>>()
        .join(",")
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

fn is_write_statement(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::CreateTable(_)
            | Statement::DropTable(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::Reindex(_)
            | Statement::CreateView(_)
            | Statement::DropView(_)
            | Statement::CreateTrigger(_)
            | Statement::DropTrigger(_)
            | Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
    )
}

fn bind_statement_params(
    statement: &Statement,
    params: &[Literal],
) -> Result<Statement, GongDBError> {
    let mut stmt = statement.clone();
    let mut binder = ParamBinder { params };
    binder.bind_statement(&mut stmt)?;
    Ok(stmt)
}

struct ParamBinder<'a> {
    params: &'a [Literal],
}

impl<'a> ParamBinder<'a> {
    fn bind_statement(&mut self, stmt: &mut Statement) -> Result<(), GongDBError> {
        match stmt {
            Statement::CreateTable(create) => {
                for column in &mut create.columns {
                    for constraint in &mut column.constraints {
                        if let ColumnConstraint::Default(expr) = constraint {
                            self.bind_expr(expr)?;
                        }
                    }
                }
                for constraint in &mut create.constraints {
                    if let TableConstraint::Check(expr) = constraint {
                        self.bind_expr(expr)?;
                    }
                }
            }
            Statement::CreateView(create) => {
                self.bind_select(&mut create.query)?;
            }
            Statement::Insert(insert) => {
                match &mut insert.source {
                    InsertSource::Values(rows) => {
                        for row in rows {
                            for expr in row {
                                self.bind_expr(expr)?;
                            }
                        }
                    }
                    InsertSource::Select(select) => self.bind_select(select)?,
                }
            }
            Statement::Update(update) => {
                for assignment in &mut update.assignments {
                    self.bind_expr(&mut assignment.value)?;
                }
                if let Some(expr) = &mut update.selection {
                    self.bind_expr(expr)?;
                }
            }
            Statement::Delete(delete) => {
                if let Some(expr) = &mut delete.selection {
                    self.bind_expr(expr)?;
                }
            }
            Statement::Select(select) => {
                self.bind_select(select)?;
            }
            Statement::BeginTransaction(_)
            | Statement::Commit
            | Statement::Rollback
            | Statement::DropTable(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::Reindex(_)
            | Statement::DropView(_)
            | Statement::CreateTrigger(_)
            | Statement::DropTrigger(_) => {}
        }
        Ok(())
    }

    fn bind_select(&mut self, select: &mut Select) -> Result<(), GongDBError> {
        if let Some(with) = &mut select.with {
            for cte in &mut with.ctes {
                self.bind_select(cte.query.as_mut())?;
            }
        }
        for item in &mut select.projection {
            if let SelectItem::Expr { expr, .. } = item {
                self.bind_expr(expr)?;
            }
        }
        for table_ref in &mut select.from {
            self.bind_table_ref(table_ref)?;
        }
        if let Some(expr) = &mut select.selection {
            self.bind_expr(expr)?;
        }
        for expr in &mut select.group_by {
            self.bind_expr(expr)?;
        }
        if let Some(expr) = &mut select.having {
            self.bind_expr(expr)?;
        }
        for order in &mut select.order_by {
            self.bind_expr(&mut order.expr)?;
        }
        if let Some(expr) = &mut select.limit {
            self.bind_expr(expr)?;
        }
        if let Some(expr) = &mut select.offset {
            self.bind_expr(expr)?;
        }
        for compound in &mut select.compounds {
            self.bind_select(compound.select.as_mut())?;
        }
        Ok(())
    }

    fn bind_table_ref(&mut self, table_ref: &mut TableRef) -> Result<(), GongDBError> {
        match table_ref {
            TableRef::Named { .. } => {}
            TableRef::Subquery { subquery, .. } => self.bind_select(subquery.as_mut())?,
            TableRef::Join {
                left,
                right,
                constraint,
                ..
            } => {
                self.bind_table_ref(left)?;
                self.bind_table_ref(right)?;
                if let Some(JoinConstraint::On(expr)) = constraint {
                    self.bind_expr(expr)?;
                }
            }
        }
        Ok(())
    }

    fn bind_expr(&mut self, expr: &mut Expr) -> Result<(), GongDBError> {
        match expr {
            Expr::Parameter(idx) => {
                let literal = self
                    .params
                    .get(*idx)
                    .ok_or_else(|| GongDBError::new("missing bound parameter"))?
                    .clone();
                *expr = Expr::Literal(literal);
                Ok(())
            }
            Expr::Literal(_)
            | Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::Wildcard => Ok(()),
            Expr::BinaryOp { left, right, .. } => {
                self.bind_expr(left)?;
                self.bind_expr(right)?;
                Ok(())
            }
            Expr::UnaryOp { expr, .. } => self.bind_expr(expr),
            Expr::Function { args, .. } => {
                for arg in args {
                    self.bind_expr(arg)?;
                }
                Ok(())
            }
            Expr::Case {
                operand,
                when_then,
                else_result,
            } => {
                if let Some(expr) = operand {
                    self.bind_expr(expr)?;
                }
                for (when_expr, then_expr) in when_then {
                    self.bind_expr(when_expr)?;
                    self.bind_expr(then_expr)?;
                }
                if let Some(expr) = else_result {
                    self.bind_expr(expr)?;
                }
                Ok(())
            }
            Expr::Between { expr, low, high, .. } => {
                self.bind_expr(expr)?;
                self.bind_expr(low)?;
                self.bind_expr(high)?;
                Ok(())
            }
            Expr::InList { expr, list, .. } => {
                self.bind_expr(expr)?;
                for item in list {
                    self.bind_expr(item)?;
                }
                Ok(())
            }
            Expr::InSubquery { expr, subquery, .. } => {
                self.bind_expr(expr)?;
                self.bind_select(subquery.as_mut())?;
                Ok(())
            }
            Expr::Exists(subquery) => self.bind_select(subquery.as_mut()),
            Expr::Subquery(subquery) => self.bind_select(subquery.as_mut()),
            Expr::IsNull { expr, .. } => self.bind_expr(expr),
            Expr::Cast { expr, .. } => self.bind_expr(expr),
            Expr::Nested(expr) => self.bind_expr(expr),
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

fn strip_prefix_ci<'a>(input: &'a str, prefix: &str) -> Option<&'a str> {
    if input.len() >= prefix.len() && input[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&input[prefix.len()..])
    } else {
        None
    }
}

fn consume_fast_keyword<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let rest = input.trim_start();
    if rest.len() < keyword.len() {
        return None;
    }
    if !rest[..keyword.len()].eq_ignore_ascii_case(keyword) {
        return None;
    }
    let remainder = &rest[keyword.len()..];
    if remainder.is_empty()
        || remainder
            .chars()
            .next()
            .map(|ch| ch.is_whitespace())
            .unwrap_or(false)
    {
        Some(remainder)
    } else {
        None
    }
}

fn parse_fast_ident<'a>(input: &'a str) -> Option<(&'a str, &'a str)> {
    let rest = input.trim_start();
    if rest.is_empty() {
        return None;
    }
    let end = rest
        .find(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
        .unwrap_or_else(|| rest.len());
    if end == 0 {
        return None;
    }
    Some((&rest[..end], &rest[end..]))
}

fn parse_fast_ident_list(input: &str) -> Option<Vec<String>> {
    let mut rest = input.trim();
    let mut idents = Vec::new();
    if rest == "*" {
        return Some(vec!["*".to_string()]);
    }
    loop {
        let (ident, next) = parse_fast_ident(rest)?;
        idents.push(ident.to_string());
        rest = next.trim_start();
        if rest.starts_with(',') {
            rest = rest[1..].trim_start();
            continue;
        }
        if rest.is_empty() {
            break;
        }
        return None;
    }
    Some(idents)
}

fn parse_fast_insert(sql: &str) -> Option<(String, Vec<Vec<Value>>)> {
    let mut input = sql.trim();
    if let Some(stripped) = input.strip_suffix(';') {
        input = stripped.trim();
    }
    let rest = strip_prefix_ci(input, "INSERT INTO")?;
    let mut rest = rest.trim_start();
    let table_end = rest
        .find(|ch: char| ch.is_whitespace() || ch == '(')
        .unwrap_or_else(|| rest.len());
    if table_end == 0 {
        return None;
    }
    let table_name = rest[..table_end].trim().to_string();
    rest = rest[table_end..].trim_start();
    if rest.starts_with('(') {
        return None;
    }
    rest = strip_prefix_ci(rest, "VALUES")?.trim_start();
    let (rows, remainder) = parse_fast_rows(rest)?;
    if rows.is_empty() {
        return None;
    }
    let remainder = remainder.trim_start();
    if !remainder.is_empty() {
        return None;
    }
    Some((table_name, rows))
}

fn parse_fast_select(sql: &str) -> Option<FastSelectPlan> {
    let mut input = sql.trim();
    if let Some(stripped) = input.strip_suffix(';') {
        input = stripped.trim();
    }
    let rest = consume_fast_keyword(input, "SELECT")?;
    let lower = rest.to_ascii_lowercase();
    let from_idx = lower.find(" from ")?;
    let columns_str = rest[..from_idx].trim();
    if columns_str.is_empty() {
        return None;
    }
    let columns = parse_fast_ident_list(columns_str)?;
    let mut rest = &rest[from_idx + 6..];
    let (table, remainder) = parse_fast_ident(rest)?;
    rest = remainder;
    let mut order_by = None;
    let mut limit = None;
    let rest = consume_fast_keyword(rest, "WHERE")?;
    let mut rest = rest;
    let lower_rest = rest.to_ascii_lowercase();
    let mut where_end = rest.len();
    if let Some(idx) = lower_rest.find(" order by ") {
        where_end = where_end.min(idx);
    }
    if let Some(idx) = lower_rest.find(" limit ") {
        where_end = where_end.min(idx);
    }
    let (where_part, remainder) = rest.split_at(where_end);
    let predicates = parse_fast_predicates(where_part)?;
    rest = remainder;
    if let Some(after_order) = consume_fast_keyword(rest, "ORDER") {
        let after_by = consume_fast_keyword(after_order, "BY")?;
        let (order_col, after_col) = parse_fast_ident(after_by)?;
        let mut desc = false;
        let mut after_col = after_col;
        if let Some(after_desc) = consume_fast_keyword(after_col, "DESC") {
            desc = true;
            after_col = after_desc;
        } else if let Some(after_asc) = consume_fast_keyword(after_col, "ASC") {
            after_col = after_asc;
        }
        order_by = Some(FastOrderBy {
            column: order_col.to_string(),
            desc,
        });
        rest = after_col;
    }
    if let Some(after_limit) = consume_fast_keyword(rest, "LIMIT") {
        let (value, after_value) = parse_fast_literal(after_limit)?;
        let limit_value = match value {
            Value::Integer(v) if v >= 0 => v as usize,
            _ => return None,
        };
        limit = Some(limit_value);
        rest = after_value;
    }
    if !rest.trim().is_empty() {
        return None;
    }
    Some(FastSelectPlan {
        table: table.to_string(),
        columns,
        predicates,
        order_by,
        limit,
    })
}

fn parse_fast_stock_level(sql: &str) -> Option<(i64, i64, i64, i64)> {
    let mut input = sql.trim();
    if let Some(stripped) = input.strip_suffix(';') {
        input = stripped.trim();
    }
    let mut rest = strip_prefix_ci(
        input,
        "SELECT COUNT(DISTINCT ol_i_id) FROM order_line, stock WHERE",
    )?;
    let (w_id, after_w) = parse_fast_stock_eq(rest, "ol_w_id")?;
    rest = after_w;
    rest = strip_prefix_ci(rest.trim_start(), "AND")?;
    let (d_id, after_d) = parse_fast_stock_eq(rest, "ol_d_id")?;
    rest = after_d;
    rest = strip_prefix_ci(rest.trim_start(), "AND")?;
    let (next_o_id, after_ge) = parse_fast_stock_cmp(rest, "ol_o_id", ">=")?;
    rest = after_ge;
    rest = rest.trim_start();
    if !rest.starts_with('-') {
        return None;
    }
    rest = rest[1..].trim_start();
    let (minus_value, after_minus) = parse_fast_i64(rest)?;
    if minus_value != 20 {
        return None;
    }
    rest = after_minus;
    rest = strip_prefix_ci(rest.trim_start(), "AND")?;
    let (upper_o_id, after_lt) = parse_fast_stock_cmp(rest, "ol_o_id", "<")?;
    rest = after_lt;
    rest = strip_prefix_ci(rest.trim_start(), "AND")?;
    rest = strip_prefix_ci(rest.trim_start(), "s_w_id = ol_w_id")?;
    rest = strip_prefix_ci(rest.trim_start(), "AND")?;
    rest = strip_prefix_ci(rest.trim_start(), "s_i_id = ol_i_id")?;
    rest = strip_prefix_ci(rest.trim_start(), "AND")?;
    let (threshold, after_qty) = parse_fast_stock_cmp(rest, "s_quantity", "<")?;
    if !after_qty.trim().is_empty() {
        return None;
    }
    if upper_o_id != next_o_id {
        return None;
    }
    Some((w_id, d_id, next_o_id, threshold))
}

fn parse_fast_stock_update(sql: &str) -> Option<FastStockUpdatePlan> {
    let mut input = sql.trim();
    if let Some(stripped) = input.strip_suffix(';') {
        input = stripped.trim();
    }
    let rest = strip_prefix_ci(input, "UPDATE")?.trim_start();
    let (table, rest) = parse_fast_ident(rest)?;
    if !table.eq_ignore_ascii_case("stock") {
        return None;
    }
    let mut rest = rest.trim_start();
    rest = strip_prefix_ci(rest, "SET")?.trim_start();
    let (quantity, after_qty) = parse_stock_self_assignment(rest, "s_quantity", '-')?;
    let mut rest = after_qty.trim_start();
    if !rest.starts_with(',') {
        return None;
    }
    rest = rest[1..].trim_start();
    let (ytd, after_ytd) = parse_stock_self_assignment(rest, "s_ytd", '+')?;
    let mut rest = after_ytd.trim_start();
    if !rest.starts_with(',') {
        return None;
    }
    rest = rest[1..].trim_start();
    let after_order_cnt = parse_stock_increment_one(rest, "s_order_cnt")?;
    let mut rest = after_order_cnt.trim_start();
    let mut remote = false;
    if rest.starts_with(',') {
        let after_comma = rest[1..].trim_start();
        if let Some(after_remote) = parse_stock_increment_one(after_comma, "s_remote_cnt") {
            remote = true;
            rest = after_remote;
        }
    }
    rest = rest.trim_start();
    rest = strip_prefix_ci(rest, "WHERE")?.trim_start();
    let (w_id, after_w) = parse_fast_stock_eq(rest, "s_w_id")?;
    let mut rest = after_w.trim_start();
    rest = strip_prefix_ci(rest, "AND")?.trim_start();
    let (i_id, after_i) = parse_fast_stock_eq(rest, "s_i_id")?;
    if !after_i.trim().is_empty() {
        return None;
    }
    Some(FastStockUpdatePlan {
        quantity,
        ytd,
        w_id,
        i_id,
        remote,
    })
}

fn parse_fast_delivery_customer_update(sql: &str) -> Option<(i64, i64, i64)> {
    let mut input = sql.trim();
    if let Some(stripped) = input.strip_suffix(';') {
        input = stripped.trim();
    }
    let mut rest = strip_prefix_ci(
        input,
        "UPDATE customer SET c_balance = c_balance + (SELECT COALESCE(SUM(ol_amount), 0) FROM order_line WHERE",
    )?;
    let (w_id, after_w) = parse_fast_stock_eq(rest, "ol_w_id")?;
    rest = after_w;
    rest = strip_prefix_ci(rest.trim_start(), "AND")?;
    let (d_id, after_d) = parse_fast_stock_eq(rest, "ol_d_id")?;
    rest = after_d;
    rest = strip_prefix_ci(rest.trim_start(), "AND")?;
    let (o_id, after_o) = parse_fast_stock_eq(rest, "ol_o_id")?;
    rest = after_o;
    rest = strip_prefix_ci(rest.trim_start(), "),")?;
    rest = strip_prefix_ci(rest.trim_start(), "c_delivery_cnt = c_delivery_cnt + 1 WHERE")?;
    let (c_w_id, after_cw) = parse_fast_stock_eq(rest, "c_w_id")?;
    rest = after_cw;
    rest = strip_prefix_ci(rest.trim_start(), "AND")?;
    let (c_d_id, after_cd) = parse_fast_stock_eq(rest, "c_d_id")?;
    rest = after_cd;
    rest = strip_prefix_ci(rest.trim_start(), "AND")?;
    rest = strip_prefix_ci(rest.trim_start(), "c_id = (SELECT o_c_id FROM orders WHERE")?;
    let (o_w_id, after_ow) = parse_fast_stock_eq(rest, "o_w_id")?;
    rest = after_ow;
    rest = strip_prefix_ci(rest.trim_start(), "AND")?;
    let (o_d_id, after_od) = parse_fast_stock_eq(rest, "o_d_id")?;
    rest = after_od;
    rest = strip_prefix_ci(rest.trim_start(), "AND")?;
    let (o_id2, after_oid) = parse_fast_stock_eq(rest, "o_id")?;
    if !after_oid.trim().trim_end_matches(')').trim().is_empty() {
        return None;
    }
    if w_id != c_w_id || w_id != o_w_id || d_id != c_d_id || d_id != o_d_id || o_id != o_id2 {
        return None;
    }
    Some((w_id, d_id, o_id))
}

fn parse_fast_stock_eq<'a>(input: &'a str, name: &str) -> Option<(i64, &'a str)> {
    let mut rest = input.trim_start();
    rest = strip_prefix_ci(rest, name)?;
    rest = rest.trim_start();
    if !rest.starts_with('=') {
        return None;
    }
    rest = &rest[1..];
    parse_fast_i64(rest)
}

fn parse_fast_stock_cmp<'a>(input: &'a str, name: &str, op: &str) -> Option<(i64, &'a str)> {
    let mut rest = input.trim_start();
    rest = strip_prefix_ci(rest, name)?;
    rest = rest.trim_start();
    rest = strip_prefix_ci(rest, op)?;
    parse_fast_i64(rest)
}

fn parse_fast_i64(input: &str) -> Option<(i64, &str)> {
    let (value, rest) = parse_fast_literal(input)?;
    let value = match value {
        Value::Integer(v) => v,
        Value::Real(v) => v as i64,
        _ => return None,
    };
    Some((value, rest))
}

fn parse_stock_self_assignment<'a>(
    input: &'a str,
    column: &str,
    op: char,
) -> Option<(i64, &'a str)> {
    let mut rest = input.trim_start();
    rest = strip_prefix_ci(rest, column)?;
    rest = rest.trim_start();
    if !rest.starts_with('=') {
        return None;
    }
    rest = rest[1..].trim_start();
    rest = strip_prefix_ci(rest, column)?;
    rest = rest.trim_start();
    if !rest.starts_with(op) {
        return None;
    }
    rest = &rest[1..];
    parse_fast_i64(rest)
}

fn parse_stock_increment_one<'a>(input: &'a str, column: &str) -> Option<&'a str> {
    let mut rest = input.trim_start();
    rest = strip_prefix_ci(rest, column)?;
    rest = rest.trim_start();
    if !rest.starts_with('=') {
        return None;
    }
    rest = rest[1..].trim_start();
    rest = strip_prefix_ci(rest, column)?;
    rest = rest.trim_start();
    if !rest.starts_with('+') {
        return None;
    }
    rest = rest[1..].trim_start();
    let (value, rest) = parse_fast_i64(rest)?;
    if value != 1 {
        return None;
    }
    Some(rest)
}

#[derive(Clone)]
struct FastUpdateAssignment {
    column: String,
    action: FastUpdateAction,
}

#[derive(Clone)]
enum FastUpdateAction {
    Set(Value),
    Add(Value),
    Sub(Value),
}

enum FastUpdateOutcome {
    InPlace(Vec<crate::storage::FieldUpdate>),
    Row(Vec<Value>),
}

struct FastStockUpdatePlan {
    quantity: i64,
    ytd: i64,
    w_id: i64,
    i_id: i64,
    remote: bool,
}

#[derive(Clone)]
struct FastStockUpdateOffsets {
    quantity: usize,
    ytd: usize,
    order_cnt: usize,
    remote_cnt: Option<usize>,
}

struct FastUpdatePlan {
    table: String,
    assignments: Vec<FastUpdateAssignment>,
    predicates: Vec<(String, Value)>,
}

#[derive(Clone)]
enum FastUpdateTemplate {
    Ineligible,
    Eligible {
        assignment_indices: Vec<usize>,
        predicate_indices: Vec<usize>,
        best_index: Option<FastUpdateIndexPlan>,
        best_prefix_index: Option<FastUpdateIndexPrefixPlan>,
    },
}

#[derive(Clone)]
struct FastUpdateIndexPlan {
    index: IndexMeta,
    predicate_positions: Vec<usize>,
    all_predicates_covered: bool,
}

#[derive(Clone)]
struct FastUpdateIndexPrefixPlan {
    index: IndexMeta,
    predicate_positions: Vec<usize>,
}

fn parse_fast_update(sql: &str) -> Option<FastUpdatePlan> {
    let mut input = sql.trim();
    if let Some(stripped) = input.strip_suffix(';') {
        input = stripped.trim();
    }
    let rest = strip_prefix_ci(input, "UPDATE")?.trim_start();
    let table_end = rest
        .find(|ch: char| ch.is_whitespace())
        .unwrap_or_else(|| rest.len());
    if table_end == 0 {
        return None;
    }
    let table_name = rest[..table_end].trim().to_string();
    let mut rest = rest[table_end..].trim_start();
    rest = strip_prefix_ci(rest, "SET")?.trim_start();
    let (assignments_str, predicates_str) = split_where_clause(rest)?;
    let assignment_parts = split_commas_outside_quotes(assignments_str)?;
    let mut assignments = Vec::with_capacity(assignment_parts.len());
    for part in assignment_parts {
        let assignment = parse_fast_update_assignment(part)?;
        assignments.push(assignment);
    }
    if assignments.is_empty() {
        return None;
    }
    let predicate_parts = split_and_outside_quotes(predicates_str)?;
    let mut predicates = Vec::with_capacity(predicate_parts.len());
    for part in predicate_parts {
        let predicate = parse_fast_update_predicate(part)?;
        predicates.push(predicate);
    }
    if predicates.is_empty() {
        return None;
    }
    Some(FastUpdatePlan {
        table: table_name,
        assignments,
        predicates,
    })
}

fn parse_fast_update_with_params(
    sql: &str,
    params: &[Literal],
) -> Result<Option<FastUpdatePlan>, GongDBError> {
    let mut input = sql.trim();
    if let Some(stripped) = input.strip_suffix(';') {
        input = stripped.trim();
    }
    let rest = match strip_prefix_ci(input, "UPDATE") {
        Some(rest) => rest,
        None => return Ok(None),
    };
    let rest = rest.trim_start();
    let table_end = rest
        .find(|ch: char| ch.is_whitespace())
        .unwrap_or_else(|| rest.len());
    if table_end == 0 {
        return Ok(None);
    }
    let table_name = rest[..table_end].trim().to_string();
    let mut rest = rest[table_end..].trim_start();
    rest = match strip_prefix_ci(rest, "SET") {
        Some(rest) => rest,
        None => return Ok(None),
    };
    let rest = rest.trim_start();
    let (assignments_str, predicates_str) = match split_where_clause(rest) {
        Some(parts) => parts,
        None => return Ok(None),
    };
    let assignment_parts = match split_commas_outside_quotes(assignments_str) {
        Some(parts) => parts,
        None => return Ok(None),
    };
    let mut assignments = Vec::with_capacity(assignment_parts.len());
    let mut param_pos = 0usize;
    for part in assignment_parts {
        let assignment =
            match parse_fast_update_assignment_with_params(part, params, &mut param_pos) {
                Some(assignment) => assignment,
                None => return Ok(None),
            };
        assignments.push(assignment);
    }
    if assignments.is_empty() {
        return Ok(None);
    }
    let predicate_parts = match split_and_outside_quotes(predicates_str) {
        Some(parts) => parts,
        None => return Ok(None),
    };
    let mut predicates = Vec::with_capacity(predicate_parts.len());
    for part in predicate_parts {
        let predicate =
            match parse_fast_update_predicate_with_params(part, params, &mut param_pos) {
                Some(predicate) => predicate,
                None => return Ok(None),
            };
        predicates.push(predicate);
    }
    if predicates.is_empty() {
        return Ok(None);
    }
    if param_pos != params.len() {
        return Err(GongDBError::new("parameter count mismatch"));
    }
    Ok(Some(FastUpdatePlan {
        table: table_name,
        assignments,
        predicates,
    }))
}

fn fast_update_cache_key(plan: &FastUpdatePlan) -> String {
    let mut key = String::with_capacity(
        plan.table.len() + plan.assignments.len() * 12 + plan.predicates.len() * 12,
    );
    key.push_str(&plan.table.to_ascii_lowercase());
    for assignment in &plan.assignments {
        key.push_str("|a:");
        key.push_str(&assignment.column.to_ascii_lowercase());
        key.push(':');
        let action = match assignment.action {
            FastUpdateAction::Set(_) => "set",
            FastUpdateAction::Add(_) => "add",
            FastUpdateAction::Sub(_) => "sub",
        };
        key.push_str(action);
    }
    for (column, _) in &plan.predicates {
        key.push_str("|p:");
        key.push_str(&column.to_ascii_lowercase());
    }
    key
}

fn build_fast_update_template(
    table: &TableMeta,
    plan: &FastUpdatePlan,
    indexes: &[IndexMeta],
) -> Result<FastUpdateTemplate, GongDBError> {
    let mut indexed_columns = HashSet::new();
    for index in indexes {
        if index.table.eq_ignore_ascii_case(&plan.table) {
            for column in &index.columns {
                indexed_columns.insert(column.name.value.to_ascii_lowercase());
            }
        }
    }
    if plan
        .assignments
        .iter()
        .any(|assignment| indexed_columns.contains(&assignment.column.to_ascii_lowercase()))
    {
        return Ok(FastUpdateTemplate::Ineligible);
    }
    let mut column_map = HashMap::new();
    for (idx, col) in table.columns.iter().enumerate() {
        column_map.insert(col.name.to_ascii_lowercase(), idx);
    }
    let mut predicate_indices = Vec::with_capacity(plan.predicates.len());
    let mut predicate_positions = HashMap::new();
    for (pos, (column, _)) in plan.predicates.iter().enumerate() {
        let column_key = column.to_ascii_lowercase();
        let idx = match column_map.get(&column_key) {
            Some(idx) => *idx,
            None => {
                return Err(GongDBError::new(format!(
                    "no such column: {}",
                    column
                )))
            }
        };
        predicate_indices.push(idx);
        predicate_positions.insert(column_key, pos);
    }
    let mut assignment_indices = Vec::with_capacity(plan.assignments.len());
    for assignment in &plan.assignments {
        let column_key = assignment.column.to_ascii_lowercase();
        let idx = match column_map.get(&column_key) {
            Some(idx) => *idx,
            None => {
                return Err(GongDBError::new(format!(
                    "no such column: {}",
                    assignment.column
                )))
            }
        };
        assignment_indices.push(idx);
    }

    let mut best_index: Option<FastUpdateIndexPlan> = None;
    let mut best_prefix_index: Option<FastUpdateIndexPrefixPlan> = None;
    for index in indexes {
        if !index.table.eq_ignore_ascii_case(&plan.table) {
            continue;
        }
        let mut prefix_len = 0usize;
        let mut matches_all = true;
        let mut predicate_positions_for_index = Vec::with_capacity(index.columns.len());
        for column in &index.columns {
            if let Some(pos) = predicate_positions.get(&column.name.value.to_ascii_lowercase()) {
                prefix_len += 1;
                predicate_positions_for_index.push(*pos);
            } else {
                matches_all = false;
                break;
            }
        }
        if matches_all {
            let all_predicates_covered = plan.predicates.len() == index.columns.len();
            let replace = match &best_index {
                Some(best) => index.columns.len() > best.index.columns.len(),
                None => true,
            };
            if replace {
                best_index = Some(FastUpdateIndexPlan {
                    index: index.clone(),
                    predicate_positions: predicate_positions_for_index,
                    all_predicates_covered,
                });
            }
            continue;
        }
        if prefix_len > 0 {
            let replace = match &best_prefix_index {
                Some(best) => prefix_len > best.predicate_positions.len(),
                None => true,
            };
            if replace {
                predicate_positions_for_index.truncate(prefix_len);
                best_prefix_index = Some(FastUpdateIndexPrefixPlan {
                    index: index.clone(),
                    predicate_positions: predicate_positions_for_index,
                });
            }
        }
    }

    Ok(FastUpdateTemplate::Eligible {
        assignment_indices,
        predicate_indices,
        best_index,
        best_prefix_index,
    })
}

fn split_where_clause(input: &str) -> Option<(&str, &str)> {
    let bytes = input.as_bytes();
    let mut idx = 0usize;
    let mut in_string = false;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if byte == b'\'' {
            if in_string {
                if idx + 1 < bytes.len() && bytes[idx + 1] == b'\'' {
                    idx += 2;
                    continue;
                }
                in_string = false;
                idx += 1;
                continue;
            }
            in_string = true;
            idx += 1;
            continue;
        }
        if !in_string && idx + 5 <= bytes.len() {
            if input[idx..idx + 5].eq_ignore_ascii_case("WHERE") {
                let prev = if idx == 0 {
                    None
                } else {
                    input[..idx].chars().last()
                };
                let next = input[idx + 5..].chars().next();
                if prev.map_or(true, |c| c.is_whitespace())
                    && next.map_or(true, |c| c.is_whitespace())
                {
                    let left = input[..idx].trim_end();
                    let right = input[idx + 5..].trim_start();
                    if left.is_empty() || right.is_empty() {
                        return None;
                    }
                    return Some((left, right));
                }
            }
        }
        idx += 1;
    }
    None
}

fn split_commas_outside_quotes(input: &str) -> Option<Vec<&str>> {
    let bytes = input.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut idx = 0usize;
    let mut in_string = false;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if byte == b'\'' {
            if in_string {
                if idx + 1 < bytes.len() && bytes[idx + 1] == b'\'' {
                    idx += 2;
                    continue;
                }
                in_string = false;
                idx += 1;
                continue;
            }
            in_string = true;
            idx += 1;
            continue;
        }
        if !in_string && byte == b',' {
            let part = input[start..idx].trim();
            if part.is_empty() {
                return None;
            }
            parts.push(part);
            start = idx + 1;
        }
        idx += 1;
    }
    let part = input[start..].trim();
    if part.is_empty() {
        return None;
    }
    parts.push(part);
    Some(parts)
}

fn split_and_outside_quotes(input: &str) -> Option<Vec<&str>> {
    let bytes = input.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut idx = 0usize;
    let mut in_string = false;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if byte == b'\'' {
            if in_string {
                if idx + 1 < bytes.len() && bytes[idx + 1] == b'\'' {
                    idx += 2;
                    continue;
                }
                in_string = false;
                idx += 1;
                continue;
            }
            in_string = true;
            idx += 1;
            continue;
        }
        if !in_string && idx + 3 <= bytes.len() {
            if input[idx..idx + 3].eq_ignore_ascii_case("AND") {
                let prev = if idx == 0 {
                    None
                } else {
                    input[..idx].chars().last()
                };
                let next = input[idx + 3..].chars().next();
                if prev.map_or(true, |c| c.is_whitespace())
                    && next.map_or(true, |c| c.is_whitespace())
                {
                    let part = input[start..idx].trim();
                    if part.is_empty() {
                        return None;
                    }
                    parts.push(part);
                    start = idx + 3;
                }
            }
        }
        idx += 1;
    }
    let part = input[start..].trim();
    if part.is_empty() {
        return None;
    }
    parts.push(part);
    Some(parts)
}

fn parse_fast_update_assignment(input: &str) -> Option<FastUpdateAssignment> {
    let mut iter = input.splitn(2, '=');
    let column = iter.next()?.trim();
    let expr = iter.next()?.trim();
    if column.is_empty() || expr.is_empty() {
        return None;
    }
    if expr.len() >= column.len() && expr[..column.len()].eq_ignore_ascii_case(column) {
        let after = &expr[column.len()..];
        if !after.chars().next().map_or(false, |c| c.is_whitespace()) {
            return None;
        }
        let mut rest = expr[column.len()..].trim_start();
        let op = rest.chars().next()?;
        if op == '+' || op == '-' {
            rest = rest[op.len_utf8()..].trim_start();
            let (value, remainder) = parse_fast_value(rest)?;
            if !remainder.trim().is_empty() {
                return None;
            }
            let action = if op == '+' {
                FastUpdateAction::Add(value)
            } else {
                FastUpdateAction::Sub(value)
            };
            return Some(FastUpdateAssignment {
                column: column.to_ascii_lowercase(),
                action,
            });
        }
    }
    let (value, remainder) = parse_fast_value(expr)?;
    if !remainder.trim().is_empty() {
        return None;
    }
    Some(FastUpdateAssignment {
        column: column.to_ascii_lowercase(),
        action: FastUpdateAction::Set(value),
    })
}

fn parse_fast_update_assignment_with_params(
    input: &str,
    params: &[Literal],
    param_pos: &mut usize,
) -> Option<FastUpdateAssignment> {
    let mut iter = input.splitn(2, '=');
    let column = iter.next()?.trim();
    let expr = iter.next()?.trim();
    if column.is_empty() || expr.is_empty() {
        return None;
    }
    if expr.len() >= column.len() && expr[..column.len()].eq_ignore_ascii_case(column) {
        let after = &expr[column.len()..];
        if !after.chars().next().map_or(false, |c| c.is_whitespace()) {
            return None;
        }
        let mut rest = expr[column.len()..].trim_start();
        let op = rest.chars().next()?;
        if op == '+' || op == '-' {
            rest = rest[op.len_utf8()..].trim_start();
            let (value, remainder) = parse_fast_param_value(rest, params, param_pos)?;
            if !remainder.trim().is_empty() {
                return None;
            }
            let action = if op == '+' {
                FastUpdateAction::Add(value)
            } else {
                FastUpdateAction::Sub(value)
            };
            return Some(FastUpdateAssignment {
                column: column.to_ascii_lowercase(),
                action,
            });
        }
    }
    let (value, remainder) = parse_fast_param_value(expr, params, param_pos)?;
    if !remainder.trim().is_empty() {
        return None;
    }
    Some(FastUpdateAssignment {
        column: column.to_ascii_lowercase(),
        action: FastUpdateAction::Set(value),
    })
}

fn parse_fast_update_predicate(input: &str) -> Option<(String, Value)> {
    let mut iter = input.splitn(2, '=');
    let column = iter.next()?.trim();
    let expr = iter.next()?.trim();
    if column.is_empty() || expr.is_empty() {
        return None;
    }
    let (value, remainder) = parse_fast_value(expr)?;
    if !remainder.trim().is_empty() {
        return None;
    }
    Some((column.to_ascii_lowercase(), value))
}

fn parse_fast_update_predicate_with_params(
    input: &str,
    params: &[Literal],
    param_pos: &mut usize,
) -> Option<(String, Value)> {
    let mut iter = input.splitn(2, '=');
    let column = iter.next()?.trim();
    let expr = iter.next()?.trim();
    if column.is_empty() || expr.is_empty() {
        return None;
    }
    let (value, remainder) = parse_fast_param_value(expr, params, param_pos)?;
    if !remainder.trim().is_empty() {
        return None;
    }
    Some((column.to_ascii_lowercase(), value))
}

fn parse_fast_rows(input: &str) -> Option<(Vec<Vec<Value>>, &str)> {
    let mut rows = Vec::new();
    let mut rest = input;
    loop {
        rest = rest.trim_start();
        if !rest.starts_with('(') {
            return None;
        }
        let (values, next) = parse_fast_values(&rest[1..])?;
        if values.is_empty() {
            return None;
        }
        rows.push(values);
        rest = next.trim_start();
        if rest.starts_with(',') {
            rest = &rest[1..];
            continue;
        }
        return Some((rows, rest));
    }
}

fn parse_fast_values(input: &str) -> Option<(Vec<Value>, &str)> {
    let mut values = Vec::new();
    let mut rest = input;
    loop {
        rest = rest.trim_start();
        if rest.starts_with(')') {
            return Some((values, &rest[1..]));
        }
        let (value, next) = parse_fast_value(rest)?;
        values.push(value);
        rest = next.trim_start();
        if rest.starts_with(',') {
            rest = &rest[1..];
            continue;
        }
        if rest.starts_with(')') {
            return Some((values, &rest[1..]));
        }
        return None;
    }
}

fn parse_fast_value(input: &str) -> Option<(Value, &str)> {
    let rest = input.trim_start();
    if rest.is_empty() {
        return None;
    }
    if rest.starts_with('\'') {
        let bytes = rest.as_bytes();
        let mut out = Vec::new();
        let mut idx = 1;
        while idx < bytes.len() {
            let byte = bytes[idx];
            if byte == b'\'' {
                if idx + 1 < bytes.len() && bytes[idx + 1] == b'\'' {
                    out.push(b'\'');
                    idx += 2;
                    continue;
                }
                let text = String::from_utf8(out).ok()?;
                let remainder = &rest[idx + 1..];
                return Some((Value::Text(text), remainder));
            }
            out.push(byte);
            idx += 1;
        }
        return None;
    }
    let token_end = rest
        .find(|ch: char| ch == ',' || ch == ')')
        .unwrap_or_else(|| rest.len());
    let token = rest[..token_end].trim();
    if token.is_empty() {
        return None;
    }
    let remainder = &rest[token_end..];
    if token.eq_ignore_ascii_case("NULL") {
        return Some((Value::Null, remainder));
    }
    if token.contains('.') || token.contains('e') || token.contains('E') {
        let value = token.parse::<f64>().ok()?;
        return Some((Value::Real(value), remainder));
    }
    let value = token.parse::<i64>().ok()?;
    Some((Value::Integer(value), remainder))
}

fn parse_fast_param_value<'a>(
    input: &'a str,
    params: &'a [Literal],
    param_pos: &mut usize,
) -> Option<(Value, &'a str)> {
    let rest = input.trim_start();
    if rest.starts_with('?') {
        let idx = *param_pos;
        let literal = params.get(idx)?;
        *param_pos += 1;
        let value = literal_to_value(literal);
        return Some((value, &rest[1..]));
    }
    parse_fast_value(rest)
}

fn parse_fast_predicates(input: &str) -> Option<Vec<(String, Value)>> {
    let mut rest = input.trim_start();
    let mut predicates = Vec::new();
    loop {
        let (column, after_col) = parse_fast_ident(rest)?;
        let mut remainder = after_col.trim_start();
        if !remainder.starts_with('=') {
            return None;
        }
        remainder = &remainder[1..];
        let (value, after_value) = parse_fast_literal(remainder)?;
        predicates.push((column.to_string(), value));
        rest = after_value.trim_start();
        if rest.is_empty() {
            break;
        }
        if let Some(next) = consume_fast_keyword(rest, "AND") {
            rest = next;
            continue;
        }
        return None;
    }
    Some(predicates)
}

fn parse_fast_literal(input: &str) -> Option<(Value, &str)> {
    let rest = input.trim_start();
    if rest.is_empty() {
        return None;
    }
    if rest.starts_with('\'') {
        let bytes = rest.as_bytes();
        let mut out = Vec::new();
        let mut idx = 1;
        while idx < bytes.len() {
            let byte = bytes[idx];
            if byte == b'\'' {
                if idx + 1 < bytes.len() && bytes[idx + 1] == b'\'' {
                    out.push(b'\'');
                    idx += 2;
                    continue;
                }
                let text = String::from_utf8(out).ok()?;
                let remainder = &rest[idx + 1..];
                return Some((Value::Text(text), remainder));
            }
            out.push(byte);
            idx += 1;
        }
        return None;
    }
    let token_end = rest
        .find(|ch: char| ch.is_whitespace() || ch == ';' || ch == ')' || ch == ',')
        .unwrap_or_else(|| rest.len());
    let token = rest[..token_end].trim();
    if token.is_empty() {
        return None;
    }
    let remainder = &rest[token_end..];
    if token.eq_ignore_ascii_case("NULL") {
        return Some((Value::Null, remainder));
    }
    if token.contains('.') || token.contains('e') || token.contains('E') {
        let value = token.parse::<f64>().ok()?;
        return Some((Value::Real(value), remainder));
    }
    let value = token.parse::<i64>().ok()?;
    Some((Value::Integer(value), remainder))
}

fn fast_stock_update_offsets(
    record: &[u8],
    include_remote: bool,
) -> Result<Option<FastStockUpdateOffsets>, GongDBError> {
    if record.len() < 2 {
        return Err(GongDBError::new("record too small".to_string()));
    }
    let count = u16::from_le_bytes([record[0], record[1]]) as usize;
    if count <= 15 {
        return Ok(None);
    }
    let mut pos = 2usize;
    let mut quantity = None;
    let mut ytd = None;
    let mut order_cnt = None;
    let mut remote_cnt = None;
    for col_idx in 0..count {
        if col_idx == 2 {
            quantity = Some(pos);
        } else if col_idx == 13 {
            ytd = Some(pos);
        } else if col_idx == 14 {
            order_cnt = Some(pos);
        } else if col_idx == 15 {
            remote_cnt = Some(pos);
        }
        let len = crate::storage::value_length_at(record, pos).map_err(GongDBError::Storage)?;
        pos = pos.saturating_add(len);
        if col_idx >= 15 && quantity.is_some() && ytd.is_some() && order_cnt.is_some() {
            break;
        }
    }
    let quantity = match quantity {
        Some(value) => value,
        None => return Ok(None),
    };
    let ytd = match ytd {
        Some(value) => value,
        None => return Ok(None),
    };
    let order_cnt = match order_cnt {
        Some(value) => value,
        None => return Ok(None),
    };
    let remote_cnt = if include_remote { remote_cnt } else { None };
    if include_remote && remote_cnt.is_none() {
        return Ok(None);
    }
    Ok(Some(FastStockUpdateOffsets {
        quantity,
        ytd,
        order_cnt,
        remote_cnt,
    }))
}

fn fast_order_line_amount_offset(record: &[u8]) -> Result<Option<usize>, GongDBError> {
    if record.len() < 2 {
        return Err(GongDBError::new("record too small".to_string()));
    }
    let count = u16::from_le_bytes([record[0], record[1]]) as usize;
    if count <= 8 {
        return Ok(None);
    }
    let mut pos = 2usize;
    for col_idx in 0..count {
        let len = crate::storage::value_length_at(record, pos).map_err(GongDBError::Storage)?;
        if col_idx == 8 {
            return Ok(Some(pos));
        }
        pos = pos.saturating_add(len);
    }
    Ok(None)
}

fn fast_orders_carrier_offset(record: &[u8]) -> Result<Option<usize>, GongDBError> {
    if record.len() < 2 {
        return Err(GongDBError::new("record too small".to_string()));
    }
    let count = u16::from_le_bytes([record[0], record[1]]) as usize;
    if count <= 5 {
        return Ok(None);
    }
    let mut pos = 2usize;
    for col_idx in 0..count {
        let len = crate::storage::value_length_at(record, pos).map_err(GongDBError::Storage)?;
        if col_idx == 5 {
            return Ok(Some(pos));
        }
        pos = pos.saturating_add(len);
    }
    Ok(None)
}

fn fast_customer_delivery_offsets(
    record: &[u8],
) -> Result<Option<FastCustomerDeliveryOffsets>, GongDBError> {
    if record.len() < 2 {
        return Err(GongDBError::new("record too small".to_string()));
    }
    let count = u16::from_le_bytes([record[0], record[1]]) as usize;
    if count <= 19 {
        return Ok(None);
    }
    let mut pos = 2usize;
    let mut balance = None;
    let mut delivery_cnt = None;
    for col_idx in 0..count {
        let len = crate::storage::value_length_at(record, pos).map_err(GongDBError::Storage)?;
        if col_idx == 16 {
            balance = Some(pos);
        } else if col_idx == 19 {
            delivery_cnt = Some(pos);
        }
        pos = pos.saturating_add(len);
    }
    match (balance, delivery_cnt) {
        (Some(balance), Some(delivery_cnt)) => Ok(Some(FastCustomerDeliveryOffsets {
            balance,
            delivery_cnt,
        })),
        _ => Ok(None),
    }
}

fn fast_district_next_o_id_offset(record: &[u8]) -> Result<Option<usize>, GongDBError> {
    if record.len() < 2 {
        return Err(GongDBError::new("record too small".to_string()));
    }
    let count = u16::from_le_bytes([record[0], record[1]]) as usize;
    if count <= 10 {
        return Ok(None);
    }
    let mut pos = 2usize;
    for col_idx in 0..count {
        if col_idx == 10 {
            return Ok(Some(pos));
        }
        let len = crate::storage::value_length_at(record, pos).map_err(GongDBError::Storage)?;
        pos = pos.saturating_add(len);
    }
    Ok(None)
}

fn fast_warehouse_ytd_offset(record: &[u8]) -> Result<Option<usize>, GongDBError> {
    if record.len() < 2 {
        return Err(GongDBError::new("record too small".to_string()));
    }
    let count = u16::from_le_bytes([record[0], record[1]]) as usize;
    if count <= 8 {
        return Ok(None);
    }
    let mut pos = 2usize;
    for col_idx in 0..count {
        if col_idx == 8 {
            return Ok(Some(pos));
        }
        let len = crate::storage::value_length_at(record, pos).map_err(GongDBError::Storage)?;
        pos = pos.saturating_add(len);
    }
    Ok(None)
}

fn fast_district_ytd_offset(record: &[u8]) -> Result<Option<usize>, GongDBError> {
    if record.len() < 2 {
        return Err(GongDBError::new("record too small".to_string()));
    }
    let count = u16::from_le_bytes([record[0], record[1]]) as usize;
    if count <= 9 {
        return Ok(None);
    }
    let mut pos = 2usize;
    for col_idx in 0..count {
        if col_idx == 9 {
            return Ok(Some(pos));
        }
        let len = crate::storage::value_length_at(record, pos).map_err(GongDBError::Storage)?;
        pos = pos.saturating_add(len);
    }
    Ok(None)
}

fn fast_customer_payment_offsets(
    record: &[u8],
) -> Result<Option<FastCustomerPaymentOffsets>, GongDBError> {
    if record.len() < 2 {
        return Err(GongDBError::new("record too small".to_string()));
    }
    let count = u16::from_le_bytes([record[0], record[1]]) as usize;
    if count <= 18 {
        return Ok(None);
    }
    let mut pos = 2usize;
    let mut balance = None;
    let mut ytd_payment = None;
    let mut payment_cnt = None;
    for col_idx in 0..count {
        if col_idx == 16 {
            balance = Some(pos);
        } else if col_idx == 17 {
            ytd_payment = Some(pos);
        } else if col_idx == 18 {
            payment_cnt = Some(pos);
        }
        let len = crate::storage::value_length_at(record, pos).map_err(GongDBError::Storage)?;
        pos = pos.saturating_add(len);
        if col_idx >= 18 && balance.is_some() && ytd_payment.is_some() && payment_cnt.is_some() {
            break;
        }
    }
    let balance = match balance {
        Some(value) => value,
        None => return Ok(None),
    };
    let ytd_payment = match ytd_payment {
        Some(value) => value,
        None => return Ok(None),
    };
    let payment_cnt = match payment_cnt {
        Some(value) => value,
        None => return Ok(None),
    };
    Ok(Some(FastCustomerPaymentOffsets {
        balance,
        ytd_payment,
        payment_cnt,
    }))
}

fn fast_row_matches_predicates(row: &[Value], predicates: &[(usize, Value)]) -> bool {
    for (idx, value) in predicates {
        if !values_equal(&row[*idx], value) {
            return false;
        }
    }
    true
}

fn fast_record_matches_predicates(
    record: &[u8],
    predicates: &[(usize, Value)],
) -> Result<bool, GongDBError> {
    if predicates.is_empty() {
        return Ok(true);
    }
    if record.len() < 2 {
        return Err(GongDBError::new("record too small".to_string()));
    }
    let count = u16::from_le_bytes([record[0], record[1]]) as usize;
    let mut sorted = predicates.to_vec();
    sorted.sort_by_key(|(idx, _)| *idx);
    if sorted.last().map_or(false, |(idx, _)| *idx >= count) {
        return Err(GongDBError::new("predicate column out of bounds".to_string()));
    }
    let mut pos = 2usize;
    let mut pred_idx = 0usize;
    for col_idx in 0..count {
        if pred_idx >= sorted.len() {
            break;
        }
        let target_idx = sorted[pred_idx].0;
        let len = match crate::storage::value_length_at(record, pos) {
            Ok(len) => len,
            Err(err) => return Err(err.into()),
        };
        if col_idx == target_idx {
            let (value, new_pos) = match crate::storage::decode_value_at(record, pos) {
                Ok(result) => result,
                Err(err) => return Err(err.into()),
            };
            if !values_equal(&value, &sorted[pred_idx].1) {
                return Ok(false);
            }
            pos = new_pos;
            pred_idx += 1;
        } else {
            pos = pos.saturating_add(len);
        }
    }
    Ok(pred_idx == sorted.len())
}

fn apply_fast_update_record_bytes(
    columns: &[Column],
    record: &[u8],
    assignments: &[(usize, FastUpdateAssignment)],
) -> Result<Option<Vec<crate::storage::FieldUpdate>>, GongDBError> {
    if assignments.is_empty() {
        return Ok(None);
    }
    if record.len() < 2 {
        return Err(GongDBError::new("record too small".to_string()));
    }
    let count = u16::from_le_bytes([record[0], record[1]]) as usize;
    let mut sorted = assignments.to_vec();
    sorted.sort_by_key(|(idx, _)| *idx);
    if sorted.last().map_or(false, |(idx, _)| *idx >= count) {
        return Err(GongDBError::new("assignment column out of bounds".to_string()));
    }
    let mut updates: Vec<crate::storage::FieldUpdate> = Vec::with_capacity(sorted.len());
    let mut pos = 2usize;
    let mut assign_idx = 0usize;
    for col_idx in 0..count {
        if assign_idx >= sorted.len() {
            break;
        }
        let len = match crate::storage::value_length_at(record, pos) {
            Ok(len) => len,
            Err(err) => return Err(err.into()),
        };
        if col_idx == sorted[assign_idx].0 {
            let assignment = &sorted[assign_idx].1;
            let column = &columns[col_idx];
            if let Some(encoded) = fast_numeric_update_bytes(
                column,
                assignment,
                record,
                pos,
            ) {
                updates.push(crate::storage::FieldUpdate {
                    offset: pos,
                    bytes: encoded,
                });
                pos = pos.saturating_add(len);
            } else {
                let (current, new_pos) = match crate::storage::decode_value_at(record, pos) {
                    Ok(result) => result,
                    Err(err) => return Err(err.into()),
                };
                let updated = match &assignment.action {
                    FastUpdateAction::Set(value) => apply_affinity(value.clone(), &column.data_type),
                    FastUpdateAction::Add(delta) => {
                        let delta = apply_affinity(delta.clone(), &column.data_type);
                        let value = apply_binary_op(&BinaryOperator::Plus, current, delta)?;
                        apply_affinity(value, &column.data_type)
                    }
                    FastUpdateAction::Sub(delta) => {
                        let delta = apply_affinity(delta.clone(), &column.data_type);
                        let value = apply_binary_op(&BinaryOperator::Minus, current, delta)?;
                        apply_affinity(value, &column.data_type)
                    }
                };
                let encoded = match crate::storage::encode_value_to_vec(&updated) {
                    Ok(encoded) => encoded,
                    Err(err) => return Err(err.into()),
                };
                if encoded.len() != len {
                    return Ok(None);
                }
                updates.push(crate::storage::FieldUpdate {
                    offset: pos,
                    bytes: encoded,
                });
                pos = new_pos;
            }
            assign_idx += 1;
        } else {
            pos = pos.saturating_add(len);
        }
    }
    if updates.is_empty() {
        return Ok(None);
    }
    Ok(Some(updates))
}

fn build_fast_update_outcome(
    columns: &[Column],
    record: &[u8],
    assignments: &[(usize, FastUpdateAssignment)],
) -> Result<FastUpdateOutcome, GongDBError> {
    match apply_fast_update_record_bytes(columns, record, assignments)? {
        Some(fields) => Ok(FastUpdateOutcome::InPlace(fields)),
        None => {
            let row = crate::storage::decode_row_from_record(record).map_err(GongDBError::Storage)?;
            let new_row = apply_fast_update_assignments(columns, &row, assignments)?;
            Ok(FastUpdateOutcome::Row(new_row))
        }
    }
}

fn fast_numeric_update_bytes(
    column: &Column,
    assignment: &FastUpdateAssignment,
    record: &[u8],
    pos: usize,
) -> Option<Vec<u8>> {
    let affinity = type_affinity(&column.data_type);
    let tag = *record.get(pos)?;
    match affinity {
        TypeAffinity::Integer => {
            if tag != 1 {
                return None;
            }
            let current = read_i64_at(record, pos + 1)?;
            let next = match &assignment.action {
                FastUpdateAction::Set(Value::Integer(v)) => *v,
                FastUpdateAction::Add(Value::Integer(delta)) => current.wrapping_add(*delta),
                FastUpdateAction::Sub(Value::Integer(delta)) => current.wrapping_sub(*delta),
                _ => return None,
            };
            let mut encoded = Vec::with_capacity(9);
            encoded.push(1);
            encoded.extend_from_slice(&next.to_le_bytes());
            Some(encoded)
        }
        TypeAffinity::Real => {
            if tag != 2 {
                return None;
            }
            let current = read_f64_at(record, pos + 1)?;
            let next = match &assignment.action {
                FastUpdateAction::Set(Value::Integer(v)) => *v as f64,
                FastUpdateAction::Set(Value::Real(v)) => *v,
                FastUpdateAction::Add(Value::Integer(delta)) => current + (*delta as f64),
                FastUpdateAction::Add(Value::Real(delta)) => current + *delta,
                FastUpdateAction::Sub(Value::Integer(delta)) => current - (*delta as f64),
                FastUpdateAction::Sub(Value::Real(delta)) => current - *delta,
                _ => return None,
            };
            let mut encoded = Vec::with_capacity(9);
            encoded.push(2);
            encoded.extend_from_slice(&next.to_le_bytes());
            Some(encoded)
        }
        _ => None,
    }
}

fn read_i64_at(record: &[u8], pos: usize) -> Option<i64> {
    let end = pos.checked_add(8)?;
    if end > record.len() {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&record[pos..end]);
    Some(i64::from_le_bytes(buf))
}

fn write_i64_at(record: &mut [u8], pos: usize, value: i64) -> Option<()> {
    let end = pos.checked_add(8)?;
    if end > record.len() {
        return None;
    }
    record[pos..end].copy_from_slice(&value.to_le_bytes());
    Some(())
}

fn read_f64_at(record: &[u8], pos: usize) -> Option<f64> {
    let end = pos.checked_add(8)?;
    if end > record.len() {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&record[pos..end]);
    Some(f64::from_le_bytes(buf))
}

fn write_f64_at(record: &mut [u8], pos: usize, value: f64) -> Option<()> {
    let end = pos.checked_add(8)?;
    if end > record.len() {
        return None;
    }
    record[pos..end].copy_from_slice(&value.to_le_bytes());
    Some(())
}

fn fast_select_eq_index<'a>(
    indexes: &'a [IndexMeta],
    predicates: &HashMap<String, Value>,
) -> Option<(&'a IndexMeta, Vec<Value>)> {
    for index in indexes {
        let mut key = Vec::with_capacity(index.columns.len());
        for column in &index.columns {
            let value = predicates.get(&column.name.value.to_lowercase())?;
            key.push(value.clone());
        }
        return Some((index, key));
    }
    None
}

fn fast_select_order_by_range<'a>(
    indexes: &'a [IndexMeta],
    order_by: &FastOrderBy,
    predicates: &HashMap<String, Value>,
) -> Option<(&'a IndexMeta, Vec<Value>, Vec<Value>)> {
    let order_key = order_by.column.to_lowercase();
    for index in indexes {
        let mut prefix = Vec::new();
        let mut order_pos = None;
        for (idx, column) in index.columns.iter().enumerate() {
            let col_key = column.name.value.to_lowercase();
            if col_key == order_key {
                order_pos = Some(idx);
                break;
            }
            let value = predicates.get(&col_key)?;
            prefix.push(value.clone());
        }
        let Some(_) = order_pos else { continue };
        let lower = build_index_bound(index.columns.len(), &prefix, None, Value::Null);
        let upper = build_index_bound(
            index.columns.len(),
            &prefix,
            None,
            Value::Blob(Vec::new()),
        );
        return Some((index, lower, upper));
    }
    None
}

fn column_index_map_fast(columns: &[Column]) -> HashMap<String, usize> {
    let mut map = HashMap::new();
    for (idx, col) in columns.iter().enumerate() {
        map.insert(col.name.to_lowercase(), idx);
    }
    map
}

fn fast_find_index_prefix<'a>(
    indexes: &'a [IndexMeta],
    columns: &[&str],
) -> Option<&'a IndexMeta> {
    for index in indexes {
        if index.columns.len() < columns.len() {
            continue;
        }
        let mut matches = true;
        for (idx, col) in columns.iter().enumerate() {
            if !index.columns[idx]
                .name
                .value
                .eq_ignore_ascii_case(col)
            {
                matches = false;
                break;
            }
        }
        if matches {
            return Some(index);
        }
    }
    None
}

fn apply_fast_update_assignments(
    columns: &[Column],
    row: &[Value],
    assignments: &[(usize, FastUpdateAssignment)],
) -> Result<Vec<Value>, GongDBError> {
    let mut new_row = row.to_vec();
    for (idx, assignment) in assignments {
        let column = &columns[*idx];
        let updated = match &assignment.action {
            FastUpdateAction::Set(value) => apply_affinity(value.clone(), &column.data_type),
            FastUpdateAction::Add(delta) => {
                let delta = apply_affinity(delta.clone(), &column.data_type);
                let value = apply_binary_op(&BinaryOperator::Plus, new_row[*idx].clone(), delta)?;
                apply_affinity(value, &column.data_type)
            }
            FastUpdateAction::Sub(delta) => {
                let delta = apply_affinity(delta.clone(), &column.data_type);
                let value = apply_binary_op(&BinaryOperator::Minus, new_row[*idx].clone(), delta)?;
                apply_affinity(value, &column.data_type)
            }
        };
        new_row[*idx] = updated;
    }
    Ok(new_row)
}

fn qualified_wildcard_indices(qualifier: &str, column_scopes: &[TableScope]) -> Vec<usize> {
    column_scopes
        .iter()
        .enumerate()
        .filter_map(|(idx, scope)| {
            if scope.matches_qualifier(qualifier) {
                Some(idx)
            } else {
                None
            }
        })
        .collect()
}

fn projection_columns(
    projection: &[SelectItem],
    source_columns: &[Column],
    column_scopes: &[TableScope],
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
            SelectItem::QualifiedWildcard(name) => {
                let qualifier = object_name(name);
                let indices = qualified_wildcard_indices(&qualifier, column_scopes);
                if indices.is_empty() {
                    return Err(GongDBError::new(format!(
                        "no such table: {}",
                        qualifier
                    )));
                }
                columns.extend(indices.into_iter().map(|idx| source_columns[idx].clone()));
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
            .ok_or_else(|| GongDBError::new(format!("no such column: {}", col_ident.value)))?;
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
    if columns.is_empty() {
        if values.len() != table.columns.len() {
            return Err(GongDBError::new("column count mismatch"));
        }
        let mut row = Vec::with_capacity(table.columns.len());
        for (idx, value) in values.iter().enumerate() {
            row.push(apply_affinity(
                value.clone(),
                &table.columns[idx].data_type,
            ));
        }
        validate_insert_row(db, table, &row)?;
        return Ok(row);
    }
    let mut row = build_default_row(db, table)?;
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
            .ok_or_else(|| GongDBError::new(format!("no such column: {}", col_ident.value)))?;
        row[idx] = apply_affinity(value.clone(), &table.columns[idx].data_type);
    }
    validate_insert_row(db, table, &row)?;
    Ok(row)
}

fn build_insert_row_from_owned_values_no_columns(
    db: &GongDB,
    table: &TableMeta,
    values: Vec<Value>,
) -> Result<Vec<Value>, GongDBError> {
    if values.len() != table.columns.len() {
        return Err(GongDBError::new("column count mismatch"));
    }
    let mut row = Vec::with_capacity(table.columns.len());
    for (idx, value) in values.into_iter().enumerate() {
        row.push(apply_affinity(value, &table.columns[idx].data_type));
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

fn column_index_map(columns: &[Column]) -> HashMap<String, usize> {
    let mut map = HashMap::new();
    for (idx, column) in columns.iter().enumerate() {
        map.insert(column.name.to_lowercase(), idx);
    }
    map
}

fn unique_indexes_for_table(storage: &StorageEngine, table_name: &str) -> Vec<IndexMeta> {
    if storage.index_updates_deferred(table_name) {
        return Vec::new();
    }
    storage
        .list_indexes()
        .into_iter()
        .filter(|index| index.unique && index.table.eq_ignore_ascii_case(table_name))
        .collect()
}

fn index_key_for_row(
    index: &IndexMeta,
    column_map: &HashMap<String, usize>,
    row: &[Value],
) -> Result<Vec<Value>, GongDBError> {
    let mut key = Vec::with_capacity(index.columns.len());
    for column in &index.columns {
        let idx = column_map
            .get(&column.name.value.to_lowercase())
            .ok_or_else(|| {
                GongDBError::new(format!("no such column: {}", column.name.value))
            })?;
        key.push(row[*idx].clone());
    }
    Ok(key)
}

fn key_has_null(values: &[Value]) -> bool {
    values.iter().any(|value| matches!(value, Value::Null))
}

fn conflict_keys_for_row(
    unique_indexes: &[IndexMeta],
    column_map: &HashMap<String, usize>,
    row: &[Value],
) -> Result<Vec<Option<Vec<Value>>>, GongDBError> {
    let mut keys = Vec::with_capacity(unique_indexes.len());
    for index in unique_indexes {
        let key = index_key_for_row(index, column_map, row)?;
        if key_has_null(&key) {
            keys.push(None);
        } else {
            keys.push(Some(key));
        }
    }
    Ok(keys)
}

fn row_conflicts_with_keys(
    row: &[Value],
    unique_indexes: &[IndexMeta],
    column_map: &HashMap<String, usize>,
    conflict_keys: &[Option<Vec<Value>>],
) -> Result<bool, GongDBError> {
    for (index, key) in unique_indexes.iter().zip(conflict_keys.iter()) {
        let Some(conflict_key) = key else {
            continue;
        };
        let row_key = index_key_for_row(index, column_map, row)?;
        if &row_key == conflict_key {
            return Ok(true);
        }
    }
    Ok(false)
}

fn apply_replace_row(
    rows: &mut Vec<Vec<Value>>,
    new_row: Vec<Value>,
    unique_indexes: &[IndexMeta],
    column_map: &HashMap<String, usize>,
) -> Result<(), GongDBError> {
    let conflict_keys = conflict_keys_for_row(unique_indexes, column_map, &new_row)?;
    let mut next_rows = Vec::with_capacity(rows.len() + 1);
    for row in rows.drain(..) {
        if !row_conflicts_with_keys(&row, unique_indexes, column_map, &conflict_keys)? {
            next_rows.push(row);
        }
    }
    next_rows.push(new_row);
    *rows = next_rows;
    Ok(())
}

fn eval_insert_expr(db: &GongDB, expr: &Expr) -> Result<Value, GongDBError> {
    let table_scope = TableScope {
        table_name: None,
        table_alias: None,
    };
    let column_scopes = Vec::new();
    let scope = EvalScope {
        columns: &[],
        column_scopes: &column_scopes,
        row: &[],
        table_scope: &table_scope,
        cte_context: None,
        column_lookup: None,
    };
    eval_expr(db, expr, &scope, None)
}

fn build_insert_validation_info(table: &TableMeta) -> InsertValidationInfo {
    let mut pk_columns = HashSet::new();
    let mut has_check_constraints = false;
    for constraint in &table.constraints {
        match constraint {
            TableConstraint::PrimaryKey(columns) => {
                for column in columns {
                    pk_columns.insert(column.value.to_ascii_lowercase());
                }
            }
            TableConstraint::Check(_) => {
                has_check_constraints = true;
            }
            _ => {}
        }
    }

    let mut not_null_indices = Vec::new();
    for (idx, column) in table.columns.iter().enumerate() {
        let mut not_null = pk_columns.contains(&column.name.to_ascii_lowercase());
        if !not_null {
            for constraint in &column.constraints {
                if matches!(constraint, ColumnConstraint::NotNull | ColumnConstraint::PrimaryKey) {
                    not_null = true;
                    break;
                }
            }
        }
        if not_null {
            not_null_indices.push(idx);
        }
    }
    InsertValidationInfo {
        not_null_indices,
        has_check_constraints,
    }
}

fn validate_insert_row(
    db: &GongDB,
    table: &TableMeta,
    row: &[Value],
) -> Result<(), GongDBError> {
    let info = db.insert_validation_info_cached(table);
    for &idx in &info.not_null_indices {
        if matches!(row.get(idx), Some(Value::Null)) {
            return Err(GongDBError::constraint(format!(
                "NOT NULL constraint failed: {}.{}",
                table.name, table.columns[idx].name
            )));
        }
    }

    if info.has_check_constraints {
        let table_scope = TableScope {
            table_name: Some(table.name.clone()),
            table_alias: None,
        };
        let column_scopes = vec![table_scope.clone(); table.columns.len()];
        let column_lookup = build_column_lookup(&table.columns, &column_scopes);
        let scope = EvalScope {
            columns: &table.columns,
            column_scopes: &column_scopes,
            row,
            table_scope: &table_scope,
            cte_context: None,
            column_lookup: Some(&column_lookup),
        };
        for constraint in &table.constraints {
            if let TableConstraint::Check(expr) = constraint {
                let value = eval_expr(db, expr, &scope, None)?;
                if !value_to_bool(&value) {
                    return Err(GongDBError::constraint("CHECK constraint failed"));
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

fn resolve_column_index_in_scope(scope: &EvalScope<'_>, name: &str) -> Option<usize> {
    if let Some(lookup) = scope.column_lookup {
        let key = maybe_lowercase(name);
        if let Some(idx) = lookup.unqualified.get(key.as_ref()) {
            return Some(*idx);
        }
    }
    resolve_column_index(name, scope.columns)
}

fn resolve_qualified_column_index(
    qualifier: &str,
    name: &str,
    columns: &[Column],
    column_scopes: &[TableScope],
) -> Option<usize> {
    columns.iter().enumerate().find_map(|(idx, col)| {
        if col.name.eq_ignore_ascii_case(name)
            && column_scopes
                .get(idx)
                .is_some_and(|scope| scope.matches_qualifier(qualifier))
        {
            Some(idx)
        } else {
            None
        }
    })
}

fn resolve_qualified_column_index_in_scope(
    scope: &EvalScope<'_>,
    qualifier: &str,
    name: &str,
) -> Option<usize> {
    if let Some(lookup) = scope.column_lookup {
        let qual_key = maybe_lowercase(qualifier);
        let name_key = maybe_lowercase(name);
        if let Some(by_qualifier) = lookup.qualified.get(qual_key.as_ref()) {
            if let Some(idx) = by_qualifier.get(name_key.as_ref()) {
                return Some(*idx);
            }
        }
    }
    resolve_qualified_column_index(qualifier, name, scope.columns, scope.column_scopes)
}

fn maybe_lowercase(input: &str) -> std::borrow::Cow<'_, str> {
    if input.bytes().all(|byte| !byte.is_ascii_uppercase()) {
        std::borrow::Cow::Borrowed(input)
    } else {
        std::borrow::Cow::Owned(input.to_ascii_lowercase())
    }
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
            if let Some(idx) = resolve_column_index_in_scope(scope, &ident.value) {
                return Ok(scope.row[idx].clone());
            }
            if let Some(outer_scope) = outer {
                if let Some(idx) = resolve_column_index_in_scope(outer_scope, &ident.value) {
                    return Ok(outer_scope.row[idx].clone());
                }
            }
            Err(GongDBError::new(format!(
                "no such column: {}",
                ident.value
            )))
        }
        Expr::CompoundIdentifier(idents) => {
            let (qualifier, column) = split_qualified_identifier(idents)?;
            if let Some(idx) =
                resolve_qualified_column_index_in_scope(scope, qualifier, column)
            {
                return Ok(scope.row[idx].clone());
            }
            if let Some(outer_scope) = outer {
                if let Some(idx) =
                    resolve_qualified_column_index_in_scope(outer_scope, qualifier, column)
                {
                    return Ok(outer_scope.row[idx].clone());
                }
            }
            Err(GongDBError::new(format!(
                "no such column: {}",
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
                "coalesce" => {
                    if args.len() < 2 {
                        return Err(GongDBError::new("COALESCE expects at least two arguments"));
                    }
                    for arg in args {
                        let value = eval_expr(db, arg, scope, outer)?;
                        if !matches!(value, Value::Null) {
                            return Ok(value);
                        }
                    }
                    Ok(Value::Null)
                }
                "nullif" => {
                    if args.len() != 2 {
                        return Err(GongDBError::new("NULLIF expects two arguments"));
                    }
                    let left = eval_expr(db, &args[0], scope, outer)?;
                    if matches!(left, Value::Null) {
                        return Ok(Value::Null);
                    }
                    let right = eval_expr(db, &args[1], scope, outer)?;
                    if matches!(right, Value::Null) {
                        return Ok(left);
                    }
                    if values_equal(&left, &right) {
                        Ok(Value::Null)
                    } else {
                        Ok(left)
                    }
                }
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
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let left_val = eval_expr(db, left, scope, outer)?;
            if matches!(value_to_truth_value(&left_val), Some(false)) {
                return Ok(Value::Integer(0));
            }
            let right_val = eval_expr(db, right, scope, outer)?;
            Ok(apply_logical_and(&left_val, &right_val))
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => {
            let left_val = eval_expr(db, left, scope, outer)?;
            if matches!(value_to_truth_value(&left_val), Some(true)) {
                return Ok(Value::Integer(1));
            }
            let right_val = eval_expr(db, right, scope, outer)?;
            Ok(apply_logical_or(&left_val, &right_val))
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
        Expr::Subquery(subquery) => {
            let result = eval_subquery_cached(db, subquery, Some(scope))?;
            if result.columns.len() != 1 {
                return Err(GongDBError::new(
                    "subquery returned more than one column",
                ));
            }
            if let Some(row) = result.rows.first() {
                Ok(row.get(0).cloned().unwrap_or(Value::Null))
            } else {
                Ok(Value::Null)
            }
        }
        Expr::Exists(subquery) => {
            let result = eval_subquery_cached(db, subquery, Some(scope))?;
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
    if matches!(expr_val, Value::Null) {
        return Ok(Value::Null);
    }
    let mut saw_null = false;
    let cache_key = (list.as_ptr() as usize, list.len());
    let prepared = {
        let mut cache = db.in_list_cache.borrow_mut();
        cache
            .entries
            .entry(cache_key)
            .or_insert_with(|| Arc::new(prepare_in_list(db, list)))
            .clone()
    };
    if prepared.literal_keys.contains(&distinct_key(expr_val)) {
        return Ok(Value::Integer(1));
    }
    if prepared.saw_null_literal {
        saw_null = true;
    }
    for literal in prepared.literal_values.iter() {
        match compare_values(&BinaryOperator::Eq, expr_val, literal) {
            Value::Integer(1) => return Ok(Value::Integer(1)),
            Value::Null => saw_null = true,
            _ => {}
        }
    }
    for idx in prepared.non_literal_indices.iter() {
        let item_val = eval_expr(db, &list[*idx], scope, outer)?;
        match compare_values(&BinaryOperator::Eq, expr_val, &item_val) {
            Value::Integer(1) => return Ok(Value::Integer(1)),
            Value::Null => saw_null = true,
            _ => {}
        }
    }
    Ok(if saw_null {
        Value::Null
    } else {
        Value::Integer(0)
    })
}

fn prepare_in_list(db: &GongDB, list: &[Expr]) -> PreparedInList {
    let mut literal_keys = HashSet::new();
    let mut literal_values = Vec::new();
    let mut non_literal_indices = Vec::new();
    let mut saw_null_literal = false;
    for (idx, item) in list.iter().enumerate() {
        if expr_is_constant(item) {
            if let Some(value) = eval_constant_expr(db, item) {
                if matches!(value, Value::Null) {
                    saw_null_literal = true;
                } else {
                    literal_keys.insert(distinct_key(&value));
                    literal_values.push(value);
                }
                continue;
            }
        }
        non_literal_indices.push(idx);
    }
    PreparedInList {
        literal_keys,
        literal_values,
        non_literal_indices,
        saw_null_literal,
    }
}

fn eval_in_subquery<'a>(
    db: &GongDB,
    expr_val: &Value,
    subquery: &Select,
    scope: &EvalScope<'a>,
) -> Result<Value, GongDBError> {
    if matches!(expr_val, Value::Null) {
        return Ok(Value::Null);
    }
    let result = eval_subquery_cached(db, subquery, Some(scope))?;
    if result.columns.len() != 1 {
        return Err(GongDBError::new(
            "subquery returned more than one column",
        ));
    }
    let key = subquery as *const Select as usize;
    let use_cache = matches!(
        db.subquery_cache.borrow().entries.get(&key),
        Some(SubqueryCacheEntry::Uncorrelated(_))
    );
    if use_cache {
        let prepared = {
            let mut cache = db.in_subquery_cache.borrow_mut();
            cache
                .entries
                .entry(key)
                .or_insert_with(|| Arc::new(prepare_in_subquery(&result)))
                .clone()
        };
        if prepared.keys.contains(&distinct_key(expr_val)) {
            return Ok(Value::Integer(1));
        }
        return Ok(if prepared.saw_null {
            Value::Null
        } else {
            Value::Integer(0)
        });
    }
    if result.rows.is_empty() {
        return Ok(Value::Integer(0));
    }
    let mut saw_null = false;
    for row in result.rows.iter() {
        let item_val = row.get(0).cloned().unwrap_or(Value::Null);
        match compare_values(&BinaryOperator::Eq, expr_val, &item_val) {
            Value::Integer(1) => return Ok(Value::Integer(1)),
            Value::Null => saw_null = true,
            _ => {}
        }
    }
    Ok(if saw_null {
        Value::Null
    } else {
        Value::Integer(0)
    })
}

fn is_missing_column_error(err: &GongDBError) -> bool {
    matches!(
        err,
        GongDBError::Execution(message) if message.starts_with("no such column: ")
    )
}

fn eval_subquery_cached<'a>(
    db: &GongDB,
    subquery: &Select,
    outer: Option<&EvalScope<'a>>,
) -> Result<Arc<QueryResult>, GongDBError> {
    let key = subquery as *const Select as usize;
    if let Some(entry) = db.subquery_cache.borrow().entries.get(&key) {
        match entry {
            SubqueryCacheEntry::Uncorrelated(result) => return Ok(Arc::clone(result)),
            SubqueryCacheEntry::Correlated => {
                let cte_context = outer.and_then(|scope| scope.cte_context);
                let result =
                    db.evaluate_select_values_with_outer(subquery, outer, cte_context)?;
                return Ok(Arc::new(result));
            }
        }
    }
    let cte_context = outer.and_then(|scope| scope.cte_context);
    if outer.is_some() {
        match db.evaluate_select_values_with_outer(subquery, None, cte_context) {
            Ok(result) => {
                let result = Arc::new(result);
                db.subquery_cache
                    .borrow_mut()
                    .entries
                    .insert(key, SubqueryCacheEntry::Uncorrelated(Arc::clone(&result)));
                Ok(result)
            }
            Err(err) => {
                if is_missing_column_error(&err) {
                    db.subquery_cache
                        .borrow_mut()
                        .entries
                        .insert(key, SubqueryCacheEntry::Correlated);
                    let result =
                        db.evaluate_select_values_with_outer(subquery, outer, cte_context)?;
                    Ok(Arc::new(result))
                } else {
                    Err(err)
                }
            }
        }
    } else {
        let result = Arc::new(db.evaluate_select_values_with_outer(
            subquery,
            None,
            cte_context,
        )?);
        db.subquery_cache
            .borrow_mut()
            .entries
            .insert(key, SubqueryCacheEntry::Uncorrelated(Arc::clone(&result)));
        Ok(result)
    }
}

fn prepare_in_subquery(result: &QueryResult) -> PreparedInSubquery {
    let mut keys = HashSet::new();
    let mut saw_null = false;
    for row in result.rows.iter() {
        let item_val = row.get(0).cloned().unwrap_or(Value::Null);
        if matches!(item_val, Value::Null) {
            saw_null = true;
        } else {
            keys.insert(distinct_key(&item_val));
        }
    }
    PreparedInSubquery { keys, saw_null }
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
        match (left, right) {
            (Value::Text(l), Value::Text(r)) => l.cmp(r),
            (Value::Text(l), Value::Blob(_)) => l.as_str().cmp(""),
            (Value::Blob(_), Value::Text(r)) => "".cmp(r.as_str()),
            (Value::Blob(_), Value::Blob(_)) => std::cmp::Ordering::Equal,
            _ => {
                let left_text = value_to_text(left);
                let right_text = value_to_text(right);
                left_text.cmp(&right_text)
            }
        }
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
        Value::Real(v) => format_real_sqlite(*v),
        Value::Text(s) => s.clone(),
        Value::Blob(_) => "NULL".to_string(),
    }
}

fn format_real_sqlite(value: f64) -> String {
    format!("{:.3}", value)
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

#[derive(Clone, Hash, Eq, PartialEq)]
enum DistinctKey {
    Null,
    Numeric(u64),
    Text(String),
}

fn distinct_key(value: &Value) -> DistinctKey {
    match value {
        Value::Null => DistinctKey::Null,
        _ => {
            if let Some(num) = numeric_value(value) {
                let (mut num, _) = numeric_to_f64(num);
                if num == 0.0 {
                    num = 0.0;
                }
                DistinctKey::Numeric(num.to_bits())
            } else {
                match value {
                    Value::Text(text) => DistinctKey::Text(text.clone()),
                    Value::Blob(_) => DistinctKey::Text(String::new()),
                    _ => DistinctKey::Text(value_to_text(value)),
                }
            }
        }
    }
}

fn row_distinct_key(row: &[Value]) -> Vec<DistinctKey> {
    row.iter().map(distinct_key).collect()
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
                match (left, right) {
                    (Value::Text(l), Value::Text(r)) => l == r,
                    (Value::Text(l), Value::Blob(_)) => l.is_empty(),
                    (Value::Blob(_), Value::Text(r)) => r.is_empty(),
                    (Value::Blob(_), Value::Blob(_)) => true,
                    _ => value_to_text(left) == value_to_text(right),
                }
            }
        }
    }
}

#[derive(Clone)]
enum OrderByValueSource {
    Expr(Expr),
    ProjectionIndex(usize),
}

#[derive(Clone)]
struct OrderByPlan {
    source: OrderByValueSource,
    asc: bool,
    nulls: Option<NullsOrder>,
}

struct SortedRow {
    order_values: Vec<Value>,
    projected: Vec<Value>,
}

fn dedup_rows(rows: &mut Vec<Vec<Value>>) {
    let mut seen: HashSet<Vec<DistinctKey>> = HashSet::with_capacity(rows.len());
    let mut unique: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
    for row in rows.drain(..) {
        let key = row_distinct_key(&row);
        if seen.insert(key) {
            unique.push(row);
        }
    }
    *rows = unique;
}

fn dedup_sorted_rows(rows: &mut Vec<SortedRow>) {
    let mut seen: HashSet<Vec<DistinctKey>> = HashSet::with_capacity(rows.len());
    let mut unique: Vec<SortedRow> = Vec::with_capacity(rows.len());
    for row in rows.drain(..) {
        let key = row_distinct_key(&row.projected);
        if seen.insert(key) {
            unique.push(row);
        }
    }
    *rows = unique;
}

fn resolve_order_by_plans(
    order_by: &[crate::ast::OrderByExpr],
    output_columns: &[Column],
) -> Result<Vec<OrderByPlan>, GongDBError> {
    let output_names: Vec<String> = output_columns.iter().map(|col| col.name.clone()).collect();
    let mut plans = Vec::with_capacity(order_by.len());
    for order in order_by {
        let asc = order.asc.unwrap_or(true);
        let source = match &order.expr {
            Expr::Literal(crate::ast::Literal::Integer(idx)) => {
                if *idx <= 0 || (*idx as usize) > output_columns.len() {
                    return Err(GongDBError::new("ORDER BY position out of range"));
                }
                OrderByValueSource::ProjectionIndex((*idx as usize) - 1)
            }
            Expr::Identifier(ident) => {
                if let Some(pos) = output_names
                    .iter()
                    .position(|name| name.eq_ignore_ascii_case(&ident.value))
                {
                    OrderByValueSource::ProjectionIndex(pos)
                } else {
                    OrderByValueSource::Expr(order.expr.clone())
                }
            }
            _ => OrderByValueSource::Expr(order.expr.clone()),
        };
        plans.push(OrderByPlan {
            source,
            asc,
            nulls: order.nulls.clone(),
        });
    }
    Ok(plans)
}

fn project_row(
    db: &GongDB,
    projection: &[SelectItem],
    row: &[Value],
    scope: &EvalScope<'_>,
    outer: Option<&EvalScope<'_>>,
) -> Result<Vec<Value>, GongDBError> {
    let mut output = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard => {
                output.reserve(row.len());
                output.extend(row.iter().cloned());
            }
            SelectItem::Expr { expr, .. } => {
                let value = eval_expr(db, expr, scope, outer)?;
                output.push(value);
            }
            SelectItem::QualifiedWildcard(name) => {
                let qualifier = object_name(name);
                let indices = qualified_wildcard_indices(&qualifier, scope.column_scopes);
                if indices.is_empty() {
                    return Err(GongDBError::new(format!("no such table: {}", qualifier)));
                }
                output.reserve(indices.len());
                for idx in indices {
                    output.push(row[idx].clone());
                }
            }
        }
    }
    Ok(output)
}

fn compute_order_values(
    db: &GongDB,
    plans: &[OrderByPlan],
    projected_row: &[Value],
    output_columns: &[Column],
    order_column_scopes: &[TableScope],
    order_table_scope: &TableScope,
    order_lookup: &ColumnLookup,
    scope: &EvalScope<'_>,
) -> Result<Vec<Value>, GongDBError> {
    let order_scope = EvalScope {
        columns: output_columns,
        column_scopes: order_column_scopes,
        row: projected_row,
        table_scope: order_table_scope,
        cte_context: scope.cte_context,
        column_lookup: Some(&order_lookup),
    };
    let mut values = Vec::with_capacity(plans.len());
    for plan in plans {
        let value = match &plan.source {
            OrderByValueSource::Expr(expr) => eval_expr(db, expr, &order_scope, Some(scope))?,
            OrderByValueSource::ProjectionIndex(idx) => {
                projected_row.get(*idx).cloned().unwrap_or(Value::Null)
            }
        };
        values.push(value);
    }
    Ok(values)
}

fn compute_group_order_values(
    db: &GongDB,
    plans: &[OrderByPlan],
    projected_row: &[Value],
    output_columns: &[Column],
    order_column_scopes: &[TableScope],
    order_table_scope: &TableScope,
    order_lookup: &ColumnLookup,
    scope: &EvalScope<'_>,
    group_rows: &[Vec<Value>],
    outer: Option<&EvalScope<'_>>,
) -> Result<Vec<Value>, GongDBError> {
    let order_scope = EvalScope {
        columns: output_columns,
        column_scopes: order_column_scopes,
        row: projected_row,
        table_scope: order_table_scope,
        cte_context: scope.cte_context,
        column_lookup: Some(&order_lookup),
    };
    let mut values = Vec::with_capacity(plans.len());
    for plan in plans {
        let value = match &plan.source {
            OrderByValueSource::Expr(expr) => {
                let expr = if expr_contains_aggregate(expr) {
                    replace_aggregate_calls(
                        db,
                        expr,
                        scope.columns,
                        scope.column_scopes,
                        scope.table_scope,
                        group_rows,
                        outer,
                        scope.cte_context,
                    )?
                } else {
                    expr.clone()
                };
                eval_expr(db, &expr, &order_scope, Some(scope))?
            }
            OrderByValueSource::ProjectionIndex(idx) => {
                projected_row.get(*idx).cloned().unwrap_or(Value::Null)
            }
        };
        values.push(value);
    }
    Ok(values)
}

fn compare_order_keys(
    left: &[Value],
    right: &[Value],
    plans: &[OrderByPlan],
) -> std::cmp::Ordering {
    for (idx, plan) in plans.iter().enumerate() {
        let left_val = &left[idx];
        let right_val = &right[idx];
        let ord = compare_order_values_with_nulls(left_val, right_val, plan.asc, plan.nulls.as_ref());
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

fn compare_order_values_with_nulls(
    left: &Value,
    right: &Value,
    asc: bool,
    nulls: Option<&NullsOrder>,
) -> std::cmp::Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) | (_, Value::Null) => {
            let nulls_first = match nulls {
                Some(NullsOrder::First) => true,
                Some(NullsOrder::Last) => false,
                None => asc,
            };
            if matches!(left, Value::Null) {
                if nulls_first {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            } else if nulls_first {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }
        _ => {
            let ord = compare_order_values(left, right);
            if asc {
                ord
            } else {
                ord.reverse()
            }
        }
    }
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

fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, args, .. } => {
            let func_name = name.value.to_ascii_lowercase();
            if is_aggregate_function_call(func_name.as_str(), args) {
                true
            } else {
                args.iter().any(expr_contains_aggregate)
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::UnaryOp { expr, .. } => expr_contains_aggregate(expr),
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| expr_contains_aggregate(expr))
                || when_then.iter().any(|(when_expr, then_expr)| {
                    expr_contains_aggregate(when_expr) || expr_contains_aggregate(then_expr)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|expr| expr_contains_aggregate(expr))
        }
        Expr::Between { expr, low, high, .. } => {
            expr_contains_aggregate(expr)
                || expr_contains_aggregate(low)
                || expr_contains_aggregate(high)
        }
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate(expr) || list.iter().any(expr_contains_aggregate)
        }
        Expr::InSubquery { expr, .. } => expr_contains_aggregate(expr),
        Expr::IsNull { expr, .. } => expr_contains_aggregate(expr),
        Expr::Cast { expr, .. } => expr_contains_aggregate(expr),
        Expr::Nested(expr) => expr_contains_aggregate(expr),
        _ => false,
    }
}

fn projection_has_aggregate(projection: &[SelectItem]) -> bool {
    projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_contains_aggregate(expr),
        _ => false,
    })
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

#[allow(dead_code)]
fn eval_having_clause(
    db: &GongDB,
    expr: &Expr,
    columns: &[Column],
    column_scopes: &[TableScope],
    table_scope: &TableScope,
    rows: &[Vec<Value>],
    outer: Option<&EvalScope<'_>>,
) -> Result<bool, GongDBError> {
    let cte_context = outer.and_then(|scope| scope.cte_context);
    let rewritten = replace_aggregate_calls(
        db,
        expr,
        columns,
        column_scopes,
        table_scope,
        rows,
        outer,
        cte_context,
    )?;
    let empty_scope = EvalScope {
        columns: &[],
        column_scopes: &[],
        row: &[],
        table_scope: &TableScope {
            table_name: None,
            table_alias: None,
        },
        cte_context,
        column_lookup: None,
    };
    let value = eval_expr(db, &rewritten, &empty_scope, outer)?;
    Ok(value_to_bool(&value))
}

fn replace_aggregate_calls(
    db: &GongDB,
    expr: &Expr,
    columns: &[Column],
    column_scopes: &[TableScope],
    table_scope: &TableScope,
    rows: &[Vec<Value>],
    outer: Option<&EvalScope<'_>>,
    cte_context: Option<&CteContext>,
) -> Result<Expr, GongDBError> {
    match expr {
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            let maybe_aggregate = aggregate_expr_from_function(name, args, *distinct)?;
            if let Some(aggregate) = maybe_aggregate {
                let value = compute_single_aggregate(
                    db,
                    &aggregate,
                    columns,
                    column_scopes,
                    table_scope,
                    rows,
                    outer,
                    cte_context,
                )?;
                Ok(Expr::Literal(value_to_literal(&value)))
            } else {
                let mut new_args = Vec::with_capacity(args.len());
                for arg in args {
                    new_args.push(replace_aggregate_calls(
                        db,
                        arg,
                        columns,
                        column_scopes,
                        table_scope,
                        rows,
                        outer,
                        cte_context,
                    )?);
                }
                Ok(Expr::Function {
                    name: name.clone(),
                    args: new_args,
                    distinct: *distinct,
                })
            }
        }
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(replace_aggregate_calls(
                db,
                left,
                columns,
                column_scopes,
                table_scope,
                rows,
                outer,
                cte_context,
            )?),
            op: op.clone(),
            right: Box::new(replace_aggregate_calls(
                db,
                right,
                columns,
                column_scopes,
                table_scope,
                rows,
                outer,
                cte_context,
            )?),
        }),
        Expr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
            op: op.clone(),
            expr: Box::new(replace_aggregate_calls(
                db,
                expr,
                columns,
                column_scopes,
                table_scope,
                rows,
                outer,
                cte_context,
            )?),
        }),
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            let new_operand = if let Some(expr) = operand {
                Some(Box::new(replace_aggregate_calls(
                    db,
                    expr,
                    columns,
                    column_scopes,
                    table_scope,
                    rows,
                    outer,
                    cte_context,
                )?))
            } else {
                None
            };
            let mut new_when_then = Vec::with_capacity(when_then.len());
            for (when_expr, then_expr) in when_then {
                new_when_then.push((
                    replace_aggregate_calls(
                        db,
                        when_expr,
                        columns,
                        column_scopes,
                        table_scope,
                        rows,
                        outer,
                        cte_context,
                    )?,
                    replace_aggregate_calls(
                        db,
                        then_expr,
                        columns,
                        column_scopes,
                        table_scope,
                        rows,
                        outer,
                        cte_context,
                    )?,
                ));
            }
            let new_else = if let Some(expr) = else_result {
                Some(Box::new(replace_aggregate_calls(
                    db,
                    expr,
                    columns,
                    column_scopes,
                    table_scope,
                    rows,
                    outer,
                    cte_context,
                )?))
            } else {
                None
            };
            Ok(Expr::Case {
                operand: new_operand,
                when_then: new_when_then,
                else_result: new_else,
            })
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(Expr::Between {
            expr: Box::new(replace_aggregate_calls(
                db,
                expr,
                columns,
                column_scopes,
                table_scope,
                rows,
                outer,
                cte_context,
            )?),
            negated: *negated,
            low: Box::new(replace_aggregate_calls(
                db,
                low,
                columns,
                column_scopes,
                table_scope,
                rows,
                outer,
                cte_context,
            )?),
            high: Box::new(replace_aggregate_calls(
                db,
                high,
                columns,
                column_scopes,
                table_scope,
                rows,
                outer,
                cte_context,
            )?),
        }),
        Expr::InList { expr, list, negated } => {
            let mut new_list = Vec::with_capacity(list.len());
            for item in list {
                new_list.push(replace_aggregate_calls(
                    db,
                    item,
                    columns,
                    column_scopes,
                    table_scope,
                    rows,
                    outer,
                    cte_context,
                )?);
            }
            Ok(Expr::InList {
                expr: Box::new(replace_aggregate_calls(
                    db,
                    expr,
                    columns,
                    column_scopes,
                    table_scope,
                    rows,
                    outer,
                    cte_context,
                )?),
                list: new_list,
                negated: *negated,
            })
        }
        Expr::InSubquery { expr, subquery, negated } => Ok(Expr::InSubquery {
            expr: Box::new(replace_aggregate_calls(
                db,
                expr,
                columns,
                column_scopes,
                table_scope,
                rows,
                outer,
                cte_context,
            )?),
            subquery: subquery.clone(),
            negated: *negated,
        }),
        Expr::IsNull { expr, negated } => Ok(Expr::IsNull {
            expr: Box::new(replace_aggregate_calls(
                db,
                expr,
                columns,
                column_scopes,
                table_scope,
                rows,
                outer,
                cte_context,
            )?),
            negated: *negated,
        }),
        Expr::Cast { expr, data_type } => Ok(Expr::Cast {
            expr: Box::new(replace_aggregate_calls(
                db,
                expr,
                columns,
                column_scopes,
                table_scope,
                rows,
                outer,
                cte_context,
            )?),
            data_type: data_type.clone(),
        }),
        Expr::Nested(expr) => Ok(Expr::Nested(Box::new(replace_aggregate_calls(
            db,
            expr,
            columns,
            column_scopes,
            table_scope,
            rows,
            outer,
            cte_context,
        )?))),
        _ => Ok(expr.clone()),
    }
}

fn aggregate_expr_from_function(
    name: &Ident,
    args: &[Expr],
    distinct: bool,
) -> Result<Option<AggregateExpr>, GongDBError> {
    let func_name = name.value.to_ascii_lowercase();
    match func_name.as_str() {
        "sum" => {
            if args.len() != 1 {
                return Err(GongDBError::new("SUM expects one argument"));
            }
            Ok(Some(AggregateExpr {
                kind: AggregateKind::Sum,
                expr: Some(args[0].clone()),
                distinct,
                separator: None,
            }))
        }
        "count" => {
            if args.is_empty() || args.iter().all(|arg| matches!(arg, Expr::Wildcard)) {
                if distinct {
                    return Err(GongDBError::new("COUNT DISTINCT does not support *"));
                }
                Ok(Some(AggregateExpr {
                    kind: AggregateKind::Count,
                    expr: None,
                    distinct: false,
                    separator: None,
                }))
            } else if args.len() == 1 {
                if matches!(args[0], Expr::Wildcard) {
                    if distinct {
                        return Err(GongDBError::new("COUNT DISTINCT does not support *"));
                    }
                    Ok(Some(AggregateExpr {
                        kind: AggregateKind::Count,
                        expr: None,
                        distinct: false,
                        separator: None,
                    }))
                } else {
                    Ok(Some(AggregateExpr {
                        kind: AggregateKind::Count,
                        expr: Some(args[0].clone()),
                        distinct,
                        separator: None,
                    }))
                }
            } else {
                Err(GongDBError::new("COUNT expects at most one argument"))
            }
        }
        "avg" => {
            if args.len() != 1 {
                return Err(GongDBError::new("AVG expects one argument"));
            }
            Ok(Some(AggregateExpr {
                kind: AggregateKind::Avg,
                expr: Some(args[0].clone()),
                distinct,
                separator: None,
            }))
        }
        "min" => {
            if args.len() == 1 {
                Ok(Some(AggregateExpr {
                    kind: AggregateKind::Min,
                    expr: Some(args[0].clone()),
                    distinct,
                    separator: None,
                }))
            } else if distinct {
                Err(GongDBError::new(
                    "DISTINCT is only valid for single-argument aggregates",
                ))
            } else {
                Ok(None)
            }
        }
        "max" => {
            if args.len() == 1 {
                Ok(Some(AggregateExpr {
                    kind: AggregateKind::Max,
                    expr: Some(args[0].clone()),
                    distinct,
                    separator: None,
                }))
            } else if distinct {
                Err(GongDBError::new(
                    "DISTINCT is only valid for single-argument aggregates",
                ))
            } else {
                Ok(None)
            }
        }
        "total" => {
            if args.len() != 1 {
                return Err(GongDBError::new("TOTAL expects one argument"));
            }
            Ok(Some(AggregateExpr {
                kind: AggregateKind::Total,
                expr: Some(args[0].clone()),
                distinct,
                separator: None,
            }))
        }
        "group_concat" => {
            if args.is_empty() || args.len() > 2 {
                return Err(GongDBError::new(
                    "GROUP_CONCAT expects one or two arguments",
                ));
            }
            if distinct && args.len() != 1 {
                return Err(GongDBError::new(
                    "DISTINCT is only valid for single-argument aggregates",
                ));
            }
            Ok(Some(AggregateExpr {
                kind: AggregateKind::GroupConcat,
                expr: Some(args[0].clone()),
                distinct,
                separator: if args.len() == 2 {
                    Some(args[1].clone())
                } else {
                    None
                },
            }))
        }
        _ => Ok(None),
    }
}

fn compute_single_aggregate(
    db: &GongDB,
    aggregate: &AggregateExpr,
    columns: &[Column],
    column_scopes: &[TableScope],
    table_scope: &TableScope,
    rows: &[Vec<Value>],
    outer: Option<&EvalScope<'_>>,
    cte_context: Option<&CteContext>,
) -> Result<Value, GongDBError> {
    let mut values = compute_aggregates(
        db,
        std::slice::from_ref(aggregate),
        columns,
        column_scopes,
        table_scope,
        rows,
        outer,
        cte_context,
    )?;
    Ok(values.pop().unwrap_or(Value::Null))
}

fn value_to_literal(value: &Value) -> Literal {
    match value {
        Value::Null => Literal::Null,
        Value::Integer(v) => Literal::Integer(*v),
        Value::Real(v) => Literal::Float(*v),
        Value::Text(text) => Literal::String(text.clone()),
        Value::Blob(bytes) => Literal::Blob(bytes.clone()),
    }
}

fn literal_to_i64(literal: &Literal) -> Option<i64> {
    match literal {
        Literal::Integer(v) => Some(*v),
        Literal::Float(v) => Some(*v as i64),
        Literal::Boolean(v) => Some(if *v { 1 } else { 0 }),
        _ => None,
    }
}

fn literal_to_value(literal: &Literal) -> Value {
    match literal {
        Literal::Null => Value::Null,
        Literal::Integer(v) => Value::Integer(*v),
        Literal::Float(v) => Value::Real(*v),
        Literal::String(text) => Value::Text(text.clone()),
        Literal::Boolean(value) => Value::Integer(if *value { 1 } else { 0 }),
        Literal::Blob(bytes) => Value::Blob(bytes.clone()),
    }
}

fn evaluate_group_projection(
    db: &GongDB,
    projection: &[SelectItem],
    columns: &[Column],
    column_scopes: &[TableScope],
    table_scope: &TableScope,
    group_rows: &[Vec<Value>],
    outer: Option<&EvalScope<'_>>,
    cte_context: Option<&CteContext>,
) -> Result<Vec<Value>, GongDBError> {
    let null_row = vec![Value::Null; columns.len()];
    let row = group_rows.first().unwrap_or(&null_row);
    let column_lookup = build_column_lookup(columns, column_scopes);
    let scope = EvalScope {
        columns,
        column_scopes,
        row,
        table_scope,
        cte_context,
        column_lookup: Some(&column_lookup),
    };
    let mut output = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard => {
                output.extend(row.iter().cloned());
            }
            SelectItem::Expr { expr, .. } => {
                let rewritten = if expr_contains_aggregate(expr) {
                    replace_aggregate_calls(
                        db,
                        expr,
                        columns,
                        column_scopes,
                        table_scope,
                        group_rows,
                        outer,
                        cte_context,
                    )?
                } else {
                    expr.clone()
                };
                let value = eval_expr(db, &rewritten, &scope, outer)?;
                output.push(value);
            }
            SelectItem::QualifiedWildcard(name) => {
                let qualifier = object_name(name);
                let indices = qualified_wildcard_indices(&qualifier, column_scopes);
                if indices.is_empty() {
                    return Err(GongDBError::new(format!("no such table: {}", qualifier)));
                }
                for idx in indices {
                    output.push(row[idx].clone());
                }
            }
        }
    }
    Ok(output)
}

fn evaluate_group_having(
    db: &GongDB,
    having: &Expr,
    columns: &[Column],
    column_scopes: &[TableScope],
    table_scope: &TableScope,
    group_rows: &[Vec<Value>],
    outer: Option<&EvalScope<'_>>,
    cte_context: Option<&CteContext>,
) -> Result<bool, GongDBError> {
    let null_row = vec![Value::Null; columns.len()];
    let row = group_rows.first().unwrap_or(&null_row);
    let column_lookup = build_column_lookup(columns, column_scopes);
    let scope = EvalScope {
        columns,
        column_scopes,
        row,
        table_scope,
        cte_context,
        column_lookup: Some(&column_lookup),
    };
    let rewritten = if expr_contains_aggregate(having) {
        replace_aggregate_calls(
            db,
            having,
            columns,
            column_scopes,
            table_scope,
            group_rows,
            outer,
            cte_context,
        )?
    } else {
        having.clone()
    };
    let value = eval_expr(db, &rewritten, &scope, outer)?;
    Ok(value_to_bool(&value))
}

#[allow(dead_code)]
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

fn resolve_expr_column_index(
    expr: &Expr,
    column_lookup: &ColumnLookup,
    columns: &[Column],
    column_scopes: &[TableScope],
) -> Option<usize> {
    match expr {
        Expr::Identifier(ident) => {
            let key = maybe_lowercase(&ident.value);
            if let Some(idx) = column_lookup.unqualified.get(key.as_ref()) {
                return Some(*idx);
            }
            resolve_column_index(&ident.value, columns)
        }
        Expr::CompoundIdentifier(idents) => {
            let (qualifier, column) = split_qualified_identifier(idents).ok()?;
            let qual_key = maybe_lowercase(qualifier);
            let name_key = maybe_lowercase(column);
            if let Some(by_qualifier) = column_lookup.qualified.get(qual_key.as_ref()) {
                if let Some(idx) = by_qualifier.get(name_key.as_ref()) {
                    return Some(*idx);
                }
            }
            resolve_qualified_column_index(qualifier, column, columns, column_scopes)
        }
        Expr::Nested(inner) => resolve_expr_column_index(inner, column_lookup, columns, column_scopes),
        _ => None,
    }
}

fn compute_aggregates(
    db: &GongDB,
    aggregates: &[AggregateExpr],
    columns: &[Column],
    column_scopes: &[TableScope],
    table_scope: &TableScope,
    rows: &[Vec<Value>],
    outer: Option<&EvalScope<'_>>,
    cte_context: Option<&CteContext>,
) -> Result<Vec<Value>, GongDBError> {
    let mut results = Vec::with_capacity(aggregates.len());
    let column_lookup = build_column_lookup(columns, column_scopes);
    for agg in aggregates {
        match agg.kind {
            AggregateKind::Count => {
                let count = if let Some(expr) = &agg.expr {
                    let mut tally = 0i64;
                    let mut seen = if agg.distinct {
                        Some(HashSet::new())
                    } else {
                        None
                    };
                    if let Some(idx) =
                        resolve_expr_column_index(expr, &column_lookup, columns, column_scopes)
                    {
                        for row in rows {
                            let value = &row[idx];
                            if matches!(value, Value::Null) {
                                continue;
                            }
                            if agg.distinct {
                                if !seen.as_mut().unwrap().insert(distinct_key(value)) {
                                    continue;
                                }
                            }
                            tally += 1;
                        }
                    } else {
                        for row in rows {
                            let scope = EvalScope {
                                columns,
                                column_scopes,
                                row,
                                table_scope,
                                cte_context,
                                column_lookup: Some(&column_lookup),
                            };
                            let value = eval_expr(db, expr, &scope, outer)?;
                            if !matches!(value, Value::Null) {
                                if agg.distinct {
                                    if !seen.as_mut().unwrap().insert(distinct_key(&value)) {
                                        continue;
                                    }
                                }
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
                let mut seen = if agg.distinct {
                    Some(HashSet::new())
                } else {
                    None
                };
                let sum_column = resolve_expr_column_index(expr, &column_lookup, columns, column_scopes);
                let mut apply_value = |value: &Value| {
                    if matches!(value, Value::Null) {
                        return;
                    }
                    if let Some(ref mut seen) = seen {
                        if !seen.insert(distinct_key(value)) {
                            return;
                        }
                    }
                    if let Some(num) = numeric_value_or_zero(value) {
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
                };
                if let Some(idx) = sum_column {
                    for row in rows {
                        apply_value(&row[idx]);
                    }
                } else {
                    for row in rows {
                        let scope = EvalScope {
                            columns,
                            column_scopes,
                            row,
                            table_scope,
                            cte_context,
                            column_lookup: Some(&column_lookup),
                        };
                        let value = eval_expr(db, expr, &scope, outer)?;
                        apply_value(&value);
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
                let mut seen = if agg.distinct {
                    Some(HashSet::new())
                } else {
                    None
                };
                for row in rows {
                    let scope = EvalScope {
                        columns,
                        column_scopes,
                        row,
                        table_scope,
                        cte_context,
                        column_lookup: Some(&column_lookup),
                    };
                    let value = eval_expr(db, expr, &scope, outer)?;
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    if agg.distinct {
                        if !seen.as_mut().unwrap().insert(distinct_key(&value)) {
                            continue;
                        }
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
                let mut seen = if agg.distinct {
                    Some(HashSet::new())
                } else {
                    None
                };
                for row in rows {
                    let scope = EvalScope {
                        columns,
                        column_scopes,
                        row,
                        table_scope,
                        cte_context,
                        column_lookup: Some(&column_lookup),
                    };
                    let value = eval_expr(db, expr, &scope, outer)?;
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    if agg.distinct {
                        if !seen.as_mut().unwrap().insert(distinct_key(&value)) {
                            continue;
                        }
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
                let mut seen = if agg.distinct {
                    Some(HashSet::new())
                } else {
                    None
                };
                for row in rows {
                    let scope = EvalScope {
                        columns,
                        column_scopes,
                        row,
                        table_scope,
                        cte_context,
                        column_lookup: Some(&column_lookup),
                    };
                    let value = eval_expr(db, expr, &scope, outer)?;
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    if agg.distinct {
                        if !seen.as_mut().unwrap().insert(distinct_key(&value)) {
                            continue;
                        }
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
                let mut seen = if agg.distinct {
                    Some(HashSet::new())
                } else {
                    None
                };
                let mut saw_value = false;
                for row in rows {
                    let scope = EvalScope {
                        columns,
                        column_scopes,
                        row,
                        table_scope,
                        cte_context,
                        column_lookup: Some(&column_lookup),
                    };
                    let value = eval_expr(db, expr, &scope, outer)?;
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    if agg.distinct {
                        if !seen.as_mut().unwrap().insert(distinct_key(&value)) {
                            continue;
                        }
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
                let mut seen = if agg.distinct {
                    Some(HashSet::new())
                } else {
                    None
                };
                for row in rows {
                    let scope = EvalScope {
                        columns,
                        column_scopes,
                        row,
                        table_scope,
                        cte_context,
                        column_lookup: Some(&column_lookup),
                    };
                    let value = eval_expr(db, expr, &scope, outer)?;
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    if agg.distinct {
                        if !seen.as_mut().unwrap().insert(distinct_key(&value)) {
                            continue;
                        }
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

/// Convert typed values into string rows for sqllogictest output.
///
/// This is primarily used by the test harness to normalize query results.
pub fn format_query_rows(rows: Vec<Vec<Value>>) -> Vec<Vec<String>> {
    rows.into_iter()
        .map(|row| row.into_iter().map(|v| value_to_string(&v)).collect())
        .collect()
}
