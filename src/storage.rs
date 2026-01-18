use crate::ast::{
    BinaryOperator, ColumnConstraint, CompoundOperator, CompoundSelect, Cte, DataType, Expr, Ident,
    IndexedColumn, Literal, NullsOrder, ObjectName, OrderByExpr, Select, SelectItem, SortOrder,
    TableConstraint, TableRef, UnaryOperator, With,
};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Mutex, OnceLock};
use std::sync::atomic::{AtomicU64, Ordering};

const PAGE_SIZE: usize = 4096;
const HEADER_PAGE_ID: u32 = 0;
const CATALOG_PAGE_ID: u32 = 1;
const FILE_MAGIC: [u8; 8] = *b"GONGDB1\0";
const CATALOG_FORMAT_VERSION: u32 = 4;
const PAGE_TYPE_DATA: u8 = 1;
const PAGE_TYPE_BTREE_LEAF: u8 = 2;
const PAGE_TYPE_BTREE_INTERNAL: u8 = 3;
const HEADER_PAGE_SIZE_OFFSET: usize = 8;
const HEADER_NEXT_PAGE_ID_OFFSET: usize = 12;
const HEADER_CATALOG_PAGE_ID_OFFSET: usize = 16;
const HEADER_SCHEMA_VERSION_OFFSET: usize = 20;
const HEADER_CATALOG_FORMAT_OFFSET: usize = 24;

static NEXT_DB_ID: AtomicU64 = AtomicU64::new(1);
static LOCK_MANAGER: OnceLock<Mutex<LockManager>> = OnceLock::new();

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RowLocation {
    page_id: u32,
    slot: u16,
}

#[derive(Debug, Clone, PartialEq)]
struct IndexEntry {
    key: Vec<Value>,
    row: RowLocation,
}

#[derive(Debug, Clone, PartialEq)]
struct InternalCell {
    key: Vec<Value>,
    right_child: u32,
}

#[derive(Debug, Clone)]
struct BtreeSplit {
    key: Vec<Value>,
    right_page: u32,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone)]
pub struct TableMeta {
    pub name: String,
    pub columns: Vec<Column>,
    pub constraints: Vec<TableConstraint>,
    pub first_page: u32,
    pub last_page: u32,
}

#[derive(Debug, Clone)]
pub struct IndexMeta {
    pub name: String,
    pub table: String,
    pub columns: Vec<IndexedColumn>,
    pub unique: bool,
    pub first_page: u32,
    pub last_page: u32,
}

#[derive(Debug, Clone)]
pub struct ViewMeta {
    pub name: String,
    pub columns: Vec<Ident>,
    pub query: Select,
}

#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    Corrupt(String),
    NotFound(String),
    Invalid(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Io(err) => write!(f, "io error: {}", err),
            StorageError::Corrupt(msg) => write!(f, "corrupt storage: {}", msg),
            StorageError::NotFound(msg) => write!(f, "not found: {}", msg),
            StorageError::Invalid(msg) => write!(f, "invalid storage: {}", msg),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        StorageError::Io(err)
    }
}

enum StorageMode {
    InMemory { pages: Vec<Vec<u8>> },
    OnDisk { file: File },
}

#[derive(Debug, Clone)]
enum StorageModeSnapshot {
    InMemory { pages: Vec<Vec<u8>> },
    OnDisk { data: Vec<u8> },
}

#[derive(Debug, Clone)]
pub struct StorageSnapshot {
    mode: StorageModeSnapshot,
    next_page_id: u32,
    schema_version: u32,
    catalog_format_version: u32,
    tables: HashMap<String, TableMeta>,
    indexes: HashMap<String, IndexMeta>,
    views: HashMap<String, ViewMeta>,
    free_pages: Vec<u32>,
}

#[derive(Debug)]
struct LockState {
    readers: HashMap<u64, usize>,
    writer: Option<u64>,
}

impl LockState {
    fn new() -> Self {
        Self {
            readers: HashMap::new(),
            writer: None,
        }
    }
}

#[derive(Debug)]
struct LockManager {
    locks: HashMap<String, LockState>,
}

impl LockManager {
    fn new() -> Self {
        Self {
            locks: HashMap::new(),
        }
    }

    fn state_for_db_mut(&mut self, db_id: &str) -> &mut LockState {
        self.locks
            .entry(db_id.to_string())
            .or_insert_with(LockState::new)
    }
}

pub struct StorageEngine {
    mode: StorageMode,
    db_id: String,
    next_page_id: u32,
    schema_version: u32,
    catalog_format_version: u32,
    tables: HashMap<String, TableMeta>,
    indexes: HashMap<String, IndexMeta>,
    views: HashMap<String, ViewMeta>,
    free_pages: Vec<u32>,
}

pub struct TableScan<'a> {
    engine: &'a StorageEngine,
    page_id: u32,
    next_page_id: u32,
    records: Vec<Vec<u8>>,
    record_index: usize,
    done: bool,
}

impl<'a> TableScan<'a> {
    fn new(engine: &'a StorageEngine, table: &TableMeta) -> Result<Self, StorageError> {
        let page_id = table.first_page;
        let page = engine.read_page(page_id)?;
        let records = read_records(&page);
        let next_page_id = get_next_page_id(&page);
        Ok(TableScan {
            engine,
            page_id,
            next_page_id,
            records,
            record_index: 0,
            done: false,
        })
    }

    fn load_page(&mut self, page_id: u32) -> Result<(), StorageError> {
        let page = self.engine.read_page(page_id)?;
        self.page_id = page_id;
        self.records = read_records(&page);
        self.record_index = 0;
        self.next_page_id = get_next_page_id(&page);
        Ok(())
    }
}

impl<'a> Iterator for TableScan<'a> {
    type Item = Result<Vec<Value>, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            if self.record_index < self.records.len() {
                let record = self.records[self.record_index].clone();
                self.record_index += 1;
                return Some(decode_row(&record));
            }
            if self.next_page_id == 0 {
                self.done = true;
                return None;
            }
            if let Err(err) = self.load_page(self.next_page_id) {
                self.done = true;
                return Some(Err(err));
            }
        }
    }
}

impl StorageEngine {
    pub fn new_in_memory() -> Result<Self, StorageError> {
        let mut pages = Vec::new();
        pages.push(vec![0; PAGE_SIZE]);
        pages.push(init_data_page());
        let id = NEXT_DB_ID.fetch_add(1, Ordering::SeqCst);
        let mut engine = StorageEngine {
            mode: StorageMode::InMemory { pages },
            db_id: format!("memory-{}", id),
            next_page_id: CATALOG_PAGE_ID + 1,
            schema_version: 0,
            catalog_format_version: CATALOG_FORMAT_VERSION,
            tables: HashMap::new(),
            indexes: HashMap::new(),
            views: HashMap::new(),
            free_pages: Vec::new(),
        };
        engine.write_header()?;
        engine.write_catalog()?;
        Ok(engine)
    }

    pub fn new_on_disk(path: &str) -> Result<Self, StorageError> {
        let exists = std::path::Path::new(path).exists();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        if !exists || file.metadata()?.len() < PAGE_SIZE as u64 {
            file.set_len((PAGE_SIZE * 2) as u64)?;
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&vec![0; PAGE_SIZE])?;
            file.write_all(&init_data_page())?;
            file.sync_all()?;
        }

        let mut header = vec![0; PAGE_SIZE];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header)?;
        let (next_page_id, schema_version, catalog_format_version, tables, indexes, views) =
            if header.starts_with(&FILE_MAGIC) {
                let next_page_id = read_u32(&header, HEADER_NEXT_PAGE_ID_OFFSET);
                let schema_version = read_u32(&header, HEADER_SCHEMA_VERSION_OFFSET);
                let loaded_format_version = read_u32(&header, HEADER_CATALOG_FORMAT_OFFSET);
                let (tables, indexes, views) =
                    load_catalog_from_file(&mut file, next_page_id, loaded_format_version)?;
                let mut catalog_format_version = if loaded_format_version == 0 {
                    1
                } else {
                    loaded_format_version
                };
                if catalog_format_version < CATALOG_FORMAT_VERSION {
                    catalog_format_version = CATALOG_FORMAT_VERSION;
                }
                (
                    next_page_id,
                    schema_version,
                    catalog_format_version,
                    tables,
                    indexes,
                    views,
                )
        } else {
            let tables = HashMap::new();
            let indexes = HashMap::new();
            let views = HashMap::new();
            (
                CATALOG_PAGE_ID + 1,
                0,
                CATALOG_FORMAT_VERSION,
                tables,
                indexes,
                views,
            )
        };

        let mut engine = StorageEngine {
            mode: StorageMode::OnDisk { file },
            db_id: format!("disk-{}", path),
            next_page_id,
            schema_version,
            catalog_format_version,
            tables,
            indexes,
            views,
            free_pages: Vec::new(),
        };
        engine.write_header()?;
        engine.write_catalog()?;
        Ok(engine)
    }

    pub fn snapshot(&self) -> Result<StorageSnapshot, StorageError> {
        let mode = match &self.mode {
            StorageMode::InMemory { pages } => StorageModeSnapshot::InMemory { pages: pages.clone() },
            StorageMode::OnDisk { file } => {
                let mut clone = file.try_clone()?;
                clone.seek(SeekFrom::Start(0))?;
                let mut data = Vec::new();
                clone.read_to_end(&mut data)?;
                StorageModeSnapshot::OnDisk { data }
            }
        };
        Ok(StorageSnapshot {
            mode,
            next_page_id: self.next_page_id,
            schema_version: self.schema_version,
            catalog_format_version: self.catalog_format_version,
            tables: self.tables.clone(),
            indexes: self.indexes.clone(),
            views: self.views.clone(),
            free_pages: self.free_pages.clone(),
        })
    }

    pub fn restore(&mut self, snapshot: StorageSnapshot) -> Result<(), StorageError> {
        match (&mut self.mode, snapshot.mode) {
            (StorageMode::InMemory { pages }, StorageModeSnapshot::InMemory { pages: snap }) => {
                *pages = snap;
            }
            (StorageMode::OnDisk { file }, StorageModeSnapshot::OnDisk { data }) => {
                file.seek(SeekFrom::Start(0))?;
                file.write_all(&data)?;
                file.set_len(data.len() as u64)?;
                file.flush()?;
            }
            _ => return Err(StorageError::Invalid("storage mode mismatch".to_string())),
        }

        self.next_page_id = snapshot.next_page_id;
        self.schema_version = snapshot.schema_version;
        self.catalog_format_version = snapshot.catalog_format_version;
        self.tables = snapshot.tables;
        self.indexes = snapshot.indexes;
        self.views = snapshot.views;
        self.free_pages = snapshot.free_pages;
        Ok(())
    }

    pub fn acquire_read_lock(&self, txn_id: u64) -> Result<(), StorageError> {
        let manager = LOCK_MANAGER.get_or_init(|| Mutex::new(LockManager::new()));
        let mut guard = manager
            .lock()
            .map_err(|_| StorageError::Invalid("lock manager poisoned".to_string()))?;
        let state = guard.state_for_db_mut(&self.db_id);
        if let Some(writer) = state.writer {
            if writer != txn_id {
                return Err(StorageError::Invalid("database is locked".to_string()));
            }
        }
        *state.readers.entry(txn_id).or_insert(0) += 1;
        Ok(())
    }

    pub fn acquire_write_lock(&self, txn_id: u64) -> Result<(), StorageError> {
        let manager = LOCK_MANAGER.get_or_init(|| Mutex::new(LockManager::new()));
        let mut guard = manager
            .lock()
            .map_err(|_| StorageError::Invalid("lock manager poisoned".to_string()))?;
        let state = guard.state_for_db_mut(&self.db_id);
        if let Some(writer) = state.writer {
            if writer != txn_id {
                return Err(StorageError::Invalid("database is locked".to_string()));
            }
        }
        let other_readers = state
            .readers
            .iter()
            .any(|(id, count)| *id != txn_id && *count > 0);
        if other_readers {
            return Err(StorageError::Invalid("deadlock detected".to_string()));
        }
        state.writer = Some(txn_id);
        Ok(())
    }

    pub fn release_locks(&self, txn_id: u64) {
        let manager = LOCK_MANAGER.get_or_init(|| Mutex::new(LockManager::new()));
        let mut guard = match manager.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        if let Some(state) = guard.locks.get_mut(&self.db_id) {
            state.readers.remove(&txn_id);
            if state.writer == Some(txn_id) {
                state.writer = None;
            }
        }
    }

    pub fn create_table(&mut self, table: TableMeta) -> Result<(), StorageError> {
        if self.tables.contains_key(&table.name) {
            return Err(StorageError::Invalid(format!(
                "table already exists: {}",
                table.name
            )));
        }
        self.tables.insert(table.name.clone(), table);
        self.write_catalog()?;
        self.bump_schema_version()
    }

    pub fn drop_table(&mut self, name: &str) -> Result<(), StorageError> {
        let table = self
            .tables
            .get(name)
            .cloned()
            .ok_or_else(|| StorageError::NotFound(format!("table not found: {}", name)))?;
        if let Some(dependent) = self.find_foreign_key_dependency(name) {
            return Err(StorageError::Invalid(format!(
                "table {} is referenced by {}",
                name, dependent
            )));
        }
        let dependent_indexes: Vec<IndexMeta> = self
            .indexes
            .values()
            .filter(|index| index.table.eq_ignore_ascii_case(name))
            .cloned()
            .collect();

        self.free_page_chain(table.first_page)?;
        for index in &dependent_indexes {
            self.free_btree_pages(index.first_page)?;
        }

        self.tables.remove(name);
        for index in &dependent_indexes {
            self.indexes.remove(&index.name);
        }
        self.write_catalog()?;
        self.bump_schema_version()
    }

    pub fn get_table(&self, name: &str) -> Option<&TableMeta> {
        self.tables.get(name)
    }

    pub fn create_view(&mut self, view: ViewMeta) -> Result<(), StorageError> {
        if self.views.contains_key(&view.name) {
            return Err(StorageError::Invalid(format!(
                "view already exists: {}",
                view.name
            )));
        }
        self.views.insert(view.name.clone(), view);
        self.write_catalog()?;
        self.bump_schema_version()
    }

    pub fn drop_view(&mut self, name: &str) -> Result<(), StorageError> {
        if self.views.remove(name).is_none() {
            return Err(StorageError::NotFound(format!(
                "view not found: {}",
                name
            )));
        }
        self.write_catalog()?;
        self.bump_schema_version()
    }

    pub fn get_view(&self, name: &str) -> Option<&ViewMeta> {
        self.views.get(name)
    }

    pub fn get_index(&self, name: &str) -> Option<&IndexMeta> {
        self.indexes.get(name)
    }

    pub fn list_tables(&self) -> Vec<TableMeta> {
        let mut tables: Vec<TableMeta> = self.tables.values().cloned().collect();
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        tables
    }

    pub fn list_indexes(&self) -> Vec<IndexMeta> {
        let mut indexes: Vec<IndexMeta> = self.indexes.values().cloned().collect();
        indexes.sort_by(|a, b| a.name.cmp(&b.name));
        indexes
    }

    #[allow(dead_code)]
    pub(crate) fn scan_index_range(
        &self,
        index_name: &str,
        lower: Option<&[Value]>,
        upper: Option<&[Value]>,
    ) -> Result<Vec<RowLocation>, StorageError> {
        let index = self
            .indexes
            .get(index_name)
            .ok_or_else(|| StorageError::NotFound(format!("index not found: {}", index_name)))?;
        self.btree_scan_range(index.first_page, lower, upper)
    }

    pub(crate) fn scan_index_rows(
        &self,
        index_name: &str,
        lower: Option<&[Value]>,
        upper: Option<&[Value]>,
    ) -> Result<Vec<Vec<Value>>, StorageError> {
        let locations = self.scan_index_range(index_name, lower, upper)?;
        let mut rows = Vec::with_capacity(locations.len());
        for location in &locations {
            rows.push(self.read_row_at(location)?);
        }
        Ok(rows)
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn insert_row(&mut self, table_name: &str, row: &[Value]) -> Result<(), StorageError> {
        let _ = self.insert_row_with_location(table_name, row)?;
        Ok(())
    }

    fn insert_row_with_location(
        &mut self,
        table_name: &str,
        row: &[Value],
    ) -> Result<RowLocation, StorageError> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| StorageError::NotFound(format!("table not found: {}", table_name)))?
            .clone();
        let mut page_id = table.last_page;
        let column_map = column_index_map(&table.columns);
        let index_names: Vec<String> = self
            .indexes
            .values()
            .filter(|index| index.table == table_name)
            .map(|index| index.name.clone())
            .collect();

        let mut index_keys = Vec::new();
        for index_name in &index_names {
            let index = self
                .indexes
                .get(index_name)
                .ok_or_else(|| StorageError::NotFound(format!("index not found: {}", index_name)))?
                .clone();
            let key = index_key_from_row(&index, &column_map, row)?;
            if index.unique && !key_has_null(&key) && self.index_contains_key(&index, &key)? {
                return Err(StorageError::Invalid(format!(
                    "unique index violation: {}",
                    index.name
                )));
            }
            index_keys.push((index.name, key));
        }

        let record = encode_row(row)?;
        let mut page = self.read_page(page_id)?;
        if !has_space_for_record(&page, record.len()) {
            let new_page_id = self.allocate_data_page()?;
            let new_page = init_data_page();
            set_next_page_id(&mut page, new_page_id);
            self.write_page(page_id, &page)?;
            page_id = new_page_id;
            page = new_page;
            if let Some(table) = self.tables.get_mut(table_name) {
                table.last_page = new_page_id;
            }
        }

        let slot = insert_record(&mut page, &record)?;
        self.write_page(page_id, &page)?;

        let location = RowLocation { page_id, slot };
        for (index_name, key) in index_keys {
            let entry = IndexEntry { key, row: location };
            self.insert_index_record(&index_name, &entry)?;
        }

        self.write_catalog()?;
        Ok(location)
    }

    pub fn create_index(&mut self, index: IndexMeta) -> Result<(), StorageError> {
        if self.indexes.contains_key(&index.name) {
            return Err(StorageError::Invalid(format!(
                "index already exists: {}",
                index.name
            )));
        }
        let table = self
            .tables
            .get(&index.table)
            .ok_or_else(|| StorageError::NotFound(format!("table not found: {}", index.table)))?
            .clone();

        let column_map = column_index_map(&table.columns);
        let mut unique_keys = HashSet::new();

        self.write_page(index.first_page, &init_btree_page(PAGE_TYPE_BTREE_LEAF))?;
        self.indexes.insert(index.name.clone(), index.clone());

        let rows = self.scan_table_with_locations(&table.name)?;
        for (location, row) in rows {
            let key = index_key_from_row(&index, &column_map, &row)?;
            if index.unique && !key_has_null(&key) {
                let encoded_key = encode_index_key(&key)?;
                if !unique_keys.insert(encoded_key) {
                    self.indexes.remove(&index.name);
                    return Err(StorageError::Invalid(format!(
                        "unique index violation: {}",
                        index.name
                    )));
                }
            }
            let entry = IndexEntry { key, row: location };
            self.insert_index_record(&index.name, &entry)?;
        }

        self.write_catalog()?;
        self.bump_schema_version()
    }

    pub fn drop_index(&mut self, name: &str) -> Result<(), StorageError> {
        let index = self
            .indexes
            .get(name)
            .cloned()
            .ok_or_else(|| StorageError::NotFound(format!("index not found: {}", name)))?;
        self.free_btree_pages(index.first_page)?;
        self.indexes.remove(name);
        self.write_catalog()?;
        self.bump_schema_version()
    }

    pub fn reindex(&mut self, target: Option<&str>) -> Result<(), StorageError> {
        match target {
            None => {
                let index_names: Vec<String> = self.indexes.keys().cloned().collect();
                for index_name in index_names {
                    self.rebuild_index(&index_name)?;
                }
                self.write_catalog()?;
                Ok(())
            }
            Some(name) => {
                if let Some(index_name) = self.find_index_name(name) {
                    self.rebuild_index(&index_name)?;
                    self.write_catalog()?;
                    return Ok(());
                }
                if let Some(table_name) = self.find_table_name(name) {
                    let index_names: Vec<String> = self
                        .indexes
                        .values()
                        .filter(|index| index.table.eq_ignore_ascii_case(&table_name))
                        .map(|index| index.name.clone())
                        .collect();
                    for index_name in index_names {
                        self.rebuild_index(&index_name)?;
                    }
                    self.write_catalog()?;
                    return Ok(());
                }
                Err(StorageError::NotFound(format!(
                    "index or table not found: {}",
                    name
                )))
            }
        }
    }

    pub fn scan_table(&self, table_name: &str) -> Result<Vec<Vec<Value>>, StorageError> {
        let mut rows = Vec::new();
        let mut scan = self.table_scan(table_name)?;
        while let Some(result) = scan.next() {
            rows.push(result?);
        }
        Ok(rows)
    }

    pub fn table_scan(&self, table_name: &str) -> Result<TableScan<'_>, StorageError> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| StorageError::NotFound(format!("table not found: {}", table_name)))?;
        TableScan::new(self, table)
    }

    fn scan_table_with_locations(
        &self,
        table_name: &str,
    ) -> Result<Vec<(RowLocation, Vec<Value>)>, StorageError> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| StorageError::NotFound(format!("table not found: {}", table_name)))?;
        let mut rows = Vec::new();
        let mut page_id = table.first_page;
        loop {
            let page = self.read_page(page_id)?;
            let records = read_records_with_slots(&page);
            for (slot, record) in records {
                rows.push((RowLocation { page_id, slot }, decode_row(&record)?));
            }
            let next = get_next_page_id(&page);
            if next == 0 {
                break;
            }
            page_id = next;
        }
        Ok(rows)
    }

    pub(crate) fn read_row_at(&self, location: &RowLocation) -> Result<Vec<Value>, StorageError> {
        let page = self.read_page(location.page_id)?;
        let record = read_record_at_slot(&page, location.slot).ok_or_else(|| {
            StorageError::Corrupt("invalid row location".to_string())
        })?;
        decode_row(&record)
    }

    pub fn replace_table_rows(
        &mut self,
        table_name: &str,
        rows: &[Vec<Value>],
    ) -> Result<(), StorageError> {
        let table = self
            .tables
            .get(table_name)
            .cloned()
            .ok_or_else(|| StorageError::NotFound(format!("table not found: {}", table_name)))?;
        self.free_page_chain(table.first_page)?;
        let new_first_page = self.allocate_data_page()?;
        if let Some(table) = self.tables.get_mut(table_name) {
            table.first_page = new_first_page;
            table.last_page = new_first_page;
        }

        let index_names: Vec<String> = self
            .indexes
            .values()
            .filter(|index| index.table.eq_ignore_ascii_case(table_name))
            .map(|index| index.name.clone())
            .collect();
        for index_name in index_names {
            let mut index = self
                .indexes
                .remove(&index_name)
                .ok_or_else(|| StorageError::NotFound(format!("index not found: {}", index_name)))?;
            self.free_btree_pages(index.first_page)?;
            let new_first_page = self.allocate_index_root()?;
            index.first_page = new_first_page;
            index.last_page = new_first_page;
            self.indexes.insert(index_name, index);
        }

        for row in rows {
            self.insert_row_with_location(table_name, row)?;
        }
        if rows.is_empty() {
            self.write_catalog()?;
        }
        Ok(())
    }

    fn insert_index_record(
        &mut self,
        index_name: &str,
        entry: &IndexEntry,
    ) -> Result<(), StorageError> {
        let mut index = self
            .indexes
            .remove(index_name)
            .ok_or_else(|| StorageError::NotFound(format!("index not found: {}", index_name)))?;
        self.insert_index_record_btree(&mut index, entry)?;
        self.indexes.insert(index_name.to_string(), index);
        Ok(())
    }

    fn rebuild_index(&mut self, index_name: &str) -> Result<(), StorageError> {
        let index = self
            .indexes
            .get(index_name)
            .cloned()
            .ok_or_else(|| StorageError::NotFound(format!("index not found: {}", index_name)))?;
        let table = self
            .tables
            .get(&index.table)
            .cloned()
            .ok_or_else(|| StorageError::NotFound(format!("table not found: {}", index.table)))?;

        if let Err(err) = self.free_btree_pages(index.first_page) {
            if !matches!(err, StorageError::Corrupt(_)) {
                return Err(err);
            }
        }

        let new_root = self.allocate_index_root()?;
        let mut updated_index = index.clone();
        updated_index.first_page = new_root;
        updated_index.last_page = new_root;
        self.indexes.insert(index_name.to_string(), updated_index);

        let column_map = column_index_map(&table.columns);
        let mut unique_keys = HashSet::new();
        let rows = self.scan_table_with_locations(&table.name)?;
        for (location, row) in rows {
            let key = index_key_from_row(&index, &column_map, &row)?;
            if index.unique && !key_has_null(&key) {
                let encoded_key = encode_index_key(&key)?;
                if !unique_keys.insert(encoded_key) {
                    return Err(StorageError::Invalid(format!(
                        "unique index violation: {}",
                        index.name
                    )));
                }
            }
            let entry = IndexEntry { key, row: location };
            self.insert_index_record(index_name, &entry)?;
        }
        Ok(())
    }

    fn find_index_name(&self, name: &str) -> Option<String> {
        self.indexes
            .keys()
            .find(|index| index.eq_ignore_ascii_case(name))
            .cloned()
    }

    fn find_table_name(&self, name: &str) -> Option<String> {
        self.tables
            .keys()
            .find(|table| table.eq_ignore_ascii_case(name))
            .cloned()
    }

    fn index_contains_key(&self, index: &IndexMeta, key: &[Value]) -> Result<bool, StorageError> {
        self.btree_contains_key(index.first_page, key)
    }

    fn insert_index_record_btree(
        &mut self,
        index: &mut IndexMeta,
        entry: &IndexEntry,
    ) -> Result<(), StorageError> {
        let split = self.btree_insert_recursive(index.first_page, entry)?;
        if let Some(split) = split {
            let new_root = self.allocate_btree_page(PAGE_TYPE_BTREE_INTERNAL)?;
            let mut page = init_btree_page(PAGE_TYPE_BTREE_INTERNAL);
            set_next_page_id(&mut page, index.first_page);
            let cell = InternalCell {
                key: split.key,
                right_child: split.right_page,
            };
            let record = encode_internal_cell(&cell)?;
            insert_record(&mut page, &record)?;
            self.write_page(new_root, &page)?;
            index.first_page = new_root;
        }
        Ok(())
    }

    fn btree_contains_key(&self, root: u32, key: &[Value]) -> Result<bool, StorageError> {
        let mut page_id = root;
        loop {
            let page = self.read_page(page_id)?;
            match page_type(&page) {
                PAGE_TYPE_BTREE_LEAF => {
                    let entries = read_leaf_entries(&page)?;
                    for entry in entries {
                        match compare_index_keys(&entry.key, key) {
                            std::cmp::Ordering::Less => continue,
                            std::cmp::Ordering::Equal => return Ok(true),
                            std::cmp::Ordering::Greater => break,
                        }
                    }
                    return Ok(false);
                }
                PAGE_TYPE_BTREE_INTERNAL => {
                    let leftmost = get_next_page_id(&page);
                    let cells = read_internal_cells(&page)?;
                    let (child, _) = btree_choose_child(key, leftmost, &cells);
                    page_id = child;
                }
                _ => {
                    return Err(StorageError::Corrupt(
                        "invalid btree page type".to_string(),
                    ))
                }
            }
        }
    }

    #[allow(dead_code)]
    fn btree_scan_range(
        &self,
        root: u32,
        lower: Option<&[Value]>,
        upper: Option<&[Value]>,
    ) -> Result<Vec<RowLocation>, StorageError> {
        let mut page_id = self.btree_find_leaf(root, lower)?;
        let mut rows = Vec::new();
        loop {
            let page = self.read_page(page_id)?;
            if page_type(&page) != PAGE_TYPE_BTREE_LEAF {
                return Err(StorageError::Corrupt(
                    "invalid btree leaf page".to_string(),
                ));
            }
            let entries = read_leaf_entries(&page)?;
            for entry in entries {
                if let Some(low) = lower {
                    if compare_index_keys(&entry.key, low) == std::cmp::Ordering::Less {
                        continue;
                    }
                }
                if let Some(high) = upper {
                    if compare_index_keys(&entry.key, high) == std::cmp::Ordering::Greater {
                        return Ok(rows);
                    }
                }
                rows.push(entry.row);
            }
            let next = get_next_page_id(&page);
            if next == 0 {
                break;
            }
            page_id = next;
        }
        Ok(rows)
    }

    #[allow(dead_code)]
    fn btree_find_leaf(
        &self,
        root: u32,
        key: Option<&[Value]>,
    ) -> Result<u32, StorageError> {
        let mut page_id = root;
        loop {
            let page = self.read_page(page_id)?;
            match page_type(&page) {
                PAGE_TYPE_BTREE_LEAF => return Ok(page_id),
                PAGE_TYPE_BTREE_INTERNAL => {
                    let leftmost = get_next_page_id(&page);
                    let cells = read_internal_cells(&page)?;
                    let child = if let Some(key) = key {
                        let (child, _) = btree_choose_child(key, leftmost, &cells);
                        child
                    } else {
                        leftmost
                    };
                    page_id = child;
                }
                _ => {
                    return Err(StorageError::Corrupt(
                        "invalid btree page type".to_string(),
                    ))
                }
            }
        }
    }

    fn btree_insert_recursive(
        &mut self,
        page_id: u32,
        entry: &IndexEntry,
    ) -> Result<Option<BtreeSplit>, StorageError> {
        let page = self.read_page(page_id)?;
        match page_type(&page) {
            PAGE_TYPE_BTREE_LEAF => self.btree_insert_leaf(page_id, &page, entry),
            PAGE_TYPE_BTREE_INTERNAL => self.btree_insert_internal(page_id, &page, entry),
            _ => Err(StorageError::Corrupt(
                "invalid btree page type".to_string(),
            )),
        }
    }

    fn btree_insert_leaf(
        &mut self,
        page_id: u32,
        page: &[u8],
        entry: &IndexEntry,
    ) -> Result<Option<BtreeSplit>, StorageError> {
        let next_leaf = get_next_page_id(page);
        let mut entries = read_leaf_entries(page)?;
        insert_leaf_entry_sorted(&mut entries, entry.clone());

        match build_leaf_page(&entries, next_leaf) {
            Ok(new_page) => {
                self.write_page(page_id, &new_page)?;
                Ok(None)
            }
            Err(StorageError::Invalid(msg)) if msg == "page full" => {
                let split = split_leaf_entries(entries);
                let right_page_id = self.allocate_btree_page(PAGE_TYPE_BTREE_LEAF)?;
                let left_page = build_leaf_page(&split.left, right_page_id)?;
                let right_page = build_leaf_page(&split.right, next_leaf)?;
                self.write_page(page_id, &left_page)?;
                self.write_page(right_page_id, &right_page)?;
                Ok(Some(BtreeSplit {
                    key: split.split_key,
                    right_page: right_page_id,
                }))
            }
            Err(err) => Err(err),
        }
    }

    fn btree_insert_internal(
        &mut self,
        page_id: u32,
        page: &[u8],
        entry: &IndexEntry,
    ) -> Result<Option<BtreeSplit>, StorageError> {
        let leftmost = get_next_page_id(page);
        let mut cells = read_internal_cells(page)?;
        let (child_page, child_index) = btree_choose_child(&entry.key, leftmost, &cells);
        let split = self.btree_insert_recursive(child_page, entry)?;
        let Some(split) = split else {
            return Ok(None);
        };

        cells.insert(
            child_index,
            InternalCell {
                key: split.key,
                right_child: split.right_page,
            },
        );

        match build_internal_page(leftmost, &cells) {
            Ok(new_page) => {
                self.write_page(page_id, &new_page)?;
                Ok(None)
            }
            Err(StorageError::Invalid(msg)) if msg == "page full" => {
                let split_result = split_internal_cells(leftmost, cells)?;
                let right_page_id = self.allocate_btree_page(PAGE_TYPE_BTREE_INTERNAL)?;
                let left_page =
                    build_internal_page(split_result.left_leftmost, &split_result.left_cells)?;
                let right_page =
                    build_internal_page(split_result.right_leftmost, &split_result.right_cells)?;
                self.write_page(page_id, &left_page)?;
                self.write_page(right_page_id, &right_page)?;
                Ok(Some(BtreeSplit {
                    key: split_result.split_key,
                    right_page: right_page_id,
                }))
            }
            Err(err) => Err(err),
        }
    }

    fn free_btree_pages(&mut self, root: u32) -> Result<(), StorageError> {
        let pages = self.collect_btree_pages(root)?;
        for page_id in pages {
            self.free_page(page_id)?;
        }
        Ok(())
    }

    fn collect_btree_pages(&self, root: u32) -> Result<Vec<u32>, StorageError> {
        if root == 0 {
            return Err(StorageError::Corrupt("invalid btree root".to_string()));
        }
        let mut pages = Vec::new();
        let mut stack = vec![root];
        let mut seen = HashSet::new();
        while let Some(page_id) = stack.pop() {
            if !seen.insert(page_id) {
                return Err(StorageError::Corrupt(format!(
                    "btree page loop at {}",
                    page_id
                )));
            }
            let page = self.read_page(page_id)?;
            pages.push(page_id);
            match page_type(&page) {
                PAGE_TYPE_BTREE_LEAF => {}
                PAGE_TYPE_BTREE_INTERNAL => {
                    let leftmost = get_next_page_id(&page);
                    if leftmost != 0 {
                        stack.push(leftmost);
                    }
                    let cells = read_internal_cells(&page)?;
                    for cell in cells {
                        if cell.right_child != 0 {
                            stack.push(cell.right_child);
                        }
                    }
                }
                _ => {
                    return Err(StorageError::Corrupt(
                        "invalid btree page type".to_string(),
                    ))
                }
            }
        }
        Ok(pages)
    }

    pub fn allocate_data_page(&mut self) -> Result<u32, StorageError> {
        if let Some(page_id) = self.free_pages.pop() {
            let page = init_data_page();
            self.write_page(page_id, &page)?;
            return Ok(page_id);
        }
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        let page = init_data_page();
        self.write_page(page_id, &page)?;
        self.write_header()?;
        Ok(page_id)
    }

    pub fn allocate_btree_page(&mut self, page_type: u8) -> Result<u32, StorageError> {
        if page_type != PAGE_TYPE_BTREE_LEAF && page_type != PAGE_TYPE_BTREE_INTERNAL {
            return Err(StorageError::Invalid(format!(
                "invalid btree page type {}",
                page_type
            )));
        }
        if let Some(page_id) = self.free_pages.pop() {
            let page = init_btree_page(page_type);
            self.write_page(page_id, &page)?;
            return Ok(page_id);
        }
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        let page = init_btree_page(page_type);
        self.write_page(page_id, &page)?;
        self.write_header()?;
        Ok(page_id)
    }

    pub fn allocate_index_root(&mut self) -> Result<u32, StorageError> {
        self.allocate_btree_page(PAGE_TYPE_BTREE_LEAF)
    }

    fn find_foreign_key_dependency(&self, target: &str) -> Option<String> {
        for table in self.tables.values() {
            if table.name.eq_ignore_ascii_case(target) {
                continue;
            }
            for constraint in &table.constraints {
                if let TableConstraint::ForeignKey { foreign_table, .. } = constraint {
                    let foreign_name = object_name_string(foreign_table);
                    if foreign_name.eq_ignore_ascii_case(target) {
                        return Some(table.name.clone());
                    }
                }
            }
        }
        None
    }

    fn free_page_chain(&mut self, start_page_id: u32) -> Result<(), StorageError> {
        let pages = self.collect_page_chain(start_page_id)?;
        for page_id in pages {
            self.free_page(page_id)?;
        }
        Ok(())
    }

    fn free_page(&mut self, page_id: u32) -> Result<(), StorageError> {
        if page_id == HEADER_PAGE_ID || page_id == CATALOG_PAGE_ID {
            return Err(StorageError::Invalid(format!(
                "cannot free reserved page {}",
                page_id
            )));
        }
        let page = init_data_page();
        self.write_page(page_id, &page)?;
        if !self.free_pages.contains(&page_id) {
            self.free_pages.push(page_id);
        }
        Ok(())
    }

    fn collect_page_chain(&self, start_page_id: u32) -> Result<Vec<u32>, StorageError> {
        if start_page_id == 0 {
            return Err(StorageError::Corrupt("invalid page chain start".to_string()));
        }
        let mut pages = Vec::new();
        let mut seen = HashSet::new();
        let mut page_id = start_page_id;
        loop {
            if !seen.insert(page_id) {
                return Err(StorageError::Corrupt(format!(
                    "page chain loop at {}",
                    page_id
                )));
            }
            pages.push(page_id);
            let page = self.read_page(page_id)?;
            let next = get_next_page_id(&page);
            if next == 0 {
                break;
            }
            page_id = next;
        }
        Ok(pages)
    }

    fn read_page(&self, page_id: u32) -> Result<Vec<u8>, StorageError> {
        match &self.mode {
            StorageMode::InMemory { pages } => pages
                .get(page_id as usize)
                .cloned()
                .ok_or_else(|| StorageError::Invalid(format!("missing page {}", page_id))),
            StorageMode::OnDisk { file } => {
                let mut buf = vec![0; PAGE_SIZE];
                let mut file = file.try_clone()?;
                file.seek(SeekFrom::Start(page_id as u64 * PAGE_SIZE as u64))?;
                file.read_exact(&mut buf)?;
                Ok(buf)
            }
        }
    }

    fn write_page(&mut self, page_id: u32, data: &[u8]) -> Result<(), StorageError> {
        match &mut self.mode {
            StorageMode::InMemory { pages } => {
                let idx = page_id as usize;
                if idx >= pages.len() {
                    pages.resize(idx + 1, vec![0; PAGE_SIZE]);
                }
                pages[idx] = data.to_vec();
                Ok(())
            }
            StorageMode::OnDisk { file } => {
                file.seek(SeekFrom::Start(page_id as u64 * PAGE_SIZE as u64))?;
                file.write_all(data)?;
                file.sync_all()?;
                Ok(())
            }
        }
    }

    fn write_header(&mut self) -> Result<(), StorageError> {
        let mut header = vec![0; PAGE_SIZE];
        header[..8].copy_from_slice(&FILE_MAGIC);
        write_u32(&mut header, HEADER_PAGE_SIZE_OFFSET, PAGE_SIZE as u32);
        write_u32(&mut header, HEADER_NEXT_PAGE_ID_OFFSET, self.next_page_id);
        write_u32(&mut header, HEADER_CATALOG_PAGE_ID_OFFSET, CATALOG_PAGE_ID);
        write_u32(&mut header, HEADER_SCHEMA_VERSION_OFFSET, self.schema_version);
        write_u32(
            &mut header,
            HEADER_CATALOG_FORMAT_OFFSET,
            self.catalog_format_version,
        );
        self.write_page(HEADER_PAGE_ID, &header)
    }

    fn write_catalog(&mut self) -> Result<(), StorageError> {
        let mut page = init_data_page();
        for table in self.tables.values() {
            if self.catalog_format_version >= 2 {
                let mut encoded = Vec::new();
                encoded.push(1);
                encoded.extend_from_slice(&encode_table_meta(table)?);
                let _ = insert_record(&mut page, &encoded)?;
            } else {
                let encoded = encode_table_meta(table)?;
                let _ = insert_record(&mut page, &encoded)?;
            }
        }
        if self.catalog_format_version >= 2 {
            for index in self.indexes.values() {
                let mut encoded = Vec::new();
                encoded.push(2);
                encoded.extend_from_slice(&encode_index_meta(index)?);
                let _ = insert_record(&mut page, &encoded)?;
            }
        }
        if self.catalog_format_version >= 3 {
            for view in self.views.values() {
                let mut encoded = Vec::new();
                encoded.push(3);
                encoded.extend_from_slice(&encode_view_meta(view)?);
                let _ = insert_record(&mut page, &encoded)?;
            }
        }
        self.write_page(CATALOG_PAGE_ID, &page)
    }

    fn bump_schema_version(&mut self) -> Result<(), StorageError> {
        self.schema_version = self.schema_version.saturating_add(1);
        self.write_header()
    }
}

fn load_catalog_from_file(
    file: &mut File,
    next_page_id: u32,
    format_version: u32,
) -> Result<
    (
        HashMap<String, TableMeta>,
        HashMap<String, IndexMeta>,
        HashMap<String, ViewMeta>,
    ),
    StorageError,
> {
    let mut page = vec![0; PAGE_SIZE];
    file.seek(SeekFrom::Start(CATALOG_PAGE_ID as u64 * PAGE_SIZE as u64))?;
    file.read_exact(&mut page)?;
    let mut tables = HashMap::new();
    let mut indexes = HashMap::new();
    let mut views = HashMap::new();
    for record in read_records(&page) {
        if format_version >= 2 {
            if record.is_empty() {
                return Err(StorageError::Corrupt("empty catalog record".to_string()));
            }
            match record[0] {
                1 => {
                    let table = decode_table_meta(&record[1..])?;
                    tables.insert(table.name.clone(), table);
                }
                2 => {
                    let index = decode_index_meta(&record[1..])?;
                    indexes.insert(index.name.clone(), index);
                }
                3 => {
                    if format_version < 3 {
                        return Err(StorageError::Corrupt(
                            "unexpected view record in catalog".to_string(),
                        ));
                    }
                    let view = decode_view_meta(&record[1..])?;
                    views.insert(view.name.clone(), view);
                }
                tag => {
                    return Err(StorageError::Corrupt(format!(
                        "unknown catalog record tag {}",
                        tag
                    )))
                }
            }
        } else {
            let table = if format_version == 0 {
                decode_table_meta_v0(&record)?
            } else {
                decode_table_meta(&record)?
            };
            tables.insert(table.name.clone(), table);
        }
    }
    if next_page_id == 0 {
        return Err(StorageError::Corrupt("invalid next page id".to_string()));
    }
    Ok((tables, indexes, views))
}

fn object_name_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|part| part.value.clone())
        .collect::<Vec<_>>()
        .join(".")
}

fn init_data_page() -> Vec<u8> {
    let mut page = vec![0; PAGE_SIZE];
    page[0] = PAGE_TYPE_DATA;
    write_u16(&mut page, 1, 0);
    write_u16(&mut page, 3, 12);
    write_u16(&mut page, 5, PAGE_SIZE as u16);
    write_u32(&mut page, 7, 0);
    page
}

fn init_btree_page(page_type: u8) -> Vec<u8> {
    let mut page = vec![0; PAGE_SIZE];
    page[0] = page_type;
    write_u16(&mut page, 1, 0);
    write_u16(&mut page, 3, 12);
    write_u16(&mut page, 5, PAGE_SIZE as u16);
    write_u32(&mut page, 7, 0);
    page
}

fn get_next_page_id(page: &[u8]) -> u32 {
    read_u32(page, 7)
}

fn set_next_page_id(page: &mut [u8], next: u32) {
    write_u32(page, 7, next);
}

fn has_space_for_record(page: &[u8], record_len: usize) -> bool {
    let free_start = read_u16(page, 3) as usize;
    let free_end = read_u16(page, 5) as usize;
    free_start + record_len + 4 <= free_end
}

fn insert_record(page: &mut [u8], record: &[u8]) -> Result<u16, StorageError> {
    if !has_space_for_record(page, record.len()) {
        return Err(StorageError::Invalid("page full".to_string()));
    }
    let slot_count = read_u16(page, 1);
    let free_start = read_u16(page, 3) as usize;
    let free_end = read_u16(page, 5) as usize;

    page[free_start..free_start + record.len()].copy_from_slice(record);
    let new_free_start = free_start + record.len();

    let slot_offset = free_end - 4;
    write_u16(page, slot_offset, free_start as u16);
    write_u16(page, slot_offset + 2, record.len() as u16);

    write_u16(page, 1, slot_count + 1);
    write_u16(page, 3, new_free_start as u16);
    write_u16(page, 5, slot_offset as u16);
    Ok(slot_count)
}

fn read_records(page: &[u8]) -> Vec<Vec<u8>> {
    let slot_count = read_u16(page, 1) as usize;
    let mut records = Vec::new();
    for idx in 0..slot_count {
        let slot_offset = PAGE_SIZE - (idx + 1) * 4;
        let record_offset = read_u16(page, slot_offset) as usize;
        let record_len = read_u16(page, slot_offset + 2) as usize;
        if record_offset + record_len <= PAGE_SIZE {
            records.push(page[record_offset..record_offset + record_len].to_vec());
        }
    }
    records
}

fn read_records_with_slots(page: &[u8]) -> Vec<(u16, Vec<u8>)> {
    let slot_count = read_u16(page, 1) as usize;
    let mut records = Vec::new();
    for idx in 0..slot_count {
        let slot_offset = PAGE_SIZE - (idx + 1) * 4;
        let record_offset = read_u16(page, slot_offset) as usize;
        let record_len = read_u16(page, slot_offset + 2) as usize;
        if record_offset + record_len <= PAGE_SIZE {
            records.push((idx as u16, page[record_offset..record_offset + record_len].to_vec()));
        }
    }
    records
}

fn read_record_at_slot(page: &[u8], slot: u16) -> Option<Vec<u8>> {
    let slot_count = read_u16(page, 1) as usize;
    let slot = slot as usize;
    if slot >= slot_count {
        return None;
    }
    let slot_offset = PAGE_SIZE - (slot + 1) * 4;
    let record_offset = read_u16(page, slot_offset) as usize;
    let record_len = read_u16(page, slot_offset + 2) as usize;
    if record_offset + record_len > PAGE_SIZE {
        return None;
    }
    Some(page[record_offset..record_offset + record_len].to_vec())
}

fn encode_row(row: &[Value]) -> Result<Vec<u8>, StorageError> {
    let mut buf = Vec::new();
    if row.len() > u16::MAX as usize {
        return Err(StorageError::Invalid("row too wide".to_string()));
    }
    buf.extend_from_slice(&(row.len() as u16).to_le_bytes());
    for value in row {
        encode_value(value, &mut buf)?;
    }
    Ok(buf)
}

fn decode_row(record: &[u8]) -> Result<Vec<Value>, StorageError> {
    if record.len() < 2 {
        return Err(StorageError::Corrupt("record too small".to_string()));
    }
    let mut pos = 0;
    let count = read_u16(record, pos) as usize;
    pos += 2;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let (value, new_pos) = decode_value(record, pos)?;
        pos = new_pos;
        values.push(value);
    }
    Ok(values)
}

fn encode_index_key(values: &[Value]) -> Result<Vec<u8>, StorageError> {
    let mut buf = Vec::new();
    if values.len() > u16::MAX as usize {
        return Err(StorageError::Invalid("index key too wide".to_string()));
    }
    buf.extend_from_slice(&(values.len() as u16).to_le_bytes());
    for value in values {
        encode_value(value, &mut buf)?;
    }
    Ok(buf)
}

fn encode_index_entry(entry: &IndexEntry) -> Result<Vec<u8>, StorageError> {
    let mut buf = encode_index_key(&entry.key)?;
    buf.extend_from_slice(&entry.row.page_id.to_le_bytes());
    buf.extend_from_slice(&entry.row.slot.to_le_bytes());
    Ok(buf)
}

fn decode_index_entry(record: &[u8]) -> Result<IndexEntry, StorageError> {
    if record.len() < 2 + 4 + 2 {
        return Err(StorageError::Corrupt("invalid index entry".to_string()));
    }
    let mut pos = 0;
    let count = read_u16(record, pos) as usize;
    pos += 2;
    let mut key = Vec::with_capacity(count);
    for _ in 0..count {
        let (value, new_pos) = decode_value(record, pos)?;
        pos = new_pos;
        key.push(value);
    }
    if pos + 6 > record.len() {
        return Err(StorageError::Corrupt("invalid index entry location".to_string()));
    }
    let page_id = read_u32(record, pos);
    pos += 4;
    let slot = read_u16(record, pos);
    Ok(IndexEntry {
        key,
        row: RowLocation { page_id, slot },
    })
}

fn encode_internal_cell(cell: &InternalCell) -> Result<Vec<u8>, StorageError> {
    let mut buf = encode_index_key(&cell.key)?;
    buf.extend_from_slice(&cell.right_child.to_le_bytes());
    Ok(buf)
}

fn decode_internal_cell(record: &[u8]) -> Result<InternalCell, StorageError> {
    if record.len() < 2 + 4 {
        return Err(StorageError::Corrupt("invalid internal cell".to_string()));
    }
    let mut pos = 0;
    let count = read_u16(record, pos) as usize;
    pos += 2;
    let mut key = Vec::with_capacity(count);
    for _ in 0..count {
        let (value, new_pos) = decode_value(record, pos)?;
        pos = new_pos;
        key.push(value);
    }
    if pos + 4 > record.len() {
        return Err(StorageError::Corrupt("invalid internal cell child".to_string()));
    }
    let right_child = read_u32(record, pos);
    Ok(InternalCell { key, right_child })
}

fn page_type(page: &[u8]) -> u8 {
    page[0]
}

fn read_leaf_entries(page: &[u8]) -> Result<Vec<IndexEntry>, StorageError> {
    let records = read_records(page);
    let mut entries = Vec::with_capacity(records.len());
    for record in records {
        entries.push(decode_index_entry(&record)?);
    }
    Ok(entries)
}

fn read_internal_cells(page: &[u8]) -> Result<Vec<InternalCell>, StorageError> {
    let records = read_records(page);
    let mut cells = Vec::with_capacity(records.len());
    for record in records {
        cells.push(decode_internal_cell(&record)?);
    }
    Ok(cells)
}

fn build_leaf_page(entries: &[IndexEntry], next_leaf: u32) -> Result<Vec<u8>, StorageError> {
    let mut page = init_btree_page(PAGE_TYPE_BTREE_LEAF);
    set_next_page_id(&mut page, next_leaf);
    for entry in entries {
        let record = encode_index_entry(entry)?;
        insert_record(&mut page, &record)?;
    }
    Ok(page)
}

fn build_internal_page(
    leftmost_child: u32,
    cells: &[InternalCell],
) -> Result<Vec<u8>, StorageError> {
    let mut page = init_btree_page(PAGE_TYPE_BTREE_INTERNAL);
    set_next_page_id(&mut page, leftmost_child);
    for cell in cells {
        let record = encode_internal_cell(cell)?;
        insert_record(&mut page, &record)?;
    }
    Ok(page)
}

fn insert_leaf_entry_sorted(entries: &mut Vec<IndexEntry>, entry: IndexEntry) {
    let idx = entries
        .binary_search_by(|existing| compare_index_entry(existing, &entry))
        .unwrap_or_else(|idx| idx);
    entries.insert(idx, entry);
}

struct LeafSplit {
    left: Vec<IndexEntry>,
    right: Vec<IndexEntry>,
    split_key: Vec<Value>,
}

fn split_leaf_entries(entries: Vec<IndexEntry>) -> LeafSplit {
    let mid = entries.len() / 2;
    let right = entries[mid..].to_vec();
    let left = entries[..mid].to_vec();
    let split_key = right
        .first()
        .map(|entry| entry.key.clone())
        .unwrap_or_default();
    LeafSplit {
        left,
        right,
        split_key,
    }
}

struct InternalSplit {
    left_leftmost: u32,
    left_cells: Vec<InternalCell>,
    right_leftmost: u32,
    right_cells: Vec<InternalCell>,
    split_key: Vec<Value>,
}

fn split_internal_cells(
    leftmost_child: u32,
    cells: Vec<InternalCell>,
) -> Result<InternalSplit, StorageError> {
    if cells.is_empty() {
        return Err(StorageError::Corrupt("empty internal node".to_string()));
    }
    let mid = cells.len() / 2;
    let split_key = cells[mid].key.clone();

    let mut children = Vec::with_capacity(cells.len() + 1);
    children.push(leftmost_child);
    for cell in &cells {
        children.push(cell.right_child);
    }

    let left_leftmost = children[0];
    let left_cells = cells[..mid].to_vec();
    let right_leftmost = children[mid + 1];
    let right_cells = cells[mid + 1..].to_vec();

    Ok(InternalSplit {
        left_leftmost,
        left_cells,
        right_leftmost,
        right_cells,
        split_key,
    })
}

fn btree_choose_child(
    key: &[Value],
    leftmost_child: u32,
    cells: &[InternalCell],
) -> (u32, usize) {
    let mut child = leftmost_child;
    let mut index = 0;
    for cell in cells {
        if compare_index_keys(key, &cell.key) == std::cmp::Ordering::Less {
            return (child, index);
        }
        child = cell.right_child;
        index += 1;
    }
    (child, index)
}

fn compare_index_entry(a: &IndexEntry, b: &IndexEntry) -> std::cmp::Ordering {
    compare_index_keys(&a.key, &b.key).then_with(|| compare_row_location(&a.row, &b.row))
}

fn compare_row_location(a: &RowLocation, b: &RowLocation) -> std::cmp::Ordering {
    a.page_id
        .cmp(&b.page_id)
        .then_with(|| a.slot.cmp(&b.slot))
}

fn compare_index_keys(a: &[Value], b: &[Value]) -> std::cmp::Ordering {
    let len = a.len().min(b.len());
    for idx in 0..len {
        let ord = compare_index_value(&a[idx], &b[idx]);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    a.len().cmp(&b.len())
}

fn compare_index_value(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
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

fn encode_value(value: &Value, buf: &mut Vec<u8>) -> Result<(), StorageError> {
    match value {
        Value::Null => buf.push(0),
        Value::Integer(v) => {
            buf.push(1);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Value::Real(v) => {
            buf.push(2);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Value::Text(s) => {
            buf.push(3);
            let bytes = s.as_bytes();
            if bytes.len() > u32::MAX as usize {
                return Err(StorageError::Invalid("text too large".to_string()));
            }
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        Value::Blob(bytes) => {
            buf.push(4);
            if bytes.len() > u32::MAX as usize {
                return Err(StorageError::Invalid("blob too large".to_string()));
            }
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
    }
    Ok(())
}

fn decode_value(record: &[u8], pos: usize) -> Result<(Value, usize), StorageError> {
    if pos >= record.len() {
        return Err(StorageError::Corrupt("invalid value".to_string()));
    }
    let tag = record[pos];
    let mut cursor = pos + 1;
    let value = match tag {
        0 => Value::Null,
        1 => {
            let end = cursor + 8;
            if end > record.len() {
                return Err(StorageError::Corrupt("invalid integer".to_string()));
            }
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&record[cursor..end]);
            cursor = end;
            Value::Integer(i64::from_le_bytes(buf))
        }
        2 => {
            let end = cursor + 8;
            if end > record.len() {
                return Err(StorageError::Corrupt("invalid real".to_string()));
            }
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&record[cursor..end]);
            cursor = end;
            Value::Real(f64::from_le_bytes(buf))
        }
        3 => {
            let end = cursor + 4;
            if end > record.len() {
                return Err(StorageError::Corrupt("invalid text".to_string()));
            }
            let len = read_u32(record, cursor) as usize;
            cursor = end;
            let end = cursor + len;
            if end > record.len() {
                return Err(StorageError::Corrupt("invalid text length".to_string()));
            }
            let text = String::from_utf8_lossy(&record[cursor..end]).to_string();
            cursor = end;
            Value::Text(text)
        }
        4 => {
            let end = cursor + 4;
            if end > record.len() {
                return Err(StorageError::Corrupt("invalid blob".to_string()));
            }
            let len = read_u32(record, cursor) as usize;
            cursor = end;
            let end = cursor + len;
            if end > record.len() {
                return Err(StorageError::Corrupt("invalid blob length".to_string()));
            }
            let data = record[cursor..end].to_vec();
            cursor = end;
            Value::Blob(data)
        }
        _ => {
            return Err(StorageError::Corrupt(format!(
                "unknown value tag {}",
                tag
            )))
        }
    };
    Ok((value, cursor))
}

fn column_index_map(columns: &[Column]) -> HashMap<String, usize> {
    let mut map = HashMap::new();
    for (idx, column) in columns.iter().enumerate() {
        map.insert(column.name.to_lowercase(), idx);
    }
    map
}

fn index_key_from_row(
    index: &IndexMeta,
    column_map: &HashMap<String, usize>,
    row: &[Value],
) -> Result<Vec<Value>, StorageError> {
    let mut key = Vec::with_capacity(index.columns.len());
    for column in &index.columns {
        let idx = column_map.get(&column.name.value.to_lowercase()).ok_or_else(|| {
            StorageError::Invalid(format!("unknown column in index {}", column.name.value))
        })?;
        key.push(row[*idx].clone());
    }
    Ok(key)
}

fn key_has_null(values: &[Value]) -> bool {
    values.iter().any(|value| matches!(value, Value::Null))
}

fn encode_ident(ident: &Ident, buf: &mut Vec<u8>) -> Result<(), StorageError> {
    buf.push(if ident.quoted { 1 } else { 0 });
    if ident.value.len() > u16::MAX as usize {
        return Err(StorageError::Invalid("identifier too long".to_string()));
    }
    buf.extend_from_slice(&(ident.value.len() as u16).to_le_bytes());
    buf.extend_from_slice(ident.value.as_bytes());
    Ok(())
}

fn decode_ident(record: &[u8], pos: usize) -> Result<(Ident, usize), StorageError> {
    if pos + 3 > record.len() {
        return Err(StorageError::Corrupt("invalid identifier".to_string()));
    }
    let quoted = record[pos] != 0;
    let len = read_u16(record, pos + 1) as usize;
    let mut cursor = pos + 3;
    if cursor + len > record.len() {
        return Err(StorageError::Corrupt("invalid identifier".to_string()));
    }
    let value = String::from_utf8_lossy(&record[cursor..cursor + len]).to_string();
    cursor += len;
    Ok((Ident { value, quoted }, cursor))
}

fn encode_ident_list(idents: &[Ident], buf: &mut Vec<u8>) -> Result<(), StorageError> {
    if idents.len() > u16::MAX as usize {
        return Err(StorageError::Invalid("too many identifiers".to_string()));
    }
    buf.extend_from_slice(&(idents.len() as u16).to_le_bytes());
    for ident in idents {
        encode_ident(ident, buf)?;
    }
    Ok(())
}

fn decode_ident_list(record: &[u8], pos: usize) -> Result<(Vec<Ident>, usize), StorageError> {
    if pos + 2 > record.len() {
        return Err(StorageError::Corrupt("invalid identifier list".to_string()));
    }
    let count = read_u16(record, pos) as usize;
    let mut cursor = pos + 2;
    let mut idents = Vec::with_capacity(count);
    for _ in 0..count {
        let (ident, new_pos) = decode_ident(record, cursor)?;
        cursor = new_pos;
        idents.push(ident);
    }
    Ok((idents, cursor))
}

fn encode_object_name(name: &ObjectName, buf: &mut Vec<u8>) -> Result<(), StorageError> {
    encode_ident_list(&name.0, buf)
}

fn decode_object_name(record: &[u8], pos: usize) -> Result<(ObjectName, usize), StorageError> {
    let (parts, cursor) = decode_ident_list(record, pos)?;
    Ok((ObjectName(parts), cursor))
}

fn encode_literal(literal: &Literal, buf: &mut Vec<u8>) -> Result<(), StorageError> {
    match literal {
        Literal::Null => buf.push(0),
        Literal::Integer(v) => {
            buf.push(1);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Literal::Float(v) => {
            buf.push(2);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Literal::String(s) => {
            buf.push(3);
            let bytes = s.as_bytes();
            if bytes.len() > u32::MAX as usize {
                return Err(StorageError::Invalid("string literal too long".to_string()));
            }
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        Literal::Boolean(v) => {
            buf.push(4);
            buf.push(if *v { 1 } else { 0 });
        }
        Literal::Blob(bytes) => {
            buf.push(5);
            if bytes.len() > u32::MAX as usize {
                return Err(StorageError::Invalid("blob literal too large".to_string()));
            }
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
    }
    Ok(())
}

fn decode_literal(record: &[u8], pos: usize) -> Result<(Literal, usize), StorageError> {
    if pos >= record.len() {
        return Err(StorageError::Corrupt("invalid literal".to_string()));
    }
    let tag = record[pos];
    let mut cursor = pos + 1;
    let literal = match tag {
        0 => Literal::Null,
        1 => {
            if cursor + 8 > record.len() {
                return Err(StorageError::Corrupt("invalid integer literal".to_string()));
            }
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&record[cursor..cursor + 8]);
            cursor += 8;
            Literal::Integer(i64::from_le_bytes(buf))
        }
        2 => {
            if cursor + 8 > record.len() {
                return Err(StorageError::Corrupt("invalid float literal".to_string()));
            }
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&record[cursor..cursor + 8]);
            cursor += 8;
            Literal::Float(f64::from_le_bytes(buf))
        }
        3 => {
            if cursor + 4 > record.len() {
                return Err(StorageError::Corrupt("invalid string literal".to_string()));
            }
            let len = read_u32(record, cursor) as usize;
            cursor += 4;
            if cursor + len > record.len() {
                return Err(StorageError::Corrupt("invalid string literal".to_string()));
            }
            let text = String::from_utf8_lossy(&record[cursor..cursor + len]).to_string();
            cursor += len;
            Literal::String(text)
        }
        4 => {
            if cursor >= record.len() {
                return Err(StorageError::Corrupt("invalid boolean literal".to_string()));
            }
            let value = record[cursor] != 0;
            cursor += 1;
            Literal::Boolean(value)
        }
        5 => {
            if cursor + 4 > record.len() {
                return Err(StorageError::Corrupt("invalid blob literal".to_string()));
            }
            let len = read_u32(record, cursor) as usize;
            cursor += 4;
            if cursor + len > record.len() {
                return Err(StorageError::Corrupt("invalid blob literal".to_string()));
            }
            let data = record[cursor..cursor + len].to_vec();
            cursor += len;
            Literal::Blob(data)
        }
        _ => {
            return Err(StorageError::Corrupt(format!(
                "unknown literal tag {}",
                tag
            )))
        }
    };
    Ok((literal, cursor))
}

fn encode_binary_operator(op: &BinaryOperator, buf: &mut Vec<u8>) {
    let tag = match op {
        BinaryOperator::Plus => 1,
        BinaryOperator::Minus => 2,
        BinaryOperator::Multiply => 3,
        BinaryOperator::Divide => 4,
        BinaryOperator::Modulo => 5,
        BinaryOperator::And => 6,
        BinaryOperator::Or => 7,
        BinaryOperator::Eq => 8,
        BinaryOperator::NotEq => 9,
        BinaryOperator::Lt => 10,
        BinaryOperator::LtEq => 11,
        BinaryOperator::Gt => 12,
        BinaryOperator::GtEq => 13,
        BinaryOperator::Like => 14,
        BinaryOperator::NotLike => 15,
        BinaryOperator::Is => 16,
        BinaryOperator::IsNot => 17,
        BinaryOperator::Concat => 18,
    };
    buf.push(tag);
}

fn decode_binary_operator(record: &[u8], pos: usize) -> Result<(BinaryOperator, usize), StorageError> {
    if pos >= record.len() {
        return Err(StorageError::Corrupt("invalid binary operator".to_string()));
    }
    let tag = record[pos];
    let op = match tag {
        1 => BinaryOperator::Plus,
        2 => BinaryOperator::Minus,
        3 => BinaryOperator::Multiply,
        4 => BinaryOperator::Divide,
        5 => BinaryOperator::Modulo,
        6 => BinaryOperator::And,
        7 => BinaryOperator::Or,
        8 => BinaryOperator::Eq,
        9 => BinaryOperator::NotEq,
        10 => BinaryOperator::Lt,
        11 => BinaryOperator::LtEq,
        12 => BinaryOperator::Gt,
        13 => BinaryOperator::GtEq,
        14 => BinaryOperator::Like,
        15 => BinaryOperator::NotLike,
        16 => BinaryOperator::Is,
        17 => BinaryOperator::IsNot,
        18 => BinaryOperator::Concat,
        _ => {
            return Err(StorageError::Corrupt(format!(
                "unknown binary operator tag {}",
                tag
            )))
        }
    };
    Ok((op, pos + 1))
}

fn encode_unary_operator(op: &UnaryOperator, buf: &mut Vec<u8>) {
    let tag = match op {
        UnaryOperator::Plus => 1,
        UnaryOperator::Minus => 2,
        UnaryOperator::Not => 3,
    };
    buf.push(tag);
}

fn decode_unary_operator(record: &[u8], pos: usize) -> Result<(UnaryOperator, usize), StorageError> {
    if pos >= record.len() {
        return Err(StorageError::Corrupt("invalid unary operator".to_string()));
    }
    let tag = record[pos];
    let op = match tag {
        1 => UnaryOperator::Plus,
        2 => UnaryOperator::Minus,
        3 => UnaryOperator::Not,
        _ => {
            return Err(StorageError::Corrupt(format!(
                "unknown unary operator tag {}",
                tag
            )))
        }
    };
    Ok((op, pos + 1))
}

fn encode_expr(expr: &Expr, buf: &mut Vec<u8>) -> Result<(), StorageError> {
    match expr {
        Expr::Identifier(ident) => {
            buf.push(1);
            encode_ident(ident, buf)?;
        }
        Expr::CompoundIdentifier(idents) => {
            buf.push(2);
            encode_ident_list(idents, buf)?;
        }
        Expr::Literal(literal) => {
            buf.push(3);
            encode_literal(literal, buf)?;
        }
        Expr::BinaryOp { left, op, right } => {
            buf.push(4);
            encode_binary_operator(op, buf);
            encode_expr(left, buf)?;
            encode_expr(right, buf)?;
        }
        Expr::UnaryOp { op, expr } => {
            buf.push(5);
            encode_unary_operator(op, buf);
            encode_expr(expr, buf)?;
        }
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            buf.push(6);
            encode_ident(name, buf)?;
            buf.push(if *distinct { 1 } else { 0 });
            if args.len() > u16::MAX as usize {
                return Err(StorageError::Invalid("too many function args".to_string()));
            }
            buf.extend_from_slice(&(args.len() as u16).to_le_bytes());
            for arg in args {
                encode_expr(arg, buf)?;
            }
        }
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => {
            buf.push(7);
            buf.push(if operand.is_some() { 1 } else { 0 });
            if let Some(expr) = operand {
                encode_expr(expr, buf)?;
            }
            if when_then.len() > u16::MAX as usize {
                return Err(StorageError::Invalid("too many case branches".to_string()));
            }
            buf.extend_from_slice(&(when_then.len() as u16).to_le_bytes());
            for (when_expr, then_expr) in when_then {
                encode_expr(when_expr, buf)?;
                encode_expr(then_expr, buf)?;
            }
            buf.push(if else_result.is_some() { 1 } else { 0 });
            if let Some(expr) = else_result {
                encode_expr(expr, buf)?;
            }
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            buf.push(8);
            buf.push(if *negated { 1 } else { 0 });
            encode_expr(expr, buf)?;
            encode_expr(low, buf)?;
            encode_expr(high, buf)?;
        }
        Expr::InList { expr, list, negated } => {
            buf.push(9);
            buf.push(if *negated { 1 } else { 0 });
            encode_expr(expr, buf)?;
            if list.len() > u16::MAX as usize {
                return Err(StorageError::Invalid("too many IN values".to_string()));
            }
            buf.extend_from_slice(&(list.len() as u16).to_le_bytes());
            for item in list {
                encode_expr(item, buf)?;
            }
        }
        Expr::IsNull { expr, negated } => {
            buf.push(10);
            buf.push(if *negated { 1 } else { 0 });
            encode_expr(expr, buf)?;
        }
        Expr::Cast { expr, data_type } => {
            buf.push(11);
            encode_expr(expr, buf)?;
            encode_data_type(data_type, buf)?;
        }
        Expr::Nested(expr) => {
            buf.push(12);
            encode_expr(expr, buf)?;
        }
        Expr::Wildcard => {
            buf.push(13);
        }
        _ => {
            return Err(StorageError::Invalid(
                "unsupported expression in schema metadata".to_string(),
            ))
        }
    }
    Ok(())
}

fn decode_expr(record: &[u8], pos: usize) -> Result<(Expr, usize), StorageError> {
    if pos >= record.len() {
        return Err(StorageError::Corrupt("invalid expression".to_string()));
    }
    let tag = record[pos];
    let mut cursor = pos + 1;
    let expr = match tag {
        1 => {
            let (ident, new_pos) = decode_ident(record, cursor)?;
            cursor = new_pos;
            Expr::Identifier(ident)
        }
        2 => {
            let (idents, new_pos) = decode_ident_list(record, cursor)?;
            cursor = new_pos;
            Expr::CompoundIdentifier(idents)
        }
        3 => {
            let (literal, new_pos) = decode_literal(record, cursor)?;
            cursor = new_pos;
            Expr::Literal(literal)
        }
        4 => {
            let (op, new_pos) = decode_binary_operator(record, cursor)?;
            cursor = new_pos;
            let (left, new_pos) = decode_expr(record, cursor)?;
            cursor = new_pos;
            let (right, new_pos) = decode_expr(record, cursor)?;
            cursor = new_pos;
            Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }
        }
        5 => {
            let (op, new_pos) = decode_unary_operator(record, cursor)?;
            cursor = new_pos;
            let (expr, new_pos) = decode_expr(record, cursor)?;
            cursor = new_pos;
            Expr::UnaryOp {
                op,
                expr: Box::new(expr),
            }
        }
        6 => {
            let (name, new_pos) = decode_ident(record, cursor)?;
            cursor = new_pos;
            if cursor >= record.len() {
                return Err(StorageError::Corrupt("invalid function expr".to_string()));
            }
            let distinct = record[cursor] != 0;
            cursor += 1;
            if cursor + 2 > record.len() {
                return Err(StorageError::Corrupt("invalid function args".to_string()));
            }
            let count = read_u16(record, cursor) as usize;
            cursor += 2;
            let mut args = Vec::with_capacity(count);
            for _ in 0..count {
                let (arg, new_pos) = decode_expr(record, cursor)?;
                cursor = new_pos;
                args.push(arg);
            }
            Expr::Function {
                name,
                args,
                distinct,
            }
        }
        7 => {
            if cursor >= record.len() {
                return Err(StorageError::Corrupt("invalid case expr".to_string()));
            }
            let has_operand = record[cursor] != 0;
            cursor += 1;
            let operand = if has_operand {
                let (expr, new_pos) = decode_expr(record, cursor)?;
                cursor = new_pos;
                Some(Box::new(expr))
            } else {
                None
            };
            if cursor + 2 > record.len() {
                return Err(StorageError::Corrupt("invalid case expr".to_string()));
            }
            let count = read_u16(record, cursor) as usize;
            cursor += 2;
            let mut when_then = Vec::with_capacity(count);
            for _ in 0..count {
                let (when_expr, new_pos) = decode_expr(record, cursor)?;
                cursor = new_pos;
                let (then_expr, new_pos) = decode_expr(record, cursor)?;
                cursor = new_pos;
                when_then.push((when_expr, then_expr));
            }
            if cursor >= record.len() {
                return Err(StorageError::Corrupt("invalid case expr".to_string()));
            }
            let has_else = record[cursor] != 0;
            cursor += 1;
            let else_result = if has_else {
                let (expr, new_pos) = decode_expr(record, cursor)?;
                cursor = new_pos;
                Some(Box::new(expr))
            } else {
                None
            };
            Expr::Case {
                operand,
                when_then,
                else_result,
            }
        }
        8 => {
            if cursor >= record.len() {
                return Err(StorageError::Corrupt("invalid between expr".to_string()));
            }
            let negated = record[cursor] != 0;
            cursor += 1;
            let (expr, new_pos) = decode_expr(record, cursor)?;
            cursor = new_pos;
            let (low, new_pos) = decode_expr(record, cursor)?;
            cursor = new_pos;
            let (high, new_pos) = decode_expr(record, cursor)?;
            cursor = new_pos;
            Expr::Between {
                expr: Box::new(expr),
                negated,
                low: Box::new(low),
                high: Box::new(high),
            }
        }
        9 => {
            if cursor >= record.len() {
                return Err(StorageError::Corrupt("invalid IN expr".to_string()));
            }
            let negated = record[cursor] != 0;
            cursor += 1;
            let (expr, new_pos) = decode_expr(record, cursor)?;
            cursor = new_pos;
            if cursor + 2 > record.len() {
                return Err(StorageError::Corrupt("invalid IN expr".to_string()));
            }
            let count = read_u16(record, cursor) as usize;
            cursor += 2;
            let mut list = Vec::with_capacity(count);
            for _ in 0..count {
                let (item, new_pos) = decode_expr(record, cursor)?;
                cursor = new_pos;
                list.push(item);
            }
            Expr::InList {
                expr: Box::new(expr),
                list,
                negated,
            }
        }
        10 => {
            if cursor >= record.len() {
                return Err(StorageError::Corrupt("invalid IS NULL expr".to_string()));
            }
            let negated = record[cursor] != 0;
            cursor += 1;
            let (expr, new_pos) = decode_expr(record, cursor)?;
            cursor = new_pos;
            Expr::IsNull {
                expr: Box::new(expr),
                negated,
            }
        }
        11 => {
            let (expr, new_pos) = decode_expr(record, cursor)?;
            cursor = new_pos;
            let (data_type, new_pos) = decode_data_type(record, cursor)?;
            cursor = new_pos;
            Expr::Cast {
                expr: Box::new(expr),
                data_type,
            }
        }
        12 => {
            let (expr, new_pos) = decode_expr(record, cursor)?;
            cursor = new_pos;
            Expr::Nested(Box::new(expr))
        }
        13 => Expr::Wildcard,
        _ => {
            return Err(StorageError::Corrupt(format!(
                "unknown expression tag {}",
                tag
            )))
        }
    };
    Ok((expr, cursor))
}

fn encode_column_constraint(
    constraint: &ColumnConstraint,
    buf: &mut Vec<u8>,
) -> Result<(), StorageError> {
    match constraint {
        ColumnConstraint::NotNull => buf.push(1),
        ColumnConstraint::Null => buf.push(2),
        ColumnConstraint::PrimaryKey => buf.push(3),
        ColumnConstraint::Unique => buf.push(4),
        ColumnConstraint::Default(expr) => {
            buf.push(5);
            encode_expr(expr, buf)?;
        }
    }
    Ok(())
}

fn decode_column_constraint(
    record: &[u8],
    pos: usize,
) -> Result<(ColumnConstraint, usize), StorageError> {
    if pos >= record.len() {
        return Err(StorageError::Corrupt("invalid column constraint".to_string()));
    }
    let tag = record[pos];
    let mut cursor = pos + 1;
    let constraint = match tag {
        1 => ColumnConstraint::NotNull,
        2 => ColumnConstraint::Null,
        3 => ColumnConstraint::PrimaryKey,
        4 => ColumnConstraint::Unique,
        5 => {
            let (expr, new_pos) = decode_expr(record, cursor)?;
            cursor = new_pos;
            ColumnConstraint::Default(expr)
        }
        _ => {
            return Err(StorageError::Corrupt(format!(
                "unknown column constraint tag {}",
                tag
            )))
        }
    };
    Ok((constraint, cursor))
}

fn encode_table_constraint(
    constraint: &TableConstraint,
    buf: &mut Vec<u8>,
) -> Result<(), StorageError> {
    match constraint {
        TableConstraint::PrimaryKey(columns) => {
            buf.push(1);
            encode_ident_list(columns, buf)?;
        }
        TableConstraint::Unique(columns) => {
            buf.push(2);
            encode_ident_list(columns, buf)?;
        }
        TableConstraint::Check(expr) => {
            buf.push(3);
            encode_expr(expr, buf)?;
        }
        TableConstraint::ForeignKey {
            columns,
            foreign_table,
            referred_columns,
        } => {
            buf.push(4);
            encode_ident_list(columns, buf)?;
            encode_object_name(foreign_table, buf)?;
            encode_ident_list(referred_columns, buf)?;
        }
    }
    Ok(())
}

fn decode_table_constraint(
    record: &[u8],
    pos: usize,
) -> Result<(TableConstraint, usize), StorageError> {
    if pos >= record.len() {
        return Err(StorageError::Corrupt("invalid table constraint".to_string()));
    }
    let tag = record[pos];
    let mut cursor = pos + 1;
    let constraint = match tag {
        1 => {
            let (columns, new_pos) = decode_ident_list(record, cursor)?;
            cursor = new_pos;
            TableConstraint::PrimaryKey(columns)
        }
        2 => {
            let (columns, new_pos) = decode_ident_list(record, cursor)?;
            cursor = new_pos;
            TableConstraint::Unique(columns)
        }
        3 => {
            let (expr, new_pos) = decode_expr(record, cursor)?;
            cursor = new_pos;
            TableConstraint::Check(expr)
        }
        4 => {
            let (columns, new_pos) = decode_ident_list(record, cursor)?;
            cursor = new_pos;
            let (foreign_table, new_pos) = decode_object_name(record, cursor)?;
            cursor = new_pos;
            let (referred_columns, new_pos) = decode_ident_list(record, cursor)?;
            cursor = new_pos;
            TableConstraint::ForeignKey {
                columns,
                foreign_table,
                referred_columns,
            }
        }
        _ => {
            return Err(StorageError::Corrupt(format!(
                "unknown table constraint tag {}",
                tag
            )))
        }
    };
    Ok((constraint, cursor))
}

fn encode_table_meta(table: &TableMeta) -> Result<Vec<u8>, StorageError> {
    let mut buf = Vec::new();
    if table.name.len() > u16::MAX as usize {
        return Err(StorageError::Invalid("table name too long".to_string()));
    }
    buf.extend_from_slice(&(table.name.len() as u16).to_le_bytes());
    buf.extend_from_slice(table.name.as_bytes());
    if table.columns.len() > u16::MAX as usize {
        return Err(StorageError::Invalid("too many columns".to_string()));
    }
    buf.extend_from_slice(&(table.columns.len() as u16).to_le_bytes());
    for col in &table.columns {
        if col.name.len() > u16::MAX as usize {
            return Err(StorageError::Invalid("column name too long".to_string()));
        }
        buf.extend_from_slice(&(col.name.len() as u16).to_le_bytes());
        buf.extend_from_slice(col.name.as_bytes());
        encode_data_type(&col.data_type, &mut buf)?;
        if col.constraints.len() > u16::MAX as usize {
            return Err(StorageError::Invalid("too many column constraints".to_string()));
        }
        buf.extend_from_slice(&(col.constraints.len() as u16).to_le_bytes());
        for constraint in &col.constraints {
            encode_column_constraint(constraint, &mut buf)?;
        }
    }
    if table.constraints.len() > u16::MAX as usize {
        return Err(StorageError::Invalid("too many table constraints".to_string()));
    }
    buf.extend_from_slice(&(table.constraints.len() as u16).to_le_bytes());
    for constraint in &table.constraints {
        encode_table_constraint(constraint, &mut buf)?;
    }
    buf.extend_from_slice(&table.first_page.to_le_bytes());
    buf.extend_from_slice(&table.last_page.to_le_bytes());
    Ok(buf)
}

fn decode_table_meta(record: &[u8]) -> Result<TableMeta, StorageError> {
    let mut pos = 0;
    if record.len() < 2 {
        return Err(StorageError::Corrupt("invalid table meta".to_string()));
    }
    let name_len = read_u16(record, pos) as usize;
    pos += 2;
    if pos + name_len > record.len() {
        return Err(StorageError::Corrupt("invalid table name".to_string()));
    }
    let name = String::from_utf8_lossy(&record[pos..pos + name_len]).to_string();
    pos += name_len;
    let col_count = read_u16(record, pos) as usize;
    pos += 2;
    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        if pos + 2 > record.len() {
            return Err(StorageError::Corrupt("invalid column name".to_string()));
        }
        let col_len = read_u16(record, pos) as usize;
        pos += 2;
        if pos + col_len > record.len() {
            return Err(StorageError::Corrupt("invalid column name".to_string()));
        }
        let col_name = String::from_utf8_lossy(&record[pos..pos + col_len]).to_string();
        pos += col_len;
        let (data_type, new_pos) = decode_data_type(record, pos)?;
        pos = new_pos;
        if pos + 2 > record.len() {
            return Err(StorageError::Corrupt("invalid column constraints".to_string()));
        }
        let constraint_count = read_u16(record, pos) as usize;
        pos += 2;
        let mut constraints = Vec::with_capacity(constraint_count);
        for _ in 0..constraint_count {
            let (constraint, new_pos) = decode_column_constraint(record, pos)?;
            pos = new_pos;
            constraints.push(constraint);
        }
        columns.push(Column {
            name: col_name,
            data_type,
            constraints,
        });
    }
    if pos + 2 > record.len() {
        return Err(StorageError::Corrupt("invalid table constraints".to_string()));
    }
    let table_constraint_count = read_u16(record, pos) as usize;
    pos += 2;
    let mut constraints = Vec::with_capacity(table_constraint_count);
    for _ in 0..table_constraint_count {
        let (constraint, new_pos) = decode_table_constraint(record, pos)?;
        pos = new_pos;
        constraints.push(constraint);
    }
    if pos + 8 > record.len() {
        return Err(StorageError::Corrupt("invalid table meta".to_string()));
    }
    let first_page = read_u32(record, pos);
    let last_page = read_u32(record, pos + 4);
    Ok(TableMeta {
        name,
        columns,
        constraints,
        first_page,
        last_page,
    })
}

fn decode_table_meta_v0(record: &[u8]) -> Result<TableMeta, StorageError> {
    let mut pos = 0;
    if record.len() < 2 {
        return Err(StorageError::Corrupt("invalid table meta".to_string()));
    }
    let name_len = read_u16(record, pos) as usize;
    pos += 2;
    if pos + name_len > record.len() {
        return Err(StorageError::Corrupt("invalid table name".to_string()));
    }
    let name = String::from_utf8_lossy(&record[pos..pos + name_len]).to_string();
    pos += name_len;
    if pos + 2 > record.len() {
        return Err(StorageError::Corrupt("invalid column count".to_string()));
    }
    let col_count = read_u16(record, pos) as usize;
    pos += 2;
    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        if pos + 2 > record.len() {
            return Err(StorageError::Corrupt("invalid column name".to_string()));
        }
        let col_len = read_u16(record, pos) as usize;
        pos += 2;
        if pos + col_len > record.len() {
            return Err(StorageError::Corrupt("invalid column name".to_string()));
        }
        let col_name = String::from_utf8_lossy(&record[pos..pos + col_len]).to_string();
        pos += col_len;
        let (data_type, new_pos) = decode_data_type(record, pos)?;
        pos = new_pos;
        columns.push(Column {
            name: col_name,
            data_type,
            constraints: Vec::new(),
        });
    }
    if pos + 8 > record.len() {
        return Err(StorageError::Corrupt("invalid table meta".to_string()));
    }
    let first_page = read_u32(record, pos);
    let last_page = read_u32(record, pos + 4);
    Ok(TableMeta {
        name,
        columns,
        constraints: Vec::new(),
        first_page,
        last_page,
    })
}

fn encode_indexed_column(column: &IndexedColumn, buf: &mut Vec<u8>) -> Result<(), StorageError> {
    encode_ident(&column.name, buf)?;
    let tag = match column.order {
        None => 0,
        Some(SortOrder::Asc) => 1,
        Some(SortOrder::Desc) => 2,
    };
    buf.push(tag);
    Ok(())
}

fn decode_indexed_column(record: &[u8], pos: usize) -> Result<(IndexedColumn, usize), StorageError> {
    let (name, mut cursor) = decode_ident(record, pos)?;
    if cursor >= record.len() {
        return Err(StorageError::Corrupt("invalid indexed column".to_string()));
    }
    let order = match record[cursor] {
        0 => None,
        1 => Some(SortOrder::Asc),
        2 => Some(SortOrder::Desc),
        tag => {
            return Err(StorageError::Corrupt(format!(
                "unknown sort order tag {}",
                tag
            )))
        }
    };
    cursor += 1;
    Ok((IndexedColumn { name, order }, cursor))
}

fn encode_index_meta(index: &IndexMeta) -> Result<Vec<u8>, StorageError> {
    let mut buf = Vec::new();
    if index.name.len() > u16::MAX as usize {
        return Err(StorageError::Invalid("index name too long".to_string()));
    }
    buf.extend_from_slice(&(index.name.len() as u16).to_le_bytes());
    buf.extend_from_slice(index.name.as_bytes());
    if index.table.len() > u16::MAX as usize {
        return Err(StorageError::Invalid("table name too long".to_string()));
    }
    buf.extend_from_slice(&(index.table.len() as u16).to_le_bytes());
    buf.extend_from_slice(index.table.as_bytes());
    buf.push(if index.unique { 1 } else { 0 });
    if index.columns.len() > u16::MAX as usize {
        return Err(StorageError::Invalid("too many index columns".to_string()));
    }
    buf.extend_from_slice(&(index.columns.len() as u16).to_le_bytes());
    for column in &index.columns {
        encode_indexed_column(column, &mut buf)?;
    }
    buf.extend_from_slice(&index.first_page.to_le_bytes());
    buf.extend_from_slice(&index.last_page.to_le_bytes());
    Ok(buf)
}

fn decode_index_meta(record: &[u8]) -> Result<IndexMeta, StorageError> {
    let mut pos = 0;
    if record.len() < 2 {
        return Err(StorageError::Corrupt("invalid index meta".to_string()));
    }
    let name_len = read_u16(record, pos) as usize;
    pos += 2;
    if pos + name_len > record.len() {
        return Err(StorageError::Corrupt("invalid index name".to_string()));
    }
    let name = String::from_utf8_lossy(&record[pos..pos + name_len]).to_string();
    pos += name_len;
    if pos + 2 > record.len() {
        return Err(StorageError::Corrupt("invalid index table".to_string()));
    }
    let table_len = read_u16(record, pos) as usize;
    pos += 2;
    if pos + table_len > record.len() {
        return Err(StorageError::Corrupt("invalid index table".to_string()));
    }
    let table = String::from_utf8_lossy(&record[pos..pos + table_len]).to_string();
    pos += table_len;
    if pos >= record.len() {
        return Err(StorageError::Corrupt("invalid index uniqueness".to_string()));
    }
    let unique = record[pos] != 0;
    pos += 1;
    if pos + 2 > record.len() {
        return Err(StorageError::Corrupt("invalid index columns".to_string()));
    }
    let column_count = read_u16(record, pos) as usize;
    pos += 2;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let (column, new_pos) = decode_indexed_column(record, pos)?;
        pos = new_pos;
        columns.push(column);
    }
    if pos + 8 > record.len() {
        return Err(StorageError::Corrupt("invalid index pages".to_string()));
    }
    let first_page = read_u32(record, pos);
    let last_page = read_u32(record, pos + 4);
    Ok(IndexMeta {
        name,
        table,
        columns,
        unique,
        first_page,
        last_page,
    })
}

fn encode_select(select: &Select, buf: &mut Vec<u8>) -> Result<(), StorageError> {
    encode_with_clause(&select.with, buf)?;
    buf.push(if select.distinct { 1 } else { 0 });
    if select.projection.len() > u16::MAX as usize {
        return Err(StorageError::Invalid("too many select items".to_string()));
    }
    buf.extend_from_slice(&(select.projection.len() as u16).to_le_bytes());
    for item in &select.projection {
        encode_select_item(item, buf)?;
    }
    if select.from.len() > u16::MAX as usize {
        return Err(StorageError::Invalid("too many from items".to_string()));
    }
    buf.extend_from_slice(&(select.from.len() as u16).to_le_bytes());
    for table_ref in &select.from {
        encode_table_ref(table_ref, buf)?;
    }
    encode_optional_expr(&select.selection, buf)?;
    if select.group_by.len() > u16::MAX as usize {
        return Err(StorageError::Invalid("too many group by items".to_string()));
    }
    buf.extend_from_slice(&(select.group_by.len() as u16).to_le_bytes());
    for expr in &select.group_by {
        encode_expr(expr, buf)?;
    }
    encode_optional_expr(&select.having, buf)?;
    if select.order_by.len() > u16::MAX as usize {
        return Err(StorageError::Invalid("too many order by items".to_string()));
    }
    buf.extend_from_slice(&(select.order_by.len() as u16).to_le_bytes());
    for order in &select.order_by {
        encode_order_by_expr(order, buf)?;
    }
    encode_optional_expr(&select.limit, buf)?;
    encode_optional_expr(&select.offset, buf)?;
    if select.compounds.len() > u16::MAX as usize {
        return Err(StorageError::Invalid("too many compound selects".to_string()));
    }
    buf.extend_from_slice(&(select.compounds.len() as u16).to_le_bytes());
    for compound in &select.compounds {
        encode_compound_operator(&compound.operator, buf)?;
        encode_select(&compound.select, buf)?;
    }
    Ok(())
}

fn decode_select(record: &[u8], pos: usize) -> Result<(Select, usize), StorageError> {
    let mut cursor = pos;
    if cursor >= record.len() {
        return Err(StorageError::Corrupt("invalid select".to_string()));
    }
    let (with, new_pos) = decode_with_clause(record, cursor)?;
    cursor = new_pos;
    let distinct = record[cursor] != 0;
    cursor += 1;
    if cursor + 2 > record.len() {
        return Err(StorageError::Corrupt("invalid select projection".to_string()));
    }
    let proj_len = read_u16(record, cursor) as usize;
    cursor += 2;
    let mut projection = Vec::with_capacity(proj_len);
    for _ in 0..proj_len {
        let (item, new_pos) = decode_select_item(record, cursor)?;
        cursor = new_pos;
        projection.push(item);
    }
    if cursor + 2 > record.len() {
        return Err(StorageError::Corrupt("invalid select from".to_string()));
    }
    let from_len = read_u16(record, cursor) as usize;
    cursor += 2;
    let mut from = Vec::with_capacity(from_len);
    for _ in 0..from_len {
        let (table_ref, new_pos) = decode_table_ref(record, cursor)?;
        cursor = new_pos;
        from.push(table_ref);
    }
    let (selection, new_pos) = decode_optional_expr(record, cursor)?;
    cursor = new_pos;
    if cursor + 2 > record.len() {
        return Err(StorageError::Corrupt("invalid select group by".to_string()));
    }
    let group_len = read_u16(record, cursor) as usize;
    cursor += 2;
    let mut group_by = Vec::with_capacity(group_len);
    for _ in 0..group_len {
        let (expr, new_pos) = decode_expr(record, cursor)?;
        cursor = new_pos;
        group_by.push(expr);
    }
    let (having, new_pos) = decode_optional_expr(record, cursor)?;
    cursor = new_pos;
    if cursor + 2 > record.len() {
        return Err(StorageError::Corrupt("invalid select order by".to_string()));
    }
    let order_len = read_u16(record, cursor) as usize;
    cursor += 2;
    let mut order_by = Vec::with_capacity(order_len);
    for _ in 0..order_len {
        let (order, new_pos) = decode_order_by_expr(record, cursor)?;
        cursor = new_pos;
        order_by.push(order);
    }
    let (limit, new_pos) = decode_optional_expr(record, cursor)?;
    cursor = new_pos;
    let (offset, new_pos) = decode_optional_expr(record, cursor)?;
    cursor = new_pos;
    let mut compounds = Vec::new();
    if cursor < record.len() {
        if cursor + 2 > record.len() {
            return Err(StorageError::Corrupt("invalid select compounds".to_string()));
        }
        let compound_len = read_u16(record, cursor) as usize;
        cursor += 2;
        for _ in 0..compound_len {
            let (operator, new_pos) = decode_compound_operator(record, cursor)?;
            cursor = new_pos;
            let (select, new_pos) = decode_select(record, cursor)?;
            cursor = new_pos;
            compounds.push(CompoundSelect {
                operator,
                select: Box::new(select),
            });
        }
    }
    Ok((
        Select {
            with,
            distinct,
            projection,
            from,
            selection,
            group_by,
            having,
            order_by,
            limit,
            offset,
            compounds,
        },
        cursor,
    ))
}

fn encode_compound_operator(op: &CompoundOperator, buf: &mut Vec<u8>) -> Result<(), StorageError> {
    let value = match op {
        CompoundOperator::Union => 0u8,
        CompoundOperator::UnionAll => 1u8,
    };
    buf.push(value);
    Ok(())
}

fn decode_compound_operator(
    record: &[u8],
    pos: usize,
) -> Result<(CompoundOperator, usize), StorageError> {
    if pos >= record.len() {
        return Err(StorageError::Corrupt("invalid compound operator".to_string()));
    }
    let op = match record[pos] {
        0 => CompoundOperator::Union,
        1 => CompoundOperator::UnionAll,
        _ => {
            return Err(StorageError::Corrupt(
                "invalid compound operator".to_string(),
            ))
        }
    };
    Ok((op, pos + 1))
}

fn encode_with_clause(with: &Option<With>, buf: &mut Vec<u8>) -> Result<(), StorageError> {
    match with {
        None => {
            buf.push(0);
            Ok(())
        }
        Some(with_clause) => {
            buf.push(1);
            buf.push(if with_clause.recursive { 1 } else { 0 });
            if with_clause.ctes.len() > u16::MAX as usize {
                return Err(StorageError::Invalid("too many CTEs".to_string()));
            }
            buf.extend_from_slice(&(with_clause.ctes.len() as u16).to_le_bytes());
            for cte in &with_clause.ctes {
                encode_ident(&cte.name, buf)?;
                encode_ident_list(&cte.columns, buf)?;
                encode_select(&cte.query, buf)?;
            }
            Ok(())
        }
    }
}

fn decode_with_clause(
    record: &[u8],
    pos: usize,
) -> Result<(Option<With>, usize), StorageError> {
    if pos >= record.len() {
        return Err(StorageError::Corrupt("invalid with clause".to_string()));
    }
    let tag = record[pos];
    let mut cursor = pos + 1;
    if tag == 0 {
        return Ok((None, cursor));
    }
    if cursor >= record.len() {
        return Err(StorageError::Corrupt("invalid with clause".to_string()));
    }
    let recursive = record[cursor] != 0;
    cursor += 1;
    if cursor + 2 > record.len() {
        return Err(StorageError::Corrupt("invalid with clause".to_string()));
    }
    let cte_len = read_u16(record, cursor) as usize;
    cursor += 2;
    let mut ctes = Vec::with_capacity(cte_len);
    for _ in 0..cte_len {
        let (name, new_pos) = decode_ident(record, cursor)?;
        cursor = new_pos;
        let (columns, new_pos) = decode_ident_list(record, cursor)?;
        cursor = new_pos;
        let (query, new_pos) = decode_select(record, cursor)?;
        cursor = new_pos;
        ctes.push(Cte {
            name,
            columns,
            query: Box::new(query),
        });
    }
    Ok((Some(With { recursive, ctes }), cursor))
}

fn encode_select_item(item: &SelectItem, buf: &mut Vec<u8>) -> Result<(), StorageError> {
    match item {
        SelectItem::Expr { expr, alias } => {
            buf.push(1);
            encode_expr(expr, buf)?;
            encode_optional_ident(alias, buf)?;
        }
        SelectItem::Wildcard => {
            buf.push(2);
        }
        SelectItem::QualifiedWildcard(name) => {
            buf.push(3);
            encode_object_name(name, buf)?;
        }
    }
    Ok(())
}

fn decode_select_item(record: &[u8], pos: usize) -> Result<(SelectItem, usize), StorageError> {
    if pos >= record.len() {
        return Err(StorageError::Corrupt("invalid select item".to_string()));
    }
    let tag = record[pos];
    let mut cursor = pos + 1;
    let item = match tag {
        1 => {
            let (expr, new_pos) = decode_expr(record, cursor)?;
            cursor = new_pos;
            let (alias, new_pos) = decode_optional_ident(record, cursor)?;
            cursor = new_pos;
            SelectItem::Expr { expr, alias }
        }
        2 => SelectItem::Wildcard,
        3 => {
            let (name, new_pos) = decode_object_name(record, cursor)?;
            cursor = new_pos;
            SelectItem::QualifiedWildcard(name)
        }
        _ => {
            return Err(StorageError::Corrupt(format!(
                "unknown select item tag {}",
                tag
            )))
        }
    };
    Ok((item, cursor))
}

fn encode_table_ref(table_ref: &TableRef, buf: &mut Vec<u8>) -> Result<(), StorageError> {
    match table_ref {
        TableRef::Named { name, alias } => {
            buf.push(1);
            encode_object_name(name, buf)?;
            encode_optional_ident(alias, buf)?;
        }
        _ => {
            return Err(StorageError::Invalid(
                "unsupported table reference in view".to_string(),
            ))
        }
    }
    Ok(())
}

fn decode_table_ref(record: &[u8], pos: usize) -> Result<(TableRef, usize), StorageError> {
    if pos >= record.len() {
        return Err(StorageError::Corrupt("invalid table ref".to_string()));
    }
    let tag = record[pos];
    let mut cursor = pos + 1;
    let table_ref = match tag {
        1 => {
            let (name, new_pos) = decode_object_name(record, cursor)?;
            cursor = new_pos;
            let (alias, new_pos) = decode_optional_ident(record, cursor)?;
            cursor = new_pos;
            TableRef::Named { name, alias }
        }
        _ => {
            return Err(StorageError::Corrupt(format!(
                "unknown table ref tag {}",
                tag
            )))
        }
    };
    Ok((table_ref, cursor))
}

fn encode_order_by_expr(order: &OrderByExpr, buf: &mut Vec<u8>) -> Result<(), StorageError> {
    encode_expr(&order.expr, buf)?;
    let tag = match (order.asc, order.nulls.as_ref()) {
        (None, None) => 0,
        (Some(true), None) => 1,
        (Some(false), None) => 2,
        (None, Some(NullsOrder::First)) => 3,
        (None, Some(NullsOrder::Last)) => 4,
        (Some(true), Some(NullsOrder::First)) => 5,
        (Some(true), Some(NullsOrder::Last)) => 6,
        (Some(false), Some(NullsOrder::First)) => 7,
        (Some(false), Some(NullsOrder::Last)) => 8,
    };
    buf.push(tag);
    Ok(())
}

fn decode_order_by_expr(record: &[u8], pos: usize) -> Result<(OrderByExpr, usize), StorageError> {
    let (expr, mut cursor) = decode_expr(record, pos)?;
    if cursor >= record.len() {
        return Err(StorageError::Corrupt("invalid order by expr".to_string()));
    }
    let (asc, nulls) = match record[cursor] {
        0 => (None, None),
        1 => (Some(true), None),
        2 => (Some(false), None),
        3 => (None, Some(NullsOrder::First)),
        4 => (None, Some(NullsOrder::Last)),
        5 => (Some(true), Some(NullsOrder::First)),
        6 => (Some(true), Some(NullsOrder::Last)),
        7 => (Some(false), Some(NullsOrder::First)),
        8 => (Some(false), Some(NullsOrder::Last)),
        tag => {
            return Err(StorageError::Corrupt(format!(
                "unknown order by tag {}",
                tag
            )))
        }
    };
    cursor += 1;
    Ok((OrderByExpr { expr, asc, nulls }, cursor))
}

fn encode_optional_expr(expr: &Option<Expr>, buf: &mut Vec<u8>) -> Result<(), StorageError> {
    match expr {
        Some(expr) => {
            buf.push(1);
            encode_expr(expr, buf)?;
        }
        None => buf.push(0),
    }
    Ok(())
}

fn decode_optional_expr(record: &[u8], pos: usize) -> Result<(Option<Expr>, usize), StorageError> {
    if pos >= record.len() {
        return Err(StorageError::Corrupt("invalid optional expr".to_string()));
    }
    match record[pos] {
        0 => Ok((None, pos + 1)),
        1 => {
            let (expr, new_pos) = decode_expr(record, pos + 1)?;
            Ok((Some(expr), new_pos))
        }
        tag => Err(StorageError::Corrupt(format!(
            "unknown optional expr tag {}",
            tag
        ))),
    }
}

fn encode_optional_ident(ident: &Option<Ident>, buf: &mut Vec<u8>) -> Result<(), StorageError> {
    match ident {
        Some(ident) => {
            buf.push(1);
            encode_ident(ident, buf)?;
        }
        None => buf.push(0),
    }
    Ok(())
}

fn decode_optional_ident(record: &[u8], pos: usize) -> Result<(Option<Ident>, usize), StorageError> {
    if pos >= record.len() {
        return Err(StorageError::Corrupt("invalid optional ident".to_string()));
    }
    match record[pos] {
        0 => Ok((None, pos + 1)),
        1 => {
            let (ident, new_pos) = decode_ident(record, pos + 1)?;
            Ok((Some(ident), new_pos))
        }
        tag => Err(StorageError::Corrupt(format!(
            "unknown optional ident tag {}",
            tag
        ))),
    }
}

fn encode_view_meta(view: &ViewMeta) -> Result<Vec<u8>, StorageError> {
    let mut buf = Vec::new();
    if view.name.len() > u16::MAX as usize {
        return Err(StorageError::Invalid("view name too long".to_string()));
    }
    buf.extend_from_slice(&(view.name.len() as u16).to_le_bytes());
    buf.extend_from_slice(view.name.as_bytes());
    if view.columns.len() > u16::MAX as usize {
        return Err(StorageError::Invalid("too many view columns".to_string()));
    }
    buf.extend_from_slice(&(view.columns.len() as u16).to_le_bytes());
    for column in &view.columns {
        encode_ident(column, &mut buf)?;
    }
    let mut select_buf = Vec::new();
    encode_select(&view.query, &mut select_buf)?;
    if select_buf.len() > u32::MAX as usize {
        return Err(StorageError::Invalid("view query too large".to_string()));
    }
    buf.extend_from_slice(&(select_buf.len() as u32).to_le_bytes());
    buf.extend_from_slice(&select_buf);
    Ok(buf)
}

fn decode_view_meta(record: &[u8]) -> Result<ViewMeta, StorageError> {
    let mut pos = 0;
    if pos + 2 > record.len() {
        return Err(StorageError::Corrupt("invalid view meta".to_string()));
    }
    let name_len = read_u16(record, pos) as usize;
    pos += 2;
    if pos + name_len > record.len() {
        return Err(StorageError::Corrupt("invalid view name".to_string()));
    }
    let name = String::from_utf8_lossy(&record[pos..pos + name_len]).to_string();
    pos += name_len;
    if pos + 2 > record.len() {
        return Err(StorageError::Corrupt("invalid view columns".to_string()));
    }
    let col_count = read_u16(record, pos) as usize;
    pos += 2;
    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let (ident, new_pos) = decode_ident(record, pos)?;
        pos = new_pos;
        columns.push(ident);
    }
    if pos + 4 > record.len() {
        return Err(StorageError::Corrupt("invalid view query length".to_string()));
    }
    let query_len = read_u32(record, pos) as usize;
    pos += 4;
    if pos + query_len > record.len() {
        return Err(StorageError::Corrupt("invalid view query".to_string()));
    }
    let (query, _) = decode_select(&record[pos..pos + query_len], 0)?;
    Ok(ViewMeta { name, columns, query })
}

fn encode_data_type(data_type: &DataType, buf: &mut Vec<u8>) -> Result<(), StorageError> {
    match data_type {
        DataType::Integer => buf.push(1),
        DataType::Real => buf.push(2),
        DataType::Text => buf.push(3),
        DataType::Blob => buf.push(4),
        DataType::Numeric => buf.push(5),
        DataType::Custom(name) => {
            buf.push(255);
            if name.len() > u16::MAX as usize {
                return Err(StorageError::Invalid("custom type too long".to_string()));
            }
            buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
            buf.extend_from_slice(name.as_bytes());
        }
    }
    Ok(())
}

fn decode_data_type(record: &[u8], pos: usize) -> Result<(DataType, usize), StorageError> {
    if pos >= record.len() {
        return Err(StorageError::Corrupt("invalid type".to_string()));
    }
    let tag = record[pos];
    let mut cursor = pos + 1;
    let data_type = match tag {
        1 => DataType::Integer,
        2 => DataType::Real,
        3 => DataType::Text,
        4 => DataType::Blob,
        5 => DataType::Numeric,
        255 => {
            let len = read_u16(record, cursor) as usize;
            cursor += 2;
            if cursor + len > record.len() {
                return Err(StorageError::Corrupt("invalid custom type".to_string()));
            }
            let name = String::from_utf8_lossy(&record[cursor..cursor + len]).to_string();
            cursor += len;
            DataType::Custom(name)
        }
        _ => {
            return Err(StorageError::Corrupt(format!(
                "unknown type tag {}",
                tag
            )))
        }
    };
    Ok((data_type, cursor))
}

fn read_u16(buf: &[u8], offset: usize) -> u16 {
    let mut bytes = [0u8; 2];
    bytes.copy_from_slice(&buf[offset..offset + 2]);
    u16::from_le_bytes(bytes)
}

fn read_u32(buf: &[u8], offset: usize) -> u32 {
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&buf[offset..offset + 4]);
    u32::from_le_bytes(bytes)
}

fn write_u16(buf: &mut [u8], offset: usize, value: u16) {
    buf[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn write_u32(buf: &mut [u8], offset: usize, value: u32) {
    buf[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}
