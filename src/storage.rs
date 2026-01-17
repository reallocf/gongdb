use crate::ast::{
    BinaryOperator, ColumnConstraint, DataType, Expr, Ident, IndexedColumn, Literal, ObjectName,
    SortOrder, TableConstraint, UnaryOperator,
};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

const PAGE_SIZE: usize = 4096;
const HEADER_PAGE_ID: u32 = 0;
const CATALOG_PAGE_ID: u32 = 1;
const FILE_MAGIC: [u8; 8] = *b"GONGDB1\0";
const CATALOG_FORMAT_VERSION: u32 = 2;
const HEADER_PAGE_SIZE_OFFSET: usize = 8;
const HEADER_NEXT_PAGE_ID_OFFSET: usize = 12;
const HEADER_CATALOG_PAGE_ID_OFFSET: usize = 16;
const HEADER_SCHEMA_VERSION_OFFSET: usize = 20;
const HEADER_CATALOG_FORMAT_OFFSET: usize = 24;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RowLocation {
    page_id: u32,
    slot: u16,
}

#[derive(Debug, Clone, PartialEq)]
struct IndexEntry {
    key: Vec<Value>,
    row: RowLocation,
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

pub struct StorageEngine {
    mode: StorageMode,
    next_page_id: u32,
    schema_version: u32,
    catalog_format_version: u32,
    tables: HashMap<String, TableMeta>,
    indexes: HashMap<String, IndexMeta>,
}

impl StorageEngine {
    pub fn new_in_memory() -> Result<Self, StorageError> {
        let mut pages = Vec::new();
        pages.push(vec![0; PAGE_SIZE]);
        pages.push(init_data_page());
        let mut engine = StorageEngine {
            mode: StorageMode::InMemory { pages },
            next_page_id: CATALOG_PAGE_ID + 1,
            schema_version: 0,
            catalog_format_version: CATALOG_FORMAT_VERSION,
            tables: HashMap::new(),
            indexes: HashMap::new(),
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
        let (next_page_id, schema_version, catalog_format_version, tables, indexes) =
            if header.starts_with(&FILE_MAGIC) {
                let next_page_id = read_u32(&header, HEADER_NEXT_PAGE_ID_OFFSET);
                let schema_version = read_u32(&header, HEADER_SCHEMA_VERSION_OFFSET);
                let loaded_format_version = read_u32(&header, HEADER_CATALOG_FORMAT_OFFSET);
                let (tables, indexes) =
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
                )
        } else {
            let tables = HashMap::new();
            let indexes = HashMap::new();
            (
                CATALOG_PAGE_ID + 1,
                0,
                CATALOG_FORMAT_VERSION,
                tables,
                indexes,
            )
        };

        let mut engine = StorageEngine {
            mode: StorageMode::OnDisk { file },
            next_page_id,
            schema_version,
            catalog_format_version,
            tables,
            indexes,
        };
        engine.write_header()?;
        engine.write_catalog()?;
        Ok(engine)
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
        if self.tables.remove(name).is_none() {
            return Err(StorageError::NotFound(format!("table not found: {}", name)));
        }
        self.indexes.retain(|_, index| index.table != name);
        self.write_catalog()?;
        self.bump_schema_version()
    }

    pub fn get_table(&self, name: &str) -> Option<&TableMeta> {
        self.tables.get(name)
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
        if self.indexes.remove(name).is_none() {
            return Err(StorageError::NotFound(format!("index not found: {}", name)));
        }
        self.write_catalog()?;
        self.bump_schema_version()
    }

    pub fn scan_table(&self, table_name: &str) -> Result<Vec<Vec<Value>>, StorageError> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| StorageError::NotFound(format!("table not found: {}", table_name)))?;
        let mut rows = Vec::new();
        let mut page_id = table.first_page;
        loop {
            let page = self.read_page(page_id)?;
            let records = read_records(&page);
            for record in records {
                rows.push(decode_row(&record)?);
            }
            let next = get_next_page_id(&page);
            if next == 0 {
                break;
            }
            page_id = next;
        }
        Ok(rows)
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

    fn insert_index_record(
        &mut self,
        index_name: &str,
        entry: &IndexEntry,
    ) -> Result<(), StorageError> {
        let mut index = self
            .indexes
            .remove(index_name)
            .ok_or_else(|| StorageError::NotFound(format!("index not found: {}", index_name)))?;
        self.insert_index_record_into_pages(&mut index, entry)?;
        self.indexes.insert(index_name.to_string(), index);
        Ok(())
    }

    fn insert_index_record_into_pages(
        &mut self,
        index: &mut IndexMeta,
        entry: &IndexEntry,
    ) -> Result<(), StorageError> {
        let record = encode_index_entry(entry)?;
        let mut page_id = index.last_page;
        let mut page = self.read_page(page_id)?;
        if !has_space_for_record(&page, record.len()) {
            let new_page_id = self.allocate_data_page()?;
            let new_page = init_data_page();
            set_next_page_id(&mut page, new_page_id);
            self.write_page(page_id, &page)?;
            page_id = new_page_id;
            page = new_page;
            index.last_page = new_page_id;
        }
        let _ = insert_record(&mut page, &record)?;
        self.write_page(page_id, &page)?;
        Ok(())
    }

    fn index_contains_key(&self, index: &IndexMeta, key: &[Value]) -> Result<bool, StorageError> {
        let mut page_id = index.first_page;
        loop {
            let page = self.read_page(page_id)?;
            let records = read_records(&page);
            for record in records {
                let entry = decode_index_entry(&record)?;
                if entry.key == key {
                    return Ok(true);
                }
            }
            let next = get_next_page_id(&page);
            if next == 0 {
                break;
            }
            page_id = next;
        }
        Ok(false)
    }

    pub fn allocate_data_page(&mut self) -> Result<u32, StorageError> {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        let page = init_data_page();
        self.write_page(page_id, &page)?;
        self.write_header()?;
        Ok(page_id)
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
) -> Result<(HashMap<String, TableMeta>, HashMap<String, IndexMeta>), StorageError> {
    let mut page = vec![0; PAGE_SIZE];
    file.seek(SeekFrom::Start(CATALOG_PAGE_ID as u64 * PAGE_SIZE as u64))?;
    file.read_exact(&mut page)?;
    let mut tables = HashMap::new();
    let mut indexes = HashMap::new();
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
    Ok((tables, indexes))
}

fn init_data_page() -> Vec<u8> {
    let mut page = vec![0; PAGE_SIZE];
    page[0] = 1;
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
