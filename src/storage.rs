use crate::ast::DataType;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

const PAGE_SIZE: usize = 4096;
const HEADER_PAGE_ID: u32 = 0;
const CATALOG_PAGE_ID: u32 = 1;
const FILE_MAGIC: [u8; 8] = *b"GONGDB1\0";

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone)]
pub struct TableMeta {
    pub name: String,
    pub columns: Vec<Column>,
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
    tables: HashMap<String, TableMeta>,
}

impl StorageEngine {
    pub fn new_in_memory() -> Result<Self, StorageError> {
        let mut pages = Vec::new();
        pages.push(vec![0; PAGE_SIZE]);
        pages.push(init_data_page());
        let mut engine = StorageEngine {
            mode: StorageMode::InMemory { pages },
            next_page_id: CATALOG_PAGE_ID + 1,
            tables: HashMap::new(),
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
        let (next_page_id, tables) = if header.starts_with(&FILE_MAGIC) {
            let next_page_id = read_u32(&header, 12);
            let tables = load_catalog_from_file(&mut file, next_page_id)?;
            (next_page_id, tables)
        } else {
            let tables = HashMap::new();
            (CATALOG_PAGE_ID + 1, tables)
        };

        let mut engine = StorageEngine {
            mode: StorageMode::OnDisk { file },
            next_page_id,
            tables,
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
        self.write_catalog()
    }

    pub fn drop_table(&mut self, name: &str) -> Result<(), StorageError> {
        if self.tables.remove(name).is_none() {
            return Err(StorageError::NotFound(format!("table not found: {}", name)));
        }
        self.write_catalog()
    }

    pub fn get_table(&self, name: &str) -> Option<&TableMeta> {
        self.tables.get(name)
    }

    pub fn insert_row(&mut self, table_name: &str, row: &[Value]) -> Result<(), StorageError> {
        let mut page_id = self
            .tables
            .get(table_name)
            .ok_or_else(|| StorageError::NotFound(format!("table not found: {}", table_name)))?
            .last_page;
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

        insert_record(&mut page, &record)?;
        self.write_page(page_id, &page)?;
        self.write_catalog()?;
        Ok(())
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
        write_u32(&mut header, 8, PAGE_SIZE as u32);
        write_u32(&mut header, 12, self.next_page_id);
        write_u32(&mut header, 16, CATALOG_PAGE_ID);
        self.write_page(HEADER_PAGE_ID, &header)
    }

    fn write_catalog(&mut self) -> Result<(), StorageError> {
        let mut page = init_data_page();
        for table in self.tables.values() {
            let encoded = encode_table_meta(table)?;
            insert_record(&mut page, &encoded)?;
        }
        self.write_page(CATALOG_PAGE_ID, &page)
    }
}

fn load_catalog_from_file(
    file: &mut File,
    next_page_id: u32,
) -> Result<HashMap<String, TableMeta>, StorageError> {
    let mut page = vec![0; PAGE_SIZE];
    file.seek(SeekFrom::Start(CATALOG_PAGE_ID as u64 * PAGE_SIZE as u64))?;
    file.read_exact(&mut page)?;
    let mut tables = HashMap::new();
    for record in read_records(&page) {
        let table = decode_table_meta(&record)?;
        tables.insert(table.name.clone(), table);
    }
    if next_page_id == 0 {
        return Err(StorageError::Corrupt("invalid next page id".to_string()));
    }
    Ok(tables)
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

fn insert_record(page: &mut [u8], record: &[u8]) -> Result<(), StorageError> {
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
    Ok(())
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
        _ => {
            return Err(StorageError::Corrupt(format!(
                "unknown value tag {}",
                tag
            )))
        }
    };
    Ok((value, cursor))
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
    }
    buf.extend_from_slice(&table.first_page.to_le_bytes());
    buf.extend_from_slice(&table.last_page.to_le_bytes());
    Ok(buf)
}

fn decode_table_meta(record: &[u8]) -> Result<TableMeta, StorageError> {
    let mut pos = 0;
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
