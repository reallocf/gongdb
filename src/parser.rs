use crate::ast::*;

#[derive(Debug, Clone, PartialEq)]
pub struct ParserError {
    pub message: String,
}

impl ParserError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum TokenKind {
    Keyword(String),
    Ident(String, bool),
    Number(String),
    String(String),
    Operator(String),
    Symbol(char),
    Eof,
}

#[derive(Debug, Clone, PartialEq)]
struct Token {
    kind: TokenKind,
}

struct Lexer<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn next_token(&mut self) -> Result<Token, ParserError> {
        self.skip_ws_and_comments();
        if self.pos >= self.input.len() {
            return Ok(Token { kind: TokenKind::Eof });
        }
        let rest = &self.input[self.pos..];
        let mut chars = rest.chars();
        let ch = chars.next().unwrap();

        if ch.is_ascii_alphabetic() || ch == '_' {
            let start = self.pos;
            self.pos += ch.len_utf8();
            while self.pos < self.input.len() {
                let c = self.input[self.pos..].chars().next().unwrap();
                if c.is_ascii_alphanumeric() || c == '_' {
                    self.pos += c.len_utf8();
                } else {
                    break;
                }
            }
            let word = &self.input[start..self.pos];
            let upper = word.to_ascii_uppercase();
            if is_keyword(&upper) {
                return Ok(Token {
                    kind: TokenKind::Keyword(upper),
                });
            }
            return Ok(Token {
                kind: TokenKind::Ident(word.to_string(), false),
            });
        }

        if ch.is_ascii_digit() {
            let start = self.pos;
            self.pos += 1;
            let mut saw_dot = false;
            while self.pos < self.input.len() {
                let c = self.input[self.pos..].chars().next().unwrap();
                if c.is_ascii_digit() {
                    self.pos += 1;
                } else if c == '.' && !saw_dot {
                    saw_dot = true;
                    self.pos += 1;
                } else {
                    break;
                }
            }
            let num = &self.input[start..self.pos];
            return Ok(Token {
                kind: TokenKind::Number(num.to_string()),
            });
        }

        match ch {
            '\'' => return self.lex_string(),
            '"' => return self.lex_quoted_ident('"', '"'),
            '`' => return self.lex_quoted_ident('`', '`'),
            '[' => return self.lex_quoted_ident('[', ']'),
            _ => {}
        }

        let two = if self.pos + 2 <= self.input.len() {
            &self.input[self.pos..self.pos + 2]
        } else {
            ""
        };
        let two_op = match two {
            "||" | ">=" | "<=" | "<>" | "!=" | "==" => Some(two),
            _ => None,
        };
        if let Some(op) = two_op {
            self.pos += 2;
            return Ok(Token {
                kind: TokenKind::Operator(op.to_string()),
            });
        }

        self.pos += ch.len_utf8();
        let token = match ch {
            '(' | ')' | ',' | '.' | ';' => TokenKind::Symbol(ch),
            '*' => TokenKind::Operator(ch.to_string()),
            '+' | '-' | '/' | '%' | '=' | '<' | '>' => {
                TokenKind::Operator(ch.to_string())
            }
            _ => {
                return Err(ParserError::new(format!(
                    "unexpected character '{}'",
                    ch
                )))
            }
        };
        Ok(Token { kind: token })
    }

    fn lex_string(&mut self) -> Result<Token, ParserError> {
        self.pos += 1;
        let mut value = String::new();
        while self.pos < self.input.len() {
            let c = self.input[self.pos..].chars().next().unwrap();
            if c == '\'' {
                if self.pos + 1 < self.input.len()
                    && self.input[self.pos + 1..].starts_with('\'')
                {
                    value.push('\'');
                    self.pos += 2;
                } else {
                    self.pos += 1;
                    return Ok(Token {
                        kind: TokenKind::String(value),
                    });
                }
            } else {
                value.push(c);
                self.pos += c.len_utf8();
            }
        }
        Err(ParserError::new("unterminated string literal"))
    }

    fn lex_quoted_ident(&mut self, open: char, close: char) -> Result<Token, ParserError> {
        self.pos += open.len_utf8();
        let mut value = String::new();
        while self.pos < self.input.len() {
            let c = self.input[self.pos..].chars().next().unwrap();
            if c == close {
                if self.pos + 1 < self.input.len()
                    && self.input[self.pos + 1..].starts_with(close)
                    && open == close
                {
                    value.push(close);
                    self.pos += 2;
                } else {
                    self.pos += close.len_utf8();
                    return Ok(Token {
                        kind: TokenKind::Ident(value, true),
                    });
                }
            } else {
                value.push(c);
                self.pos += c.len_utf8();
            }
        }
        Err(ParserError::new("unterminated quoted identifier"))
    }

    fn skip_ws_and_comments(&mut self) {
        while self.pos < self.input.len() {
            let rest = &self.input[self.pos..];
            if rest.starts_with("--") {
                if let Some(idx) = rest.find('\n') {
                    self.pos += idx + 1;
                } else {
                    self.pos = self.input.len();
                }
                continue;
            }
            if rest.starts_with("/*") {
                if let Some(idx) = rest.find("*/") {
                    self.pos += idx + 2;
                } else {
                    self.pos = self.input.len();
                }
                continue;
            }
            let c = rest.chars().next().unwrap();
            if c.is_whitespace() {
                self.pos += c.len_utf8();
                continue;
            }
            break;
        }
    }
}

fn is_keyword(word: &str) -> bool {
    matches!(
        word,
        "SELECT"
            | "FROM"
            | "WHERE"
            | "GROUP"
            | "BY"
            | "HAVING"
            | "ORDER"
            | "LIMIT"
            | "OFFSET"
            | "INSERT"
            | "INTO"
            | "VALUES"
            | "UPDATE"
            | "SET"
            | "DELETE"
            | "CREATE"
            | "TABLE"
            | "DROP"
            | "INDEX"
            | "VIEW"
            | "AS"
            | "DISTINCT"
            | "AND"
            | "OR"
            | "NOT"
            | "NULL"
            | "PRIMARY"
            | "KEY"
            | "UNIQUE"
            | "CHECK"
            | "FOREIGN"
            | "REFERENCES"
            | "DEFAULT"
            | "EXISTS"
            | "IN"
            | "IS"
            | "LIKE"
            | "BETWEEN"
            | "CASE"
            | "WHEN"
            | "THEN"
            | "ELSE"
            | "END"
            | "JOIN"
            | "LEFT"
            | "RIGHT"
            | "FULL"
            | "CROSS"
            | "INNER"
            | "OUTER"
            | "ON"
            | "USING"
            | "ASC"
            | "DESC"
            | "IF"
            | "WITHOUT"
            | "ROWID"
            | "UNION"
            | "ALL"
            | "CAST"
            | "INTEGER"
            | "TEXT"
            | "REAL"
            | "BLOB"
            | "NUMERIC"
            | "REPLACE"
    )
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn current(&self) -> &TokenKind {
        &self.tokens[self.pos].kind
    }

    fn advance(&mut self) {
        if self.pos < self.tokens.len() - 1 {
            self.pos += 1;
        }
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<(), ParserError> {
        match self.current() {
            TokenKind::Keyword(k) if k == keyword => {
                self.advance();
                Ok(())
            }
            _ => Err(ParserError::new(format!(
                "expected keyword {}",
                keyword
            ))),
        }
    }

    fn eat_keyword(&mut self, keyword: &str) -> bool {
        match self.current() {
            TokenKind::Keyword(k) if k == keyword => {
                self.advance();
                true
            }
            _ => false,
        }
    }

    fn eat_symbol(&mut self, symbol: char) -> bool {
        match self.current() {
            TokenKind::Symbol(c) if *c == symbol => {
                self.advance();
                true
            }
            _ => false,
        }
    }

    fn expect_symbol(&mut self, symbol: char) -> Result<(), ParserError> {
        if self.eat_symbol(symbol) {
            Ok(())
        } else {
            Err(ParserError::new(format!(
                "expected symbol {}",
                symbol
            )))
        }
    }

    fn eat_operator(&mut self, op: &str) -> bool {
        match self.current() {
            TokenKind::Operator(o) if o == op => {
                self.advance();
                true
            }
            _ => false,
        }
    }

    fn expect_operator(&mut self, op: &str) -> Result<(), ParserError> {
        if self.eat_operator(op) {
            Ok(())
        } else {
            Err(ParserError::new(format!("expected operator {}", op)))
        }
    }

    fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        match self.current() {
            TokenKind::Keyword(k) if k == "SELECT" => Ok(Statement::Select(self.parse_select()?)),
            TokenKind::Keyword(k) if k == "INSERT" => Ok(Statement::Insert(self.parse_insert()?)),
            TokenKind::Keyword(k) if k == "UPDATE" => Ok(Statement::Update(self.parse_update()?)),
            TokenKind::Keyword(k) if k == "DELETE" => Ok(Statement::Delete(self.parse_delete()?)),
            TokenKind::Keyword(k) if k == "CREATE" => self.parse_create(),
            TokenKind::Keyword(k) if k == "DROP" => self.parse_drop(),
            _ => Err(ParserError::new("unexpected statement")),
        }
    }

    fn parse_create(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword("CREATE")?;
        let unique = self.eat_keyword("UNIQUE");
        let _temp = self.eat_keyword("TEMP") || self.eat_keyword("TEMPORARY");
        if self.eat_keyword("TABLE") {
            let if_not_exists = self.eat_keyword("IF");
            if if_not_exists {
                self.expect_keyword("NOT")?;
                self.expect_keyword("EXISTS")?;
            }
            let name = self.parse_object_name()?;
            self.expect_symbol('(')?;
            let mut columns = Vec::new();
            let mut constraints = Vec::new();
            loop {
                if self.eat_symbol(')') {
                    break;
                }
                if self.is_table_constraint_start() {
                    constraints.push(self.parse_table_constraint()?);
                } else {
                    columns.push(self.parse_column_def()?);
                }
                if self.eat_symbol(')') {
                    break;
                }
                self.expect_symbol(',')?;
            }
            let without_rowid = if self.eat_keyword("WITHOUT") {
                self.expect_keyword("ROWID")?;
                true
            } else {
                false
            };
            return Ok(Statement::CreateTable(CreateTable {
                if_not_exists,
                name,
                columns,
                constraints,
                without_rowid,
            }));
        }
        if self.eat_keyword("INDEX") {
            let if_not_exists = self.eat_keyword("IF");
            if if_not_exists {
                self.expect_keyword("NOT")?;
                self.expect_keyword("EXISTS")?;
            }
            let name = self.parse_object_name()?;
            self.expect_keyword("ON")?;
            let table = self.parse_object_name()?;
            self.expect_symbol('(')?;
            let mut columns = Vec::new();
            loop {
                let name = self.parse_ident()?;
                let order = if self.eat_keyword("ASC") {
                    Some(SortOrder::Asc)
                } else if self.eat_keyword("DESC") {
                    Some(SortOrder::Desc)
                } else {
                    None
                };
                columns.push(IndexedColumn { name, order });
                if self.eat_symbol(')') {
                    break;
                }
                self.expect_symbol(',')?;
            }
            return Ok(Statement::CreateIndex(CreateIndex {
                if_not_exists,
                name,
                table,
                columns,
                unique,
            }));
        }
        if self.eat_keyword("VIEW") {
            let if_not_exists = self.eat_keyword("IF");
            if if_not_exists {
                self.expect_keyword("NOT")?;
                self.expect_keyword("EXISTS")?;
            }
            let name = self.parse_object_name()?;
            let columns = if self.eat_symbol('(') {
                let cols = self.parse_ident_list(')')?;
                cols
            } else {
                Vec::new()
            };
            self.expect_keyword("AS")?;
            let query = self.parse_select()?;
            return Ok(Statement::CreateView(CreateView {
                if_not_exists,
                name,
                columns,
                query,
            }));
        }
        Err(ParserError::new("unsupported CREATE statement"))
    }

    fn parse_drop(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword("DROP")?;
        if self.eat_keyword("TABLE") {
            let if_exists = self.eat_keyword("IF");
            if if_exists {
                self.expect_keyword("EXISTS")?;
            }
            let name = self.parse_object_name()?;
            return Ok(Statement::DropTable(DropTable { if_exists, name }));
        }
        if self.eat_keyword("INDEX") {
            let if_exists = self.eat_keyword("IF");
            if if_exists {
                self.expect_keyword("EXISTS")?;
            }
            let name = self.parse_object_name()?;
            return Ok(Statement::DropIndex(DropIndex { if_exists, name }));
        }
        if self.eat_keyword("VIEW") {
            let if_exists = self.eat_keyword("IF");
            if if_exists {
                self.expect_keyword("EXISTS")?;
            }
            let name = self.parse_object_name()?;
            return Ok(Statement::DropView(DropView { if_exists, name }));
        }
        Err(ParserError::new("unsupported DROP statement"))
    }

    fn parse_insert(&mut self) -> Result<Insert, ParserError> {
        self.expect_keyword("INSERT")?;
        self.eat_keyword("INTO");
        let table = self.parse_object_name()?;
        let columns = if self.eat_symbol('(') {
            self.parse_ident_list(')')?
        } else {
            Vec::new()
        };
        if self.eat_keyword("VALUES") {
            let mut rows = Vec::new();
            loop {
                self.expect_symbol('(')?;
                let mut values = Vec::new();
                if !self.eat_symbol(')') {
                    loop {
                        values.push(self.parse_expr()?);
                        if self.eat_symbol(')') {
                            break;
                        }
                        self.expect_symbol(',')?;
                    }
                }
                rows.push(values);
                if !self.eat_symbol(',') {
                    break;
                }
            }
            Ok(Insert {
                table,
                columns,
                source: InsertSource::Values(rows),
            })
        } else {
            let select = self.parse_select()?;
            Ok(Insert {
                table,
                columns,
                source: InsertSource::Select(Box::new(select)),
            })
        }
    }

    fn parse_update(&mut self) -> Result<Update, ParserError> {
        self.expect_keyword("UPDATE")?;
        let table = self.parse_object_name()?;
        self.expect_keyword("SET")?;
        let mut assignments = Vec::new();
        loop {
            let column = self.parse_ident()?;
            self.expect_operator("=")?;
            let value = self.parse_expr()?;
            assignments.push(Assignment { column, value });
            if !self.eat_symbol(',') {
                break;
            }
        }
        let selection = if self.eat_keyword("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Update {
            table,
            assignments,
            selection,
        })
    }

    fn parse_delete(&mut self) -> Result<Delete, ParserError> {
        self.expect_keyword("DELETE")?;
        self.expect_keyword("FROM")?;
        let table = self.parse_object_name()?;
        let selection = if self.eat_keyword("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Delete { table, selection })
    }

    fn parse_select(&mut self) -> Result<Select, ParserError> {
        self.expect_keyword("SELECT")?;
        let distinct = self.eat_keyword("DISTINCT");
        let projection = self.parse_select_list()?;
        let from = if self.eat_keyword("FROM") {
            self.parse_table_refs()?
        } else {
            Vec::new()
        };
        let selection = if self.eat_keyword("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let group_by = if self.eat_keyword("GROUP") {
            self.expect_keyword("BY")?;
            self.parse_expr_list()?
        } else {
            Vec::new()
        };
        let having = if self.eat_keyword("HAVING") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let order_by = if self.eat_keyword("ORDER") {
            self.expect_keyword("BY")?;
            self.parse_order_by_list()?
        } else {
            Vec::new()
        };
        let mut limit = None;
        let mut offset = None;
        if self.eat_keyword("LIMIT") {
            let first = self.parse_expr()?;
            if self.eat_keyword("OFFSET") {
                offset = Some(self.parse_expr()?);
                limit = Some(first);
            } else if self.eat_symbol(',') {
                let second = self.parse_expr()?;
                offset = Some(first);
                limit = Some(second);
            } else {
                limit = Some(first);
            }
        }
        Ok(Select {
            distinct,
            projection,
            from,
            selection,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_select_list(&mut self) -> Result<Vec<SelectItem>, ParserError> {
        let mut items = Vec::new();
        loop {
            if self.eat_operator("*") {
                items.push(SelectItem::Wildcard);
            } else if self.is_qualified_wildcard() {
                let name = self.parse_qualified_wildcard_name()?;
                items.push(SelectItem::QualifiedWildcard(name));
            } else {
                let expr = self.parse_expr()?;
                let alias = if self.eat_keyword("AS") {
                    Some(self.parse_ident()?)
                } else if self.is_alias_start() {
                    Some(self.parse_ident()?)
                } else {
                    None
                };
                items.push(SelectItem::Expr { expr, alias });
            }
            if !self.eat_symbol(',') {
                break;
            }
        }
        Ok(items)
    }

    fn parse_table_refs(&mut self) -> Result<Vec<TableRef>, ParserError> {
        let mut tables = Vec::new();
        loop {
            tables.push(self.parse_table_ref()?);
            if !self.eat_symbol(',') {
                break;
            }
        }
        Ok(tables)
    }

    fn parse_table_ref(&mut self) -> Result<TableRef, ParserError> {
        let mut left = self.parse_table_factor()?;
        loop {
            let operator = if self.eat_keyword("JOIN") {
                Some(JoinOperator::Inner)
            } else if self.eat_keyword("INNER") {
                self.expect_keyword("JOIN")?;
                Some(JoinOperator::Inner)
            } else if self.eat_keyword("LEFT") {
                let _ = self.eat_keyword("OUTER");
                self.expect_keyword("JOIN")?;
                Some(JoinOperator::Left)
            } else if self.eat_keyword("RIGHT") {
                let _ = self.eat_keyword("OUTER");
                self.expect_keyword("JOIN")?;
                Some(JoinOperator::Right)
            } else if self.eat_keyword("FULL") {
                let _ = self.eat_keyword("OUTER");
                self.expect_keyword("JOIN")?;
                Some(JoinOperator::Full)
            } else if self.eat_keyword("CROSS") {
                self.expect_keyword("JOIN")?;
                Some(JoinOperator::Cross)
            } else {
                None
            };
            let operator = match operator {
                Some(op) => op,
                None => break,
            };
            let right = self.parse_table_factor()?;
            let constraint = if self.eat_keyword("ON") {
                Some(JoinConstraint::On(self.parse_expr()?))
            } else if self.eat_keyword("USING") {
                self.expect_symbol('(')?;
                let cols = self.parse_ident_list(')')?;
                Some(JoinConstraint::Using(cols))
            } else {
                None
            };
            left = TableRef::Join {
                left: Box::new(left),
                right: Box::new(right),
                operator,
                constraint,
            };
        }
        Ok(left)
    }

    fn parse_table_factor(&mut self) -> Result<TableRef, ParserError> {
        if self.eat_symbol('(') {
            if self.current_is_keyword("SELECT") {
                let subquery = self.parse_select()?;
                self.expect_symbol(')')?;
                let alias = self.parse_optional_alias()?;
                return Ok(TableRef::Subquery {
                    subquery: Box::new(subquery),
                    alias,
                });
            }
            return Err(ParserError::new("unexpected '(' in FROM"));
        }
        let name = self.parse_object_name()?;
        let alias = self.parse_optional_alias()?;
        Ok(TableRef::Named { name, alias })
    }

    fn parse_optional_alias(&mut self) -> Result<Option<Ident>, ParserError> {
        if self.eat_keyword("AS") {
            return Ok(Some(self.parse_ident()?));
        }
        if self.is_alias_start() {
            return Ok(Some(self.parse_ident()?));
        }
        Ok(None)
    }

    fn parse_order_by_list(&mut self) -> Result<Vec<OrderByExpr>, ParserError> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let asc = if self.eat_keyword("ASC") {
                Some(true)
            } else if self.eat_keyword("DESC") {
                Some(false)
            } else {
                None
            };
            items.push(OrderByExpr { expr, asc });
            if !self.eat_symbol(',') {
                break;
            }
        }
        Ok(items)
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>, ParserError> {
        let mut items = Vec::new();
        loop {
            items.push(self.parse_expr()?);
            if !self.eat_symbol(',') {
                break;
            }
        }
        Ok(items)
    }

    fn parse_expr(&mut self) -> Result<Expr, ParserError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, ParserError> {
        let mut expr = self.parse_and()?;
        while self.eat_keyword("OR") {
            let right = self.parse_and()?;
            expr = Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> Result<Expr, ParserError> {
        let mut expr = self.parse_comparison()?;
        while self.eat_keyword("AND") {
            let right = self.parse_comparison()?;
            expr = Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_comparison(&mut self) -> Result<Expr, ParserError> {
        let mut expr = self.parse_concat()?;
        loop {
            if self.eat_keyword("BETWEEN") {
                let low = self.parse_concat()?;
                self.expect_keyword("AND")?;
                let high = self.parse_concat()?;
                expr = Expr::Between {
                    expr: Box::new(expr),
                    negated: false,
                    low: Box::new(low),
                    high: Box::new(high),
                };
                continue;
            }
            if self.eat_keyword("NOT") {
                if self.eat_keyword("BETWEEN") {
                    let low = self.parse_concat()?;
                    self.expect_keyword("AND")?;
                    let high = self.parse_concat()?;
                    expr = Expr::Between {
                        expr: Box::new(expr),
                        negated: true,
                        low: Box::new(low),
                        high: Box::new(high),
                    };
                    continue;
                }
                if self.eat_keyword("IN") {
                    expr = self.parse_in_expr(expr, true)?;
                    continue;
                }
                if self.eat_keyword("LIKE") {
                    let right = self.parse_concat()?;
                    expr = Expr::BinaryOp {
                        left: Box::new(expr),
                        op: BinaryOperator::NotLike,
                        right: Box::new(right),
                    };
                    continue;
                }
                if self.eat_keyword("IS") {
                    if self.eat_keyword("NULL") {
                        expr = Expr::IsNull {
                            expr: Box::new(expr),
                            negated: true,
                        };
                    } else {
                        let right = self.parse_concat()?;
                        expr = Expr::BinaryOp {
                            left: Box::new(expr),
                            op: BinaryOperator::IsNot,
                            right: Box::new(right),
                        };
                    }
                    continue;
                }
                return Err(ParserError::new("unexpected NOT"));
            }
            if self.eat_keyword("IN") {
                expr = self.parse_in_expr(expr, false)?;
                continue;
            }
            if self.eat_keyword("LIKE") {
                let right = self.parse_concat()?;
                expr = Expr::BinaryOp {
                    left: Box::new(expr),
                    op: BinaryOperator::Like,
                    right: Box::new(right),
                };
                continue;
            }
            if self.eat_keyword("IS") {
                if self.eat_keyword("NOT") {
                    if self.eat_keyword("NULL") {
                        expr = Expr::IsNull {
                            expr: Box::new(expr),
                            negated: true,
                        };
                    } else {
                        let right = self.parse_concat()?;
                        expr = Expr::BinaryOp {
                            left: Box::new(expr),
                            op: BinaryOperator::IsNot,
                            right: Box::new(right),
                        };
                    }
                } else if self.eat_keyword("NULL") {
                    expr = Expr::IsNull {
                        expr: Box::new(expr),
                        negated: false,
                    };
                } else {
                    let right = self.parse_concat()?;
                    expr = Expr::BinaryOp {
                        left: Box::new(expr),
                        op: BinaryOperator::Is,
                        right: Box::new(right),
                    };
                }
                continue;
            }
            if let Some(op) = self.parse_comparison_operator() {
                let right = self.parse_concat()?;
                expr = Expr::BinaryOp {
                    left: Box::new(expr),
                    op,
                    right: Box::new(right),
                };
                continue;
            }
            break;
        }
        Ok(expr)
    }

    fn parse_concat(&mut self) -> Result<Expr, ParserError> {
        let mut expr = self.parse_add()?;
        while self.eat_operator("||") {
            let right = self.parse_add()?;
            expr = Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOperator::Concat,
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_add(&mut self) -> Result<Expr, ParserError> {
        let mut expr = self.parse_mul()?;
        loop {
            if self.eat_operator("+") {
                let right = self.parse_mul()?;
                expr = Expr::BinaryOp {
                    left: Box::new(expr),
                    op: BinaryOperator::Plus,
                    right: Box::new(right),
                };
            } else if self.eat_operator("-") {
                let right = self.parse_mul()?;
                expr = Expr::BinaryOp {
                    left: Box::new(expr),
                    op: BinaryOperator::Minus,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn parse_mul(&mut self) -> Result<Expr, ParserError> {
        let mut expr = self.parse_unary()?;
        loop {
            if self.eat_operator("*") {
                let right = self.parse_unary()?;
                expr = Expr::BinaryOp {
                    left: Box::new(expr),
                    op: BinaryOperator::Multiply,
                    right: Box::new(right),
                };
            } else if self.eat_operator("/") {
                let right = self.parse_unary()?;
                expr = Expr::BinaryOp {
                    left: Box::new(expr),
                    op: BinaryOperator::Divide,
                    right: Box::new(right),
                };
            } else if self.eat_operator("%") {
                let right = self.parse_unary()?;
                expr = Expr::BinaryOp {
                    left: Box::new(expr),
                    op: BinaryOperator::Modulo,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn parse_unary(&mut self) -> Result<Expr, ParserError> {
        if self.eat_operator("+") {
            return Ok(Expr::UnaryOp {
                op: UnaryOperator::Plus,
                expr: Box::new(self.parse_unary()?),
            });
        }
        if self.eat_operator("-") {
            return Ok(Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(self.parse_unary()?),
            });
        }
        if self.eat_keyword("NOT") {
            return Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(self.parse_unary()?),
            });
        }
        if self.eat_keyword("EXISTS") {
            self.expect_symbol('(')?;
            let select = self.parse_select()?;
            self.expect_symbol(')')?;
            return Ok(Expr::Exists(Box::new(select)));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr, ParserError> {
        match self.current() {
            TokenKind::Keyword(k) if k == "NULL" => {
                self.advance();
                Ok(Expr::Literal(Literal::Null))
            }
            TokenKind::Keyword(k) if k == "CASE" => self.parse_case_expr(),
            TokenKind::Keyword(k) if k == "CAST" => self.parse_cast_expr(),
            TokenKind::Ident(_, _) | TokenKind::Keyword(_) => self.parse_ident_or_function(),
            TokenKind::Number(n) => {
                let text = n.clone();
                self.advance();
                if text.contains('.') {
                    let value = text.parse::<f64>().map_err(|_| {
                        ParserError::new("invalid float literal")
                    })?;
                    Ok(Expr::Literal(Literal::Float(value)))
                } else {
                    let value = text.parse::<i64>().map_err(|_| {
                        ParserError::new("invalid integer literal")
                    })?;
                    Ok(Expr::Literal(Literal::Integer(value)))
                }
            }
            TokenKind::String(s) => {
                let value = s.clone();
                self.advance();
                Ok(Expr::Literal(Literal::String(value)))
            }
            TokenKind::Symbol('(') => self.parse_paren_expr_or_subquery(),
            TokenKind::Operator(op) if op == "*" => {
                self.advance();
                Ok(Expr::Wildcard)
            }
            _ => Err(ParserError::new("unexpected token in expression")),
        }
    }

    fn parse_ident_or_function(&mut self) -> Result<Expr, ParserError> {
        let name = self.parse_ident()?;
        if self.eat_symbol('.') {
            let mut parts = vec![name];
            loop {
                let ident = self.parse_ident()?;
                parts.push(ident);
                if !self.eat_symbol('.') {
                    break;
                }
            }
            return Ok(Expr::CompoundIdentifier(parts));
        }
        if self.eat_symbol('(') {
            let distinct = self.eat_keyword("DISTINCT");
            let mut args = Vec::new();
            if !self.eat_symbol(')') {
                loop {
                    args.push(self.parse_expr()?);
                    if self.eat_symbol(')') {
                        break;
                    }
                    self.expect_symbol(',')?;
                }
            }
            return Ok(Expr::Function {
                name,
                args,
                distinct,
            });
        }
        Ok(Expr::Identifier(name))
    }

    fn parse_cast_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_keyword("CAST")?;
        self.expect_symbol('(')?;
        let expr = self.parse_expr()?;
        self.expect_keyword("AS")?;
        let data_type = self.parse_data_type()?;
        self.expect_symbol(')')?;
        Ok(Expr::Cast {
            expr: Box::new(expr),
            data_type,
        })
    }

    fn parse_case_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_keyword("CASE")?;
        let operand = if self.current_is_keyword("WHEN") {
            None
        } else {
            Some(Box::new(self.parse_expr()?))
        };
        let mut when_then = Vec::new();
        while self.eat_keyword("WHEN") {
            let condition = self.parse_expr()?;
            self.expect_keyword("THEN")?;
            let result = self.parse_expr()?;
            when_then.push((condition, result));
        }
        let else_result = if self.eat_keyword("ELSE") {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        self.expect_keyword("END")?;
        Ok(Expr::Case {
            operand,
            when_then,
            else_result,
        })
    }

    fn parse_paren_expr_or_subquery(&mut self) -> Result<Expr, ParserError> {
        self.expect_symbol('(')?;
        if self.current_is_keyword("SELECT") {
            let select = self.parse_select()?;
            self.expect_symbol(')')?;
            return Ok(Expr::Subquery(Box::new(select)));
        }
        let expr = self.parse_expr()?;
        self.expect_symbol(')')?;
        Ok(Expr::Nested(Box::new(expr)))
    }

    fn parse_in_expr(&mut self, left: Expr, negated: bool) -> Result<Expr, ParserError> {
        if self.eat_symbol('(') {
            if self.current_is_keyword("SELECT") {
                let select = self.parse_select()?;
                self.expect_symbol(')')?;
                Ok(Expr::InSubquery {
                    expr: Box::new(left),
                    subquery: Box::new(select),
                    negated,
                })
            } else {
                let mut list = Vec::new();
                if !self.eat_symbol(')') {
                    loop {
                        list.push(self.parse_expr()?);
                        if self.eat_symbol(')') {
                            break;
                        }
                        self.expect_symbol(',')?;
                    }
                }
                Ok(Expr::InList {
                    expr: Box::new(left),
                    list,
                    negated,
                })
            }
        } else {
            let name = self.parse_object_name()?;
            let select = Select {
                distinct: false,
                projection: vec![SelectItem::Wildcard],
                from: vec![TableRef::Named { name, alias: None }],
                selection: None,
                group_by: Vec::new(),
                having: None,
                order_by: Vec::new(),
                limit: None,
                offset: None,
            };
            Ok(Expr::InSubquery {
                expr: Box::new(left),
                subquery: Box::new(select),
                negated,
            })
        }
    }

    fn parse_comparison_operator(&mut self) -> Option<BinaryOperator> {
        match self.current() {
            TokenKind::Operator(op) if op == "=" || op == "==" => {
                self.advance();
                Some(BinaryOperator::Eq)
            }
            TokenKind::Operator(op) if op == "!=" || op == "<>" => {
                self.advance();
                Some(BinaryOperator::NotEq)
            }
            TokenKind::Operator(op) if op == "<" => {
                self.advance();
                Some(BinaryOperator::Lt)
            }
            TokenKind::Operator(op) if op == "<=" => {
                self.advance();
                Some(BinaryOperator::LtEq)
            }
            TokenKind::Operator(op) if op == ">" => {
                self.advance();
                Some(BinaryOperator::Gt)
            }
            TokenKind::Operator(op) if op == ">=" => {
                self.advance();
                Some(BinaryOperator::GtEq)
            }
            _ => None,
        }
    }

    fn parse_object_name(&mut self) -> Result<ObjectName, ParserError> {
        let mut parts = Vec::new();
        parts.push(self.parse_ident()?);
        while self.eat_symbol('.') {
            parts.push(self.parse_ident()?);
        }
        Ok(ObjectName(parts))
    }

    fn parse_ident(&mut self) -> Result<Ident, ParserError> {
        match self.current() {
            TokenKind::Ident(value, quoted) => {
                let ident = Ident {
                    value: value.clone(),
                    quoted: *quoted,
                };
                self.advance();
                Ok(ident)
            }
            TokenKind::Keyword(value) => {
                let ident = Ident {
                    value: value.clone(),
                    quoted: false,
                };
                self.advance();
                Ok(ident)
            }
            _ => Err(ParserError::new("expected identifier")),
        }
    }

    fn parse_ident_list(&mut self, terminator: char) -> Result<Vec<Ident>, ParserError> {
        let mut items = Vec::new();
        loop {
            items.push(self.parse_ident()?);
            if self.eat_symbol(terminator) {
                break;
            }
            self.expect_symbol(',')?;
        }
        Ok(items)
    }

    fn parse_qualified_wildcard_name(&mut self) -> Result<ObjectName, ParserError> {
        let mut parts = Vec::new();
        parts.push(self.parse_ident()?);
        loop {
            if !matches!(
                self.tokens.get(self.pos).map(|t| &t.kind),
                Some(TokenKind::Symbol('.'))
            ) {
                break;
            }
            if matches!(
                self.tokens.get(self.pos + 1).map(|t| &t.kind),
                Some(TokenKind::Symbol('*'))
            ) {
                break;
            }
            self.expect_symbol('.')?;
            parts.push(self.parse_ident()?);
        }
        self.expect_symbol('.')?;
        self.expect_operator("*")?;
        Ok(ObjectName(parts))
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef, ParserError> {
        let name = self.parse_ident()?;
        let data_type = if self.is_column_constraint_start() {
            None
        } else if self.is_ident_like() {
            Some(self.parse_data_type()?)
        } else {
            None
        };
        let mut constraints = Vec::new();
        loop {
            if self.eat_keyword("NOT") {
                self.expect_keyword("NULL")?;
                constraints.push(ColumnConstraint::NotNull);
                continue;
            }
            if self.eat_keyword("NULL") {
                constraints.push(ColumnConstraint::Null);
                continue;
            }
            if self.eat_keyword("PRIMARY") {
                self.expect_keyword("KEY")?;
                constraints.push(ColumnConstraint::PrimaryKey);
                continue;
            }
            if self.eat_keyword("UNIQUE") {
                constraints.push(ColumnConstraint::Unique);
                continue;
            }
            if self.eat_keyword("DEFAULT") {
                let expr = self.parse_expr()?;
                constraints.push(ColumnConstraint::Default(expr));
                continue;
            }
            break;
        }
        Ok(ColumnDef {
            name,
            data_type,
            constraints,
        })
    }

    fn parse_table_constraint(&mut self) -> Result<TableConstraint, ParserError> {
        if self.eat_keyword("PRIMARY") {
            self.expect_keyword("KEY")?;
            self.expect_symbol('(')?;
            let cols = self.parse_ident_list(')')?;
            return Ok(TableConstraint::PrimaryKey(cols));
        }
        if self.eat_keyword("UNIQUE") {
            self.expect_symbol('(')?;
            let cols = self.parse_ident_list(')')?;
            return Ok(TableConstraint::Unique(cols));
        }
        if self.eat_keyword("CHECK") {
            self.expect_symbol('(')?;
            let expr = self.parse_expr()?;
            self.expect_symbol(')')?;
            return Ok(TableConstraint::Check(expr));
        }
        if self.eat_keyword("FOREIGN") {
            self.expect_keyword("KEY")?;
            self.expect_symbol('(')?;
            let columns = self.parse_ident_list(')')?;
            self.expect_keyword("REFERENCES")?;
            let foreign_table = self.parse_object_name()?;
            self.expect_symbol('(')?;
            let referred_columns = self.parse_ident_list(')')?;
            return Ok(TableConstraint::ForeignKey {
                columns,
                foreign_table,
                referred_columns,
            });
        }
        Err(ParserError::new("unknown table constraint"))
    }

    fn parse_data_type(&mut self) -> Result<DataType, ParserError> {
        let mut parts = Vec::new();
        let first = self.parse_ident()?;
        parts.push(first.value);
        while self.is_ident_like() {
            if self.is_column_constraint_start() {
                break;
            }
            match self.current() {
                TokenKind::Ident(value, _) => {
                    parts.push(value.clone());
                    self.advance();
                }
                TokenKind::Keyword(value) => {
                    parts.push(value.clone());
                    self.advance();
                }
                _ => break,
            }
        }
        if self.eat_symbol('(') {
            let mut inner = String::new();
            while !self.eat_symbol(')') {
                match self.current() {
                    TokenKind::Number(n) => {
                        inner.push_str(n);
                        self.advance();
                    }
                    TokenKind::Symbol(c) if *c == ',' => {
                        inner.push(',');
                        self.advance();
                    }
                    _ => break,
                }
            }
            parts.push(format!("({})", inner));
        }
        let type_name = parts.join(" ");
        let upper = type_name.to_ascii_uppercase();
        let data_type = if upper.contains("INT") {
            DataType::Integer
        } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
            DataType::Text
        } else if upper.contains("BLOB") {
            DataType::Blob
        } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
            DataType::Real
        } else if upper.contains("NUM") {
            DataType::Numeric
        } else {
            DataType::Custom(type_name)
        };
        Ok(data_type)
    }

    fn is_column_constraint_start(&self) -> bool {
        matches!(
            self.current(),
            TokenKind::Keyword(k)
                if k == "NOT"
                    || k == "NULL"
                    || k == "PRIMARY"
                    || k == "UNIQUE"
                    || k == "DEFAULT"
        )
    }

    fn is_table_constraint_start(&self) -> bool {
        matches!(
            self.current(),
            TokenKind::Keyword(k)
                if k == "PRIMARY" || k == "UNIQUE" || k == "CHECK" || k == "FOREIGN"
        )
    }

    fn is_ident_like(&self) -> bool {
        matches!(self.current(), TokenKind::Ident(_, _) | TokenKind::Keyword(_))
    }

    fn is_alias_start(&self) -> bool {
        matches!(self.current(), TokenKind::Ident(_, _))
    }

    fn is_qualified_wildcard(&self) -> bool {
        if !matches!(self.current(), TokenKind::Ident(_, _) | TokenKind::Keyword(_)) {
            return false;
        }
        let mut idx = self.pos;
        loop {
            let dot = self.tokens.get(idx + 1).map(|t| &t.kind);
            let next = self.tokens.get(idx + 2).map(|t| &t.kind);
            match (dot, next) {
                (Some(TokenKind::Symbol('.')), Some(TokenKind::Ident(_, _)))
                | (Some(TokenKind::Symbol('.')), Some(TokenKind::Keyword(_))) => {
                    idx += 2;
                    continue;
                }
                (Some(TokenKind::Symbol('.')), Some(TokenKind::Operator(op))) if op == "*" => {
                    return true;
                }
                _ => return false,
            }
        }
    }

    fn current_is_keyword(&self, keyword: &str) -> bool {
        matches!(self.current(), TokenKind::Keyword(k) if k == keyword)
    }
}

pub fn parse_statement(input: &str) -> Result<Statement, ParserError> {
    let mut lexer = Lexer::new(input);
    let mut tokens = Vec::new();
    loop {
        let token = lexer.next_token()?;
        let is_eof = matches!(token.kind, TokenKind::Eof);
        tokens.push(token);
        if is_eof {
            break;
        }
    }
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse_statement()?;
    Ok(stmt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_table_basic() {
        let sql = "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER)";
        let stmt = parse_statement(sql).unwrap();
        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.name.0[0].value, "t1");
                assert_eq!(create.columns.len(), 3);
            }
            _ => panic!("expected create table"),
        }
    }

    #[test]
    fn parse_insert_values_basic() {
        let sql = "INSERT INTO t1(a,b) VALUES(1,2)";
        let stmt = parse_statement(sql).unwrap();
        match stmt {
            Statement::Insert(insert) => match insert.source {
                InsertSource::Values(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0].len(), 2);
                }
                _ => panic!("expected values"),
            },
            _ => panic!("expected insert"),
        }
    }

    #[test]
    fn parse_select_with_case_and_subquery() {
        let sql = "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END FROM t1 ORDER BY 1";
        let stmt = parse_statement(sql).unwrap();
        match stmt {
            Statement::Select(select) => {
                assert_eq!(select.projection.len(), 1);
                assert_eq!(select.from.len(), 1);
                assert_eq!(select.order_by.len(), 1);
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_select_with_exists() {
        let sql = "SELECT a FROM t1 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)";
        let stmt = parse_statement(sql).unwrap();
        match stmt {
            Statement::Select(select) => {
                assert!(select.selection.is_some());
            }
            _ => panic!("expected select"),
        }
    }
}
