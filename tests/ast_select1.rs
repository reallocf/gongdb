use gongdb::ast::*;
use std::collections::HashSet;
use std::fs;

fn extract_sql_blocks(path: &str) -> std::io::Result<Vec<String>> {
    let content = fs::read_to_string(path)?;
    let mut sql_blocks = Vec::new();
    let mut lines = content.lines().peekable();

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.starts_with("statement ") {
            let mut sql = String::new();
            while let Some(sql_line) = lines.next() {
                if sql_line.trim().is_empty() {
                    break;
                }
                if !sql.is_empty() {
                    sql.push('\n');
                }
                sql.push_str(sql_line);
            }
            if !sql.trim().is_empty() {
                sql_blocks.push(sql);
            }
        } else if trimmed.starts_with("query ") {
            let mut sql = String::new();
            while let Some(sql_line) = lines.next() {
                if sql_line.trim() == "----" {
                    break;
                }
                if !sql.is_empty() {
                    sql.push('\n');
                }
                sql.push_str(sql_line);
            }
            if !sql.trim().is_empty() {
                sql_blocks.push(sql);
            }
            while let Some(next_line) = lines.peek() {
                if next_line.trim().is_empty() {
                    lines.next();
                    break;
                }
                lines.next();
            }
        }
    }

    Ok(sql_blocks)
}

fn build_select1_sample_ast() -> Vec<Statement> {
    let create_table = Statement::CreateTable(CreateTable {
        if_not_exists: false,
        name: ObjectName::new([Ident::new("t1")]),
        columns: vec![
            ColumnDef {
                name: Ident::new("a"),
                data_type: Some(DataType::Integer),
                constraints: Vec::new(),
            },
            ColumnDef {
                name: Ident::new("b"),
                data_type: Some(DataType::Integer),
                constraints: Vec::new(),
            },
            ColumnDef {
                name: Ident::new("c"),
                data_type: Some(DataType::Integer),
                constraints: Vec::new(),
            },
            ColumnDef {
                name: Ident::new("d"),
                data_type: Some(DataType::Integer),
                constraints: Vec::new(),
            },
            ColumnDef {
                name: Ident::new("e"),
                data_type: Some(DataType::Integer),
                constraints: Vec::new(),
            },
        ],
        constraints: Vec::new(),
        without_rowid: false,
    });

    let insert = Statement::Insert(Insert {
        table: ObjectName::new([Ident::new("t1")]),
        columns: vec![
            Ident::new("e"),
            Ident::new("c"),
            Ident::new("b"),
            Ident::new("d"),
            Ident::new("a"),
        ],
        source: InsertSource::Values(vec![vec![
            Expr::Literal(Literal::Integer(103)),
            Expr::Literal(Literal::Integer(102)),
            Expr::Literal(Literal::Integer(100)),
            Expr::Literal(Literal::Integer(101)),
            Expr::Literal(Literal::Integer(104)),
        ]]),
    });

    let avg_c = Select {
        distinct: false,
        projection: vec![SelectItem::Expr {
            expr: Expr::Function {
                name: Ident::new("avg"),
                args: vec![Expr::Identifier(Ident::new("c"))],
                distinct: false,
            },
            alias: None,
        }],
        from: vec![TableRef::Named {
            name: ObjectName::new([Ident::new("t1")]),
            alias: None,
        }],
        selection: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        limit: None,
        offset: None,
    };

    let count_subquery = Select {
        distinct: false,
        projection: vec![SelectItem::Expr {
            expr: Expr::Function {
                name: Ident::new("count"),
                args: vec![Expr::Wildcard],
                distinct: false,
            },
            alias: None,
        }],
        from: vec![TableRef::Named {
            name: ObjectName::new([Ident::new("t1")]),
            alias: Some(Ident::new("x")),
        }],
        selection: Some(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::CompoundIdentifier(vec![
                    Ident::new("x"),
                    Ident::new("c"),
                ])),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::CompoundIdentifier(vec![
                    Ident::new("t1"),
                    Ident::new("c"),
                ])),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::CompoundIdentifier(vec![
                    Ident::new("x"),
                    Ident::new("d"),
                ])),
                op: BinaryOperator::Lt,
                right: Box::new(Expr::CompoundIdentifier(vec![
                    Ident::new("t1"),
                    Ident::new("d"),
                ])),
            }),
        }),
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        limit: None,
        offset: None,
    };

    let case_expr = Expr::Case {
        operand: None,
        when_then: vec![(
            Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("c"))),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Subquery(Box::new(avg_c))),
            },
            Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("a"))),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Literal(Literal::Integer(2))),
            },
        )],
        else_result: Some(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("b"))),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Literal(Literal::Integer(10))),
        })),
    };

    let between_expr = Expr::Between {
        expr: Box::new(Expr::Identifier(Ident::new("d"))),
        negated: true,
        low: Box::new(Expr::Literal(Literal::Integer(110))),
        high: Box::new(Expr::Literal(Literal::Integer(150))),
    };

    let exists_expr = Expr::Exists(Box::new(Select {
        distinct: false,
        projection: vec![SelectItem::Expr {
            expr: Expr::Literal(Literal::Integer(1)),
            alias: None,
        }],
        from: vec![TableRef::Named {
            name: ObjectName::new([Ident::new("t1")]),
            alias: Some(Ident::new("x")),
        }],
        selection: Some(Expr::BinaryOp {
            left: Box::new(Expr::CompoundIdentifier(vec![
                Ident::new("x"),
                Ident::new("b"),
            ])),
            op: BinaryOperator::Lt,
            right: Box::new(Expr::CompoundIdentifier(vec![
                Ident::new("t1"),
                Ident::new("b"),
            ])),
        }),
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }));

    let select = Statement::Select(Select {
        distinct: false,
        projection: vec![
            SelectItem::Expr {
                expr: case_expr,
                alias: None,
            },
            SelectItem::Expr {
                expr: Expr::Subquery(Box::new(count_subquery)),
                alias: None,
            },
            SelectItem::Expr {
                expr: Expr::Function {
                    name: Ident::new("abs"),
                    args: vec![Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new("b"))),
                        op: BinaryOperator::Minus,
                        right: Box::new(Expr::Identifier(Ident::new("c"))),
                    }],
                    distinct: false,
                },
                alias: None,
            },
        ],
        from: vec![TableRef::Named {
            name: ObjectName::new([Ident::new("t1")]),
            alias: None,
        }],
        selection: Some(Expr::BinaryOp {
            left: Box::new(between_expr),
            op: BinaryOperator::Or,
            right: Box::new(exists_expr),
        }),
        group_by: Vec::new(),
        having: None,
        order_by: vec![OrderByExpr {
            expr: Expr::Identifier(Ident::new("a")),
            asc: Some(true),
        }],
        limit: None,
        offset: None,
    });

    vec![create_table, insert, select]
}

#[test]
fn ast_represents_select1_statement_types() {
    let sql_blocks =
        extract_sql_blocks("tests/sqlite/select1.test").expect("read select1.test");
    let mut kinds = HashSet::new();

    for sql in sql_blocks {
        let upper = sql.trim().to_uppercase();
        if upper.starts_with("CREATE TABLE") {
            kinds.insert("create_table");
        } else if upper.starts_with("INSERT") {
            kinds.insert("insert");
        } else if upper.starts_with("SELECT") {
            kinds.insert("select");
        }
    }

    assert!(kinds.contains("create_table"));
    assert!(kinds.contains("insert"));
    assert!(kinds.contains("select"));

    let samples = build_select1_sample_ast();
    assert!(matches!(samples[0], Statement::CreateTable(_)));
    assert!(matches!(samples[1], Statement::Insert(_)));
    assert!(matches!(samples[2], Statement::Select(_)));
}
