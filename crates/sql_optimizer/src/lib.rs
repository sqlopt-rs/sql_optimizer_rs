use std::collections::{BTreeSet, HashMap};

use sqlparser::ast::{
    BinaryOperator, Expr, JoinOperator, ObjectNamePart, SetExpr, Statement, TableFactor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqlOptError {
    #[error("failed to parse SQL: {0}")]
    ParseError(String),
    #[error("unsupported query shape: {0}")]
    Unsupported(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct IndexSuggestion {
    pub table: String,
    pub column: String,
    pub statement: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyzeResult {
    pub index_suggestions: Vec<IndexSuggestion>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewriteResult {
    pub original: String,
    pub rewritten: Option<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct N1Finding {
    pub template: String,
    pub count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct N1Report {
    pub threshold: usize,
    pub findings: Vec<N1Finding>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ColumnRef {
    qualifier: Option<String>,
    column: String,
}

pub fn analyze(query: &str) -> Result<AnalyzeResult, SqlOptError> {
    let statement = parse_single_statement(query)?;
    let select = match statement {
        Statement::Query(query) => match *query.body {
            SetExpr::Select(select) => select,
            _ => {
                return Err(SqlOptError::Unsupported(
                    "only SELECT queries are supported".to_string(),
                ));
            }
        },
        _ => {
            return Err(SqlOptError::Unsupported(
                "only queries are supported".to_string(),
            ));
        }
    };

    let alias_to_table = build_alias_to_table(&select);
    let base_table = if select.from.len() == 1 && select.from[0].joins.is_empty() {
        alias_to_table
            .values()
            .next()
            .cloned()
            .or_else(|| table_factor_name(&select.from[0].relation))
    } else {
        None
    };

    let mut refs = Vec::new();
    if let Some(selection) = select.selection.as_ref() {
        collect_column_refs(selection, &mut refs);
    }
    for table_with_joins in &select.from {
        for join in &table_with_joins.joins {
            let constraint = match &join.join_operator {
                JoinOperator::Join(constraint)
                | JoinOperator::Inner(constraint)
                | JoinOperator::Left(constraint)
                | JoinOperator::LeftOuter(constraint)
                | JoinOperator::Right(constraint)
                | JoinOperator::RightOuter(constraint)
                | JoinOperator::FullOuter(constraint)
                | JoinOperator::CrossJoin(constraint) => Some(constraint),
                _ => None,
            };
            if let Some(sqlparser::ast::JoinConstraint::On(expr)) = constraint {
                collect_column_refs(expr, &mut refs);
            }
        }
    }

    let mut pairs = BTreeSet::<(String, String)>::new();
    for column_ref in refs {
        let table = match column_ref.qualifier.as_deref() {
            Some(qualifier) => alias_to_table
                .get(qualifier)
                .cloned()
                .unwrap_or_else(|| qualifier.to_string()),
            None => match base_table.as_deref() {
                Some(table) => table.to_string(),
                None => continue,
            },
        };
        pairs.insert((table, column_ref.column));
    }

    let index_suggestions = pairs
        .into_iter()
        .map(|(table, column)| IndexSuggestion {
            statement: format!(
                "CREATE INDEX {} ON {}({});",
                format!("idx_{}_{}", ident_slug(&table), ident_slug(&column)),
                table,
                column,
            ),
            table,
            column,
        })
        .collect::<Vec<_>>();

    let warnings = if index_suggestions.is_empty() {
        vec!["no obvious index opportunities detected".to_string()]
    } else {
        Vec::new()
    };

    Ok(AnalyzeResult {
        index_suggestions,
        warnings,
    })
}

pub fn rewrite(query: &str) -> Result<RewriteResult, SqlOptError> {
    let original = query.to_string();
    let statement = parse_single_statement(query)?;
    let select = match statement {
        Statement::Query(query) => match *query.body {
            SetExpr::Select(select) => select,
            _ => {
                return Ok(RewriteResult {
                    original,
                    rewritten: None,
                    warnings: vec!["rewrite currently supports a single SELECT".to_string()],
                });
            }
        },
        _ => {
            return Ok(RewriteResult {
                original,
                rewritten: None,
                warnings: vec!["rewrite currently supports a single SELECT".to_string()],
            });
        }
    };

    if select.from.len() != 1 {
        return Ok(RewriteResult {
            original,
            rewritten: None,
            warnings: vec!["rewrite currently supports a single FROM entry".to_string()],
        });
    }

    let table_with_joins = &select.from[0];
    if table_with_joins.joins.len() != 1 {
        return Ok(RewriteResult {
            original,
            rewritten: None,
            warnings: vec!["rewrite currently supports exactly one JOIN".to_string()],
        });
    }

    let left_table = match &table_with_joins.relation {
        TableFactor::Table { name, alias, .. } => (
            name.to_string(),
            qualifier_for_table(name, alias),
            alias.as_ref().map(|a| a.name.value.clone()),
        ),
        _ => {
            return Ok(RewriteResult {
                original,
                rewritten: None,
                warnings: vec!["rewrite currently supports table FROM".to_string()],
            });
        }
    };

    let join = &table_with_joins.joins[0];
    let right_table = match &join.relation {
        TableFactor::Table { name, alias, .. } => (
            name.to_string(),
            qualifier_for_table(name, alias),
            alias.as_ref().map(|a| a.name.value.clone()),
        ),
        _ => {
            return Ok(RewriteResult {
                original,
                rewritten: None,
                warnings: vec!["rewrite currently supports table JOIN".to_string()],
            });
        }
    };

    let on_expr = match &join.join_operator {
        JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => match constraint {
            sqlparser::ast::JoinConstraint::On(expr) => expr,
            _ => {
                return Ok(RewriteResult {
                    original,
                    rewritten: None,
                    warnings: vec!["rewrite currently requires JOIN ... ON ...".to_string()],
                });
            }
        },
        _ => {
            return Ok(RewriteResult {
                original,
                rewritten: None,
                warnings: vec!["rewrite currently supports JOIN/INNER JOIN".to_string()],
            });
        }
    };

    let (left_join_col, right_join_col) =
        match extract_join_columns(on_expr, &left_table.1, &right_table.1) {
            Some(pair) => pair,
            None => {
                return Ok(RewriteResult {
                    original,
                    rewritten: None,
                    warnings: vec!["rewrite currently supports simple equality joins".to_string()],
                });
            }
        };

    let selection = match select.selection.as_ref() {
        Some(selection) => selection,
        None => {
            return Ok(RewriteResult {
                original,
                rewritten: None,
                warnings: vec!["rewrite requires a WHERE clause on the joined table".to_string()],
            });
        }
    };

    if !expr_refs_only_qualifier(selection, &right_table.1) {
        return Ok(RewriteResult {
            original,
            rewritten: None,
            warnings: vec!["rewrite requires WHERE to reference only the joined table".to_string()],
        });
    }

    let where_expr = strip_qualifier(selection.clone(), &right_table.1);
    let rewritten = format!(
        "SELECT {left_q}.* FROM {left_table}{left_alias} WHERE {left_q}.{left_join_col} IN (SELECT {right_join_col} FROM {right_table} WHERE {where_expr})",
        left_q = left_table.1,
        left_table = left_table.0,
        left_alias = alias_clause(left_table.2.as_deref()),
        left_join_col = left_join_col,
        right_join_col = right_join_col,
        right_table = right_table.0,
        where_expr = where_expr,
    );

    Ok(RewriteResult {
        original,
        rewritten: Some(rewritten),
        warnings: vec![
            "heuristic rewrite: verify projection if you relied on JOINed columns".to_string(),
        ],
    })
}

pub fn normalize_query_template(query: &str) -> String {
    let mut out = String::with_capacity(query.len());

    let mut chars = query.trim().trim_end_matches(';').chars().peekable();
    let mut prev_was_space = false;

    while let Some(ch) = chars.next() {
        if ch.is_whitespace() {
            if !prev_was_space && !out.is_empty() {
                out.push(' ');
                prev_was_space = true;
            }
            continue;
        }
        prev_was_space = false;

        if ch == '\'' {
            out.push('?');
            while let Some(next) = chars.next() {
                if next == '\'' {
                    if chars.peek() == Some(&'\'') {
                        chars.next();
                        continue;
                    }
                    break;
                }
            }
            continue;
        }

        if ch == '-' && matches!(chars.peek(), Some(peek) if peek.is_ascii_digit()) {
            out.push('?');
            while matches!(chars.peek(), Some(peek) if peek.is_ascii_digit() || *peek == '.') {
                chars.next();
            }
            continue;
        }

        if ch.is_ascii_digit() {
            out.push('?');
            while matches!(chars.peek(), Some(peek) if peek.is_ascii_digit() || *peek == '.') {
                chars.next();
            }
            continue;
        }

        for lower in ch.to_lowercase() {
            out.push(lower);
        }
    }

    out.trim().to_string()
}

pub fn detect_n1_from_log(log_contents: &str, threshold: usize) -> N1Report {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for line in log_contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let template = normalize_query_template(line);
        if template.is_empty() {
            continue;
        }
        *counts.entry(template).or_default() += 1;
    }

    let mut findings = counts
        .into_iter()
        .filter(|(_, count)| *count >= threshold)
        .map(|(template, count)| N1Finding { template, count })
        .collect::<Vec<_>>();
    findings.sort_by(|a, b| {
        b.count
            .cmp(&a.count)
            .then_with(|| a.template.cmp(&b.template))
    });

    N1Report {
        threshold,
        findings,
    }
}

fn parse_single_statement(query: &str) -> Result<Statement, SqlOptError> {
    let dialect = GenericDialect {};
    let mut statements =
        Parser::parse_sql(&dialect, query).map_err(|e| SqlOptError::ParseError(e.to_string()))?;
    if statements.len() != 1 {
        return Err(SqlOptError::Unsupported(
            "expected exactly one statement".to_string(),
        ));
    }
    Ok(statements.remove(0))
}

fn build_alias_to_table(select: &sqlparser::ast::Select) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for table_with_joins in &select.from {
        if let TableFactor::Table { name, alias, .. } = &table_with_joins.relation {
            let table = name.to_string();
            let qualifier = qualifier_for_table(name, alias);
            map.insert(qualifier, table);
        }
        for join in &table_with_joins.joins {
            if let TableFactor::Table { name, alias, .. } = &join.relation {
                let table = name.to_string();
                let qualifier = qualifier_for_table(name, alias);
                map.insert(qualifier, table);
            }
        }
    }
    map
}

fn table_factor_name(table: &TableFactor) -> Option<String> {
    match table {
        TableFactor::Table { name, .. } => Some(name.to_string()),
        _ => None,
    }
}

fn qualifier_for_table(
    name: &sqlparser::ast::ObjectName,
    alias: &Option<sqlparser::ast::TableAlias>,
) -> String {
    match alias.as_ref() {
        Some(alias) => alias.name.value.clone(),
        None => name
            .0
            .last()
            .and_then(ObjectNamePart::as_ident)
            .map(|ident| ident.value.clone())
            .unwrap_or_else(|| name.to_string()),
    }
}

fn alias_clause(alias: Option<&str>) -> String {
    alias.map(|alias| format!(" {alias}")).unwrap_or_default()
}

fn collect_column_refs(expr: &Expr, out: &mut Vec<ColumnRef>) {
    if let Some(column_ref) = column_ref(expr) {
        out.push(column_ref);
        return;
    }

    match expr {
        Expr::BinaryOp { left, right, .. } => {
            collect_column_refs(left, out);
            collect_column_refs(right, out);
        }
        Expr::UnaryOp { expr, .. } => collect_column_refs(expr, out),
        Expr::Nested(expr) => collect_column_refs(expr, out),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_column_refs(expr, out);
            collect_column_refs(low, out);
            collect_column_refs(high, out);
        }
        Expr::InList { expr, list, .. } => {
            collect_column_refs(expr, out);
            for item in list {
                collect_column_refs(item, out);
            }
        }
        Expr::IsNull(expr) | Expr::IsNotNull(expr) => collect_column_refs(expr, out),
        Expr::Cast { expr, .. } => collect_column_refs(expr, out),
        Expr::Function(func) => match &func.args {
            sqlparser::ast::FunctionArguments::List(list) => {
                for arg in &list.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Unnamed(arg) => {
                            if let sqlparser::ast::FunctionArgExpr::Expr(expr) = arg {
                                collect_column_refs(expr, out);
                            }
                        }
                        sqlparser::ast::FunctionArg::Named { arg, .. } => {
                            if let sqlparser::ast::FunctionArgExpr::Expr(expr) = arg {
                                collect_column_refs(expr, out);
                            }
                        }
                        sqlparser::ast::FunctionArg::ExprNamed { name, arg, .. } => {
                            collect_column_refs(name, out);
                            if let sqlparser::ast::FunctionArgExpr::Expr(expr) = arg {
                                collect_column_refs(expr, out);
                            }
                        }
                    }
                }
            }
            sqlparser::ast::FunctionArguments::Subquery(_)
            | sqlparser::ast::FunctionArguments::None => {}
        },
        _ => {}
    }
}

fn column_ref(expr: &Expr) -> Option<ColumnRef> {
    match expr {
        Expr::Identifier(ident) => Some(ColumnRef {
            qualifier: None,
            column: ident.value.clone(),
        }),
        Expr::CompoundIdentifier(idents) if idents.len() >= 2 => Some(ColumnRef {
            qualifier: Some(idents[idents.len() - 2].value.clone()),
            column: idents.last()?.value.clone(),
        }),
        _ => None,
    }
}

fn ident_slug(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut prev_us = false;
    for ch in value.chars() {
        let is_ok = ch.is_ascii_alphanumeric();
        if is_ok {
            out.push(ch.to_ascii_lowercase());
            prev_us = false;
            continue;
        }
        if !prev_us {
            out.push('_');
            prev_us = true;
        }
    }
    out.trim_matches('_').to_string()
}

fn extract_join_columns(on_expr: &Expr, left_q: &str, right_q: &str) -> Option<(String, String)> {
    let Expr::BinaryOp { left, op, right } = on_expr else {
        return None;
    };
    if *op != BinaryOperator::Eq {
        return None;
    }

    let left_ref = column_ref(left)?;
    let right_ref = column_ref(right)?;

    match (
        left_ref.qualifier.as_deref(),
        right_ref.qualifier.as_deref(),
    ) {
        (Some(lq), Some(rq)) if lq == left_q && rq == right_q => {
            Some((left_ref.column, right_ref.column))
        }
        (Some(lq), Some(rq)) if lq == right_q && rq == left_q => {
            Some((right_ref.column, left_ref.column))
        }
        _ => None,
    }
}

fn expr_refs_only_qualifier(expr: &Expr, qualifier: &str) -> bool {
    let mut refs = Vec::new();
    collect_column_refs(expr, &mut refs);
    refs.iter().all(|r| match r.qualifier.as_deref() {
        Some(q) => q == qualifier,
        None => false,
    })
}

fn strip_qualifier(expr: Expr, qualifier: &str) -> Expr {
    match expr {
        Expr::CompoundIdentifier(mut idents) if idents.len() >= 2 => {
            if idents[idents.len() - 2].value == qualifier {
                let col = idents.pop().expect("len >= 2");
                return Expr::Identifier(col);
            }
            Expr::CompoundIdentifier(idents)
        }
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(strip_qualifier(*left, qualifier)),
            op,
            right: Box::new(strip_qualifier(*right, qualifier)),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op,
            expr: Box::new(strip_qualifier(*expr, qualifier)),
        },
        Expr::Nested(expr) => Expr::Nested(Box::new(strip_qualifier(*expr, qualifier))),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Expr::Between {
            expr: Box::new(strip_qualifier(*expr, qualifier)),
            negated,
            low: Box::new(strip_qualifier(*low, qualifier)),
            high: Box::new(strip_qualifier(*high, qualifier)),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(strip_qualifier(*expr, qualifier)),
            list: list
                .into_iter()
                .map(|item| strip_qualifier(item, qualifier))
                .collect(),
            negated,
        },
        Expr::IsNull(expr) => Expr::IsNull(Box::new(strip_qualifier(*expr, qualifier))),
        Expr::IsNotNull(expr) => Expr::IsNotNull(Box::new(strip_qualifier(*expr, qualifier))),
        Expr::Cast {
            kind,
            expr,
            data_type,
            format,
        } => Expr::Cast {
            kind,
            expr: Box::new(strip_qualifier(*expr, qualifier)),
            data_type,
            format,
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_query_template_replaces_literals_and_numbers() {
        let input = "SELECT * FROM users WHERE age > 18 AND name = 'O''Reilly';";
        assert_eq!(
            normalize_query_template(input),
            "select * from users where age > ? and name = ?"
        );
    }

    #[test]
    fn detect_n1_from_log_flags_repeated_templates() {
        let log = r#"
SELECT * FROM users WHERE id = 1;
SELECT * FROM users WHERE id = 2;
SELECT * FROM users WHERE id = 3;
SELECT * FROM users WHERE id = 4;
SELECT * FROM users WHERE id = 5;
"#;
        let report = detect_n1_from_log(log, 5);
        assert_eq!(report.findings.len(), 1);
        assert_eq!(report.findings[0].count, 5);
        assert_eq!(
            report.findings[0].template,
            "select * from users where id = ?"
        );
    }

    #[test]
    fn rewrite_join_to_in_works_for_simple_filter_join() {
        let query = "SELECT * FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = true";
        let rewritten = rewrite(query).unwrap().rewritten.unwrap();
        assert_eq!(
            rewritten,
            "SELECT o.* FROM orders o WHERE o.user_id IN (SELECT id FROM users WHERE active = true)"
        );
    }

    #[test]
    fn analyze_suggests_indexes_for_where_column() {
        let result = analyze("SELECT * FROM users WHERE age > 18").unwrap();
        assert_eq!(result.index_suggestions.len(), 1);
        assert_eq!(result.index_suggestions[0].table, "users");
        assert_eq!(result.index_suggestions[0].column, "age");
    }
}
