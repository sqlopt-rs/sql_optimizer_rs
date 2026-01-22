use std::collections::{BTreeSet, HashMap, VecDeque};

use sqlparser::ast::{
    BinaryOperator, Expr, JoinOperator, ObjectNamePart, Query, SelectItem, SetExpr, Statement,
    TableFactor,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSuggestion {
    pub table: String,
    pub columns: Vec<String>,
    pub ddl: String,
    pub reason: String,
    pub estimated_before_ms: u64,
    pub estimated_after_ms: u64,
    pub estimated_improvement_pct: u8,
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
    pub applied_rules: Vec<&'static str>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct N1Options {
    pub threshold: usize,
    pub window: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct N1Finding {
    pub template: String,
    pub max_count_in_window: usize,
    pub total_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct N1Report {
    pub threshold: usize,
    pub window: usize,
    pub findings: Vec<N1Finding>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ColumnRef {
    qualifier: Option<String>,
    column: String,
}

pub fn analyze(query: &str) -> Result<AnalyzeResult, SqlOptError> {
    let statement = parse_single_statement(query)?;

    let mut candidates = BTreeSet::<(String, String)>::new();
    collect_index_candidates_from_statement(&statement, &mut candidates)?;

    let index_suggestions = candidates
        .into_iter()
        .map(|(table, column)| {
            let columns = vec![column.clone()];
            let index_name = format!(
                "idx_{}_{}",
                ident_slug(&table),
                ident_slug(columns.first().expect("1 column")),
            );
            let ddl = format!("CREATE INDEX CONCURRENTLY {index_name} ON {table}({column});");
            let (estimated_before_ms, estimated_after_ms, estimated_improvement_pct) =
                estimate_index_benefit_ms(&table, &[&column]);
            IndexSuggestion {
                table,
                columns,
                ddl,
                reason: "used in WHERE/JOIN predicate".to_string(),
                estimated_before_ms,
                estimated_after_ms,
                estimated_improvement_pct,
            }
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
                    applied_rules: Vec::new(),
                    warnings: vec!["rewrite currently supports a single SELECT".to_string()],
                });
            }
        },
        _ => {
            return Ok(RewriteResult {
                original,
                rewritten: None,
                applied_rules: Vec::new(),
                warnings: vec!["rewrite currently supports a single SELECT".to_string()],
            });
        }
    };

    let mut warnings = Vec::new();
    if select_has_wildcard(&select) {
        warnings
            .push("avoid wildcard projections (SELECT *); select only needed columns".to_string());
    }

    if select.from.len() != 1 {
        warnings.push("rewrite currently supports a single FROM entry".to_string());
        return Ok(RewriteResult {
            original,
            rewritten: None,
            applied_rules: Vec::new(),
            warnings,
        });
    }

    let table_with_joins = &select.from[0];
    if table_with_joins.joins.len() != 1 {
        warnings.push("rewrite currently supports exactly one JOIN".to_string());
        return Ok(RewriteResult {
            original,
            rewritten: None,
            applied_rules: Vec::new(),
            warnings,
        });
    }

    let left_table = match &table_with_joins.relation {
        TableFactor::Table { name, alias, .. } => (
            name.to_string(),
            qualifier_for_table(name, alias),
            alias.as_ref().map(|a| a.name.value.clone()),
        ),
        _ => {
            warnings.push("rewrite currently supports table FROM".to_string());
            return Ok(RewriteResult {
                original,
                rewritten: None,
                applied_rules: Vec::new(),
                warnings,
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
            warnings.push("rewrite currently supports table JOIN".to_string());
            return Ok(RewriteResult {
                original,
                rewritten: None,
                applied_rules: Vec::new(),
                warnings,
            });
        }
    };

    let on_expr = match &join.join_operator {
        JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => match constraint {
            sqlparser::ast::JoinConstraint::On(expr) => expr,
            _ => {
                warnings.push("rewrite currently requires JOIN ... ON ...".to_string());
                return Ok(RewriteResult {
                    original,
                    rewritten: None,
                    applied_rules: Vec::new(),
                    warnings,
                });
            }
        },
        _ => {
            warnings.push("rewrite currently supports JOIN/INNER JOIN".to_string());
            return Ok(RewriteResult {
                original,
                rewritten: None,
                applied_rules: Vec::new(),
                warnings,
            });
        }
    };

    let (left_join_col, right_join_col) =
        match extract_join_columns(on_expr, &left_table.1, &right_table.1) {
            Some(pair) => pair,
            None => {
                warnings.push("rewrite currently supports simple equality joins".to_string());
                return Ok(RewriteResult {
                    original,
                    rewritten: None,
                    applied_rules: Vec::new(),
                    warnings,
                });
            }
        };

    let selection = match select.selection.as_ref() {
        Some(selection) => selection,
        None => {
            warnings.push("rewrite requires a WHERE clause on the joined table".to_string());
            return Ok(RewriteResult {
                original,
                rewritten: None,
                applied_rules: Vec::new(),
                warnings,
            });
        }
    };

    if !expr_refs_only_qualifier(selection, &right_table.1) {
        warnings.push("rewrite requires WHERE to reference only the joined table".to_string());
        return Ok(RewriteResult {
            original,
            rewritten: None,
            applied_rules: Vec::new(),
            warnings,
        });
    }

    if !projection_is_safe_for_join_to_in(&select.projection, &left_table.1) {
        warnings.push(
            "rewrite is not applied because projection may depend on the joined table".to_string(),
        );
        return Ok(RewriteResult {
            original,
            rewritten: None,
            applied_rules: Vec::new(),
            warnings,
        });
    }

    let projection = projection_sql(&select.projection);

    let where_expr = strip_qualifier(selection.clone(), &right_table.1);
    let rewritten = format!(
        "SELECT {projection} FROM {left_table}{left_alias} WHERE {left_q}.{left_join_col} IN (SELECT {right_join_col} FROM {right_table} WHERE {where_expr})",
        projection = projection,
        left_q = left_table.1,
        left_table = left_table.0,
        left_alias = alias_clause(left_table.2.as_deref()),
        left_join_col = left_join_col,
        right_join_col = right_join_col,
        right_table = right_table.0,
        where_expr = where_expr,
    );

    warnings
        .push("heuristic rewrite: verify projection if you relied on JOINed columns".to_string());
    Ok(RewriteResult {
        original,
        rewritten: Some(rewritten),
        applied_rules: vec!["join_to_in"],
        warnings,
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

pub fn detect_n1_from_log(log_contents: &str, opts: N1Options) -> N1Report {
    let threshold = opts.threshold.max(1);
    let window = opts.window.max(1);

    let mut window_queue: VecDeque<String> = VecDeque::new();
    let mut window_counts: HashMap<String, usize> = HashMap::new();
    let mut total_counts: HashMap<String, usize> = HashMap::new();
    let mut max_in_window: HashMap<String, usize> = HashMap::new();

    for line in log_contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with("--") {
            continue;
        }

        let template = normalize_query_template(line);
        if template.is_empty() {
            continue;
        }

        *total_counts.entry(template.clone()).or_default() += 1;

        window_queue.push_back(template.clone());
        let current = {
            let entry = window_counts.entry(template.clone()).or_default();
            *entry += 1;
            *entry
        };
        max_in_window
            .entry(template.clone())
            .and_modify(|max| *max = (*max).max(current))
            .or_insert(current);

        if window_queue.len() > window {
            if let Some(oldest) = window_queue.pop_front() {
                if let Some(count) = window_counts.get_mut(&oldest) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        window_counts.remove(&oldest);
                    }
                }
            }
        }
    }

    let mut findings = max_in_window
        .into_iter()
        .filter_map(|(template, max_count_in_window)| {
            if max_count_in_window < threshold {
                return None;
            }
            let total_count = total_counts
                .get(&template)
                .copied()
                .unwrap_or(max_count_in_window);
            Some(N1Finding {
                template,
                max_count_in_window,
                total_count,
            })
        })
        .collect::<Vec<_>>();
    findings.sort_by(|a, b| {
        b.max_count_in_window
            .cmp(&a.max_count_in_window)
            .then_with(|| a.template.cmp(&b.template))
    });

    N1Report {
        threshold,
        window,
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

#[derive(Debug, Clone)]
struct ResolveContext {
    alias_to_table: HashMap<String, String>,
    base_table: Option<String>,
}

fn collect_index_candidates_from_statement(
    statement: &Statement,
    out: &mut BTreeSet<(String, String)>,
) -> Result<(), SqlOptError> {
    match statement {
        Statement::Query(query) => {
            collect_index_candidates_from_query(query, out);
            Ok(())
        }
        _ => Err(SqlOptError::Unsupported(
            "only queries are supported".to_string(),
        )),
    }
}

fn collect_index_candidates_from_query(query: &Query, out: &mut BTreeSet<(String, String)>) {
    if let Some(with) = query.with.as_ref() {
        for cte in &with.cte_tables {
            collect_index_candidates_from_query(&cte.query, out);
        }
    }

    collect_index_candidates_from_setexpr(query.body.as_ref(), out);
}

fn collect_index_candidates_from_setexpr(setexpr: &SetExpr, out: &mut BTreeSet<(String, String)>) {
    match setexpr {
        SetExpr::Select(select) => collect_index_candidates_from_select(select.as_ref(), out),
        SetExpr::Query(query) => collect_index_candidates_from_query(query.as_ref(), out),
        SetExpr::SetOperation { left, right, .. } => {
            collect_index_candidates_from_setexpr(left.as_ref(), out);
            collect_index_candidates_from_setexpr(right.as_ref(), out);
        }
        _ => {}
    }
}

fn collect_index_candidates_from_select(
    select: &sqlparser::ast::Select,
    out: &mut BTreeSet<(String, String)>,
) {
    let alias_to_table = build_alias_to_table(select);
    let base_table = if select.from.len() == 1 && select.from[0].joins.is_empty() {
        alias_to_table
            .values()
            .next()
            .cloned()
            .or_else(|| table_factor_name(&select.from[0].relation))
    } else {
        None
    };

    let ctx = ResolveContext {
        alias_to_table,
        base_table,
    };

    if let Some(expr) = select.prewhere.as_ref() {
        collect_predicate_candidates(expr, &ctx, out);
    }
    if let Some(expr) = select.selection.as_ref() {
        collect_predicate_candidates(expr, &ctx, out);
    }
    if let Some(expr) = select.having.as_ref() {
        collect_predicate_candidates(expr, &ctx, out);
    }
    if let Some(expr) = select.qualify.as_ref() {
        collect_predicate_candidates(expr, &ctx, out);
    }

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                collect_subqueries(expr, out);
            }
            SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => {}
        }
    }

    for table_with_joins in &select.from {
        collect_index_candidates_from_table_factor(&table_with_joins.relation, out);
        for join in &table_with_joins.joins {
            collect_index_candidates_from_table_factor(&join.relation, out);
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
                collect_predicate_candidates(expr, &ctx, out);
            }
        }
    }
}

fn collect_index_candidates_from_table_factor(
    table_factor: &TableFactor,
    out: &mut BTreeSet<(String, String)>,
) {
    match table_factor {
        TableFactor::Derived { subquery, .. } => collect_index_candidates_from_query(subquery, out),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            if let TableFactor::Derived { subquery, .. } = &table_with_joins.relation {
                collect_index_candidates_from_query(subquery, out);
            }
            for join in &table_with_joins.joins {
                collect_index_candidates_from_table_factor(&join.relation, out);
                if let JoinOperator::Join(sqlparser::ast::JoinConstraint::On(expr))
                | JoinOperator::Inner(sqlparser::ast::JoinConstraint::On(expr))
                | JoinOperator::Left(sqlparser::ast::JoinConstraint::On(expr))
                | JoinOperator::LeftOuter(sqlparser::ast::JoinConstraint::On(expr))
                | JoinOperator::Right(sqlparser::ast::JoinConstraint::On(expr))
                | JoinOperator::RightOuter(sqlparser::ast::JoinConstraint::On(expr))
                | JoinOperator::FullOuter(sqlparser::ast::JoinConstraint::On(expr))
                | JoinOperator::CrossJoin(sqlparser::ast::JoinConstraint::On(expr)) =
                    &join.join_operator
                {
                    collect_subqueries(expr, out);
                }
            }
        }
        _ => {}
    }
}

fn collect_predicate_candidates(
    expr: &Expr,
    ctx: &ResolveContext,
    out: &mut BTreeSet<(String, String)>,
) {
    if let Some(column_ref) = column_ref(expr) {
        if let Some(table) = resolve_table_for_column_ref(&column_ref, ctx) {
            out.insert((table, column_ref.column));
        }
        return;
    }

    match expr {
        Expr::BinaryOp { left, right, .. } => {
            collect_predicate_candidates(left, ctx, out);
            collect_predicate_candidates(right, ctx, out);
        }
        Expr::UnaryOp { expr, .. } => collect_predicate_candidates(expr, ctx, out),
        Expr::Nested(expr) => collect_predicate_candidates(expr, ctx, out),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_predicate_candidates(expr, ctx, out);
            collect_predicate_candidates(low, ctx, out);
            collect_predicate_candidates(high, ctx, out);
        }
        Expr::InList { expr, list, .. } => {
            collect_predicate_candidates(expr, ctx, out);
            for item in list {
                collect_predicate_candidates(item, ctx, out);
            }
        }
        Expr::InSubquery { expr, subquery, .. } => {
            collect_predicate_candidates(expr, ctx, out);
            collect_index_candidates_from_query(subquery, out);
        }
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery) => {
            collect_index_candidates_from_query(subquery, out);
        }
        Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr) => collect_predicate_candidates(expr, ctx, out),
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => {
            collect_predicate_candidates(expr, ctx, out);
            collect_predicate_candidates(pattern, ctx, out);
        }
        Expr::Cast { expr, .. } => collect_predicate_candidates(expr, ctx, out),
        Expr::Function(func) => {
            collect_function_subqueries(func, out);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand.as_ref() {
                collect_predicate_candidates(operand, ctx, out);
            }
            for when in conditions {
                collect_predicate_candidates(&when.condition, ctx, out);
                collect_predicate_candidates(&when.result, ctx, out);
            }
            if let Some(else_result) = else_result.as_ref() {
                collect_predicate_candidates(else_result, ctx, out);
            }
        }
        Expr::Tuple(items) => {
            for item in items {
                collect_predicate_candidates(item, ctx, out);
            }
        }
        Expr::GroupingSets(items) | Expr::Cube(items) | Expr::Rollup(items) => {
            for group in items {
                for item in group {
                    collect_predicate_candidates(item, ctx, out);
                }
            }
        }
        _ => {}
    }
}

fn collect_subqueries(expr: &Expr, out: &mut BTreeSet<(String, String)>) {
    match expr {
        Expr::InSubquery { subquery, .. }
        | Expr::Exists { subquery, .. }
        | Expr::Subquery(subquery) => {
            collect_index_candidates_from_query(subquery, out);
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_subqueries(left, out);
            collect_subqueries(right, out);
        }
        Expr::UnaryOp { expr, .. } => collect_subqueries(expr, out),
        Expr::Nested(expr) => collect_subqueries(expr, out),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_subqueries(expr, out);
            collect_subqueries(low, out);
            collect_subqueries(high, out);
        }
        Expr::InList { expr, list, .. } => {
            collect_subqueries(expr, out);
            for item in list {
                collect_subqueries(item, out);
            }
        }
        Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr) => collect_subqueries(expr, out),
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => {
            collect_subqueries(expr, out);
            collect_subqueries(pattern, out);
        }
        Expr::Cast { expr, .. } => collect_subqueries(expr, out),
        Expr::Function(func) => collect_function_subqueries(func, out),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand.as_ref() {
                collect_subqueries(operand, out);
            }
            for when in conditions {
                collect_subqueries(&when.condition, out);
                collect_subqueries(&when.result, out);
            }
            if let Some(else_result) = else_result.as_ref() {
                collect_subqueries(else_result, out);
            }
        }
        Expr::Tuple(items) => {
            for item in items {
                collect_subqueries(item, out);
            }
        }
        Expr::GroupingSets(items) | Expr::Cube(items) | Expr::Rollup(items) => {
            for group in items {
                for item in group {
                    collect_subqueries(item, out);
                }
            }
        }
        _ => {}
    }
}

fn collect_function_subqueries(
    func: &sqlparser::ast::Function,
    out: &mut BTreeSet<(String, String)>,
) {
    match &func.args {
        sqlparser::ast::FunctionArguments::None => {}
        sqlparser::ast::FunctionArguments::Subquery(query) => {
            collect_index_candidates_from_query(query, out);
        }
        sqlparser::ast::FunctionArguments::List(list) => {
            for arg in &list.args {
                match arg {
                    sqlparser::ast::FunctionArg::Unnamed(arg) => {
                        if let sqlparser::ast::FunctionArgExpr::Expr(expr) = arg {
                            collect_subqueries(expr, out);
                        }
                    }
                    sqlparser::ast::FunctionArg::Named { arg, .. } => {
                        if let sqlparser::ast::FunctionArgExpr::Expr(expr) = arg {
                            collect_subqueries(expr, out);
                        }
                    }
                    sqlparser::ast::FunctionArg::ExprNamed { name, arg, .. } => {
                        collect_subqueries(name, out);
                        if let sqlparser::ast::FunctionArgExpr::Expr(expr) = arg {
                            collect_subqueries(expr, out);
                        }
                    }
                }
            }
        }
    }
}

fn resolve_table_for_column_ref(column_ref: &ColumnRef, ctx: &ResolveContext) -> Option<String> {
    match column_ref.qualifier.as_deref() {
        Some(qualifier) => Some(
            ctx.alias_to_table
                .get(qualifier)
                .cloned()
                .unwrap_or_else(|| qualifier.to_string()),
        ),
        None => ctx.base_table.clone(),
    }
}

fn select_has_wildcard(select: &sqlparser::ast::Select) -> bool {
    select.projection.iter().any(|item| match item {
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => true,
        SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. } => false,
    })
}

fn projection_is_safe_for_join_to_in(projection: &[SelectItem], left_qualifier: &str) -> bool {
    if projection.is_empty() {
        return false;
    }

    for item in projection {
        match item {
            SelectItem::Wildcard(_) => return false,
            SelectItem::QualifiedWildcard(kind, _) => match kind {
                sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(object_name) => {
                    let qualifier = object_name
                        .0
                        .last()
                        .and_then(ObjectNamePart::as_ident)
                        .map(|ident| ident.value.as_str());
                    if qualifier != Some(left_qualifier) {
                        return false;
                    }
                }
                sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(_) => return false,
            },
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                let mut refs = Vec::new();
                collect_column_refs(expr, &mut refs);
                for column_ref in refs {
                    match column_ref.qualifier.as_deref() {
                        Some(q) if q == left_qualifier => {}
                        _ => return false,
                    }
                }
            }
        }
    }

    true
}

fn projection_sql(projection: &[SelectItem]) -> String {
    projection
        .iter()
        .map(|item| item.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn estimate_index_benefit_ms(table: &str, columns: &[&str]) -> (u64, u64, u8) {
    let row_count = mock_row_count(table);
    let before_ms = (row_count / 4_000).max(1);

    let mut key = String::new();
    key.push_str(table);
    for col in columns {
        key.push('.');
        key.push_str(col);
    }

    let hash = fnv1a64(key.as_bytes());
    let divisor = 10 + (hash % 200); // 10..=209
    let after_ms = (before_ms / divisor).max(1);

    let pct = if before_ms <= after_ms {
        0
    } else {
        (((before_ms - after_ms) * 100) / before_ms).min(99) as u8
    };

    (before_ms, after_ms, pct)
}

fn mock_row_count(table: &str) -> u64 {
    // 50k..=5M rows (deterministic).
    let hash = fnv1a64(table.as_bytes());
    50_000 + (hash % 4_950_001)
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x00000100000001B3;

    let mut hash = OFFSET_BASIS;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
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
        Expr::Function(mut func) => {
            strip_qualifier_in_function_arguments(&mut func.parameters, qualifier);
            strip_qualifier_in_function_arguments(&mut func.args, qualifier);

            if let Some(filter) = func.filter.as_mut() {
                **filter = strip_qualifier((**filter).clone(), qualifier);
            }
            for order_by in &mut func.within_group {
                strip_qualifier_in_order_by_expr(order_by, qualifier);
            }

            Expr::Function(func)
        }
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

fn strip_qualifier_in_function_arguments(
    arguments: &mut sqlparser::ast::FunctionArguments,
    qualifier: &str,
) {
    match arguments {
        sqlparser::ast::FunctionArguments::None => {}
        sqlparser::ast::FunctionArguments::Subquery(_) => {}
        sqlparser::ast::FunctionArguments::List(list) => {
            for arg in &mut list.args {
                match arg {
                    sqlparser::ast::FunctionArg::Named { arg, .. }
                    | sqlparser::ast::FunctionArg::Unnamed(arg) => {
                        strip_qualifier_in_function_arg_expr(arg, qualifier);
                    }
                    sqlparser::ast::FunctionArg::ExprNamed { name, arg, .. } => {
                        *name = strip_qualifier(name.clone(), qualifier);
                        strip_qualifier_in_function_arg_expr(arg, qualifier);
                    }
                }
            }

            for clause in &mut list.clauses {
                match clause {
                    sqlparser::ast::FunctionArgumentClause::OrderBy(order_by) => {
                        for expr in order_by {
                            strip_qualifier_in_order_by_expr(expr, qualifier);
                        }
                    }
                    sqlparser::ast::FunctionArgumentClause::Limit(expr) => {
                        *expr = strip_qualifier(expr.clone(), qualifier);
                    }
                    _ => {}
                }
            }
        }
    }
}

fn strip_qualifier_in_function_arg_expr(
    expr: &mut sqlparser::ast::FunctionArgExpr,
    qualifier: &str,
) {
    match expr {
        sqlparser::ast::FunctionArgExpr::Expr(expr) => {
            *expr = strip_qualifier(expr.clone(), qualifier);
        }
        sqlparser::ast::FunctionArgExpr::QualifiedWildcard(_)
        | sqlparser::ast::FunctionArgExpr::Wildcard => {}
    }
}

fn strip_qualifier_in_order_by_expr(order_by: &mut sqlparser::ast::OrderByExpr, qualifier: &str) {
    order_by.expr = strip_qualifier(order_by.expr.clone(), qualifier);

    if let Some(with_fill) = order_by.with_fill.as_mut() {
        if let Some(expr) = with_fill.from.as_mut() {
            *expr = strip_qualifier(expr.clone(), qualifier);
        }
        if let Some(expr) = with_fill.to.as_mut() {
            *expr = strip_qualifier(expr.clone(), qualifier);
        }
        if let Some(expr) = with_fill.step.as_mut() {
            *expr = strip_qualifier(expr.clone(), qualifier);
        }
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
        let report = detect_n1_from_log(
            log,
            N1Options {
                threshold: 5,
                window: 10,
            },
        );
        assert_eq!(report.findings.len(), 1);
        assert_eq!(report.threshold, 5);
        assert_eq!(report.window, 10);
        assert_eq!(report.findings[0].max_count_in_window, 5);
        assert_eq!(report.findings[0].total_count, 5);
        assert_eq!(
            report.findings[0].template,
            "select * from users where id = ?"
        );
    }

    #[test]
    fn rewrite_join_to_in_works_for_simple_filter_join() {
        let query =
            "SELECT o.* FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = true";
        let rewritten = rewrite(query).unwrap().rewritten.unwrap();
        assert_eq!(
            rewritten,
            "SELECT o.* FROM orders o WHERE o.user_id IN (SELECT id FROM users WHERE active = true)"
        );
    }

    #[test]
    fn rewrite_join_to_in_refuses_when_projection_mentions_joined_table() {
        let query = "SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = true";
        let result = rewrite(query).unwrap();
        assert!(result.rewritten.is_none());
    }

    #[test]
    fn rewrite_join_to_in_refuses_when_projection_is_unqualified_or_star() {
        let query = "SELECT * FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = true";
        let result = rewrite(query).unwrap();
        assert!(result.rewritten.is_none());

        let query =
            "SELECT name FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = true";
        let result = rewrite(query).unwrap();
        assert!(result.rewritten.is_none());
    }

    #[test]
    fn rewrite_join_to_in_strips_qualifier_inside_function_where() {
        let query =
            "SELECT o.* FROM orders o JOIN users u ON o.user_id = u.id WHERE upper(u.name) = 'BOB'";
        let rewritten = rewrite(query).unwrap().rewritten.unwrap();
        assert_eq!(
            rewritten,
            "SELECT o.* FROM orders o WHERE o.user_id IN (SELECT id FROM users WHERE upper(name) = 'BOB')"
        );
    }

    #[test]
    fn analyze_suggests_indexes_for_where_column() {
        let result = analyze("SELECT * FROM users WHERE age > 18").unwrap();
        assert_eq!(result.index_suggestions.len(), 1);
        assert_eq!(result.index_suggestions[0].table, "users");
        assert_eq!(result.index_suggestions[0].columns, vec!["age"]);
        assert_eq!(
            result.index_suggestions[0].ddl,
            "CREATE INDEX CONCURRENTLY idx_users_age ON users(age);"
        );
    }
}
