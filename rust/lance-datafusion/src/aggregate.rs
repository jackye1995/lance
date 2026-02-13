// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Aggregate specification for DataFusion aggregates.

use datafusion::logical_expr::Expr;

use crate::planner::Planner;

/// Aggregate specification with group by and aggregate expressions.
#[derive(Debug, Clone)]
pub struct Aggregate {
    /// Expressions to group by (e.g., column references).
    pub group_by: Vec<Expr>,
    /// Aggregate function expressions (e.g., SUM, COUNT, AVG).
    /// Use `.alias()` on the expression to set output column names.
    pub aggregates: Vec<Expr>,
    /// Column names required by this aggregate (computed at construction).
    /// For COUNT(*), this is empty. For SUM(x), GROUP BY y, this contains [x, y].
    pub required_columns: Vec<String>,
}

impl Aggregate {
    /// Create a new Aggregate, computing required columns from the expressions.
    pub fn new(group_by: Vec<Expr>, aggregates: Vec<Expr>) -> Self {
        let mut required_columns = Vec::new();
        for expr in group_by.iter().chain(aggregates.iter()) {
            required_columns.extend(Planner::column_names_in_expr(expr));
        }
        required_columns.sort();
        required_columns.dedup();
        Self {
            group_by,
            aggregates,
            required_columns,
        }
    }
}
