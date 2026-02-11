// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Aggregate specification for DataFusion aggregates.

use datafusion::logical_expr::Expr;

/// Aggregate specification with group by and aggregate expressions.
#[derive(Debug, Clone)]
pub struct Aggregate {
    /// Expressions to group by (e.g., column references).
    pub group_by: Vec<Expr>,
    /// Aggregate function expressions (e.g., SUM, COUNT, AVG).
    /// Use `.alias()` on the expression to set output column names.
    pub aggregates: Vec<Expr>,
}
