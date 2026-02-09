// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Aggregate specification for DataFusion aggregates.

use datafusion::logical_expr::Expr;

/// Aggregate specification with group by and aggregate expressions.
#[derive(Debug, Clone)]
pub struct Aggregate {
    pub group_by: Vec<Expr>,
    pub aggregates: Vec<Expr>,
    /// Output column names in order: group_by columns first, then aggregates.
    pub output_names: Vec<String>,
}
