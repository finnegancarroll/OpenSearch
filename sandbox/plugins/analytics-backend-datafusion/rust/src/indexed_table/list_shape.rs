/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Read-path reconciliation of scalar vs LIST column shapes across a shard's
//! parquet files.
//!
//! A field declared `multi_value` is written as an Arrow `LIST<element>` column
//! only in files that actually saw an array; files that only ever saw single
//! values stay physically scalar (per-file adaptive shape latch on the write
//! side). A shard can therefore contain a MIX of scalar and LIST files for the
//! same column. DataFusion's `Schema::try_merge` rejects that mix
//! (`List(Utf8) does not equal Utf8`), so a query spanning both files fails at
//! schema-inference time — before any batch is read.
//!
//! [`unify_list_shapes`] mirrors the merge-side reconciliation
//! (`parquet-data-format` `merge::schema`) on the READ path: it rewrites scalar
//! schemas to their LIST form ("LIST wins") so the union succeeds. Once the
//! table schema is unified to LIST, the actual scalar→singleton-list conversion
//! of the batch data is handled downstream by DataFusion's own
//! `PhysicalExprAdapter` (the ParquetSource/ParquetOpener schema rewriter), which
//! inserts a `CastExpr` that arrow implements as a singleton-list wrap — so no
//! batch-level up-cast is needed in our scan code. See the note in
//! `stream.rs::poll_inner` for the arrow source references.

use std::collections::HashMap;

use arrow::datatypes::{DataType as ArrowDataType, FieldRef, Schema as ArrowSchema};
use std::sync::Arc;

/// Rewrites scalar fields to their LIST form wherever any input schema carries
/// the same column as `LIST<scalar>`, so `ArrowSchema::try_merge` sees one
/// consistent shape per column instead of failing on a scalar/LIST conflict.
///
/// Only promotes when the scalar type equals the list's element type; a genuine
/// type conflict is left untouched and will surface the usual union error.
pub fn unify_list_shapes(schemas: Vec<ArrowSchema>) -> Vec<ArrowSchema> {
    // First pass: collect the LIST form of every column that has one.
    let mut list_fields: HashMap<String, FieldRef> = HashMap::new();
    for schema in &schemas {
        for field in schema.fields() {
            if matches!(field.data_type(), ArrowDataType::List(_)) {
                list_fields
                    .entry(field.name().clone())
                    .or_insert_with(|| Arc::clone(field));
            }
        }
    }
    if list_fields.is_empty() {
        return schemas;
    }
    // Second pass: replace scalar occurrences whose type matches the list's
    // element type.
    schemas
        .into_iter()
        .map(|schema| {
            let fields: Vec<FieldRef> = schema
                .fields()
                .iter()
                .map(|field| match list_fields.get(field.name()) {
                    Some(list_field) if !matches!(field.data_type(), ArrowDataType::List(_)) => {
                        if let ArrowDataType::List(child) = list_field.data_type() {
                            if child.data_type() == field.data_type() {
                                return Arc::clone(list_field);
                            }
                        }
                        Arc::clone(field)
                    }
                    _ => Arc::clone(field),
                })
                .collect();
            ArrowSchema::new(fields)
        })
        .collect()
}
