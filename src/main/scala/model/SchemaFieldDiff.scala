package model

import model.types.SchemaIssue

case class SchemaFieldDiff(
                            fieldName: String,
                            issue: SchemaIssue,
                          )
