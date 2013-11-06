/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.express.flow

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.SchemaSpec
import org.kiji.express.SchemaSpec.Writer
import org.kiji.express.SchemaSpec.Specific
import org.kiji.express.SchemaSpec.Generic
import org.kiji.express.KijiSlice
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiInvalidNameException
import org.kiji.schema.filter.KijiColumnFilter

@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
/**
 * Interface for all column input request specification objects.
 *
 * Note that the subclasses of ColumnRequestInput are case classes, and so they override
 * ColumnRequestInput's abstract methods (e.g., schema) with vals.
 */
trait ColumnRequestInput {

  /**
   * Family which this [[org.kiji.express.flow.ColumnRequestInput]] belongs to.
   *
   * @return family name of column
   */
  def family: String

  /**
   * The [[org.kiji.schema.KijiColumnName]] of the column.
   */
  def columnName: KijiColumnName

  /**
   * Specifies the schema of data to be read from the column.
   */
  def schema: SchemaSpec

  /**
   * Specifies the maximum number of cells (from the most recent) to retrieve from a column.
   *
   * By default, only the most recent cell is retrieved.
   */
  def maxVersions: Int

  /**
   * Specifies a filter that a cell must pass for this request to retrieve it.
   *
   * If None, no filter is used.
   */
  def filter: Option[KijiColumnFilter]

  /**
   * Specifies a default value to use for missing cells during a read.
   *
   * If None, rows with missing values are ignored.
   */
  def default: Option[KijiSlice[_]]

  /**
   * Specifies the maximum number of cells to maintain in memory when paging through a column.
   *
   * If None, paging is disabled.
   */
  def pageSize: Option[Int]
}

object ColumnRequestInput {

  /**
   * Convenience function for creating a [[org.kiji.express.flow.ColumnRequestInput]].  The input
   * spec will be for a qualified column if the column parameter contains a ':',
   * otherwise the input will assumed to be for a column family.
   *
   * @param column The requested column name.
   * @param schema of data to read from column.  Defaults to default reader schema.
   * @return ColumnRequestInput with supplied options.
   */
  def apply(
    column: String,
    maxVersions: Int = latest,
    filter: Option[KijiColumnFilter] = None,
    default: Option[KijiSlice[_]] = None,
    pageSize: Option[Int] = None,
    schema: SchemaSpec = Writer
  ): ColumnRequestInput = {
    column.split(':').toList match {
      case family :: qualifier :: Nil =>
        QualifiedColumnRequestInput(family, qualifier, maxVersions, filter, default, pageSize,
            schema)
      case family :: Nil =>
        ColumnFamilyRequestInput(family, maxVersions, filter, default, pageSize, schema)
      case _ => throw new IllegalArgumentException("column name must contain 'family:qualifier'" +
          " for a group-type, or 'family' for a map-type column.")
    }
  }

  /**
   * Convenience function for creating a [[org.kiji.express.flow.ColumnRequestInput]].  The input
   * spec will be for a qualified column if the column parameter contains a ':',
   * otherwise the input will assumed to be for a column family. The column will be read with the
   * schema of the provided specific Avro record.
   *
   * @param column The requested column name.
   * @param specificRecord class to read from the column.
   * @return ColumnRequestInput with supplied options.
   */
  def apply(
      column: String,
      specificRecord: Class[_ <: SpecificRecord]
  ): ColumnRequestInput = {
    ColumnRequestInput(column, schema = Specific(specificRecord))
  }

  /**
   * Convenience function for creating a [[org.kiji.express.flow.ColumnRequestInput]].  The input
   * spec will be for a qualified column if the column parameter contains a ':',
   * otherwise the input will assumed to be for a column family.  The column will be read with the
   * provided generic Avro schema.
   *
   * @param column The requested column name.
   * @param schema of generic Avro type to read from the column.
   * @return ColumnRequestInput with supplied options.
   */
  def apply(
    column: String,
    schema: Schema
  ): ColumnRequestInput = {
    ColumnRequestInput(column, schema = Generic(schema))
  }
}

/**
 * Specification for reading from a fully qualified column in a Kiji table.
 *
 * @param family The requested column family name.
 * @param qualifier The requested column qualifier name.
 * @param maxVersions The maximum number of versions to read back (default is only most recent).
 * @param filter Filter to use when reading back cells (default is None).
 * @param default Default KijiSlice to return in case column is empty in row.
 * @param pageSize Maximum number of cells to request from HBase per RPC.
 * @param schema Reader schema specification.  Defaults to the default reader schema.
 */
final case class QualifiedColumnRequestInput (
    family: String,
    qualifier: String,
    maxVersions: Int = latest,
    filter: Option[KijiColumnFilter] = None,
    default: Option[KijiSlice[_]] = None,
    pageSize: Option[Int] = None,
    schema: SchemaSpec = Writer
) extends ColumnRequestInput {
  @transient override lazy val columnName: KijiColumnName = new KijiColumnName(family, qualifier)
}

object QualifiedColumnRequestInput {
  /**
   * Convenience function for creating a [[org.kiji.express.flow.QualifiedColumnRequestInput]] with
   * a specific Avro record type.
   *
   * @param family The requested column family name.
   * @param qualifier The requested column qualifier name.
   * @param specificRecord class to read from the column.
   * @return QualifiedColumnRequestInput with supplied options.
   */
  def apply(
      family: String,
      qualifier: String,
      specificRecord: Class[_ <: SpecificRecord]
  ) : QualifiedColumnRequestInput = {
    QualifiedColumnRequestInput(family, qualifier, schema = Specific(specificRecord))
  }

  /**
   * Convenience function for creating a [[org.kiji.express.flow.QualifiedColumnRequestInput]] with
   * a generic Avro type specified by a [[org.apache.avro.Schema]].
   *
   * @param family The requested column family name.
   * @param qualifier The requested column qualifier name.
   * @param schema of generic Avro type to read from the column.
   * @return QualifiedColumnRequestInput with supplied options.
   */
  def apply(
      family: String,
      qualifier: String,
      schema: Schema
  ): QualifiedColumnRequestInput = {
    QualifiedColumnRequestInput(family, qualifier, schema = Generic(schema))
  }
}

/**
 * Specification for reading from a column family in a Kiji table.
 *
 * @param family The requested column family name.
 * @param maxVersions The maximum number of versions to read back (default is only most recent).
 * @param filter Filter to use when reading back cells (default is None).
 * @param default Default KijiSlice to return in case column is empty in row.
 * @param pageSize Maximum number of cells to request from HBase per RPC.
 * @param schema Reader schema specification.  Defaults to [[org.kiji.express.SchemaSpec.Writer]].
 */
final case class ColumnFamilyRequestInput(
    family: String,
    maxVersions: Int = latest,
    filter: Option[KijiColumnFilter] = None,
    default: Option[KijiSlice[_]] = None,
    pageSize: Option[Int] = None,
    schema: SchemaSpec = Writer
) extends ColumnRequestInput {
  if (family.contains(':')) {
    throw new KijiInvalidNameException("Cannot have a ':' in family name for column family request")
  }
  @transient override lazy val columnName: KijiColumnName = new KijiColumnName(family)
}

object ColumnFamilyRequestInput {
  /**
   * Convenience function for creating a [[org.kiji.express.flow.ColumnFamilyRequestInput]] with a
   * specific Avro record type.
   *
   * @param family The requested column family name.
   * @param specificRecord class to read from the column.
   * @return ColumnFamilyRequestInput with supplied options.
   */
  def apply(
      family: String,
      specificRecord: Class[_ <: SpecificRecord]
  ): ColumnFamilyRequestInput = {
    ColumnFamilyRequestInput(family, schema = Specific(specificRecord))
  }

  /**
   * Convenience function for creating a [[org.kiji.express.flow.ColumnFamilyRequestInput]] with a
   * generic Avro type specified by a [[org.apache.avro.Schema]].
   *
   * @param family The requested column family name.
   * @param genericSchema of Avro type to read from the column.
   * @return ColumnFamilyRequestInput with supplied options.
   */
  def apply(
      family: String,
      genericSchema: Schema
  ): ColumnFamilyRequestInput = {
    ColumnFamilyRequestInput(family, schema = Generic(genericSchema))
  }
}
