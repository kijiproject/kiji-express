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

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiInvalidNameException
import org.kiji.express.util.AvroUtil

/**
 * Interface for all column output request specification objects.
 *
 * Note that the subclasses of ColumnRequestOutput are case classes, and so they override
 * ColumnRequestOutput's abstract methods (e.g., schema) with vals.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
trait ColumnRequestOutput {

  /**
   * Family which this [[org.kiji.express.flow.ColumnRequestOutput]] belongs to.
   *
   * @return family name of column
   */
  def family: String

  /**
   * [[org.kiji.schema.KijiColumnName]] of this [[org.kiji.express.flow.ColumnRequestOutput]].
   *
   *  @return the name of the column this ColumnRequest specifies.
   */
  def columnName: KijiColumnName

  /**
   * [[org.apache.avro.Schema]] to be written to the output column.
   * @return
   */
  def schema: Option[Schema]

  /**
   * Make a best effort attempt to encode a provided value to a type that will be compatible with
   * the column.  If no such conversion can be made, the original value will be returned.
   */
  def encode: Any => Any = schema.map(AvroUtil.avroEncoder).getOrElse(identity)
}

/**
 * Specification for writing to a Kiji column.
 *
 * @param family The group-type family containing the column to write to.
 * @param qualifier The column qualifier to write to.
 * @param tSchema The schema to write with.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class QualifiedColumnRequestOutput(
    family: String,
    qualifier: String,
    @transient tSchema: Option[Schema] = None
) extends ColumnRequestOutput {
  // Schema is not Serializable, so only hold the JSON representation in non-transient state
  val schemaJson = tSchema.map(_.toString(false))
  lazy val schema = schemaJson.map(new Schema.Parser().parse)
  @transient lazy val columnName: KijiColumnName = new KijiColumnName(family, qualifier)
}

/**
 * Companion object to [[org.kiji.express.flow.QualifiedColumnRequestOutput]] which provides
 * convenient factory functions.
 */
@ApiAudience.Public
@ApiStability.Experimental
object QualifiedColumnRequestOutput {
  /**
   * Convenience factory function for [[org.kiji.express.flow.QualifiedColumnRequestOutput]].
   * This constructor takes an unwrapped schema.
   *
   * @param family The group-type family containing the column to write to.
   * @param qualifier The column qualifier to write to.
   * @param schema The schema to write with.  If None, the schema is resolved from the data
   *               type at runtime.
   */
  def apply(
    family: String,
    qualifier: String,
    schema: Schema
  ): QualifiedColumnRequestOutput = {
    QualifiedColumnRequestOutput(family, qualifier, Some(schema))
  }

  /**
   * Convenience factory function for [[org.kiji.express.flow.QualifiedColumnRequestOutput]].
   * This constructor takes a column string which should contain the column family and qualifier
   * in the form 'family:qualifier'.
   *
   * @param column The group-type family and column in format 'family:column'.
   * @param schema The schema to write with.
   */
  def apply(
      column: String,
      schema: Option[Schema]
  ): QualifiedColumnRequestOutput = {
    column.split(':').toList match {
      case family :: qualifier :: Nil => QualifiedColumnRequestOutput(family, qualifier, schema)
      case _ => throw new IllegalArgumentException(
          "Must specify column to GroupTypeOutputColumnSpec in the format 'family:qualifier'.")
    }
  }

  /**
   * Convenience factory function for [[org.kiji.express.flow.QualifiedColumnRequestOutput]].
   * This constructor takes a column string which should contain the column family and qualifier
   * in the form 'family:qualifier', and an unwrapped schema.
   *
   * @param column The group-type family and column in format 'family:column'.
   * @param schema The schema to write with.
   */
  def apply(
      column: String,
      schema: Schema
  ): QualifiedColumnRequestOutput = {
    QualifiedColumnRequestOutput(column, Some(schema))
  }

  /**
   * Convenience factory function for [[org.kiji.express.flow.QualifiedColumnRequestOutput]].
   * This constructor takes a column string which should contain the column family and qualifier
   * in the form 'family:qualifier', and no schema.
   *
   * @param column The group-type family and column in format 'family:column'.
   */
  def apply(
      column: String
  ): QualifiedColumnRequestOutput = {
    QualifiedColumnRequestOutput(column, None)
  }
}

/**
 * Specification for writing to a Kiji column family.
 *
 * @param family The map-type family to write to.
 * @param qualifierSelector The field in the Express flow indicating what column to write to.
 * @param tSchema The schema to use for writes.  If None, the schema is resolved from the data
 *               type at runtime.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class ColumnFamilyRequestOutput(
    family: String,
    qualifierSelector: Symbol,
    @transient tSchema: Option[Schema] = None
) extends ColumnRequestOutput {
  if (family.contains(':')) {
    throw new KijiInvalidNameException("Cannot have a ':' in family name for column family request")
  }
  // Schema is not Serializable, so only hold the JSON representation in non-transient state
  val schemaJson = tSchema.map(_.toString(false))
  lazy val schema = schemaJson.map(new Schema.Parser().parse)
  lazy val columnName: KijiColumnName = new KijiColumnName(family)
}

/**
 * Companion object to [[org.kiji.express.flow.ColumnFamilyRequestOutput]] which provides
 * convenient factory functions.
 */
@ApiAudience.Public
@ApiStability.Experimental
object ColumnFamilyRequestOutput {
  /**
   * Convenience factory function for [[org.kiji.express.flow.ColumnFamilyRequestOutput]].
   * This constructor takes an unwrapped schema.
   *
   * @param family The map-type family to write to.
   * @param qualifierSelector The field in the Express flow indicating what column to write to.
   * @param schema The schema to use for writes.
   */
  def apply(
      family: String,
      qualifierSelector: Symbol,
      schema: Schema
  ): ColumnFamilyRequestOutput = {
    ColumnFamilyRequestOutput(family, qualifierSelector, Some(schema))
  }
}
