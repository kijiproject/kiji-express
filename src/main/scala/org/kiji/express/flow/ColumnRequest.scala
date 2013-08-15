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

import java.io.Serializable

import org.apache.hadoop.hbase.HConstants

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.Cell
import org.kiji.express.KijiSlice
import org.kiji.schema.KijiColumnName
import org.kiji.schema.filter.KijiColumnFilter

/**
 * A trait that marks case classes that hold column-level options for cell requests to Kiji.
 *
 * End-users receive instances of this trait, used to request cells from qualified columns or
 * map-type column families, using the factories [[org.kiji.express.flow.Column]] and
 * [[org.kiji.express.flow.MapFamily]]. They can then use these requests to obtain a
 * [[org.kiji.express.flow.KijiSource]] that reads cells into tuples while obeying the specified
 * request options.
 *
 * If desired, end-users can add information about how to handle missing values in this column,
 * with the methods `replaceMissingWith` or `ignoreMissing`, only one of which can be called before
 * the column request is used.
 *
 * If a ColumnRequest is used without calling `replaceMissingWith` or `ignoreMissing`,
 * `ignoreMissing` is the default.
 */
@ApiAudience.Public
@ApiStability.Experimental
private[express] sealed trait ColumnRequest extends Serializable {
  /**
   * Specifies that missing values on this column mean the row should be skipped.
   * This is the default behavior if neither this nor `replaceMissingWith` are called
   * on a ColumnRequest.
   *
   * @return this ColumnRequest with skipping behavior configured.
   */
  def ignoreMissing(): ColumnRequest

  /**
   * Returns the standard KijiColumnName representation of the name of the column this
   * ColumnRequests is for.
   *
   * @return the name of the column this ColumnRequest specifies.
   */
  private[express] def getColumnName(): KijiColumnName

  /**
   * Specifies that null values should not be stored in hbase.
   * This is NOT the default behavior.
   *
   * @return this ColumnRequest with null-skipping on write configured.
   */
  def skipNullsOnWrite: ColumnRequest
}

/**
 * A request for cells from a fully qualified column in a Kiji table.
 *
 * @param family of columns that the requested column belongs to.
 * @param qualifier of the requested column.
 * @param options that will be used to request cells from the column. If unspecified, default
 *     request options will be used.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class QualifiedColumn private[express] (
    family: String,
    qualifier: String,
    options: ColumnRequestOptions = ColumnRequestOptions())
    extends ColumnRequest {

  /**
   * Specifies a single value to be used as the default when reading rows missing a value for this
   * column. The value will be used to create a slice for the column with the value at the current
   * timestamp.
   *
   * @param datum to use if any row is missing this column.
   * @return this ColumnRequest with replacement configured.
   */
  def replaceMissingWith[T](datum: T): QualifiedColumn = {
    val replacement: KijiSlice[T] =
        new KijiSlice(List(Cell(family, qualifier, HConstants.LATEST_TIMESTAMP, datum)))
    return copy(options = options.newWithReplacement(Some(replacement)))
  }

  /**
   * Specifies a single value and its version to be used as the default when reading rows missing a
   * value for this column. The value will be used to create a slice for the column with the value
   * at the specified version.
   *
   * @param version of the datum to use if any row is missing this column.
   * @param datum to use if any row is missing this column.
   * @return this ColumnRequest with replacement configured.
   */
  def replaceMissingWithVersioned[T](version: Long, datum: T): QualifiedColumn = {
    val replacement: KijiSlice[T] =
        new KijiSlice(List(Cell(family, qualifier, version, datum)))
    return copy(options = options.newWithReplacement(Some(replacement)))
  }

  /**
   * Specifies a sequence of data to be used as the default when reading rows missing a value for
   * this column. The data will be used to create a slice for the column with each value at the
   * current timestamp.
   *
   * @param data to use if any row is missing this column.
   * @return this ColumnRequest with replacement configured.
   */
  def replaceMissingWith[T](data: Seq[T]): QualifiedColumn = {
    val replacement: KijiSlice[T] =
      new KijiSlice(data.map { datum: T =>
        Cell(family, qualifier, HConstants.LATEST_TIMESTAMP, datum) })
    return copy(options = options.newWithReplacement(Some(replacement)))
  }

  /**
   * Specifies a sequence of data, as (version, datum), to be used as the default when reading rows
   * missing a value for this column. The data will be used to create a slice for the column with
   * each datum at the specified version.
   *
   * @param versionedData is tuples of (version, datum) to use if any row is missing this column.
   * @return this ColumnRequest with replacement configured.
   */
  def replaceMissingWithVersioned[T](versionedData: Seq[(Long, T)]): QualifiedColumn = {
    val replacement: KijiSlice[_] =
      new KijiSlice(versionedData.map { case (version: Long, datum: Any) =>
        Cell(family, qualifier, version, datum) })
    return copy(options = options.newWithReplacement(Some(replacement)))
  }

  override def ignoreMissing(): QualifiedColumn = {
    return copy(options = options.newWithReplacement(None))
  }

  override def getColumnName(): KijiColumnName = new KijiColumnName(family, qualifier)

  override def skipNullsOnWrite: QualifiedColumn = copy(options = options.newSkipNullsOnWrite)
}

/**
 * A request for cells from columns in a map-type column family in a Kiji table.
 *
 * @param family (map-type) of the Kiji table whose columns are being requested.
 * @param qualifierSelector is the name of the field in the tuple that will contain the qualifier
 *     to write to.
 * @param options that will be used to request cells from the columns of the family. If
 *     unspecified, default request options are used.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class ColumnFamily private[express] (
    family: String,
    qualifierSelector: Option[String] = None,
    options: ColumnRequestOptions = ColumnRequestOptions())
    extends ColumnRequest {

  /**
   * Specifies a single value, along with its qualifier, to be used as the default when reading rows
   * missing a value for this column. The value will be used to create a slice for the column with
   * the value at the specified qualifier and the current timestamp.
   *
   * @param qualifier of the data to use if any row is missing this column.
   * @param datum to use if any row is missing this column.
   * @return this ColumnRequest with replacement configured.
   */
  def replaceMissingWith[T](qualifier: String, datum: T): ColumnFamily = {
    val replacement: KijiSlice[T] =
        new KijiSlice(List(Cell(family, qualifier, HConstants.LATEST_TIMESTAMP, datum)))
    return copy(options = options.newWithReplacement(Some(replacement)))
  }

  /**
   * Specifies a single value, its qualifier, and its version to be used as the default when reading
   * rows missing a value for this column. The value will be used to create a slice for the column
   * with the value at the specified qualifier and version.
   *
   * @param qualifier of the data to use if any row is missing this column.
   * @param version of the datum to use if any row is missing this column.
   * @param datum to use if any row is missing this column.
   * @return this ColumnRequest with replacement configured.
   */
  def replaceMissingWithVersioned[T](qualifier: String, version: Long, datum: T): ColumnFamily = {
    val replacement: KijiSlice[T] =
        new KijiSlice(List(Cell(family, qualifier, version, datum)))
    return copy(options = options.newWithReplacement(Some(replacement)))
  }

  /**
   * Specifies a sequence of data along with qualifiers for each datum, as (qualifier, datum), to be
   * used as the default when reading rows missing a value for this column. The data will be used to
   * create a slice for the column with each value at the corresponding specified qualifier and the
   * current timestamp.
   *
   * @param data to use if any row is missing this column. It is in (qualifier, datum) tuples.
   * @return this ColumnRequest with replacement configured.
   */
  def replaceMissingWith[T](data: Seq[(String, T)]): ColumnFamily = {
    val replacement: KijiSlice[_] =
      new KijiSlice(data.map { case (qualifier: String, datum: Any) =>
        Cell(family, qualifier, HConstants.LATEST_TIMESTAMP, datum) })
    return copy(options = options.newWithReplacement(Some(replacement)))
  }

  /**
   * Specifies a sequence of data, as (qualifier, version, datum), to be used as the default when
   * reading rows missing a value for this column. The data will be used to create a slice for the
   * column with each datum at the specified qualifier and version.
   *
   * @param versionedData is tuples of (qualifier, version, datum) to use if any row is missing
   *     this column.
   * @return this ColumnRequest with replacement configured.
   */
  def replaceMissingWithVersioned[T](
      versionedData: Seq[(String, Long, T)]): ColumnFamily = {
    val replacement: KijiSlice[_] =
      new KijiSlice(versionedData.map { case (qualifier: String, ts: Long, datum: Any) =>
        Cell(family, qualifier, ts, datum) })
    return copy(options = options.newWithReplacement(Some(replacement)))
  }

  override def ignoreMissing(): ColumnFamily = {
    return new ColumnFamily(family, qualifierSelector, options.newWithReplacement(None))
  }

  override def getColumnName(): KijiColumnName = new KijiColumnName(family)

  override def skipNullsOnWrite: ColumnFamily = copy(options = options.newSkipNullsOnWrite)
}

/**
 * The column-level options for cell requests to Kiji.
 *
 * @param maxVersions is the maximum number of cells (from the most recent) that will be
 *     retrieved for the column. By default only the most recent cell is retrieved.
 * @param filter that a cell must pass to be retrieved by the request. By default no filter is
 *     applied.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class ColumnRequestOptions private[express] (
  maxVersions: Int = 1,
  // Not accessible to end-users because the type is soon to be replaced by a
  // KijiExpress-specific implementation.
  private[express] val filter: Option[KijiColumnFilter] = None,
  replacementSlice: Option[KijiSlice[_]] = None,
  skipNullsOnWrite: Boolean = false)
    extends Serializable {
  def newWithReplacement(newReplacement: Option[KijiSlice[_]]): ColumnRequestOptions = copy(replacementSlice = newReplacement)
  def newSkipNullsOnWrite = copy(skipNullsOnWrite = true)
}
