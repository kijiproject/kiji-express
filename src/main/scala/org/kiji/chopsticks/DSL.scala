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

package org.kiji.chopsticks

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.chopsticks.ColumnRequest.InputOptions
import org.kiji.schema.filter.KijiColumnFilter
import org.kiji.schema.filter.RegexQualifierColumnFilter

@ApiAudience.Public
@ApiStability.Unstable
object DSL {
  // TODO(CHOP-36): Support request-level options.
  /**
   * Factory method for KijiSource.
   *
   * @param tableURI Address of the Kiji table to use.
   * @param columns Columns to read from.
   */
  def KijiInput(
      tableURI: String) (
        columns: (String, Symbol)*): KijiSource = {
    val columnMap = columns
        .map { case (col, field) => (field, col) }
        .toMap
        .mapValues(Column(_))
    new KijiSource(tableURI, columnMap)
  }

  /**
   * Factory method for KijiSource.
   *
   * @param tableURI Address of the Kiji table to use.
   * @param columns Columns to read from.
   */
  def KijiInput(
      tableURI: String,
      columns: Map[ColumnRequest, Symbol]): KijiSource = {
    val columnMap = columns
        .map { case (col, field) => (field, col) }
    new KijiSource(tableURI, columnMap)
  }

  /**
   * Factory method for KijiSource.
   *
   * @param tableURI Address of the Kiji table to use.
   * @param columns Columns to write to.
   */
  def KijiOutput(
      tableURI: String) (
        columns: (Symbol, String)*)
    : KijiSource = {
    val columnMap = columns
        .toMap
        .mapValues(Column(_))
    new KijiSource(tableURI, columnMap)
  }

  /**
   * Factory method for KijiSource.
   *
   * @param tableURI Address of the Kiji table to use.
   * @param columns Columns to write to.
   */
  def KijiOutput(
      tableURI: String,
      columns: Map[Symbol, ColumnRequest])
    : KijiSource = new KijiSource(tableURI, columns)

  /**
   * Factory method for Column that is a map-type column.
   *
   * @param name of column in "family:qualifier" or "family" form.
   * @param qualifierMatches Regex for filtering qualifiers.
   * @param timestamp specification for column.
   * @param versions of column to get.
   */
  def MapColumn(
    name: String,
    qualifierMatches: String = null,
//    timestamp: TimestampSpec = latest,
    versions: Int = 1
  ): ColumnRequest = {
    val regexColumnFilter: KijiColumnFilter =
        if (null == qualifierMatches) {
          null
        } else {
          new RegexQualifierColumnFilter(qualifierMatches)
        }
    val inputOptions: InputOptions = new InputOptions(versions, regexColumnFilter)
    new ColumnRequest(name, inputOptions)
  }

  /**
   * Factory method for Column that is a group-type column.
   *
   * @param name of column in "family:qualifier" form.
   * @param timestamp specification for column.
   * @param versions of column to get.
   */
  def Column(
    name: String,
//    timestamp: TimestampSpec = latest,
    versions: Int = 1
  ): ColumnRequest = {
    val inputOptions: InputOptions = new InputOptions(versions, null)
    new ColumnRequest(name, inputOptions) // TODO: create inputOptions here.
  }

  // Convenience vals for specifying versions.
  val all = Integer.MAX_VALUE
  val latest = 1

  /**
   * Case class representing the timestamp specification.
   * Follows conventions: If end is None, only start at the
   * start timestamp. If both are None, no timestamp specification.
   */
//  case class TimestampSpec (start: Option[Long], end: Option[Long])

//   val latest = new TimestampSpec(None, None)
//   def range(start: Long, end: Long): TimestampSpec = new TimestampSpec(Some(start), Some(end))
//   def start(start: Long): TimestampSpec = new TimestampSpec(Some(start), None)
}
