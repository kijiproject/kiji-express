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

package org.kiji.lang

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

import org.kiji.lang.Column.InputOptions
import org.kiji.schema.filter.RegexQualifierColumnFilter

class ColumnSpec extends FunSuite with ShouldMatchers {
  // TODO(CHOP-37): Test with non-null filter once the new method of specifying filters
  // correctly implements the .equals() and hashCode() methods.
  // Should be able to change the following line to:
  // def filter = new RegexQualifierColumnFilter(".*")
  val filter = new RegexQualifierColumnFilter(".*")
  def opts: InputOptions = new InputOptions(1, filter)
  val colName = "myname"

  test("Fields of the column are the same as those it is constructed with.") {
    val col: Column = new Column(colName, opts)

    col.name should equal (colName)
    col.inputOptions should equal (opts)
  }

  test("Two columns with the same parameters are equal and hash to the same value.") {
    val col1: Column = new Column(colName, opts)
    val col2: Column = new Column(colName, opts)

    col1 should equal (col2)
    col1.hashCode() should equal (col2.hashCode())
  }
}
