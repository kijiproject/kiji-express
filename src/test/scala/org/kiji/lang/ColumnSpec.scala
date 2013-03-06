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

import org.kiji.lang.Column.InputOptions
import org.kiji.schema.filter.RegexQualifierColumnFilter

class ColumnSpec extends FunSuite {
  val colName = "myname"
  // TODO(CHOP-37): Test with non-null filter once the new method of specifying filters
  // correctly implements the .equals() and hashCode() methods.
  def opts: InputOptions = new InputOptions(1, null)
  val filter = new RegexQualifierColumnFilter(".*")

  test("Fields of the column are the same as those it is constructed with.") {
    val col: Column = new Column(colName, opts)

    assert(col.name == colName)
    assert(col.inputOptions == opts)
  }

  test("Two columns with the same parameters are equal and hash to the same value.") {
    val col1: Column = new Column(colName, opts)
    val col2: Column = new Column(colName, opts)

    assert(col1.equals(col2))
    assert(col1.hashCode() == col2.hashCode())
  }
}

class InputOptionsSpec extends FunSuite {
  def filter = new RegexQualifierColumnFilter(".*")
  val maxVersions = 2

  // TODO(CHOP-37): Make sure that whatever new method of filters are
  // implements the .equals() method.
  ignore("maxVersions and filter are the same as constructed with.") {
    val opts = new InputOptions(maxVersions, filter)

    assert(opts.maxVersions == maxVersions)
    assert(opts.filter == filter)
  }

  // TODO(CHOP-37): Make sure that whatever new method of filters are
  // implements the .equals() method.
  ignore("Two InputOptions with the same options are equal and hash to the same value.") {
    val opts1 = new InputOptions(maxVersions, filter)
    val opts2 = new InputOptions(maxVersions, filter)

    assert(opts1.equals(opts2))
    assert(opts1.hashCode() == opts2.hashCode())
  }
}
