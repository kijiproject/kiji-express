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

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import org.scalatest.FunSuite

import org.kiji.chopsticks.ColumnRequest.InputOptions
import org.kiji.schema.filter.RegexQualifierColumnFilter

class ColumnRequestSuite extends FunSuite {
  // TODO(CHOP-37): Test with non-null filter once the new method of specifying filters
  // correctly implements the .equals() and hashCode() methods.
  // Should be able to change the following line to:
  // def filter = new RegexQualifierColumnFilter(".*")
  val filter = new RegexQualifierColumnFilter(".*")
  def opts: InputOptions = new InputOptions(1, filter)
  val colName = "myname"

  test("Fields of the column are the same as those it is constructed with.") {
    val col: ColumnRequest = new ColumnRequest(colName, opts)

    assert(colName == col.name)
    assert(opts == col.inputOptions)
  }

  test("Two columns with the same parameters are equal and hash to the same value.") {
    val col1: ColumnRequest = new ColumnRequest(colName, opts)
    val col2: ColumnRequest = new ColumnRequest(colName, opts)

    assert(col1 == col2)
    assert(col1.hashCode() == col2.hashCode())
  }

  test("A column must be serializable.") {
    // Serialize and deserialize using java ObjectInputStream and ObjectOutputStream.
    // TODO(CHOP-37): The filter is null because it's not serializable. Once CHOP-37 is
    // done, use the same inputoptions as the other tests in the line below.
    val col: ColumnRequest = new ColumnRequest(colName, new InputOptions(1, null))
    val bytesOut = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bytesOut)
    out.writeObject(col)
    val serializedColumn = bytesOut.toByteArray()
    val bytesIn = new ByteArrayInputStream(serializedColumn)
    val in = new ObjectInputStream(bytesIn)
    val deserializedColumn = in.readObject()

    assert(col == deserializedColumn)
  }
}
