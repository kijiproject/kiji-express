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

import scala.collection.JavaConversions

import cascading.tuple.Fields
import com.twitter.scalding._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.generic.GenericData
import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiClientTest
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiColumnName
import org.kiji.express.EntityId
import org.kiji.express.KijiSuite
import org.kiji.express.util.AvroTypesComplete
import com.twitter.scalding.IterableSource


@RunWith(classOf[JUnitRunner])
class WriterSchemaSuite extends KijiClientTest with KijiSuite {
  import WriterSchemaSuite._
  import AvroTypesComplete._

  // TODO: These non-test things can be moved to the companion object after SCHEMA-539 fix
  setupKijiTest()
  val kiji: Kiji = createTestKiji()
  val table: KijiTable = {
    kiji.createTable(AvroTypesComplete.layout.getDesc)
    kiji.openTable(AvroTypesComplete.layout.getName)
  }
  val conf: Configuration = getConf
  val uri = table.getURI.toString
  val reader: KijiTableReader = table.openTableReader()
  val writer: KijiTableWriter = table.openTableWriter()

  /**
   * Get value from HBase.
   * @param eid string of row
   * @param column column containing requested value
   * @tparam T expected type of value
   * @return the value
   */
  def getValue[T](eid: String, column: KijiColumnName): T = {
    def entityId(s: String) = table.getEntityId(s)
    val (family, qualifier) = column.getFamily -> column.getQualifier
    val get = reader.get(entityId(eid), KijiDataRequest.create(family, qualifier))
    require(get.containsColumn(family, qualifier)) // Require the cell exists for null case
    get.getMostRecentValue(family, qualifier)
  }

  /**
   * Verify that the inputs have been persisted into the Kiji column.  Checks that the types and
   * values match.
   * @param inputs to check against
   * @param column column that the inputs are stored in
   * @tparam T expected return type of value in HBase
   */
  def verify[T](inputs: Iterable[(EntityId, T)],
                column: KijiColumnName,
                verifier: (T, T) => Unit) = {
    inputs.foreach { input: (EntityId, T) =>
      val (eid, value) = input
      val retrieved: T = getValue(eid.components.head.toString, column)
      verifier(value, retrieved)
    }
  }

  def valueVerifier[T](input: T, retrieved: T) = {
    assert(input === retrieved)
  }

  def nullVerifier(input: Any, retrieved: Any) = {
    assert(input === null)
    assert(retrieved === null)
  }

  def arrayVerifier[T](input: T, retrieved: T) = {
    assert(retrieved.isInstanceOf[GenericData.Array[_]])
    val ret = JavaConversions.JListWrapper(retrieved.asInstanceOf[GenericData.Array[_]]).toSeq
    assert(input.asInstanceOf[Iterable[_]].toSeq === ret)
  }

  def fixedVerifier[T](input: T, retrieved: T) = {
    assert(retrieved.isInstanceOf[Fixed])
    assert(input === retrieved.asInstanceOf[Fixed].bytes())
  }

  def enumVerifier[T](schema: Schema)(input: T, retrieved: T) = {
    assert(retrieved.isInstanceOf[GenericData.EnumSymbol])
    assert(
        retrieved.asInstanceOf[GenericData.EnumSymbol] ===
        new GenericData().createEnum(input.toString, schema))
  }

  /**
   * Write provided values with express into an HBase column with options as specified in output,
   * and verify that the values have been persisted correctly.
   * @param values to test
   * @param output options to write with
   * @tparam T type of values to write
   * @return
   */
  def testWrite[T](values: Iterable[T],
                   output: ColumnRequestOutput,
                   verifier: (T, T) => Unit =  valueVerifier _) {
    val outputSource = KijiOutput(uri, Map('value -> output))
    val inputs = eids.zip(values)
    expressWrite(conf, new Fields("entityId", "value"), inputs, outputSource)
    verify(inputs, output.columnName, verifier)
  }

  test("A KijiJob can write to a counter column without a specified writer schema.") {
    testWrite(longs, QualifiedColumnRequestOutput(family, counterColumn))
  }

  test("A KijiJob can write to a raw bytes column without a specified writer schema.") {
    testWrite(bytes, QualifiedColumnRequestOutput(family, rawColumn))
  }

  test("A KijiJob can write to an Avro null column with a specified writer schema.") {
    testWrite(nulls, QualifiedColumnRequestOutput(family, nullColumn, nullSchema), nullVerifier)
  }

  test("A KijiJob can write to an Avro null column without a specified writer schema.") {
    testWrite(nulls, QualifiedColumnRequestOutput(family, nullColumn), nullVerifier)
  }

  test("A KijiJob can write to an Avro boolean column with a specified writer schema.") {
    testWrite(booleans, QualifiedColumnRequestOutput(family, booleanColumn, booleanSchema))
  }

  test("A KijiJob can write to an Avro boolean column without a specified writer schema.") {
    testWrite(booleans, QualifiedColumnRequestOutput(family, booleanColumn))
  }

  test("A KijiJob can write to an Avro int column with a specified writer schema.") {
    testWrite(ints, QualifiedColumnRequestOutput(family, intColumn, intSchema))
  }

  test("A KijiJob can write to an Avro int column without a specified writer schema.") {
    testWrite(ints, QualifiedColumnRequestOutput(family, intColumn))
  }

  test("A KijiJob can write to an Avro long column with a specified writer schema.") {
    testWrite(longs, QualifiedColumnRequestOutput(family, longColumn, longSchema))
  }

  test("A KijiJob can write to an Avro long column without a specified writer schema.") {
    testWrite(longs, QualifiedColumnRequestOutput(family, longColumn))
  }

  test("A KijiJob can write ints to an Avro long column with an int schema.") {
    testWrite(ints, QualifiedColumnRequestOutput(family, longColumn, intSchema))
  }

  test("A KijiJob can write to an Avro float column with a specified writer schema.") {
    testWrite(floats, QualifiedColumnRequestOutput(family, floatColumn, floatSchema))
  }

  test("A KijiJob can write to an Avro float column without a specified writer schema.") {
    testWrite(floats, QualifiedColumnRequestOutput(family, floatColumn))
  }

  test("A KijiJob can write to an Avro double column with a specified writer schema.") {
    testWrite(doubles, QualifiedColumnRequestOutput(family, doubleColumn, doubleSchema))
  }

  test("A KijiJob can write to an Avro double column without a specified writer schema.") {
    testWrite(doubles, QualifiedColumnRequestOutput(family, doubleColumn))
  }

  test("A KijiJob can write floats to an Avro double column with a float schema.") {
    testWrite(floats, QualifiedColumnRequestOutput(family, doubleColumn, intSchema))
  }

  /** TODO: reenable when Schema-594 is fixed. */
  ignore("A KijiJob can write to an Avro bytes column with a specified writer schema.") {
    testWrite(bytes, QualifiedColumnRequestOutput(family, bytesColumn, bytesSchema))
  }

  /** TODO: reenable when Schema-594 is fixed. */
  ignore("A KijiJob can write to an Avro bytes column without a specified writer schema.") {
    testWrite(bytes, QualifiedColumnRequestOutput(family, bytesColumn))
  }

  test("A KijiJob can write to an Avro string column with a specified writer schema.") {
    testWrite(strings, QualifiedColumnRequestOutput(family, stringColumn, stringSchema))
  }

  test("A KijiJob can write to an Avro string column without a specified writer schema.") {
    testWrite(strings, QualifiedColumnRequestOutput(family, stringColumn))
  }

  test("A KijiJob can write to an Avro specific record column with a specified writer schema.") {
    testWrite(specificRecords, QualifiedColumnRequestOutput(family, specificColumn, specificSchema))
  }

  test("A KijiJob can write to an Avro specific record column without a specified writer schema.") {
    testWrite(specificRecords, QualifiedColumnRequestOutput(family, specificColumn))
  }

  /** TODO: fix generic record serialization. */
  test("A KijiJob can write to a generic record column with a specified writer schema.") {
    testWrite(genericRecords, QualifiedColumnRequestOutput(family, genericColumn, genericSchema))
  }

  /** TODO: fix generic record serialization. */
  test("A KijiJob can write to a generic record column without a specified writer schema.") {
    testWrite(genericRecords, QualifiedColumnRequestOutput(family, genericColumn))
  }

  test("A KijiJob can write to an enum column with a specified writer schema.") {
    testWrite(enums, QualifiedColumnRequestOutput(family, enumColumn, enumSchema))
  }

  test("A KijiJob can write to an enum column without a specified writer schema.") {
    testWrite(enums, QualifiedColumnRequestOutput(family, enumColumn))
  }

  test("A KijiJob can write a string to an enum column with a specified writer schema.") {
    testWrite(enumStrings, QualifiedColumnRequestOutput(family, enumColumn, enumSchema),
      enumVerifier(enumSchema))
  }

  /** TODO: fix GenericArray serialization. */
  test("A KijiJob can write an avro array to an array column with a specified writer schema.") {
    testWrite(avroArrays, QualifiedColumnRequestOutput(family, arrayColumn, arraySchema))
  }

  /** TODO: fix GenericArray serialization. */
  test("A KijiJob can write an avro array to an array column without a specified writer schema."){
    testWrite(avroArrays, QualifiedColumnRequestOutput(family, arrayColumn))
  }

  test("A KijiJob can write an Iterable to an array column with a specified writer schema.") {
    testWrite(arrays, QualifiedColumnRequestOutput(family, arrayColumn, arraySchema), arrayVerifier)
  }

  test("A KijiJob can write to a union column with a specified writer schema.") {
    testWrite(unions, QualifiedColumnRequestOutput(family, unionColumn, unionSchema))
  }

  test("A KijiJob can write to a union column without a specified writer schema.") {
    testWrite(unions, QualifiedColumnRequestOutput(family, unionColumn))
  }

  test("A KijiJob can write to a fixed column with a specified writer schema.") {
    testWrite(fixeds, QualifiedColumnRequestOutput(family, fixedColumn, fixedSchema))
  }

  test("A KijiJob can write to a fixed column without a specified writer schema.") {
    testWrite(fixeds, QualifiedColumnRequestOutput(family, fixedColumn))
  }

  test("A KijiJob can write a byte array to a fixed column with a specified writer schema.") {
    testWrite(fixedByteArrays, QualifiedColumnRequestOutput(family, fixedColumn, fixedSchema),
      fixedVerifier)
  }
}

object WriterSchemaSuite {
  /**
   * Writes inputs to outputSource with Express.
   * @param fs fields contained in the input tuples.
   * @param inputs contains tuples to write to HBase with Express.
   * @param outputSource KijiSource with options for how to write values to HBase.
   * @param setter necessary for some implicit shenanigans.  Don't explicitly pass in.
   * @tparam A type of values to be written.
   */
  def expressWrite[A](conf: Configuration,
                      fs: Fields,
                      inputs: Iterable[A],
                      outputSource: KijiSource)
                     (implicit setter: TupleSetter[A]): Boolean = {
    val args = Args("--hdfs")
    Mode.mode = Mode(args, conf) // HDFS mode
    new IdentityJob(fs, inputs, outputSource, args).run
  }
}

// Must be its own top-level class for mystical serialization reasons
class IdentityJob[A](fs: Fields, inputs: Iterable[A], output: KijiSource, args: Args)
                    (implicit setter: TupleSetter[A]) extends KijiJob(args) {
  IterableSource(inputs, fs)(setter, implicitly[TupleConverter[A]]).write(output)
}