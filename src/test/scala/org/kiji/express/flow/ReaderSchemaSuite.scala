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

import com.twitter.scalding.{HadoopTest, Mode, Args}
import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.kiji.schema.layout.KijiTableLayout
import org.scalatest.junit.JUnitRunner

import org.kiji.express.EntityId
import org.kiji.express.KijiSlice
import org.kiji.express.KijiSuite
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.KijiClientTest
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiDataRequest
import org.apache.avro.Schema
import org.kiji.express.avro.SimpleRecord

@RunWith(classOf[JUnitRunner])
class ReaderSchemaSuite extends KijiClientTest with KijiSuite {
  import org.kiji.express.util.AvroTypesComplete._
  setupKijiTest()
  val kiji: Kiji = createTestKiji()
  val layout: KijiTableLayout = layout("layout/avro-types-complete.json")
  val table: KijiTable = {
    kiji.createTable(layout.getDesc)
    kiji.openTable(layout.getName)
  }
  val conf: Configuration = getConf
  val uri = table.getURI.toString
  val reader: KijiTableReader = table.openTableReader()
  val writer: KijiTableWriter = table.openTableWriter()

  private def entityId(s: String) = table.getEntityId(s)

  private def writeValue[T](eid: String, column: String, value: T) = {
    writer.put(entityId(eid), family, column, value)
    writer.flush()
  }

  private def getValue[T](eid: String, column: String): T = {
    val get = reader.get(entityId(eid), KijiDataRequest.create(family, column))
    require(get.containsColumn(family, column)) // Require the cell exists for null case
    get.getMostRecentValue(family, column)
  }

  private def testExpressReadWrite[T](column: String, value: T, schema: ReaderSchema = Default) = {
    val readEid = column + "-in"
    val writeEid = column + "-out"
    writeValue(readEid, column, value)

    val outputSchema: Option[Schema] = schema match {
      case Default => None
      case Specific(klass) => Some(klass.getMethod("getSchema").invoke(klass).asInstanceOf[Schema])
      case Generic(schema) => Some(Generic.schema(schema))
    }

    val inputCol = QualifiedColumnRequestInput(family, column, schema = schema)
    val outputCol = QualifiedColumnRequestOutput(family, column, outputSchema)

    val args = Args("--hdfs")
    Mode.mode = Mode(args, conf)
    new ReadWriteJob[T](uri, inputCol, outputCol, writeEid, args).run
    assert(value === getValue[T](writeEid, column))
  }

  test("A KijiJob can read a counter column without a reader schema.") {
    testExpressReadWrite[Long](counterColumn, longs.head)
  }

  test("A KijiJob can read a raw bytes column without a reader schema.") {
    testExpressReadWrite[Array[Byte]](rawColumn, bytes.head)
  }

  test("A KijiJob can read a null column without a reader schema.") {
    testExpressReadWrite[Null](nullColumn, null)
  }

  test("A KijiJob can read a null column with a generic reader schema.") {
    testExpressReadWrite[Null](nullColumn, null, Generic(nullSchema))
  }

  test("A KijiJob can read a boolean column without a reader schema.") {
    testExpressReadWrite[Boolean](booleanColumn, booleans.head)
  }

  test("A KijiJob can read a boolean column with a generic reader schema.") {
    testExpressReadWrite[Boolean](booleanColumn, booleans.head, Generic(booleanSchema))
  }

  test("A KijiJob can read an int column without a reader schema.") {
    testExpressReadWrite[Int](intColumn, ints.head)
  }

  test("A KijiJob can read an int column with a generic reader schema.") {
    testExpressReadWrite[Int](intColumn, ints.head, Generic(intSchema))
  }

  test("A KijiJob can read a long column without a reader schema.") {
    testExpressReadWrite[Long](longColumn, longs.head)
  }

  test("A KijiJob can read a long column with a generic reader schema.") {
    testExpressReadWrite[Long](longColumn, longs.head, Generic(longSchema))
  }

  test("A KijiJob can read a float column without a reader schema.") {
    testExpressReadWrite[Float](floatColumn, floats.head)
  }

  test("A KijiJob can read a float column with a generic reader schema.") {
    testExpressReadWrite[Float](floatColumn, floats.head, Generic(floatSchema))
  }

  test("A KijiJob can read a double column without a reader schema.") {
    testExpressReadWrite[Double](doubleColumn, doubles.head)
  }

  test("A KijiJob can read a double column with a generic reader schema.") {
    testExpressReadWrite[Double](doubleColumn, doubles.head, Generic(doubleSchema))
  }

  /** TODO: reenable when Schema-594 is fixed. */
  ignore("A KijiJob can read a bytes column without a reader schema.") {
    testExpressReadWrite[Array[Byte]](bytesColumn, bytes.head)
  }

  /** TODO: reenable when Schema-594 is fixed. */
  ignore("A KijiJob can read a bytes column with a generic reader schema.") {
    testExpressReadWrite[Array[Byte]](bytesColumn, bytes.head, Generic(bytesSchema))
  }

  test("A KijiJob can read a string column without a reader schema.") {
    testExpressReadWrite[String](stringColumn, strings.head)
  }

  test("A KijiJob can read a string column with a generic reader schema.") {
    testExpressReadWrite[String](stringColumn, strings.head, Generic(stringSchema))
  }

  test("A KijiJob can read a specific record column without a reader schema.") {
    testExpressReadWrite[SimpleRecord](specificColumn, specificRecords.head)
  }

  test("A KijiJob can read a specific record column with a generic reader schema.") {
    testExpressReadWrite[SimpleRecord](specificColumn, specificRecords.head,
        Generic(specificSchema))
  }




  test("generic record avro type column results in a KijiSlice[GenericRecord]") {}
  test("enum avro type column results in a KijiSlice[String field") {}
  test("array[T] avro type column results in a KijiSlice[List[T]]") {}
  test("map[T] avro type column results in a KijiSlice[Map[String, T]]") {}
  test("union avro type column results in a ??? field") {}
  test("fixed avro type column results in an KijiSlice[Array[Byte]] field") {}
}

// Must be its own top-level class for mystical serialization reasons
class ReadWriteJob[T](
    uri: String,
    input: ColumnRequestInput,
    output: ColumnRequestOutput,
    writeEid: String,
    args: Args
) extends KijiJob(args) {

  /**
   * Unwraps latest value from [[org.kiji.express.KijiSlice]] and verifies that the type is as
   * expected.
   * @param slice [[org.kiji.express.KijiSlice]] containing value to unwrap.
   * @return unwrapped value of type T.
   */
  private def unwrap(slice: KijiSlice[T]): (T, Long) = {
    require(slice.size == 1)
    val cell = slice.getFirst()
    if (cell.datum != null) { require(cell.datum.isInstanceOf[T]) }
    (cell.datum, cell.version)
  }

  KijiInput(uri, Map(input -> 'slice))
      .read
      .mapTo('slice -> ('value, 'time))(unwrap)
      .map('value -> 'entityId) { _: T => EntityId(writeEid)}
      .write(KijiOutput(uri, 'time, Map('value -> output)))
}