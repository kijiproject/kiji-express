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

import com.twitter.scalding.Args
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

@RunWith(classOf[JUnitRunner])
class AvroSourceTypesSuite extends KijiClientTest with KijiSuite {
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

  val timestamp = 100L
  val family = "family"

  private def entityId(s: String) = table.getEntityId(s)

  private def writeValue[T](eid: String, column: String, value: T) = {
    writer.put(entityId(eid), family, column, timestamp, value)
    writer.flush()
  }

  private def getValue[T](eid: String, column: String): T = {
    val get = reader.get(entityId(eid), KijiDataRequest.create(family, column))
    require(get.containsCell(family, column, timestamp)) // Require the cell exists for null case
    get.getValue(family, column, timestamp)
  }

  /**
   * Unwraps latest value from [[org.kiji.express.KijiSlice]] and verifies that the type is as
   * expected.
   * @param slice [[org.kiji.express.KijiSlice]] containing value to unwrap.
   * @tparam T expected type of value contained in KijiSlice.
   * @return unwrapped value of type T.
   */
  private def unwrap[T](slice: KijiSlice[T]): (T, Long) = {
    val cell = slice.getFirst()
    (cell.datum, cell.version)
  }

  private def testExpressReadWrite[T](column: String, value: T) = {
    val input = column + "-in"
    val output = column + "-out"
    val colfam = "family:" + column
    writeValue(input, column, value)

    class ReadWriteJob(args: Args) extends KijiJob(args) {
      KijiInput(uri, colfam -> 'slice)
          .read
          .mapTo('slice -> ('value, 'time))(unwrap[T])
          .map('value -> 'entityId) { _: T => EntityId(output)}
          .write(KijiOutput(uri, 'time, 'value -> colfam))
    }
    new ReadWriteJob(Args("--hdfs")).run
    assert(value === getValue[T](output, column))
  }

  test("counter type column results in a KijiSlice[Long]") {
    testExpressReadWrite[Long]("counter", 13L)
  }

  test("raw bytes type column results in a KijiSlice[Array[Byte]]") {
    testExpressReadWrite[Array[Byte]]("raw", "Who do voodoo?".getBytes("UTF8"))
  }

  test("null avro type column results in a KijiSlice[Null]") {}
  test("boolean avro type column results in a KijiSlice[Boolean]") {}
  test("int avro type column results in a KijiSlice[Int]") {}
  test("long avro type column results in a KijiSlice[Long]") {}
  test("float avro type column results in a KijiSlice[Float]") {}
  test("double avro type column results in a KijiSlice[Double]") {}
  test("bytes avro type column results in an KijiSlice[Array[Byte]]") {}
  test("string avro type column results in a KijiSlice[String]") {}
  test("specific record T avro type column results in a KijiSlice[T]") {}
  test("generic record avro type column results in a KijiSlice[GenericRecord]") {}
  test("enum avro type column results in a KijiSlice[String field") {}
  test("array[T] avro type column results in a KijiSlice[List[T]]") {}
  test("map[T] avro type column results in a KijiSlice[Map[String, T]]") {}
  test("union avro type column results in a ??? field") {}
  test("fixed avro type column results in an KijiSlice[Array[Byte]] field") {}
}
