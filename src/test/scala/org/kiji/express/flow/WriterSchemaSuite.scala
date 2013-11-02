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

import scala.util.Random

import cascading.tuple.Fields
import com.twitter.scalding.Args
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.TupleSetter
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.generic.GenericRecord
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
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.express.EntityId
import org.kiji.express.KijiSuite
import com.twitter.scalding.IterableSource

@RunWith(classOf[JUnitRunner])
class WriterSchemaSuite extends KijiClientTest with KijiSuite {
  import WriterSchemaSuite._

  // TODO: These non-test things can be moved to the companion object after SCHEMA-539 fix
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

  val family = "strict"

  def defaultSchema(column: String) = {
    val json = layout.getCellSchema(new KijiColumnName(family, column)).getDefaultReader.getJson
    new Schema.Parser().parse(json)
  }

  def entityId(s: String) = table.getEntityId(s)

  def getValue[T](eid: String, column: String): T = {
    val get = reader.get(entityId(eid), KijiDataRequest.create(family, column))
    require(get.containsColumn(family, column)) // Require the cell exists for null case
    get.getMostRecentValue(family, column)
  }

  def verify[T](inputs: Iterable[(EntityId, T)], column: String) = {
    inputs.foreach { input: (EntityId, T) =>
      val (eid, record) = input
      val retrieved: GenericRecord = getValue(eid.components.head.toString, genericColumn)
      assert(record === retrieved)
    }
  }

  val genericColumn = "generic"
  val genericSchema: Schema = defaultSchema(genericColumn)
  val genericInputs: Iterable[(EntityId, GenericRecord)] = {
    val rand = new Random
    val base = Range(1, 10)
    val lengths = base.map { _ => rand.nextInt() }
    val angles = base.map { _ => rand.nextFloat() }
    val eids = base
        .map { _ => rand.nextString(32) }
        .map(EntityId(_))

    val builder = new GenericRecordBuilder(genericSchema)

    val records = lengths
        .zip(angles)
        .map { fields => builder.set("length", fields._1).set("angle", fields._2).build() }

    eids.zip(records)
  }


  test("A job can write to a generic record column with a specified writer schema.") {
    val fields = new Fields("entityId", "record")
    val outputSource = KijiOutput(
      uri,
      Map('record -> QualifiedColumnRequestOutput(family, genericColumn, genericSchema))
    )
    expressWrite(fields, genericInputs, outputSource)
    verify(genericInputs, genericColumn)
  }

  test("A job can write to a generic record column without specifying writer schema.") {


  }

}

object WriterSchemaSuite {
  def expressWrite[A](fs: Fields, inputs: Iterable[A], outputSource: KijiSource)
                     (implicit setter: TupleSetter[A]) = {

    class IdentityJob(
        fs: Fields,
        inputs: Iterable[A],
        output: KijiSource, args: Args) extends KijiJob(args) {
      IterableSource(inputs, fs)(setter, implicitly[TupleConverter[A]]).write(output)
    }

    new IdentityJob(fs, inputs, outputSource, Args("--hdfs")).run
  }


}
