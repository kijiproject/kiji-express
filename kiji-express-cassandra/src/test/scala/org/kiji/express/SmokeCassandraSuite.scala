/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.express

import java.io.InputStream
import scala.collection.JavaConverters.mapAsScalaMapConverter

import com.twitter.scalding.Args
import com.twitter.scalding.GroupBuilder
import com.twitter.scalding.Local
import com.twitter.scalding.Mode
import com.twitter.scalding.NullSource
import com.twitter.scalding.Stat
import org.apache.commons.io.IOUtils
import org.junit.Assert
import org.junit.Test

import org.kiji.mapreduce.avro.generated.JobHistoryEntry
import org.kiji.mapreduce.framework.JobHistoryKijiTable
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.shell.api.Client
import org.kiji.schema.util.InstanceBuilder
import org.kiji.express.flow.{KijiOutput, FlowCell, KijiInput, KijiJob}
import org.kiji.schema.cassandra.CassandraKijiClientTest

/** Very basic smoke test for local unit tests with Cassandra-backed Kiji. */
class SmokeCassandraSuite extends CassandraKijiClientTest {
  import SmokeCassandraSuite._

  @Test
  def testSimpleFlow(): Unit = {
    // Set up the Kiji table to use for testing.
    val kiji: Kiji = getKiji
    createTableFromDDL(DdlPath, kiji.getURI)
    val table: KijiTable = kiji.openTable(TableName)
    try {
      new InstanceBuilder(kiji)
          .withTable(table)
              .withRow("row1")
                  .withFamily("info")
                      .withQualifier("name").withValue("name1")
                      .withQualifier("email").withValue("email1")
              .withRow("row2")
                  .withFamily("info")
                      .withQualifier("name").withValue("name2")
                      .withQualifier("email").withValue("email2")
          .build()

      val args = Mode.putMode(
        Local(strictSources = false),
        Args(List("--tableUri", table.getURI.toString))
      )

      // Run a very simple Job!
      val job: SimpleJob = new SimpleJob(args)
      Assert.assertTrue(job.run)

    } finally {
      table.release()
    }
  }
}

object SmokeCassandraSuite {
  private final val DdlPath: String = "layout/org.kiji.express.flow.ITSimpleFlow.ddl"
  private final val TableName: String = "table"

  /**
   * Applies a table's DDL definition on the specified Kiji instance.
   *
   * @param resourcePath Path of the resource containing the DDL to create the table.
   * @param instanceURI URI of the Kiji instance to use.
   * @throws IOException on I/O error.
   */
  def createTableFromDDL(resourcePath: String, instanceURI: KijiURI): Unit = {
    val client: Client = Client.newInstance(instanceURI)
    try {
      val ddl: String = readResource(resourcePath)
      client.executeUpdate(ddl)
    } finally {
      client.close()
    }
  }

  /**
   * Loads a text resource by name.
   *
   * @param resourcePath Path of the resource to load.
   * @return the resource content, as a string.
   * @throws IOException on I/O error.
   */
  def readResource(resourcePath: String): String = {
    val istream: InputStream = getClass.getClassLoader.getResourceAsStream(resourcePath)
    try {
      val content: String = IOUtils.toString(istream)
      return content
    } finally {
      istream.close()
    }
  }

  class SimpleJob(args: Args) extends KijiJob(args) {
    val tableUri: String = args("tableUri")

    KijiInput.builder
      .withTableURI(tableUri)
      .withColumns("info:email" -> 'slice)
      .build
      // Extract the most recent value out of the list of flow cells.
      .map('slice -> 'email) { slice: Seq[FlowCell[CharSequence]] =>
        assert(slice.size == 1)
        slice.head.datum.toString
      }
      .project('entityId, 'email)
      .debug
      .map('email -> 'email) { email: String => email.toUpperCase }
      .debug
      .write(KijiOutput.builder
          .withTableURI(tableUri)
          .withColumns('email -> "info:email")
          .build)

  }
}
