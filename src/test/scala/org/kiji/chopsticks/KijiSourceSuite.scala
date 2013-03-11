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

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer
import java.util.NavigableMap
import java.util.UUID

import com.twitter.scalding._
import org.apache.avro.util.Utf8

import org.kiji.chopsticks.DSL._
import org.kiji.schema.EntityId
import org.kiji.schema.layout.KijiTableLayouts

class KijiSourceSuite
    extends KijiSuite {
  import KijiSourceSuite._

  test("a word-count job that reads from a Kiji table is run") {
    // Create test Kiji table.
    val table = makeTestKijiTable(layout(KijiTableLayouts.SIMPLE))
    val uri = table.getURI().toString()

    // Create input rows.
    val rows = List(
      ( id("row01"), singleton(new Utf8("hello")) ),
      ( id("row02"), singleton(new Utf8("hello")) ),
      ( id("row03"), singleton(new Utf8("world")) ),
      ( id("row04"), singleton(new Utf8("hello")) )
    )

    // Build test job.
    JobTest(new WordCountJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput(uri)("family:column" -> 'word), rows)
        .sink[(String,Int)](Tsv("outputFile")) { outputBuffer =>
          val outMap = outputBuffer.toMap

          // Validate that the output is as expected.
          assert(3 === outMap("hello"))
          assert(1 === outMap("world"))
        }
        // Run the test job.
        .run
        .finish

    // Cleanup resources.
    table.release()
  }

  test("an import job that writes to a Kiji table is run") {
    // Create test Kiji table.
    val table = makeTestKijiTable(layout(KijiTableLayouts.SIMPLE))
    val uri = table.getURI().toString()

    // Create input lines.
    val lines = List(
      ( "0", "hello hello hello world world hello" ),
      ( "1", "world hello   world      hello" )
    )

    // Build test job.
    JobTest(new ImportJob(_))
        .arg("input", "inputFile")
        .arg("output", uri)
        .source(TextLine("inputFile"), lines)
        .sink[(EntityId, NavigableMap[Long, Utf8])](KijiOutput(uri)('word -> "family:column")) {
            outputBuffer: Buffer[(EntityId, NavigableMap[Long, Utf8])] =>

          assert(10 === outputBuffer.size)

          // Perform a non-distributed word count.
          val wordCounts: (Int, Int) = outputBuffer
              // Extract words from each row.
              .flatMap { row =>
                val (_, timeline) = row
                timeline
                    .asScala
                    .map { case (_, word) => word }
              }
              // Count the words.
              .foldLeft((0, 0)) { (counts, word) =>
                // Unpack the counters.
                val (helloCount, worldCount) = counts

                // Increment the appropriate counter and return both.
                word.toString() match {
                  case "hello" => (helloCount + 1, worldCount)
                  case "world" => (helloCount, worldCount + 1)
                }
              }

          // Make sure that the counts are as expected.
          assert((6, 4) === wordCounts)
        }
        .run
        .finish

    table.release()
  }

  // TODO(CHOP-39): Write this test.
  test("a word-count job that uses the type-safe api is run") {
    pending
  }

  // TODO(CHOP-40): Write this test.
  test("a job that uses the matrix api is run") {
    pending
  }
}

/** Companion object for KijiSourceSuite. */
object KijiSourceSuite extends KijiSuite {
  /** Convenience method for getting the latest value in a timeline. */
  def getMostRecent[T](timeline: NavigableMap[Long, T]): T = timeline.firstEntry().getValue()

  /** Performs a word count on a Kiji table that contains rows, each containing one word. */
  class WordCountJob(args: Args) extends Job(args) {
    // Setup input.
    KijiInput(args("input"))("family:column" -> 'word)
        // Sanitize the word.
        .map('word -> 'cleanword) { words: NavigableMap[Long, Utf8] =>
          getMostRecent(words)
              .toString()
              .toLowerCase()
        }
        // Count the occurrences of each word.
        .groupBy('cleanword) { _.size }
        // Write the result to a file.
        .write(Tsv(args("output")))
  }

  /** Reads in words from a text file and then loads the words into a Kiji table. */
  class ImportJob(args: Args) extends Job(args) {
    // Setup input.
    TextLine(args("input"))
        .read
        // Get the words in each line.
        .flatMap('line -> 'word) { line : String => line.split("\\s+") }
        // Generate an entityId for each word.
        .map('word -> 'entityid) { _: String => id(UUID.randomUUID().toString()) }
        // Write the results to a Kiji table.
        .write(KijiOutput(args("output"))('word -> "family:column"))
  }
}
