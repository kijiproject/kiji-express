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

import java.util.NavigableMap
import java.util.TreeMap

import com.twitter.scalding.TupleConversions
import org.scalatest.FunSuite

import org.kiji.chopsticks.DSL._
import org.kiji.schema.EntityId
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.util.InstanceBuilder

/** Contains convenience methods for writing tests that use Kiji. */
trait KijiSuite
    extends FunSuite
    with TupleConversions {
  /** Builds a [[RawEntityId]] with the provided string. */
  def id(identifier: String): EntityId = {
    val rowKeyFmt: RowKeyFormat2 = RowKeyFormat2.newBuilder()
        .setEncoding(RowKeyEncoding.RAW)
        .build()

    val factory = EntityIdFactory.getFactory(rowKeyFmt)

    factory.getEntityId(identifier)
  }

  /** Builds a timeline from a single value. */
  def singleton[T](value: T): NavigableMap[Long, T] = {
    val timeline: NavigableMap[Long, T] = new TreeMap()
    timeline.put(Long.MaxValue, value)
    timeline
  }

  /** Builds a timeline from a list of timestamp, value pairs. */
  def timeline[T](values: (Long, T)*): NavigableMap[Long, T] = {
    values.foldLeft(new TreeMap[Long, T]) { (tree, entry) =>
      val (timestamp, value) = entry

      tree.put(timestamp, value)
      tree
    }
  }

  /** Constructs and starts a test Kiji instance that uses fake-hbase. */
  def makeTestKiji(
      /** Name of the test Kiji instance. */
      instanceName: String = "default"): Kiji = {
    new InstanceBuilder(instanceName).build()
  }

  /** Constructs and starts a test Kiji instance and creates a Kiji table. */
  def makeTestKijiTable(
      /** Layout of the test table. */
      layout: KijiTableLayout,
      /** Name of the Kiji instance to create. */
      instanceName: String = "default"): KijiTable = {
    val tableName = layout.getName()
    val kiji: Kiji = new InstanceBuilder(instanceName)
        .withTable(tableName, layout)
        .build()

    val table: KijiTable = kiji.openTable(tableName)
    kiji.release()
    table
  }

  /**
   * Loads a [[KijiTableLayout]] from the classpath. See [[KijiTableLayouts]] for
   * some layouts that get put on the classpath by KijiSchema.
   */
  def layout(
      /** Path to the layout definition file. */
      resourcePath: String): KijiTableLayout = {
    val tableLayoutDef = KijiTableLayouts.getLayout(resourcePath)
    KijiTableLayout.newLayout(tableLayoutDef)
  }
}
