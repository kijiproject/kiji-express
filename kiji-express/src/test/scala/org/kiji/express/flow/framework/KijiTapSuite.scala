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

package org.kiji.express.flow.framework

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite
import org.kiji.express.flow.ColumnInputSpec
import org.kiji.express.flow.InvalidKijiTapException
import org.kiji.express.flow.RowFilterSpec
import org.kiji.express.flow.RowRangeSpec
import org.kiji.express.flow.TimeRangeSpec
import org.kiji.express.flow.util.ResourceUtil
import org.kiji.express.flow.util.TestingResourceUtil
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.KijiTableLayout

@RunWith(classOf[JUnitRunner])
class KijiTapSuite extends KijiSuite {
  val instanceName: String = "test_KijiTap_instance"
  val testKijiTableLayout: KijiTableLayout = TestingResourceUtil.layout("layout/avro-types.json")
  val config: JobConf = new JobConf(HBaseConfiguration.create())

  test("KijiTap validates a valid instance/table/column.") {
    val testTable = makeTestKijiTable(testKijiTableLayout, instanceName)
    val kijiURI = testTable.getURI

    val testScheme: KijiScheme = new KijiScheme(
        tableAddress = kijiURI.toString,
        timeRange = TimeRangeSpec.All,
        timestampField = None,
        icolumns = Map(
            "dummy_field1" -> ColumnInputSpec("searches"),
            "dummy_field2" -> ColumnInputSpec("family:column1")),
        rowRangeSpec = RowRangeSpec.All,
        rowFilterSpec = RowFilterSpec.NoFilter)

    val testTap: KijiTap = new KijiTap(kijiURI, testScheme)

    testTap.validate(config)
  }

  test("KijiTap validates a nonexistent instance.") {
    val testTable = makeTestKijiTable(testKijiTableLayout, instanceName)
    val kijiURI = testTable.getURI

    val testScheme: KijiScheme = new KijiScheme(
        tableAddress = kijiURI.toString,
        timeRange = TimeRangeSpec.All,
        timestampField = None,
        icolumns = Map(
            "dummy_field1" -> ColumnInputSpec("searches"),
            "dummy_field2" -> ColumnInputSpec("family:column1")),
        rowRangeSpec = RowRangeSpec.All,
        rowFilterSpec = RowFilterSpec.NoFilter)

    val testURI: KijiURI = KijiURI.newBuilder(kijiURI)
        .withInstanceName("nonexistent_instance")
        .build()

    val testTap: KijiTap = new KijiTap(testURI, testScheme)

    intercept[InvalidKijiTapException] {
      testTap.validate(config)
    }
  }

  test("KijiTap validates a nonexistent table.") {
    val testTable = makeTestKijiTable(testKijiTableLayout, instanceName)
    val kijiURI = testTable.getURI

    val testScheme: KijiScheme = new KijiScheme(
        tableAddress = kijiURI.toString,
        timeRange = TimeRangeSpec.All,
        timestampField = None,
        icolumns = Map(
            "dummy_field1" -> ColumnInputSpec("searches"),
            "dummy_field2" -> ColumnInputSpec("family:column1")),
        rowRangeSpec = RowRangeSpec.All,
        rowFilterSpec = RowFilterSpec.NoFilter)

    val testURI: KijiURI = KijiURI.newBuilder(kijiURI)
        .withTableName("nonexistent_table")
        .build()

    val testTap: KijiTap = new KijiTap(testURI, testScheme)

    intercept[InvalidKijiTapException] {
      testTap.validate(config)
    }
  }

  test("KijiTap validates a nonexistent column.") {
    val testTable = makeTestKijiTable(testKijiTableLayout, instanceName)
    val kijiURI = testTable.getURI

    val testScheme: KijiScheme = new KijiScheme(
        tableAddress = kijiURI.toString,
        timeRange = TimeRangeSpec.All,
        timestampField = None,
        icolumns = Map(
            "dummy_field1" -> ColumnInputSpec("searches"),
            "dummy_field2" -> ColumnInputSpec("family:nonexistent")),
        rowRangeSpec = RowRangeSpec.All,
        rowFilterSpec = RowFilterSpec.NoFilter)

    val testTap: KijiTap = new KijiTap(kijiURI, testScheme)

    val exception = intercept[InvalidKijiTapException] {
      testTap.validate(config)
    }

    assert(exception.getMessage.contains("nonexistent"))
  }

  test("KijiTap validates multiple nonexistent columns.") {
    val testTable = makeTestKijiTable(testKijiTableLayout, instanceName)
    val kijiURI = testTable.getURI

    val testScheme: KijiScheme = new KijiScheme(
        tableAddress = kijiURI.toString,
        timeRange = TimeRangeSpec.All,
        timestampField = None,
        icolumns = Map(
            "dummy_field1" -> ColumnInputSpec("nonexistent1"),
            "dummy_field2" -> ColumnInputSpec("family:nonexistent2")),
        rowRangeSpec = RowRangeSpec.All,
        rowFilterSpec = RowFilterSpec.NoFilter)

    val testTap: KijiTap = new KijiTap(kijiURI, testScheme)

    val exception = intercept[InvalidKijiTapException] {
      testTap.validate(config)
    }

    assert(exception.getMessage.contains("nonexistent1"))
    assert(exception.getMessage.contains("nonexistent2"))
  }
}
