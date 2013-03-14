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
import java.io.IOException
import java.util.Iterator
import java.util.List

import com.google.common.base.Preconditions.checkNotNull
import com.google.common.base.Preconditions.checkState
import com.google.common.collect.Lists
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.mapreduce.TableSplit
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.Reporter

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.chopsticks.Resources.doAndRelease
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.HBaseEntityId
import org.kiji.schema.Kiji
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiRegion
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiRowScanner
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiTableReader.KijiScannerOptions
import org.kiji.schema.KijiURI
import org.kiji.schema.impl.HBaseKijiTable
import org.kiji.schema.util.ResourceUtils

/**
 * <p>An [[InputFormat]] for use with Cascading whose input source is a Kiji table. This
 * input format will scan over a subset of rows in a Kiji table, retrieving [[KijiRowData]]
 * as a result. The columns retrieved during the scan, as well as the time and row ranges used,
 * are configured through an [[KijiDataRequest]].</p>
 *
 * <p>The input format itself is responsible for computing input splits over the Kiji table,
 * and retrieving a [[RecordReader]] for a particular split. See
 * [[KijiRecordReader]] for more information on how a particular input split is scanned
 * for rows. Record readers returned by this input format return key-value pairs of type
 * [[KijiKey]] and [[KijiValue]], which are simple wrappers around
 * [[org.kiji.schema.EntityId]] and [[KijiRowData]], respectively.
 *
 * <p>This input format uses the "old" style MapReduce API for compatibility with Cascading.</p>
 */
@ApiAudience.Framework
@ApiStability.Unstable
class KijiInputFormat
    extends InputFormat[KijiKey, KijiValue] {
  /**
   * Gets a set of input splits for a MapReduce job running over a Kiji table. One split is
   * created per region in the input Kiji table.
   *
   * @param configuration of the job using the splits. The configuration should specify the
   *     input Kiji table being used, through the configuration variable
   *     [[KijiConfKeys#KIJI_INPUT_TABLE_URI]].
   * @param numSplits desired for the job. This framework hint is ignored by this method.
   * @return an array of input splits to be operated on in the MapReduce job.
   */
  override def getSplits(configuration: JobConf, numSplits: Int): Array[InputSplit] = {
    val uriString: String = checkNotNull(configuration.get(KijiConfKeys.KIJI_INPUT_TABLE_URI))
    val inputTableURI: KijiURI = KijiURI.newBuilder(uriString).build()

    doAndRelease(Kiji.Factory.open(inputTableURI, configuration)) { kiji: Kiji =>
      doAndRelease(kiji.openTable(inputTableURI.getTable())) { table: KijiTable =>
        val htable: HTableInterface = HBaseKijiTable.downcast(table).getHTable()

        table.getRegions().asScala
            .map { region: KijiRegion =>
              val startKey: Array[Byte] = region.getStartKey()
              // TODO(KIJIMR-65): For now pick the first available location (ie. region server),
              //     if any.
              val location: String = if (region.getLocations().isEmpty()) {
                    null
                  } else {
                    region.getLocations().iterator().next()
                  }
              val tableSplit: TableSplit = new TableSplit(
                  htable.getTableName(),
                  startKey,
                  region.getEndKey(),
                  location)

              new KijiTableSplit(tableSplit)
            }
            .toArray
      }
    }
  }

  /**
   * Gets a record reader that will scan over a subset of rows in a Kiji table.
   *
   * @param split the record reader should operate over.
   * @param configuration of the job that uses the record reader. The configuration should specify
   *     the input Kiji table through the configuration variable
   *     [[KijiConfKeys#KIJI_INPUT_TABLE_URI]] and a serialized [[KijiDataRequest]] through
   *     the configuration variable [[KijiConfKeys#KIJI_INPUT_DATA_REQUEST]].
   * @param reporter is ignored by this method.
   * @return An [[KijiRecordReader]] that will scan over a subset of rows in a Kiji table.
   */
  override def getRecordReader(
      split: InputSplit,
      configuration: JobConf,
      reporter: Reporter): RecordReader[KijiKey, KijiValue] = {
    split match {
      // TODO: Use reporter to report progress.
      case kijiSplit: KijiTableSplit => new KijiRecordReader(kijiSplit, configuration)
      case _ => sys.error("KijiInputFormat requires a KijiTableSplit.")
    }
  }
}

/**
 * A record reader that can scan a subset of rows in a Kiji table. This record reader is
 * configured to read from a Kiji table under certain data request parameters through a Hadoop
 * [[Configuration]] It returns key-value pairs of type [[KijiKey]] (a wrapper around
 * [[org.kiji.schema.EntityId]] and [[KijiValue]] (a wrapper around [[KijiRowData]]).
 *
 * The record reader will scan over rows in the table specified in the provided input split,
 * subject to row limits specified in the data request serialized into the specified
 * configuration.
 *
 * @param split for the MapReduce task that will use this record reader. The split specifies a
 *     subset of rows from a Kiji table.
 * @param configuration for the MapReduce job using this record reader. The configuration
 *     should specify the input Kiji table through the configuration variable
 *     [[KijiConfKeys#KIJI_INPUT_TABLE_URI]] and a serialized [[KijiDataRequest]]
 *     through the configuration variable [[KijiConfKeys#KIJI_INPUT_DATA_REQUEST]].
 */
class KijiRecordReader(
    val split: KijiTableSplit,
    val configuration: Configuration)
    extends RecordReader[KijiKey, KijiValue] {
  if (!split.isInstanceOf[KijiTableSplit]) {
    sys.error("KijiRecordReader received an InputSplit that was not a KijiTableSplit.")
  }

  /** The data request used to read from the Kiji table. */
  val dataRequest: KijiDataRequest = {
    // Get data request from the job configuration.
    val dataRequestB64: String = {
      Option(configuration.get(KijiConfKeys.KIJI_INPUT_DATA_REQUEST)) match {
        case Some(str) => str
        case None => sys.error("Missing data request in job configuration.")
      }
    }

    val dataRequestBytes: Array[Byte] = Base64.decodeBase64(Bytes.toBytes(dataRequestB64))
    SerializationUtils.deserialize(dataRequestBytes)
        .asInstanceOf[KijiDataRequest]
  }

  val inputURI: KijiURI = KijiURI
      .newBuilder(configuration.get(KijiConfKeys.KIJI_INPUT_TABLE_URI))
      .build()

  /** A reader for the above table. */
  val reader: KijiTableReader = {
    doAndRelease(Kiji.Factory.open(inputURI, configuration)) { kiji: Kiji =>
      doAndRelease(kiji.openTable(inputURI.getTable())) { table: KijiTable =>
        table.openTableReader()
      }
    }
  }
  /** Used to scan a subset of rows from the table. */
  val scanner: KijiRowScanner = {
    val scannerOptions: KijiScannerOptions = new KijiScannerOptions()
        .setStartRow(HBaseEntityId.fromHBaseRowKey(split.getStartRow()))
        .setStopRow(HBaseEntityId.fromHBaseRowKey(split.getEndRow()))

    reader.getScanner(dataRequest, scannerOptions)
  }
  /** An interator over the rows retrieved by the scanner. */
  val iterator: java.util.Iterator[KijiRowData] = scanner.iterator()

  /**
   * @return a new, empty, reusable instance of [[KijiKey]] which will hold keys read by
   *     this record reader.
   */
  override def createKey(): KijiKey = new KijiKey()

  /**
   * @return a new, empty, reusable instance of [[KijiValue]] which will hold values read by
   *     this record reader.
   */
  override def createValue(): KijiValue = new KijiValue()

  /**
   * @return <code>OL</code> always, because it's impossible to tell how much we've read through
   *     a particular key range, because we have no knowledge of how many rows are actually in
   *     the range.
   */
  override def getPos(): Long = 0L

  /**
   * @return <code>0.0</code> always, because it's impossible to tell how much we've read through
   *     a particular key range, because we have no knowledge of how many rows are actually in
   *     the range.
   */
  override def getProgress(): Float = 0.0f

  /**
   * Populates the specified key and value with the next key-value pair read from the input
   * split.
   *
   * @param key instance to populate with the next key read.
   * @param value instance to popualte with the next value read.
   * @return <code>true</code> if a new key-value was read, <code>false</code> if we have reached
   *     the end of the input split.
   */
  override def next(key: KijiKey, value: KijiValue): Boolean = {
    if (iterator.hasNext()) {
      // Read the next row and store it in the provided key/value pair.
      val row: KijiRowData = iterator.next()
      if (null != key) {
        key.set(row.getEntityId())
      }
      if (null != value) {
        value.set(row)
      }
      true
    } else {
      false
    }
  }

  /** Release all resources used by this record reader. */
  override def close() {
    scanner.close()
    reader.close()
  }
}
