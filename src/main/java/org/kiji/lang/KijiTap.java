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

package org.kiji.lang;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeCollector;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.DistributedCacheJars;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.util.Jars;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;

/**
 * A {@link Tap} for reading data from a Kiji table. The tap is responsible for configuring a
 * MapReduce job with the correct input format for reading from a Kiji table,
 * as well as the proper classpath dependencies for MapReduce tasks.
 *
 * Note: Warnings about a missing serialVersionUID are ignored here. When KijiTap is serialized,
 * the result is not persisted anywhere making serialVersionUID unnecessary.
 */
@SuppressWarnings({ "serial", "rawtypes" })
@ApiAudience.Framework
@ApiStability.Unstable
public final class KijiTap
    extends Tap<JobConf, RecordReader, OutputCollector> {
  private static final Logger LOG = LoggerFactory.getLogger(KijiTap.class);

  private static final String DEPENDENCY_ERROR_MSG =
      "Chopsticks was unable to find dependency jars for the Kiji framework. If you are using "
      + "KijiBento, make sure you have sourced the 'kiji-env.sh' script. Otherwise, ensure you "
      + "have the environment variable $KIJI_HOME set to the path to a KijiSchema distribution, "
      + "and the environment variable $KIJI_MR_HOME set to the path to a KijiMR distribution.";

  /** The URI of the table to be read through this tap. */
  private final String mTableURI;
  /** The scheme to be used with this tap. */
  private final KijiScheme mScheme;
  /** A unique identifier for this tap instance. */
  private final String mId;

  /**
   * Creates a new instance of this tap.
   *
   * @param tableURI for the Kiji table this tap will be used to read.
   * @param scheme to be used with this tap that will convert data read from Kiji into Cascading's
   *     tuple model. Note: You must use {@link KijiScheme} with this tap.
   * @throws IOException if there is an error creating the tap.
   */
  public KijiTap(KijiURI tableURI, KijiScheme scheme) throws IOException {
    // Set the scheme for this tap.
    super(scheme);
    mTableURI = tableURI.toString();
    mScheme = scheme;
    mId = UUID.randomUUID().toString();
  }

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that reads from a Kiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param process Current Cascading flow being built.
   * @param conf The job configuration object.
   */
  @Override
  public void sourceConfInit(FlowProcess<JobConf> process, JobConf conf) {
    // Configure the job's input format.
    conf.setInputFormat(KijiInputFormat.class);

    // Store the input table.
    conf.set(KijiConfKeys.KIJI_INPUT_TABLE_URI, mTableURI);

    // Put Kiji dependency jars on the distributed cache.
    try {
      getStepConfigDef().setProperty(
          "tmpjars",
          StringUtils.join(findKijiJars(conf), ","));
    } catch (IOException ioe) {
      throw new RuntimeException(DEPENDENCY_ERROR_MSG, ioe);
    }

    super.sourceConfInit(process, conf);
  }

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that writes to a Kiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param process Current Cascading flow being built.
   * @param conf The job configuration object.
   */
  @Override
  public void sinkConfInit(FlowProcess<JobConf> process, JobConf conf) {
    // TODO(CHOP-35): Use an output format that writes to HFiles.
    // Configure the job's output format.
    conf.setOutputFormat(NullOutputFormat.class);

    // Store the output table.
    conf.set(KijiConfKeys.KIJI_OUTPUT_TABLE_URI, mTableURI);

    // Put Kiji dependency jars on the distributed cache.
    try {
      getStepConfigDef().setProperty(
          "tmpjars",
          StringUtils.join(findKijiJars(conf), ","));
    } catch (IOException ioe) {
      throw new RuntimeException(DEPENDENCY_ERROR_MSG, ioe);
    }

    super.sinkConfInit(process, conf);
  }

  /** {@inheritDoc} */
  @Override
  public String getIdentifier() {
    return mId;
  }

  /** {@inheritDoc} */
  @Override
  public TupleEntryIterator openForRead(FlowProcess<JobConf> jobConfFlowProcess,
      RecordReader recordReader) throws IOException {
    return new HadoopTupleEntrySchemeIterator(jobConfFlowProcess, this, recordReader);
  }

  /** {@inheritDoc} */
  @Override
  public TupleEntryCollector openForWrite(FlowProcess<JobConf> jobConfFlowProcess,
      OutputCollector outputCollector) throws IOException {
    return new HadoopTupleEntrySchemeCollector(jobConfFlowProcess, this, outputCollector);
  }

  /** {@inheritDoc} */
  @Override
  public boolean createResource(JobConf jobConf) throws IOException {
    throw new UnsupportedOperationException("KijiTap does not support creating tables for you.");
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteResource(JobConf jobConf) throws IOException {
    throw new UnsupportedOperationException("KijiTap does not support deleting tables for you.");
  }

  /** {@inheritDoc} */
  @Override
  public boolean resourceExists(JobConf jobConf) throws IOException {
    final KijiURI uri = KijiURI.newBuilder(mTableURI).build();
    final String tableName = uri.getTable();
    final Kiji kiji = Kiji.Factory.open(uri);

    return kiji.getTableNames().contains(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public long getModifiedTime(JobConf jobConf) throws IOException {
    return System.currentTimeMillis(); // currently unable to find last mod time on a table
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof KijiTap)) {
      return false;
    }

    final KijiTap tap = (KijiTap) other;
    return Objects.equal(mTableURI, tap.mTableURI)
        && Objects.equal(mScheme, tap.mScheme)
        && Objects.equal(mId, tap.mId);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mTableURI, mScheme, mId);
  }

  /**
   * Gets the path to the jar file containing a specific class.
   *
   * @param clazz whose jar file should be found.
   * @param libName of the library provided by the jar being searched for. Used only for debug
   *     messages.
   * @return the path to the jar file containing the specified class.
   * @throws IOException if there is a problem retrieving the file for the jar.
   * @throws ClassNotFoundException if the requested class is not on the classpath.
   */
  private static Path findJarForClass(Class<?> clazz, String libName)
      throws IOException, ClassNotFoundException {
    final File jarFile = new File(Jars.getJarPathForClass(clazz));
    Path jarPath = new Path(jarFile.getCanonicalPath());
    LOG.debug("Found {} jar: {}", libName, jarPath);
    return jarPath;
  }

  /**
   * Finds Kiji dependency jars and returns a list of their paths. Use this method to find
   * jars that need to be sent to Hadoop's DistributedCache.
   *
   * @param fsConf Configuration containing options for the filesystem containing jars.
   * @throws IOException If there is an error.
   * @return A list of paths to dependency jars.
   */
  private static List<Path> findKijiJars(Configuration fsConf) throws IOException {
    final List<Path> jars = Lists.newArrayList();

    // Find the kiji jars.
    Path schemaJar;
    Path mapreduceJar;
    Path chopsticksJar;
    try {
      schemaJar = findJarForClass(Kiji.class, "kiji-schema");
      mapreduceJar = findJarForClass(KijiProducer.class, "kiji-mapreduce");
      chopsticksJar = findJarForClass(KijiTap.class, "kiji-chopsticks");
    } catch (ClassNotFoundException cnfe) {
      LOG.warn("At least one kiji dependency jar could not be found; no kiji dependency jars will "
          + "be loaded onto the distributed cache.");
      return jars;
    }

    // Add all dependency jars from the directories containing the kiji-schema, kiji-mapreduce,
    // and kiji-chopsticks jars.
    jars.addAll(DistributedCacheJars.listJarFilesFromDirectory(fsConf, schemaJar.getParent()));
    jars.addAll(DistributedCacheJars.listJarFilesFromDirectory(fsConf, mapreduceJar.getParent()));
    jars.addAll(DistributedCacheJars.listJarFilesFromDirectory(fsConf, chopsticksJar.getParent()));

    // Remove duplicate jars and return.
    return DistributedCacheJars.deDuplicateFilenames(jars);
  }
}
