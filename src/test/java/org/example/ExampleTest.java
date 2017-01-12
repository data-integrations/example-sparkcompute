/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.example;

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.TestConfiguration;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.util.Set;

/**
 * Test for Example plugin.
 */
public class ExampleTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-pipeline", "4.0.0");
  private static final String OUTPUT_COLUMN = "input";

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl batch app
    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);
    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(NamespaceId.DEFAULT, DATAPIPELINE_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true));
    addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), parents,
                      Example.class);
  }

  /**
   * @param mockNameOfSourcePlugin used while adding ETLStage for mock source
   * @param mockNameOfSinkPlugin used while adding ETLStage for mock sink
   * @return ETLBatchConfig
   */
  private ETLBatchConfig buildETLBatchConfig(String mockNameOfSourcePlugin,
                                             String mockNameOfSinkPlugin) {
    return ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(mockNameOfSourcePlugin)))
      .addStage(new ETLStage("sparkcompute", new ETLPlugin(Example.PLUGIN_NAME, SparkCompute.PLUGIN_TYPE,
                                           ImmutableMap.of("fieldname", OUTPUT_COLUMN), null)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(mockNameOfSinkPlugin)))
      .addConnection("source", "sparkcompute")
      .addConnection("sparkcompute", "sink")
      .build();
  }
}
