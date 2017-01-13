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
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.TestConfiguration;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

/**
 * Test for Example plugin.
 */
public class ExampleTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-pipeline", "4.0.0");

  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "4.0.0");

  private static final String OUTPUT_COLUMN = "input";
  private static final String APP_NAME = "ExampleTest";

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

  @Test
  public void testExample() throws Exception {
    /**
     * Create a mocked pipeline and deploy an instance of the application.
     */

//    ETLBatchConfig etlConfig = buildETLBatchConfig("source", "sink");
//    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
//    ApplicationId appId = NamespaceId.DEFAULT.app(APP_NAME);
//    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    /**
     * Get handle to the Table Dataset embedded within the pipeline.
     */
//    DataSetManager<Table> inputManager = getDataset("source");

    /**
     * Create the input record to be written to source -- which is a table dataset.
     */
//    List<StructuredRecord> input = ImmutableList.of(
//      StructuredRecord.builder(SOURCE_SCHEMA_MULTIPLE).set(COLUMN_TOKENIZED, SENTENCE1).set(NAME_COLUMN, "CDAP")
//        .build(),
//      StructuredRecord.builder(SOURCE_SCHEMA_MULTIPLE).set(COLUMN_TOKENIZED, SENTENCE2).set(NAME_COLUMN, "Hydrator")
//        .build(),
//      StructuredRecord.builder(SOURCE_SCHEMA_MULTIPLE).set(COLUMN_TOKENIZED, SENTENCE3).set(NAME_COLUMN, "Studio")
//        .build(),
//      StructuredRecord.builder(SOURCE_SCHEMA_MULTIPLE).set(COLUMN_TOKENIZED, SENTENCE4).set(NAME_COLUMN, "Plugins")
//        .build());

    /**
     * Write the record to the input dataset.
     */
//   MockSource.writeInput(inputManager, input);

    /**
     * Manually trigger the workflow to kick-off the pipeline.
     */
//  WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
//  workflowManager.start();
//  workflowManager.waitForFinish(5, TimeUnit.MINUTES);

    /**
     * Get handle to the output dataset to validate the results.
     */
//    DataSetManager<Table> texts = getDataset("sink");
//    List<StructuredRecord> output = MockSink.readOutput(texts);
//    Set<List> results = new HashSet<>();
//    for (StructuredRecord structuredRecord : output) {
//      results.add((ArrayList) structuredRecord.get(OUTPUT_COLUMN));
//    }

    /**
     * Validate against expected results.
     */
//    //Create expected data
//    Set<List<String>> expected = getExpectedData();
//    Assert.assertEquals(expected, results);
//    Assert.assertEquals(4, output.size());
  }
}
