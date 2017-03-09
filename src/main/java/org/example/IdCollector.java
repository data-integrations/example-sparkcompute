package org.example;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Collects columns by unique id
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(IdCollector.PLUGIN_NAME)
@Description("Id collecting Spark Compute Plugin")
public class IdCollector extends SparkCompute<StructuredRecord, StructuredRecord> {
  public static final String PLUGIN_NAME = "IdCollector";

  Schema schema = Schema.recordOf(
    "record",
    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("col", Schema.arrayOf(Schema.of(Schema.Type.STRING)))
  );

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> input) throws Exception {
    // the  structure record has structure of id:String, timestamp:long, column:String
    JavaPairRDD<String, Iterable<String>> colGroupedById = input.mapToPair(new PairFunction<StructuredRecord, String, String>() {
      @Override
      public Tuple2<String, String> call(StructuredRecord structuredRecord) throws Exception {
        String id = structuredRecord.get("id");
        String column = structuredRecord.get("col");
        return new Tuple2<>(id, column);
      }
    }).groupByKey();

    return colGroupedById.map(new Tuple2StructuredRecordFunction(schema));
  }

  private class Tuple2StructuredRecordFunction implements Function<Tuple2<String, Iterable<String>>, StructuredRecord> {

    private final Schema schema;

    Tuple2StructuredRecordFunction(Schema schema) {
      this.schema = schema;
    }

    @Override
    public StructuredRecord call(Tuple2<String, Iterable<String>> v1) throws Exception {
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      builder.set("id", v1._1);
      builder.set("col", Iterables.toArray(v1._2(), String.class));
      return builder.build();
    }
  }
}
