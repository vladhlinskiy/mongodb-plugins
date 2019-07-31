/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.batch.source;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.ErrorHandling;
import io.cdap.plugin.MongoDBConfig;
import io.cdap.plugin.MongoDBConstants;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSource;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.common.SourceInputFormatProvider;
import org.apache.hadoop.conf.Configuration;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * A {@link BatchSource} that reads data from MongoDB and converts each document into 
 * a {@link StructuredRecord} using the specified Schema.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(MongoDBConstants.PLUGIN_NAME)
@Description("MongoDB Batch Source will read documents from MongoDB and convert each document " +
  "into a StructuredRecord with the help of the specified Schema. ")
public class MongoDBBatchSource extends ReferenceBatchSource<Object, BSONObject, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDBBatchSource.class);
  private final MongoDBSourceConfig config;
  private BSONObjectToRecordTransformer bsonObjectToRecordTransformer;

  public MongoDBBatchSource(MongoDBSourceConfig config) {
    super(new ReferencePluginConfig(config.referenceName));
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
    Schema schema = config.getSchema();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    config.validate();
    Configuration conf = new Configuration();
    conf.clear();

    MongoConfigUtil.setInputFormat(conf, MongoInputFormat.class);
    MongoConfigUtil.setInputURI(conf, config.getConnectionString());
    if (!Strings.isNullOrEmpty(config.inputQuery)) {
      MongoConfigUtil.setQuery(conf, config.inputQuery);
    }
    if (!Strings.isNullOrEmpty(config.authConnectionString)) {
      MongoConfigUtil.setAuthURI(conf, config.authConnectionString);
    }

    emitLineage(context);
    context.setInput(Input.of(config.referenceName,
                              new SourceInputFormatProvider(MongoConfigUtil.getInputFormat(conf), conf)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    bsonObjectToRecordTransformer = new BSONObjectToRecordTransformer(config.getSchema());
  }

  @Override
  public void transform(KeyValue<Object, BSONObject> input, Emitter<StructuredRecord> emitter) throws Exception {
    try {
    BSONObject bsonObject = input.getValue();
    emitter.emit(bsonObjectToRecordTransformer.transform(bsonObject));
    } catch (Exception e) {
      switch (config.getErrorHandling()) {
        case SKIP:
          LOG.warn("Failed to process record, skipping it", e);
          break;
        case FAIL_PIPELINE:
          throw new RuntimeException("Failed to process record", e);
        default:
          // this should never happen because it is validated at configure and prepare time
          throw new IllegalStateException(String.format("Unknown error handling strategy '%s'",
                                                        config.getErrorHandling()));
      }
    }
  }

  private void emitLineage(BatchSourceContext context) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(config.getSchema());
    List<Schema.Field> fields = Objects.requireNonNull(config.getSchema()).getFields();
    if (fields != null && !fields.isEmpty()) {
      lineageRecorder.recordRead("Read",
                                 String.format("Read from '%s' MongoDB collection.", config.collection),
                                 fields.stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  /**
   * Config class for {@link MongoDBBatchSource}.
   */
  public static class MongoDBSourceConfig extends MongoDBConfig {

    public static final Set<Schema.Type> SUPPORTED_SIMPLE_TYPES = ImmutableSet.of(Schema.Type.BOOLEAN, Schema.Type.INT,
                                                                                  Schema.Type.DOUBLE, Schema.Type.BYTES,
                                                                                  Schema.Type.LONG, Schema.Type.STRING,
                                                                                  Schema.Type.ARRAY, Schema.Type.RECORD,
                                                                                  Schema.Type.MAP);

    public static final Set<Schema.LogicalType> SUPPORTED_LOGICAL_TYPES = ImmutableSet.of(
      Schema.LogicalType.DECIMAL, Schema.LogicalType.TIMESTAMP_MILLIS);

    @Name(MongoDBConstants.SCHEMA)
    @Description("Schema of records output by the source.")
    public String schema;

    @Name(MongoDBConstants.INPUT_QUERY)
    @Description("Optionally filter the input collection with a query. This query must be represented in JSON " +
      "format, and use the MongoDB extended JSON format to represent non-native JSON data types.")
    @Nullable
    @Macro
    public String inputQuery;

    @Name(MongoDBConstants.ON_ERROR)
    @Description("Specifies how to handle error in record processing. Error will be thrown if failed to parse value " +
      "according to provided schema.")
    @Macro
    public String onError;

    @Name(MongoDBConstants.AUTH_CONNECTION_STRING)
    @Nullable
    @Description("Auxiliary MongoDB connection string to authenticate against when constructing splits.")
    @Macro
    public String authConnectionString;

    @Nullable
    public Schema getSchema() {
      try {
        return null == schema ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        throw new InvalidConfigPropertyException("Invalid schema", e, MongoDBConstants.SCHEMA);
      }
    }

    public ErrorHandling getErrorHandling() {
      return Objects.requireNonNull(ErrorHandling.fromDisplayName(onError));
    }

    @Override
    public void validate() {
      super.validate();
      if (!containsMacro(MongoDBConstants.ON_ERROR) && null != onError) {
        if (Strings.isNullOrEmpty(onError)) {
          throw new InvalidConfigPropertyException("Error handling must be specified", MongoDBConstants.ON_ERROR);
        }
        if (null == ErrorHandling.fromDisplayName(onError)) {
          throw new InvalidConfigPropertyException("Invalid record error handling strategy name",
                                                   MongoDBConstants.ON_ERROR);
        }
      }
      if (!containsMacro(MongoDBConstants.SCHEMA)) {
        Schema parsedSchema = getSchema();
        if (parsedSchema == null) {
          throw new InvalidConfigPropertyException("Schema must be specified", MongoDBConstants.SCHEMA);
        }
        List<Schema.Field> fields = parsedSchema.getFields();
        if (fields == null || fields.isEmpty()) {
          throw new InvalidConfigPropertyException("Schema should contain fields to map", MongoDBConstants.SCHEMA);
        }
        for (Schema.Field field : fields) {

          Schema nonNullableSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable()
            : field.getSchema();

          if (!SUPPORTED_SIMPLE_TYPES.contains(nonNullableSchema.getType()) &&
            !SUPPORTED_LOGICAL_TYPES.contains(nonNullableSchema.getLogicalType())) {
            String supportedTypes = Stream.concat(SUPPORTED_SIMPLE_TYPES.stream(), SUPPORTED_LOGICAL_TYPES.stream())
              .map(Enum::name)
              .map(String::toLowerCase)
              .collect(Collectors.joining(", "));

            String actualTypeName = nonNullableSchema.getLogicalType() != null
              ? nonNullableSchema.getLogicalType().name().toLowerCase()
              : nonNullableSchema.getType().name().toLowerCase();

            String errorMessage = String.format("Field '%s' is of unsupported type '%s'. Supported types are: %s. ",
                                                field.getName(), actualTypeName, supportedTypes);
            throw new InvalidConfigPropertyException(errorMessage, MongoDBConstants.SCHEMA);
          }
        }
      }
    }
  }
}
