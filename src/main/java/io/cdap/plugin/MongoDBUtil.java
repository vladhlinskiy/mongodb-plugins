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

package io.cdap.plugin;

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.data.schema.Schema;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Common Util class for big query plugins such as {@link io.cdap.plugin.batch.source.MongoDBBatchSource}
 * and {@link io.cdap.plugin.batch.sink.MongoDBBatchSink}
 */
public final class MongoDBUtil {

  private MongoDBUtil() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * Validates given input/output schema according the the specified supported types. Fields of types
   * {@link Schema.Type#RECORD}, {@link Schema.Type#ARRAY}, {@link Schema.Type#MAP} will be validated recursively.
   *
   * @param schema                schema to validate.
   * @param supportedLogicalTypes set of supported logical types.
   * @param supportedTypes        set of supported types.
   * @throws IllegalArgumentException in the case when schema is invalid.
   */
  public static void validateSchema(Schema schema,
                                    Set<Schema.LogicalType> supportedLogicalTypes,
                                    Set<Schema.Type> supportedTypes) {

    Preconditions.checkNotNull(supportedLogicalTypes, "Supported logical types can not be null");
    Preconditions.checkNotNull(supportedTypes, "Supported types can not be null");
    if (schema == null) {
      throw new IllegalArgumentException("Schema must be specified");
    }
    validateRecordSchema(null, schema, supportedLogicalTypes, supportedTypes);
  }

  private static void validateRecordSchema(@Nullable String fieldName,
                                           Schema schema,
                                           Set<Schema.LogicalType> supportedLogicalTypes,
                                           Set<Schema.Type> supportedTypes) {
    Schema nonNullableSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    List<Schema.Field> fields = nonNullableSchema.getFields();
    if (fields == null || fields.isEmpty()) {
      throw new IllegalArgumentException("Schema must contain fields");
    }
    for (Schema.Field field : fields) {
      // Use full field name for nested records to construct meaningful errors messages.
      // Full field names will be in the following format: 'record_field_name.nested_record_field_name'
      String fullFieldName = fieldName != null ? String.format("%s.%s", fieldName, field.getName()) : field.getName();
      validateFieldSchema(fullFieldName, field.getSchema(), supportedLogicalTypes, supportedTypes);
    }
  }

  private static void validateFieldSchema(String fieldName,
                                          Schema schema,
                                          Set<Schema.LogicalType> supportedLogicalTypes,
                                          Set<Schema.Type> supportedTypes) {
    Schema nonNullableSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    Schema.Type type = nonNullableSchema.getType();
    Schema.LogicalType logicalType = nonNullableSchema.getLogicalType();
    String supportedTypeNames = Stream.concat(supportedTypes.stream(), supportedLogicalTypes.stream())
      .map(Enum::name)
      .map(String::toLowerCase)
      .collect(Collectors.joining(", "));
    switch (type) {
      case RECORD:
        validateRecordSchema(fieldName, schema, supportedLogicalTypes, supportedTypes);
        break;
      case ARRAY:
        validateArraySchema(fieldName, schema, supportedLogicalTypes, supportedTypes);
        break;
      case MAP:
        validateMapSchema(fieldName, schema, supportedLogicalTypes, supportedTypes);
        break;
      default:
        if (!isSchemaTypeSupported(nonNullableSchema, supportedLogicalTypes, supportedTypes)) {
          String actualTypeName = logicalType != null ? logicalType.name().toLowerCase() : type.name().toLowerCase();
          throw new IllegalArgumentException(
            String.format("Field '%s' is of unsupported type '%s'. Supported types are: %s.", fieldName, actualTypeName,
                          supportedTypeNames));
        }
    }
  }

  private static void validateMapSchema(String fieldName,
                                        Schema schema,
                                        Set<Schema.LogicalType> supportedLogicalTypes,
                                        Set<Schema.Type> supportedTypes) {
    Schema nonNullableSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    Schema keySchema = nonNullableSchema.getMapSchema().getKey();
    if (keySchema.isNullable()) {
      throw new IllegalArgumentException(String.format("Key schema can not be nullable for map field '%s'", fieldName));
    }

    Schema.LogicalType keyLogicalType = keySchema.isNullable() ? keySchema.getNonNullable().getLogicalType()
      : keySchema.getLogicalType();
    Schema.Type keyType = keySchema.isNullable() ? keySchema.getNonNullable().getType() : keySchema.getType();
    if (keyType != Schema.Type.STRING) {
      String actualTypeName = keyLogicalType != null ? keyLogicalType.name().toLowerCase()
        : keyType.name().toLowerCase();
      throw new IllegalArgumentException(String.format("Key schema must be of type 'string', but was '%s' for map " +
                                                         "field '%s'.", actualTypeName, fieldName));
    }
    validateFieldSchema(fieldName, nonNullableSchema.getMapSchema().getValue(), supportedLogicalTypes, supportedTypes);
  }

  private static void validateArraySchema(String fieldName,
                                          Schema schema,
                                          Set<Schema.LogicalType> supportedLogicalTypes,
                                          Set<Schema.Type> supportedTypes) {
    Schema nonNullableSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    Schema componentSchema = nonNullableSchema.getComponentSchema().isNullable()
      ? nonNullableSchema.getComponentSchema().getNonNullable() : nonNullableSchema.getComponentSchema();
    validateFieldSchema(fieldName, componentSchema, supportedLogicalTypes, supportedTypes);
  }

  private static boolean isSchemaTypeSupported(Schema fieldSchema,
                                               Set<Schema.LogicalType> supportedLogicalTypes,
                                               Set<Schema.Type> supportedTypes) {
    Schema nonNullableSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
    if (supportedTypes.contains(nonNullableSchema.getType()) ||
      supportedLogicalTypes.contains(nonNullableSchema.getLogicalType())) {
      return true;
    }
    return false;
  }

}
