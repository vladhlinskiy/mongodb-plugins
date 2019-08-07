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

package io.cdap.plugin.batch.sink;

import com.google.common.base.Strings;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.io.BSONWritable;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Transforms {@link StructuredRecord} to {@link BSONWritable}.
 */
public class RecordToBSONWritableTransformer {

  private static final String DEFAULT_ID_FIELD_NAME = "_id";

  private String idFieldName;

  public RecordToBSONWritableTransformer() {
  }

  /**
   * @param idFieldName specifies which of the incoming fields should be used as an document identifier. Identifier will
   *                be generated if no value is specified.
   */
  public RecordToBSONWritableTransformer(@Nullable String idFieldName) {
    this.idFieldName = idFieldName;
  }

  /**
   * Transforms given {@link StructuredRecord} to {@link BSONWritable}.
   *
   * @param original structured record to be transformed.
   * @return {@link BSONWritable} that corresponds to the given {@link StructuredRecord}.
   */
  public BSONWritable transform(StructuredRecord original) {
    StructuredRecord record = Strings.isNullOrEmpty(idFieldName) || DEFAULT_ID_FIELD_NAME.equals(idFieldName) ?
      original : createRecordWithId(original);
    return new BSONWritable(extractRecord(record, DEFAULT_ID_FIELD_NAME));
  }

  private StructuredRecord createRecordWithId(StructuredRecord original) {
    Schema.Field idField = original.getSchema().getField(idFieldName);

    if (idField == null) {
      throw new UnexpectedFormatException(String.format("Record does not contain identifier field '%s'.", idFieldName));
    }

    Schema.Field defaultIdField = original.getSchema().getField(DEFAULT_ID_FIELD_NAME);
    if (defaultIdField != null) {
      throw new UnexpectedFormatException(String.format("Record already contains identifier field '%s'.",
                                                        DEFAULT_ID_FIELD_NAME));
    }

    List<Schema.Field> copiedFields = Objects.requireNonNull(original.getSchema().getFields()).stream()
      .filter(field -> !idFieldName.equals(field.getName()))
      .collect(Collectors.toList());

    List<Schema.Field> recordWithIdFields = new ArrayList<>();
    recordWithIdFields.add(Schema.Field.of(DEFAULT_ID_FIELD_NAME, idField.getSchema()));
    recordWithIdFields.addAll(copiedFields);

    String recordName = Objects.requireNonNull(original.getSchema().getRecordName(), "Record name can not be empty");
    Schema recordWithIdSchema = Schema.recordOf(recordName, recordWithIdFields);
    StructuredRecord.Builder builder = StructuredRecord.builder(recordWithIdSchema);
    for (Schema.Field field : original.getSchema().getFields()) {
      if (idFieldName.equals(field.getName())) {
        builder.set(DEFAULT_ID_FIELD_NAME, original.get(idFieldName));
        continue;
      }

      builder.set(field.getName(), original.get(field.getName()));
    }

    return builder.build();
  }


  private DBObject extractRecord(Object value, @Nullable String idFieldName) {
    if (value == null) {
      // Return 'null' value as it is
      return null;
    }
    StructuredRecord record = (StructuredRecord) value;
    List<Schema.Field> fields = Objects.requireNonNull(record.getSchema().getFields(), "Schema fields cannot be empty");
    BasicDBObjectBuilder bsonBuilder = BasicDBObjectBuilder.start();
    for (Schema.Field field : fields) {
      Schema.LogicalType fieldLogicalType = field.getSchema().isNullable()
        ? field.getSchema().getNonNullable().getLogicalType()
        : field.getSchema().getLogicalType();

      if (!Strings.isNullOrEmpty(idFieldName) && idFieldName.equals(field.getName())) {
        bsonBuilder.add(idFieldName, extractObjectId(record.get(field.getName())));
        continue;
      }

      if (fieldLogicalType == null) {
        bsonBuilder.add(field.getName(), extractValue(record.get(field.getName()), field.getSchema()));
        continue;
      }

      // Get values of logical types properly
      switch (fieldLogicalType) {
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          ZonedDateTime timestamp = Objects.requireNonNull(record.getTimestamp(field.getName()));
          bsonBuilder.add(field.getName(), Date.from(timestamp.toInstant()));
          break;
        case TIME_MILLIS:
        case TIME_MICROS:
          LocalTime time = Objects.requireNonNull(record.getTime(field.getName()));
          bsonBuilder.add(field.getName(), time.toString());
          break;
        case DATE:
          LocalDate localDate = Objects.requireNonNull(record.getDate(field.getName()));
          Date date = Date.from(localDate.atStartOfDay(ZoneId.ofOffset("UTC", ZoneOffset.UTC)).toInstant());
          bsonBuilder.add(field.getName(), date);
          break;
        case DECIMAL:
          BigDecimal decimal = Objects.requireNonNull(record.getDecimal(field.getName()));
          bsonBuilder.add(field.getName(), new Decimal128(decimal));
          break;
        default:
          throw new UnexpectedFormatException("Field type '" + fieldLogicalType + "' is not supported.");
      }
    }
    return bsonBuilder.get();
  }

  private ObjectId extractObjectId(Object value) {
    if (value instanceof byte[]) {
      return new ObjectId((byte[]) value);
    } else if (value instanceof ByteBuffer) {
      byte[] idBytes = ((ByteBuffer) value).array();
      return new ObjectId(idBytes);
    } else if (value instanceof String) {
      return new ObjectId((String) value);
    }

    throw new UnexpectedFormatException("Invalid ID field value: " + value);
  }

  private Object extractValue(Object value, Schema fieldSchema) {
    if (value == null) {
      // Return 'null' value as it is
      return null;
    }

    Schema nonNullableSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
    Schema.Type fieldType = nonNullableSchema.getType();
    switch (fieldType) {
      case BOOLEAN:
        return (Boolean) value;
      case INT:
        return (Integer) value;
      case DOUBLE:
        return (Double) value;
      case FLOAT:
        return (Float) value;
      case BYTES:
        if (value instanceof ByteBuffer) {
          return Bytes.getBytes((ByteBuffer) value);
        } else {
          return (byte[]) value;
        }
      case LONG:
        return (Long) value;
      case STRING:
        return (String) value;
      case MAP:
        return extractMap(value, nonNullableSchema);
      case ARRAY:
        return extractArray(value, nonNullableSchema);
      case RECORD:
        return extractRecord(value, null);
      case UNION:
        return extractUnion(value, nonNullableSchema);
      case ENUM:
        String enumValue = (String) value;
        if (!Objects.requireNonNull(nonNullableSchema.getEnumValues()).contains(enumValue)) {
          throw new UnexpectedFormatException(String.format("Value '%s' is not enum value. Enum values are: '%s'",
                                                            enumValue, nonNullableSchema.getEnumValues()));
        }

        return enumValue;
      default:
        throw new UnexpectedFormatException("Field type '" + fieldType + "' is not supported.");
    }
  }

  private DBObject extractMap(Object value, Schema schema) {
    if (value == null) {
      // Return 'null' value as it is
      return null;
    }
    Map.Entry<Schema, Schema> mapSchema = Objects.requireNonNull(schema.getMapSchema());
    if (mapSchema.getKey().getType() != Schema.Type.STRING) {
      throw new UnexpectedFormatException("Key of the 'map' schema must be of the 'string' type.");
    }
    if (!(value instanceof Map)) {
      throw new UnexpectedFormatException(String.format("Unexpected value of 'map' type: '%s'", value));
    }

    Map<String, ?> map = (Map<String, ?>) value;
    BasicDBObjectBuilder bsonBuilder = BasicDBObjectBuilder.start();
    map.forEach((k, v) -> bsonBuilder.add(k, extractValue(v, mapSchema.getValue())));

    return bsonBuilder.get();
  }

  private List extractArray(Object value, Schema schema) {
    if (value == null) {
      // Return 'null' value as it is
      return null;
    }
    if (!(value instanceof List)) {
      throw new UnexpectedFormatException(String.format("Unexpected value of 'array' type '%s'", value));
    }
    List values = (List) value;
    List<Object> extracted = new ArrayList<>();
    Schema componentSchema = schema.getComponentSchema();
    for (Object obj : values) {
      extracted.add(extractValue(obj, componentSchema));
    }
    return extracted;
  }

  private Object extractUnion(Object value, Schema schema) {
    if (value == null) {
      // Return 'null' value as it is
      return null;
    }

    List<Schema.Type> unionTypes = schema.getUnionSchemas().stream()
      .map(s -> s.isNullable() ? s.getNonNullable() : s)
      .map(Schema::getType)
      .collect(Collectors.toList());
    // Extract values of types that can not be written directly or may contain nested values of such types
    if (unionTypes.indexOf(Schema.Type.BYTES) != -1 && value instanceof ByteBuffer) {
      // ByteBuffer -> byte[] conversion required
      int schemaIndex = unionTypes.indexOf(Schema.Type.BYTES);
      return extractValue(value, schema.getUnionSchema(schemaIndex));
    } else if (unionTypes.indexOf(Schema.Type.ENUM) != -1 && value instanceof String) {
      // Enum validation required
      int schemaIndex = unionTypes.indexOf(Schema.Type.ENUM);
      return extractValue(value, schema.getUnionSchema(schemaIndex));
    } else if (unionTypes.indexOf(Schema.Type.ARRAY) != -1 && value instanceof List) {
      int schemaIndex = unionTypes.indexOf(Schema.Type.ARRAY);
      return extractArray(value, schema.getUnionSchema(schemaIndex));
    } else if (unionTypes.indexOf(Schema.Type.MAP) != -1 && value instanceof Map) {
      int schemaIndex = unionTypes.indexOf(Schema.Type.MAP);
      return extractMap(value, schema.getUnionSchema(schemaIndex));
    } else if (unionTypes.indexOf(Schema.Type.RECORD) != -1 && value instanceof StructuredRecord) {
      return extractRecord(value, null);
    }

    return value;
  }
}
