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

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.io.BSONWritable;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import org.bson.types.Decimal128;

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

/**
 * Transforms {@link StructuredRecord} to {@link BSONWritable}.
 */
public class RecordToBSONWritableTransformer {

  /**
   * Transforms given {@link StructuredRecord} to {@link BSONWritable}.
   *
   * @param record structured record to be transformed.
   * @return {@link BSONWritable} that corresponds to the given {@link StructuredRecord}.
   */
  public BSONWritable transform(StructuredRecord record) {
    return new BSONWritable(extractRecord(record));
  }

  private DBObject extractRecord(Object value) {
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
        // TODO handle ID field properly
        if (value instanceof ByteBuffer) {
          return Bytes.getBytes((ByteBuffer) value);
        } else {
          return (byte[]) value;
        }
      case LONG:
        return (Long) value;
      case STRING:
        // TODO handle ID field properly
        return (String) value;
      case MAP:
        return extractMap(value, nonNullableSchema);
      case ARRAY:
        return extractArray(value, nonNullableSchema);
      case RECORD:
        return extractRecord(value);
      case UNION:
        return extractUnion(value, nonNullableSchema);
      case ENUM:
        String enumValue = (String) value;
        if (!Objects.requireNonNull(nonNullableSchema.getEnumValues()).contains(enumValue)) {
          throw new IllegalArgumentException(String.format("Value '%s' is not enum value. Enum values are: '%s'",
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
      throw new UnexpectedFormatException("Value of 'map' type must be instance of 'java.util.Map'.");
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
      throw new UnexpectedFormatException("Value of 'array' type must be instance of 'java.util.List'.");
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
    if (unionTypes.indexOf(Schema.Type.BYTES) != -1 || value instanceof ByteBuffer) {
      // ByteBuffer -> byte[] conversion required
      int schemaIndex = unionTypes.indexOf(Schema.Type.BYTES);
      extractValue(value, schema.getUnionSchema(schemaIndex));
    } else if (unionTypes.indexOf(Schema.Type.ENUM) != -1 || value instanceof String) {
      // Enum validation required
      int schemaIndex = unionTypes.indexOf(Schema.Type.ENUM);
      extractValue(value, schema.getUnionSchema(schemaIndex));
    } else if (unionTypes.indexOf(Schema.Type.ARRAY) != -1 || value instanceof List) {
      int schemaIndex = unionTypes.indexOf(Schema.Type.ARRAY);
      extractArray(value, schema.getUnionSchema(schemaIndex));
    } else if (unionTypes.indexOf(Schema.Type.MAP) != -1 || value instanceof Map) {
      int schemaIndex = unionTypes.indexOf(Schema.Type.MAP);
      extractMap(value, schema.getUnionSchema(schemaIndex));
    } else if (unionTypes.indexOf(Schema.Type.RECORD) != -1 || value instanceof StructuredRecord) {
      extractRecord(value);
    }

    return value;
  }
}
