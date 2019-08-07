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

import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import org.bson.BSONObject;
import org.bson.BsonUndefined;
import org.bson.types.BasicBSONList;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Transforms {@link BSONObject} to {@link StructuredRecord}.
 */
public class BSONObjectToRecordTransformer {

  private final Schema schema;

  public BSONObjectToRecordTransformer(Schema schema) {
    this.schema = schema;
  }

  /**
   * Transforms given {@link BSONObject} to {@link StructuredRecord}.
   *
   * @param bsonObject BSON object to be transformed.
   * @return {@link StructuredRecord} that corresponds to the given {@link BSONObject}.
   */
  public StructuredRecord transform(BSONObject bsonObject) {
    return convertRecord(bsonObject, schema);
  }

  private StructuredRecord convertRecord(Object object, Schema schema) {
    BSONObject bsonObject = (BSONObject) object;
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    List<Schema.Field> fields = Objects.requireNonNull(schema.getFields(), "Schema fields cannot be empty");
    for (Schema.Field field : fields) {
      Object value = extractValue(bsonObject.get(field.getName()), field.getSchema());
      Schema.LogicalType fieldLogicalType = field.getSchema().isNullable()
        ? field.getSchema().getNonNullable().getLogicalType()
        : field.getSchema().getLogicalType();

      if (fieldLogicalType == null) {
        // Set values of simple types
        builder.set(field.getName(), value);
        continue;
      }

      // Set values of logical types properly
      switch (fieldLogicalType) {
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          builder.setTimestamp(field.getName(), (ZonedDateTime) value);
          break;
        case DECIMAL:
          builder.setDecimal(field.getName(), (BigDecimal) value);
          break;
        default:
          throw new UnexpectedFormatException("Field type '" + fieldLogicalType + "' is not supported.");
      }
    }
    return builder.build();
  }

  private Object extractValue(Object object, final Schema schema) {
    if (object == null || object instanceof BsonUndefined) {
      return null;
    }

    Schema.LogicalType fieldLogicalType = schema.isNullable()
      ? schema.getNonNullable().getLogicalType()
      : schema.getLogicalType();
    if (fieldLogicalType != null) {
      switch (fieldLogicalType) {
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          Date date = (Date) object;
          return date.toInstant().atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC));
        case DECIMAL:
          Decimal128 decimal = (Decimal128) object;
          int scale = schema.isNullable() ? schema.getNonNullable().getScale() : schema.getScale();
          return decimal.bigDecimalValue().setScale(scale, BigDecimal.ROUND_HALF_EVEN);
        default:
          throw new UnexpectedFormatException("Field type '" + fieldLogicalType + "' is not supported.");
      }
    }

    Schema.Type fieldType = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
    switch (fieldType) {
      case BOOLEAN:
        return (Boolean) object;
      case INT:
        return (Integer) object;
      case DOUBLE:
        return (Double) object;
      case BYTES:
        if (object instanceof ObjectId) {
          // ObjectId can be read as a bytes. Exactly 12 bytes are required
          ByteBuffer buffer = ByteBuffer.allocate(12);
          ObjectId objectId = (ObjectId) object;
          objectId.putToByteBuffer(buffer);
          return buffer;
        } else {
          return (byte[]) object;
        }
      case LONG:
        return (Long) object;
      case STRING:
        if (object instanceof ObjectId) {
          // ObjectId can be read as a string
          return ((ObjectId) object).toString();
        } else {
          return (String) object;
        }
      case MAP:
        Map.Entry<Schema, Schema> mapSchema = schema.isNullable()
         ? schema.getNonNullable().getMapSchema()
         : schema.getMapSchema();
       if (mapSchema.getKey().getType() != Schema.Type.STRING) {
         throw new UnexpectedFormatException("Key of the 'map' schema must be of the 'string' type.");
       }
       BSONObject bsonObject = (BSONObject) object;
       return bsonObject.toMap();
      case ARRAY:
        return convertArray(object, schema);
      case RECORD:
          return convertRecord(object, schema);
      default:
        throw new UnexpectedFormatException("Field type '" + fieldType + "' is not supported.");
    }
  }

  private Object convertArray(Object object, Schema schema) {
    BasicBSONList bsonList = (BasicBSONList) object;
    List<Object> values = Lists.newArrayListWithCapacity(bsonList.size());
    Schema componentSchema = schema.isNullable() ? schema.getNonNullable().getComponentSchema()
      : schema.getComponentSchema();
    for (Object obj : bsonList) {
      values.add(extractValue(obj, componentSchema));
    }
    return values;
  }
}
