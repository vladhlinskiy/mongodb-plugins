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

import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBObjectBuilder;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import org.bson.BSONObject;
import org.bson.types.BasicBSONList;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

/**
 * {@link BSONObjectToRecordTransformer} test.
 */
public class BSONObjectToRecordTransformerTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransform() {
    Schema nestedRecordSchema = Schema.recordOf("nested_object",
                                                Schema.Field.of("nested_string_field", Schema.of(Schema.Type.STRING)),
                                                Schema.Field.of("nested_long_field", Schema.of(Schema.Type.LONG)));

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                    Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                    Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                    Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                                    Schema.Field.of("bytes_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                                    Schema.Field.of("null_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("array_field",
                                                    Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.LONG)))),
                                    Schema.Field.of("nested_object", nestedRecordSchema),
                                    Schema.Field.of("object_to_map", Schema.mapOf(
                                      Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))));

    StructuredRecord nestedRecord = StructuredRecord.builder(nestedRecordSchema)
      .set("nested_string_field", "some")
      .set("nested_long_field", Long.MAX_VALUE)
      .build();

    StructuredRecord expected = StructuredRecord.builder(schema)
      .set("int_field", 15)
      .set("long_field", 10L)
      .set("double_field", 10.5D)
      .set("string_field", "string_value")
      .set("boolean_field", true)
      .set("bytes_field", "test_blob".getBytes())
      .set("null_field", null)
      .set("array_field", Arrays.asList(1L, null, 2L, null, 3L))
      .set("nested_object", nestedRecord)
      .set("object_to_map", ImmutableMap.<String, String>builder()
        .put("key", "value")
        .build()
      )
      .build();

    BasicBSONList bsonList = new BasicBSONList();
    bsonList.addAll(expected.get("array_field"));

    BSONObject nestedBsonObject = BasicDBObjectBuilder.start()
      .add("nested_string_field", nestedRecord.get("nested_string_field"))
      .add("nested_long_field", nestedRecord.get("nested_long_field"))
      .get();

    BSONObject mapObject = BasicDBObjectBuilder.start()
      .add("key", "value")
      .get();

    BSONObject bsonObject = BasicDBObjectBuilder.start()
      .add("int_field", expected.get("int_field"))
      .add("long_field", expected.get("long_field"))
      .add("double_field", expected.get("double_field"))
      .add("float_field", expected.get("float_field"))
      .add("string_field", expected.get("string_field"))
      .add("boolean_field", expected.get("boolean_field"))
      .add("bytes_field", expected.get("bytes_field"))
      .add("null_field", expected.get("null_field"))
      .add("array_field", bsonList)
      .add("nested_object", nestedBsonObject)
      .add("object_to_map", mapObject)
      .get();

    BSONObjectToRecordTransformer transformer = new BSONObjectToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(bsonObject);

    Assert.assertEquals(expected, transformed);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformEmptyObject() {
    Schema nestedRecordSchema = Schema.recordOf("nested_object",
                                                Schema.Field.of("nested_string_field", Schema.of(Schema.Type.STRING)),
                                                Schema.Field.of("nested_long_field", Schema.of(Schema.Type.LONG)));

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                    Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                    Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                    Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                                    Schema.Field.of("bytes_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                                    Schema.Field.of("null_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("array_field", Schema.nullableOf(
                                      Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.LONG))))),
                                    Schema.Field.of("nested_object", Schema.nullableOf(nestedRecordSchema)),
                                    Schema.Field.of("object_to_map", Schema.nullableOf(
                                      Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)))));

    BSONObject bsonObject = BasicDBObjectBuilder.start().get();
    BSONObjectToRecordTransformer transformer = new BSONObjectToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(bsonObject);
    Assert.assertNull(transformed.get("int_field"));
    Assert.assertNull(transformed.get("long_field"));
    Assert.assertNull(transformed.get("double_field"));
    Assert.assertNull(transformed.get("string_field"));
    Assert.assertNull(transformed.get("boolean_field"));
    Assert.assertNull(transformed.get("bytes_field"));
    Assert.assertNull(bsonObject.get("null_field"));
    Assert.assertNull(transformed.get("array_field"));
    Assert.assertNull(transformed.get("nested_object"));
    Assert.assertNull(transformed.get("object_to_map"));
  }

  @Test
  public void testTransformUnexpectedFormat() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("union_field", Schema.unionOf(
      Schema.of(Schema.Type.LONG),
      Schema.of(Schema.Type.STRING)))
    );

    BSONObject bsonObject = BasicDBObjectBuilder.start()
      .add("union_field", 2019L)
      .get();

    thrown.expect(UnexpectedFormatException.class);
    BSONObjectToRecordTransformer transformer = new BSONObjectToRecordTransformer(schema);
    transformer.transform(bsonObject);
  }

  @Test
  public void testTransformInvalidMapSchema() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("invalid_key_type", Schema.mapOf(
      Schema.of(Schema.Type.LONG), Schema.of(Schema.Type.STRING))));

    BSONObject mapObject = BasicDBObjectBuilder.start()
      .add("key", "value")
      .get();

    BSONObject bsonObject = BasicDBObjectBuilder.start()
      .add("invalid_key_type", mapObject)
      .get();

    thrown.expect(UnexpectedFormatException.class);
    BSONObjectToRecordTransformer transformer = new BSONObjectToRecordTransformer(schema);
    transformer.transform(bsonObject);
  }
}
