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

/**
 * Builder class that provides handy methods to construct {@link MongoDBBatchSink.MongoDBSinkConfig} for testing.
 */
public class MongoDBSinkConfigBuilder {

  protected final MongoDBBatchSink.MongoDBSinkConfig config;

  public static MongoDBSinkConfigBuilder builder() {
    return new MongoDBSinkConfigBuilder(new MongoDBBatchSink.MongoDBSinkConfig());
  }

  public static MongoDBSinkConfigBuilder builder(MongoDBBatchSink.MongoDBSinkConfig original) {
    return builder()
      .setReferenceName(original.referenceName)
      .setHost(original.host)
      .setPort(original.port)
      .setDatabase(original.database)
      .setCollection(original.collection)
      .setUser(original.user)
      .setPassword(original.password)
      .setConnectionArguments(original.connectionArguments)
      .setIdField(original.idField);
  }

  public MongoDBSinkConfigBuilder(MongoDBBatchSink.MongoDBSinkConfig config) {
    this.config = config;
  }

  public MongoDBSinkConfigBuilder setReferenceName(String referenceName) {
    this.config.referenceName = referenceName;
    return this;
  }

  public MongoDBSinkConfigBuilder setHost(String host) {
    this.config.host = host;
    return this;
  }

  public MongoDBSinkConfigBuilder setPort(Integer port) {
    this.config.port = port;
    return this;
  }

  public MongoDBSinkConfigBuilder setDatabase(String database) {
    this.config.database = database;
    return this;
  }

  public MongoDBSinkConfigBuilder setCollection(String collection) {
    this.config.collection = collection;
    return this;
  }

  public MongoDBSinkConfigBuilder setUser(String user) {
    this.config.user = user;
    return this;
  }

  public MongoDBSinkConfigBuilder setPassword(String password) {
    this.config.password = password;
    return this;
  }

  public MongoDBSinkConfigBuilder setConnectionArguments(String connectionArguments) {
    this.config.connectionArguments = connectionArguments;
    return this;
  }

  public MongoDBSinkConfigBuilder setIdField(String idField) {
    this.config.idField = idField;
    return this;
  }

  public MongoDBBatchSink.MongoDBSinkConfig build() {
    return this.config;
  }
}
