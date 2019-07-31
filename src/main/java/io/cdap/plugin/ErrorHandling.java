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

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Indicates error handling strategy during record processing.
 */
public enum ErrorHandling {
  SKIP("skip-error"),
  FAIL_PIPELINE("fail-pipeline");

  private static final Map<String, ErrorHandling> byDisplayName = Arrays.stream(values())
    .collect(Collectors.toMap(ErrorHandling::getDisplayName, Function.identity()));

  private final String displayName;

  ErrorHandling(String displayName) {
    this.displayName = displayName;
  }

  @Nullable
  public static ErrorHandling fromDisplayName(String displayName) {
    return byDisplayName.get(displayName);
  }

  public String getDisplayName() {
    return displayName;
  }
}
