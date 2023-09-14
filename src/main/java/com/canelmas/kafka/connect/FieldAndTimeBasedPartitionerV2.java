/*
 * Copyright (C) 2020 Can Elmas <canelm@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.canelmas.kafka.connect;

import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.util.DataUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FieldAndTimeBasedPartitionerV2<T> extends TimeBasedPartitioner<T> {

  public static final String PARTITION_FIELD_FORMAT_PATH_CONFIG = "partition.field.format.path";
  public static final String PARTITION_FIELD_FORMAT_FIELD_FIRST_CONFIG = "partition.field.format.fieldfirst";
  public static final String PARTITION_FIELD_FORMAT_LOWERCASE_CONFIG = "partition.field.format.lowercase";
  public static final String PARTITION_FIELD_RENAME = "partition.field.rename";
  public static final String PARTITION_FIELD_FORMAT_PATH_DOC =
      "Whether directory labels should be included when partitioning for custom fields e.g. " +
          "whether this 'orgId=XXXX/appId=ZZZZ/customField=YYYY' or this 'XXXX/ZZZZ/YYYY'.";
  public static final String PARTITION_FIELD_FORMAT_PATH_DISPLAY = "Partition Field Format Path";
  public static final String PARTITION_FIELD_FORMAT_PATH_DEFAULT = "true";
  public static final String PARTITION_FIELD_FORMAT_LOWERCASE_DEFAULT = "true";
  public static final String PARTITION_FIELD_FORMAT_FIELD_FIRST_DEFAULT = "false";
  private static final Logger log = LoggerFactory.getLogger(FieldAndTimeBasedPartitionerV2.class);
  private boolean formatFieldFirst; 
  private boolean formatLowercase;
  private PartitionFieldExtractor partitionFieldExtractor;

  protected void init(long partitionDurationMs, String pathFormat, Locale locale,
                      DateTimeZone timeZone, Map<String, Object> config) {
    super.init(partitionDurationMs, pathFormat, locale, timeZone, config);

    final List<String> fieldNames =
        (List<String>) config.get(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
    final boolean formatPath = Boolean.parseBoolean((String) config.getOrDefault(PARTITION_FIELD_FORMAT_PATH_CONFIG, PARTITION_FIELD_FORMAT_PATH_DEFAULT));
    formatFieldFirst = Boolean.parseBoolean((String) config.getOrDefault(PARTITION_FIELD_FORMAT_FIELD_FIRST_CONFIG, PARTITION_FIELD_FORMAT_FIELD_FIRST_DEFAULT));
    formatLowercase = Boolean.parseBoolean((String) config.getOrDefault(PARTITION_FIELD_FORMAT_LOWERCASE_CONFIG, PARTITION_FIELD_FORMAT_LOWERCASE_DEFAULT));
    String partitionsToRename = (String) config.getOrDefault(PARTITION_FIELD_RENAME, "");

    log.info("Partitions fields to rename: {}", partitionsToRename);
    Map<String, String> partitionsToRenameMap = partitionsToRename.isEmpty() ?
        new HashMap<>()
        : Arrays.stream(partitionsToRename.split(","))
        .map(s -> s.split(":"))
        .collect(Collectors.toMap(k -> k[0], v -> v[1]));

    this.partitionFieldExtractor =
        new PartitionFieldExtractor(fieldNames, formatPath, partitionsToRenameMap);
  }

  public String encodePartition(final SinkRecord sinkRecord, final long nowInMillis) {
    final String partitionsForTimestamp = super.encodePartition(sinkRecord, nowInMillis);
    final String partitionsForFields = this.partitionFieldExtractor.extract(sinkRecord);
    String partition = formatFieldFirst
                       ? String.join(this.delim, partitionsForFields, partitionsForTimestamp)
                       : String.join(this.delim, partitionsForTimestamp, partitionsForFields);

    if (formatLowercase) {
        partition = partition.toLowerCase();
    }

    log.debug("Encoded partition : {}", partition);

    return partition;
  }

  public String encodePartition(final SinkRecord sinkRecord) {
    final String partitionsForTimestamp = super.encodePartition(sinkRecord);
    final String partitionsForFields = this.partitionFieldExtractor.extract(sinkRecord);
    String partition = formatFieldFirst
                       ? String.join(this.delim, partitionsForFields, partitionsForTimestamp)
                       : String.join(this.delim, partitionsForTimestamp, partitionsForFields);

    if (formatLowercase) {
        partition = partition.toLowerCase();
    }

    log.debug("Encoded partition : {}", partition);

    return partition;
  }

  public static class PartitionFieldExtractor {

    private static final String DELIMITER_EQ = "=";

    private final boolean formatPath;
    private final List<String> fieldNames;
    private final Map<String, String> partitionsOtherNames;

    PartitionFieldExtractor(final List<String> fieldNames, final boolean formatPath,
                            Map<String, String> partitionsOtherNames) {
      this.fieldNames = fieldNames;
      this.formatPath = formatPath;
      this.partitionsOtherNames = partitionsOtherNames;
    }

    public String extract(final ConnectRecord<?> record) {

      final Object value = record.value();
      final StringBuilder builder = new StringBuilder();

      log.debug("Partitions to rename: {}", partitionsOtherNames);
      for (final String fieldName : this.fieldNames) {
        if (builder.length() != 0) {
          builder.append(StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);
        }
        if (value instanceof Struct || value instanceof Map) {
          final String partitionField = (String) DataUtils.getNestedFieldValue(value, fieldName);
          String partitionName = partitionsOtherNames.getOrDefault(fieldName, fieldName);
          if (formatPath) {
            builder.append(String.join(DELIMITER_EQ, partitionName, partitionField));
          } else {
            builder.append(partitionField);
          }
        } else {
          log.error("Value is not of Struct or Map type.");
          throw new PartitionException("Error encoding partition.");
        }
      }
      return builder.toString();
    }
  }
}
