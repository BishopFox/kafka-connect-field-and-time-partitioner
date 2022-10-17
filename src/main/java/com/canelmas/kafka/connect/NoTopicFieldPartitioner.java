package com.canelmas.kafka.connect;

import io.confluent.connect.storage.partitioner.FieldPartitioner;

public final class NoTopicFieldPartitioner<T> extends FieldPartitioner<T> {

  @Override
  public String generatePartitionedPath(String topic, String encodedPartition) {
    // we don't want to use topic name so we ignore it
    return encodedPartition;
  }
}
