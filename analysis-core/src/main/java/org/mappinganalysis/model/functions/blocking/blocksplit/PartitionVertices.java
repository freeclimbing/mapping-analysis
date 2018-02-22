package org.mappinganalysis.model.functions.blocking.blocksplit;

import org.apache.flink.api.common.functions.Partitioner;

public class PartitionVertices implements Partitioner<Integer> {
  @Override
  public int partition(Integer key, int numPartitions) {
    return  key;
  }
}