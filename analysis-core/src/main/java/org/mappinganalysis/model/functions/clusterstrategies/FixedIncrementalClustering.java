package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;

class FixedIncrementalClustering extends IncrementalClustering {
  FixedIncrementalClustering(BlockingStrategy blockingStrategy, ExecutionEnvironment env) {
    super(new FixedIncrementalClusteringFunction(blockingStrategy, env));
  }
}
