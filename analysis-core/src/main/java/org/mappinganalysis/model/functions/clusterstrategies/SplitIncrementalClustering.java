package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;

public class SplitIncrementalClustering extends IncrementalClustering {
  SplitIncrementalClustering(BlockingStrategy blockingStrategy, String part, String metric, ExecutionEnvironment env) {
    super(new SplitIncrementalClusteringFunction(blockingStrategy, metric, part, env));
  }
}
