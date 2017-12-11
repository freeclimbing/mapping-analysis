package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;

public class SplitIncrementalClustering extends IncrementalClustering {
  SplitIncrementalClustering(BlockingStrategy blockingStrategy, String part, ExecutionEnvironment env) {
    super(new SplitIncrementalClusteringFunction(blockingStrategy, part, env));
  }
}
