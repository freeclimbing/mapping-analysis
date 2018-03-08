package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.List;

class MinSizeIncClustering extends IncrementalClustering {
  MinSizeIncClustering(List<String> sources, String metric, ExecutionEnvironment env) {
    super(new MinSizeIncClusteringFunction(sources, metric, env));
  }
}
