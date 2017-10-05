package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.List;

class MinSizeIncClustering extends IncrementalClustering {
  MinSizeIncClustering(List<String> sources, ExecutionEnvironment env) {
    super(new MinSizeIncClusteringFunction(sources, env));
  }
}
