package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.List;

public class MinSizeIncClustering extends IncrementalClustering {
  public MinSizeIncClustering(List<String> sources, ExecutionEnvironment env) {
    super(new MinSizeIncClusteringFunction(sources, env));
  }
}
