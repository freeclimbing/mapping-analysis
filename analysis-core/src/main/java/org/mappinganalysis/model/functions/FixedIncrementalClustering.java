package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.ExecutionEnvironment;

class FixedIncrementalClustering extends IncrementalClustering {
  FixedIncrementalClustering(ExecutionEnvironment env) {
    super(new FixedIncrementalClusteringFunction(env));
  }
}
