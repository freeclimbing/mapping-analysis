package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.config.IncrementalConfig;

class MultiIncrementalClustering extends IncrementalClustering {
  MultiIncrementalClustering(IncrementalConfig config) {
    super(new MultiIncrementalClusteringFunction(config));
  }

  MultiIncrementalClustering(DataSet<Vertex<Long, ObjectMap>> toBeMergedElements,
                             IncrementalConfig config) {
    super(new MultiIncrementalClusteringFunction(toBeMergedElements, config));
  }
}
