package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.config.IncrementalConfig;

class SingleSourceIncrementalClustering extends IncrementalClustering {
  SingleSourceIncrementalClustering(
      DataSet<Vertex<Long, ObjectMap>> newElements,
      IncrementalConfig config) {
    super(new SingleSourceIncrementalClusteringFunction(newElements, config));
  }
}