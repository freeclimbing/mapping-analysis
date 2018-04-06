package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.config.IncrementalConfig;

class SingleSourceIncrementalClustering extends IncrementalClustering {
//  SingleSourceIncrementalClustering(BlockingStrategy blockingStrategy,
//                                    DataSet<Vertex<Long, ObjectMap>> newElements,
//                                    String metric,
//                                    String source,
//                                    int sourcesCount,
//                                    ExecutionEnvironment env) {
//    super(new SingleSourceIncrementalClusteringFunction(blockingStrategy,
//        newElements, metric, source, sourcesCount, env));
//  }

  SingleSourceIncrementalClustering(
      DataSet<Vertex<Long, ObjectMap>> newElements,
      IncrementalConfig config) {
    super(new SingleSourceIncrementalClusteringFunction(newElements, config));
  }
}