package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;

class SingleSourceIncrementalClustering extends IncrementalClustering {
  SingleSourceIncrementalClustering(BlockingStrategy blockingStrategy,
                                    DataSet<Vertex<Long, ObjectMap>> newElements,
                                    String source,
                                    int sourcesCount,
                                    ExecutionEnvironment env) {
    super(new SingleSourceIncrementalClusteringFunction(blockingStrategy,
        newElements, source, sourcesCount, env));
  }
}