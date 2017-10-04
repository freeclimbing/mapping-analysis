package org.mappinganalysis.model.functions;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.mappinganalysis.model.ObjectMap;

public abstract class IncrementalClusteringFunction
    implements GraphAlgorithm<Long, ObjectMap, ObjectMap, Graph<Long, ObjectMap, ObjectMap>> {
}
