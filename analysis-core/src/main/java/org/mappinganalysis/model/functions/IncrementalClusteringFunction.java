package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;

public abstract class IncrementalClusteringFunction
    implements GraphAlgorithm<Long, ObjectMap, NullValue, DataSet<Vertex<Long, ObjectMap>>> {
}
