package org.mappinganalysis.graph;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.mappinganalysis.model.ObjectMap;

/**
 * abstract
 */
public abstract class LinkFilterFunction
    implements GraphAlgorithm<Long, ObjectMap, ObjectMap, Graph<Long, ObjectMap, ObjectMap>> {
}
