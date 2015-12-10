package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;

/**
 * INITIAL CLUSTERING: merge old edges (without properties) with computed properties
 */
public class CcResultEdgesJoin implements JoinFunction<Edge<Long, NullValue>,
    Triplet<Long, Vertex<Long, ObjectMap>, ObjectMap>, Edge<Long, ObjectMap>> {
  @Override
  public Edge<Long, ObjectMap> join(Edge<Long, NullValue> edge,
                                    Triplet<Long, Vertex<Long, ObjectMap>, ObjectMap> triplet)
      throws Exception {
    return new Edge<>(edge.getSource(), edge.getTarget(), triplet.f4);
  }
}
