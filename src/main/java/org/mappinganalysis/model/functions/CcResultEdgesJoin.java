package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.FlinkVertex;

import java.util.Map;

/**
 * INITIAL CLUSTERING: merge old edges (without properties) with computed properties
 */
public class CcResultEdgesJoin implements JoinFunction<Edge<Long, NullValue>,
    Triplet<Long, FlinkVertex, Map<String, Object>>, Edge<Long, Map<String, Object>>> {
  @Override
  public Edge<Long, Map<String, Object>> join(Edge<Long, NullValue> edge,
                                              Triplet<Long, FlinkVertex, Map<String, Object>> triplet)
      throws Exception {
    return new Edge<>(edge.getSource(), edge.getTarget(), triplet.f4);
  }
}
