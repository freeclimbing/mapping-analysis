package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.FlinkVertex;
import org.mappinganalysis.utils.Utils;

/**
 * INITIAL CLUSTERING: add cluster identifier (e.g., cc id) to vertices
 */
public class CcResultVerticesJoin implements JoinFunction<Vertex<Long, FlinkVertex>,
    Tuple2<Long, Long>, Vertex<Long, FlinkVertex>> {
  @Override
  public Vertex<Long, FlinkVertex> join(Vertex<Long, FlinkVertex> vertex, Tuple2<Long, Long> tuple)
      throws Exception {
    vertex.getValue().getProperties().put(Utils.CC_ID, tuple.f1);
    return vertex;
  }
}
