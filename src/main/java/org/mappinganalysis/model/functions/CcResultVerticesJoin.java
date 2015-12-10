package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * INITIAL CLUSTERING: add cluster identifier (e.g., cc id) to vertices
 */
public class CcResultVerticesJoin implements JoinFunction<Vertex<Long, ObjectMap>,
    Tuple2<Long, Long>, Vertex<Long, ObjectMap>> {
  @Override
  public Vertex<Long, ObjectMap> join(Vertex<Long, ObjectMap> vertex, Tuple2<Long, Long> tuple)
      throws Exception {
    vertex.getValue().put(Utils.CC_ID, tuple.f1);
    return vertex;
  }
}
