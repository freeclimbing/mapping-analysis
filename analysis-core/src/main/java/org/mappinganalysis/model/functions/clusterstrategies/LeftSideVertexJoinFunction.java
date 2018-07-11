package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;

/**
 * Remove if not used up to 18/8
 */
@Deprecated
class LeftSideVertexJoinFunction
    implements JoinFunction<
    Tuple3<Long,Long,Double>,
    Vertex<Long,ObjectMap>,
    Tuple3<Vertex<Long, ObjectMap>, Long, Double>> {
  @Override
  public Tuple3<Vertex<Long, ObjectMap>, Long, Double> join(
      Tuple3<Long, Long, Double> first, Vertex<Long, ObjectMap> second) throws Exception {
    return new Tuple3<>(second, first.f1, first.f2);
  }
}
