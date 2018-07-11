package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;

/**
 * Remove if not used up to 18/8
 */
@Deprecated
class RightSideVertexJoinFunction
    implements JoinFunction<
    Tuple3<Vertex<Long,ObjectMap>,Long,Double>,
    Vertex<Long,ObjectMap>,
    Triplet<Long, ObjectMap, ObjectMap>> {
  private DataDomain dataDomain;

  public RightSideVertexJoinFunction(DataDomain dataDomain) {
    this.dataDomain = dataDomain;
  }

  @Override
  public Triplet<Long, ObjectMap, ObjectMap> join(
      Tuple3<Vertex<Long, ObjectMap>, Long, Double> first,
      Vertex<Long, ObjectMap> second) throws Exception {

    ObjectMap map = new ObjectMap(dataDomain);
    map.setEdgeSimilarity(first.f2);

    return new Triplet<>(
        first.f0.getId(),
        second.getId(),
        first.f0.getValue(),
        second.getValue(),
        map
    );
  }
}
