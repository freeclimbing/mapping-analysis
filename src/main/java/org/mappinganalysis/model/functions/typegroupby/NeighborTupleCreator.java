package org.mappinganalysis.model.functions.typegroupby;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.NeighborTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * Extract similarities and relevant properties from neighbor vertices.
 */
class NeighborTupleCreator
    implements NeighborsFunctionWithVertexValue<Long, ObjectMap, ObjectMap, NeighborTuple> {
  @Override
  public void iterateNeighbors(Vertex<Long, ObjectMap> vertex,
                               Iterable<Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>>> neighbors,
                               Collector<NeighborTuple> out) throws Exception {
    String vertexType = vertex.getValue().getTypes(Utils.TYPE_INTERN).stream().findFirst().get();
    if (vertexType.equals(Utils.NO_TYPE)) {
      neighbors.forEach(neighbor -> out.collect(new NeighborTuple(vertex.getId(),
          neighbor.f0.getValue().getSimilarity(),
          neighbor.f1.getValue().getTypes(Utils.TYPE_INTERN),
          neighbor.f1.getValue().getHashCcId())));
    }
  }
}
