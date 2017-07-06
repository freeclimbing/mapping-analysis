package org.mappinganalysis.model.functions.decomposition.typegroupby;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.NeighborTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * For each vertex having no type, extract similarities and relevant properties
 * from neighbor vertices.
 */
class NeighborTupleCreator
    implements NeighborsFunctionWithVertexValue<Long, ObjectMap, ObjectMap, NeighborTuple> {
  @Override
  public void iterateNeighbors(Vertex<Long, ObjectMap> vertex,
                               Iterable<Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>>> neighbors,
                               Collector<NeighborTuple> out) throws Exception {
    if (hasNoType(vertex)) {
      neighbors.forEach(neighbor -> out.collect(new NeighborTuple(vertex.getId(),
          neighbor.f0.getValue().getEdgeSimilarity(),
          neighbor.f1.getValue().getTypes(Constants.TYPE_INTERN),
          neighbor.f1.getValue().getHashCcId())));
    }
  }

  private boolean hasNoType(Vertex<Long, ObjectMap> vertex) {
    return vertex
        .getValue()
        .getTypes(Constants.TYPE_INTERN)
        .stream()
        .findFirst()
        .get()
        .equals(Constants.NO_TYPE);
  }
}
