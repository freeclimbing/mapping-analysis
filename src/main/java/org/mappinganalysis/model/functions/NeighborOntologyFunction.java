package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * Find neighbors with same ontology (to exclude or handle them later)
 */
public class NeighborOntologyFunction
    implements NeighborsFunctionWithVertexValue<Long, ObjectMap, NullValue,
    Tuple4<Edge<Long, NullValue>, Long, String, Integer>> {

  @Override
  public void iterateNeighbors(Vertex<Long, ObjectMap> vertex,
                               Iterable<Tuple2<Edge<Long, NullValue>, Vertex<Long, ObjectMap>>> neighbors,
                               Collector<Tuple4<Edge<Long, NullValue>, Long, String, Integer>> collector)
      throws Exception {

    for (Tuple2<Edge<Long, NullValue>, Vertex<Long, ObjectMap>> neighbor : neighbors) {
      Edge<Long, NullValue> edge = neighbor.f0;
      Long neighborId = neighbor.f1.getId();
      String ontology = neighbor.f1.getValue().get(Utils.ONTOLOGY).toString();

      collector.collect(new Tuple4<>(edge, neighborId, ontology, 1));
    }
  }
}
