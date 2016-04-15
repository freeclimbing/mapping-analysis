package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * Find neighbors with same ontology (to exclude or handle them later)
 */
public class NeighborOntologyFunction
    implements NeighborsFunctionWithVertexValue<Long, ObjectMap, ObjectMap,
    Tuple5<Edge<Long, ObjectMap>, Long, String, Integer, Double>> {

  @Override
  public void iterateNeighbors(Vertex<Long, ObjectMap> vertex,
      Iterable<Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>>> neighbors,
      Collector<Tuple5<Edge<Long, ObjectMap>, Long, String, Integer, Double>> collector)
      throws Exception {
    Long vertexId = vertex.getId();

    for (Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>> neighbor : neighbors) {
      Edge<Long, ObjectMap> edge = neighbor.f0;
      String ontology = neighbor.f1.getValue().get(Utils.ONTOLOGY).toString();
      Double edgeSim = (Double) edge.getValue().get(Utils.AGGREGATED_SIM_VALUE);

      collector.collect(new Tuple5<>(edge, vertexId, ontology, 1, edgeSim));
    }
  }
}
