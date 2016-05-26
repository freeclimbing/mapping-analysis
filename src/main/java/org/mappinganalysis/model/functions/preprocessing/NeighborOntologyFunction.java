package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
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
    Tuple7<Long, Long, Long, String, Integer, Double, Long>> {

  @Override
  public void iterateNeighbors(Vertex<Long, ObjectMap> vertex,
      Iterable<Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>>> neighbors,
      Collector<Tuple7<Long, Long, Long, String, Integer, Double, Long>> collector)
      throws Exception {
    for (Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>> neighbor : neighbors) {
      Edge<Long, ObjectMap> edge = neighbor.f0;
      String ontology = neighbor.f1.getValue().get(Utils.ONTOLOGY).toString();
      Double edgeSim = (Double) edge.getValue().get(Utils.AGGREGATED_SIM_VALUE);

      collector.collect(new Tuple7<>(edge.getSource(),
          edge.getTarget(),
          vertex.getId(),
          ontology,
          1,
          edgeSim,
          (long) vertex.getValue().get(Utils.CC_ID)));
    }
  }
}
