package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * Find neighbors with same ontology (to exclude or handle them later)
 */
public class NeighborOntologyFunction
    implements NeighborsFunctionWithVertexValue<Long, ObjectMap, ObjectMap,
    Tuple6<Long, Long, Long, String, Integer, Double>> {
  private static final Logger LOG = Logger.getLogger(NeighborOntologyFunction.class);


  @Override
  public void iterateNeighbors(Vertex<Long, ObjectMap> vertex,
      Iterable<Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>>> neighbors,
      Collector<Tuple6<Long, Long, Long, String, Integer, Double>> collector)
      throws Exception {
    for (Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>> neighbor : neighbors) {
      Edge<Long, ObjectMap> edge = neighbor.f0;
      String ontology = neighbor.f1.getValue().getOntology();
      Double edgeSim = edge.getValue().getEdgeSimilarity();

      Tuple6<Long, Long, Long, String, Integer, Double> resultTuple
          = new Tuple6<>(edge.getSource(),
            edge.getTarget(),
            vertex.getId(),
            ontology,
            1,
            edgeSim);
      LOG.info("Tuple6: " + resultTuple.toString());
      collector.collect(resultTuple);
    }
  }
}
