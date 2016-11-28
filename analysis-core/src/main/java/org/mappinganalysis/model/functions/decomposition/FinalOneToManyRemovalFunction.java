package org.mappinganalysis.model.functions.decomposition;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;

/**
 * One to many link removal after initial clustering.
 */
public class FinalOneToManyRemovalFunction
    implements NeighborsFunctionWithVertexValue<Long, ObjectMap, ObjectMap,
    Tuple3<Long, String, Double>> {
  @Override
  public void iterateNeighbors(
      Vertex<Long, ObjectMap> vertex,
      Iterable<Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>>> neighborEdgeVertices,
      Collector<Tuple3<Long, String, Double>> out) throws Exception {
    String ontology = vertex.getValue().getOntology();
    int neighborCount = 0;
    double vertexAggSim = 0d;
    boolean isRelevant = false;

    for (Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>> edgeVertex : neighborEdgeVertices) {
      Edge<Long, ObjectMap> edge = edgeVertex.f0;
      Vertex<Long, ObjectMap> neighbor = edgeVertex.f1;
      ++neighborCount;
      if (!isRelevant && neighbor.getValue().getOntology().equals(ontology)) {
        isRelevant = true;
      }
      vertexAggSim += edge.getValue().getEdgeSimilarity();
    }

    if (isRelevant) {
      vertexAggSim /= neighborCount;
      out.collect(new Tuple3<>(vertex.getId(), ontology, vertexAggSim));
    }
  }
}
