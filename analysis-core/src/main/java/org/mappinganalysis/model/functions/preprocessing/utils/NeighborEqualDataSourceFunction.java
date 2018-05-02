package org.mappinganalysis.model.functions.preprocessing.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;

/**
 * Find neighbors with same ontology (to exclude or handle them later)
 */
public class NeighborEqualDataSourceFunction
    implements NeighborsFunctionWithVertexValue<Long, ObjectMap, ObjectMap,
    EdgeSourceSimTuple> {
  private static final Logger LOG = Logger.getLogger(NeighborEqualDataSourceFunction.class);

  @Override
  public void iterateNeighbors(
      Vertex<Long, ObjectMap> vertex,
      Iterable<Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>>> neighbors,
      Collector<EdgeSourceSimTuple> collector) throws Exception {
    for (Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>> neighbor : neighbors) {
//      LOG.info("neighbor: " + neighbor.toString());
      Edge<Long, ObjectMap> edge = neighbor.f0;
      String neighborDataSource = neighbor.f1.getValue().getDataSource();
      Double edgeSim = edge.getValue().getEdgeSimilarity();

      EdgeSourceSimTuple resultTuple
          = new EdgeSourceSimTuple(vertex.getValue().getCcId(),
          edge.getSource(),
          edge.getTarget(),
          vertex.getValue().getDataSource(),
          neighborDataSource,
          edgeSim);
//      LOG.info("Tuple6: " + resultTuple.toString());
      collector.collect(resultTuple);
    }
  }
}
