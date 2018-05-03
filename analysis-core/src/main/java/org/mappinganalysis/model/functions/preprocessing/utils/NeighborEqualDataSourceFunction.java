package org.mappinganalysis.model.functions.preprocessing.utils;

import com.google.common.collect.Sets;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.AbstractionUtils;

import java.util.Set;

/**
 * Find neighbors with same ontology (to exclude or handle them later)
 */
public class NeighborEqualDataSourceFunction
    implements NeighborsFunctionWithVertexValue<Long, ObjectMap, ObjectMap,
    EdgeSourceSimTuple> {
  private static final Logger LOG = Logger.getLogger(NeighborEqualDataSourceFunction.class);
  private String mode;

  public NeighborEqualDataSourceFunction(String mode) {
    this.mode = mode;
  }

  private int getSourcesForVertex(Vertex<Long, ObjectMap> vertex, String mode) {
    Set<String> vertexSourceSet = Sets.newHashSet();
    if (vertex.getValue().getDataSourcesList().isEmpty()) {
      vertexSourceSet.add(vertex.getValue().getDataSource());
    } else {
      vertexSourceSet.addAll(vertex.getValue().getDataSourcesList());
    }

    return AbstractionUtils.getSourcesInt(mode, vertexSourceSet);
  }

  @Override
  public void iterateNeighbors(
      Vertex<Long, ObjectMap> vertex,
      Iterable<Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>>> neighbors,
      Collector<EdgeSourceSimTuple> collector) throws Exception {

    int vertexSources = getSourcesForVertex(vertex, mode);

    for (Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>> neighbor : neighbors) {
      Edge<Long, ObjectMap> edge = neighbor.f0;
      int neighborSources = getSourcesForVertex(neighbor.f1, mode);
      Double edgeSim = edge.getValue().getEdgeSimilarity();
      EdgeSourceSimTuple resultTuple
          = new EdgeSourceSimTuple(vertex.getValue().getCcId(),
          edge.getSource(),
          edge.getTarget(),
          vertexSources,
          neighborSources,
          edgeSim);

      collector.collect(resultTuple);
    }
  }
}
