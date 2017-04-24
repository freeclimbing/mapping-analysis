package org.mappinganalysis.graph.utils;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;

public class SimpleEdgesCreator
      implements CustomUnaryOperation<Vertex<Long, ObjectMap>, Edge<Long, NullValue>> {
  private static final Logger LOG = Logger.getLogger(SimpleEdgesCreator.class);

  private DataSet<Vertex<Long, ObjectMap>> vertices;
  private KeySelector<Vertex<Long, ObjectMap>, Long> keySelector;

  /**
   * Create edges for set of vertices having cc id - optionally create only as many edges
   * to connect all vertices within cc.
   */
  public SimpleEdgesCreator(KeySelector<Vertex<Long, ObjectMap>, Long> keySelector) {
    this.keySelector = keySelector;
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> vertices) {
    this.vertices = vertices;
  }

  @Override
  public DataSet<Edge<Long, NullValue>> createResult() {
    return vertices.groupBy(keySelector)
        .reduceGroup(new SimpleEdgeCreatorGroupReducer());
  }

  private static class SimpleEdgeCreatorGroupReducer implements GroupReduceFunction<Vertex<Long,ObjectMap>, Edge<Long, NullValue>> {
    @Override
    public void reduce(
        Iterable<Vertex<Long, ObjectMap>> vertexIterable,
        Collector<Edge<Long, NullValue>> out) throws Exception {
      boolean isFirstEdge = true;
      Long firstVertexId = null;
      for (Vertex<Long, ObjectMap> vertex : vertexIterable) {
        if (isFirstEdge) {
          firstVertexId = vertex.getId();
//          LOG.info("first: " + firstVertexId);
          isFirstEdge = false;
        } else {
//          LOG.info("new Edge: " + firstVertexId + " " + vertex.getId());
          out.collect(new Edge<>(firstVertexId, vertex.getId(), NullValue.getInstance()));
        }

      }
    }
  }
}
