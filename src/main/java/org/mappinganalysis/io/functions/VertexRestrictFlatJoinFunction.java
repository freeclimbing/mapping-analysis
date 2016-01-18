package org.mappinganalysis.io.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;

/**
 * input restriction
 */
public class VertexRestrictFlatJoinFunction implements FlatJoinFunction<Vertex<Long, ObjectMap>,
    Edge<Long, NullValue>, Vertex<Long, ObjectMap>> {
  @Override
  public void join(Vertex<Long, ObjectMap> vertex, Edge<Long, NullValue> edge,
                   Collector<Vertex<Long, ObjectMap>> collector) throws Exception {
    if (edge != null) {
      collector.collect(vertex);
    }
  }
}
