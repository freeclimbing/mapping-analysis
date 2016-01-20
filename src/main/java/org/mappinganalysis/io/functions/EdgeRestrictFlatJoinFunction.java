package org.mappinganalysis.io.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;

public class EdgeRestrictFlatJoinFunction implements FlatJoinFunction<Edge<Long, NullValue>,
    Vertex<Long, ObjectMap>, Edge<Long, NullValue>> {
  @Override
  public void join(Edge<Long, NullValue> edge, Vertex<Long, ObjectMap> vertex,
                   Collector<Edge<Long, NullValue>> collector) throws Exception {
    if (vertex != null) {
      collector.collect(edge);
    }
  }
}