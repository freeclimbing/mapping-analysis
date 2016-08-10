package org.mappinganalysis.io.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;

public class EdgeRestrictFlatJoinFunction<EV> implements FlatJoinFunction<Edge<Long, EV>,
    Vertex<Long, ObjectMap>, Edge<Long, EV>> {
  @Override
  public void join(Edge<Long, EV> edge, Vertex<Long, ObjectMap> vertex,
                   Collector<Edge<Long, EV>> collector) throws Exception {
    if (vertex != null) {
      collector.collect(edge);
    }
  }
}
