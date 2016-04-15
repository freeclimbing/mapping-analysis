package org.mappinganalysis.io.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;

/**
 * input restriction
 */
public class VertexRestrictFlatJoinFunction implements FlatJoinFunction<Vertex<Long, ObjectMap>,
    Tuple2<Long, Long>, Vertex<Long, ObjectMap>> {
  @Override
  public void join(Vertex<Long, ObjectMap> vertex, Tuple2<Long, Long> edge,
                   Collector<Vertex<Long, ObjectMap>> collector) throws Exception {
    if (edge != null) {
      collector.collect(vertex);
    }
  }
}
