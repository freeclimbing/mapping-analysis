package org.mappinganalysis.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;

/**
 * Map any property of a vertex to a Tuple2 containing the value and a 1.
 */
public class MapVertexToPropertyLongFunction implements MapFunction<Vertex<Long, ObjectMap>,
    Tuple2<Long, Long>> {
  private final String property;

  public MapVertexToPropertyLongFunction(String property) {
    this.property = property;
  }

  @Override
  public Tuple2<Long, Long> map(Vertex<Long, ObjectMap> vertex) throws Exception {
    return new Tuple2<>((long) vertex.getValue().get(property), 1L);
  }
}
