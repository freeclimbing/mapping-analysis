package org.mappinganalysis.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;

public class MapVertexToPropertyStringFunction implements MapFunction<Vertex<Long, ObjectMap>,
    Tuple2<String, Long>> {
  private final String property;

  public MapVertexToPropertyStringFunction(String property) {
    this.property = property;
  }

  @Override
  public Tuple2<String, Long> map(Vertex<Long, ObjectMap> vertex) throws Exception {
    return new Tuple2<>(vertex.getValue().get(property).toString(), 1L);
  }
}
