package org.mappinganalysis.model.functions.blocking.lsh.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;

public class VertexWithNewObjectMapFunction
    implements MapFunction<Vertex<Long, NullValue>, ObjectMap> {
  @Override
  public ObjectMap map(Vertex<Long, NullValue> vertex) throws Exception {
    return new ObjectMap();
  }
}
