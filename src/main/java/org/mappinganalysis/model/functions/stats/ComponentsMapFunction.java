package org.mappinganalysis.model.functions.stats;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class ComponentsMapFunction implements MapFunction<Vertex<Long, ObjectMap>, Tuple2<Long, Long>> {
  @Override
  public Tuple2<Long, Long> map(Vertex<Long, ObjectMap> vertex) throws Exception {
    return new Tuple2<>(vertex.getId(), (long) vertex.getValue().get(Utils.HASH_CC));
  }
}
