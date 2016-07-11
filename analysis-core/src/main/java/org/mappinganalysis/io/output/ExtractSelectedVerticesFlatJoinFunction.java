package org.mappinganalysis.io.output;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;

class ExtractSelectedVerticesFlatJoinFunction implements FlatJoinFunction<Tuple1<Long>, Vertex<Long, ObjectMap>,
    Vertex<Long, ObjectMap>> {
  @Override
  public void join(Tuple1<Long> left, Vertex<Long, ObjectMap> right,
                   Collector<Vertex<Long, ObjectMap>> collector) throws Exception {
    if (left != null) {
      collector.collect(right);
    }
  }
}
