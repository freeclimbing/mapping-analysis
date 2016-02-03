package org.mappinganalysis.model.functions.clustering;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class ExcludeInputJoinFunction implements FlatJoinFunction<Edge<Long, NullValue>,
    Edge<Long, NullValue>, Edge<Long, NullValue>> {
  @Override
  public void join(Edge<Long, NullValue> left, Edge<Long, NullValue> right,
                   Collector<Edge<Long, NullValue>> collector) throws Exception {
    if (right == null) {
      collector.collect(left);
    }
  }
}
