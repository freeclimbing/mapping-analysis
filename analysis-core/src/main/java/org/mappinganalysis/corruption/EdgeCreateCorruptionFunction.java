package org.mappinganalysis.corruption;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

/**
 */
public class EdgeCreateCorruptionFunction implements MapPartitionFunction<Long, Edge<Long, NullValue>> {
  private int addEveryXthElement;

  public EdgeCreateCorruptionFunction(int addEveryXthElement) {
    this.addEveryXthElement = addEveryXthElement;
  }

  @Override
  public void mapPartition(Iterable<Long> values, Collector<Edge<Long, NullValue>> out) throws Exception {
    int reset = addEveryXthElement;
    Long tmp = null;
    for (Long value : values) {
      if (reset != 0) {
        if (tmp == null) {
          tmp = value;
        }
        reset--;
      } else {
        out.collect(new Edge<>(tmp, value, NullValue.getInstance()));
        tmp = null;
        reset = addEveryXthElement;
      }
    }
  }
}
