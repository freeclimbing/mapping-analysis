package org.mappinganalysis.corruption;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

/**
 * Remove edges for data corruption.
 */
public class EdgeRemoveCorruptionFunction
    implements MapPartitionFunction<Edge<Long, NullValue>, Edge<Long, NullValue>> {
  private int removeEveryXthElement;

  public EdgeRemoveCorruptionFunction(int removeEveryXthElement) {
    this.removeEveryXthElement = removeEveryXthElement;
  }

  @Override
  public void mapPartition(Iterable<Edge<Long, NullValue>> values,
                           Collector<Edge<Long, NullValue>> out) throws Exception {
    int reset = removeEveryXthElement;
    for (Edge<Long, NullValue> value : values) {
      if (reset != 0) {
        out.collect(value);
        reset--;
      } else {
        reset = removeEveryXthElement;
      }
    }
  }
}
