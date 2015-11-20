package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

/**
 * Preprocessing strategy for cleaning link set.
 */
public class ExcludeOneToManyOntologiesFilter
    implements FilterFunction<Tuple4<Edge<Long, NullValue>, Long, String, Integer>> {
  @Override
  public boolean filter(Tuple4<Edge<Long, NullValue>, Long, String, Integer> tuple) throws Exception {
    return tuple.f3 < 2;
  }
}
