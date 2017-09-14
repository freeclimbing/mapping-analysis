package org.mappinganalysis.util.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

/**
 * Always have edges with small id first.
 */
public class SmallEdgeIdFirstMapFunction
    implements MapFunction<Edge<Long, NullValue>, Edge<Long, NullValue>> {
  @Override
  public Edge<Long, NullValue> map(Edge<Long, NullValue> edge) throws Exception {
    return edge.getSource() < edge.getTarget() ? edge : edge.reverse();
  }
}