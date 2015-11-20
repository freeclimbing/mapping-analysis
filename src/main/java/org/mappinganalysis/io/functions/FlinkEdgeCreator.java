package org.mappinganalysis.io.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

/**
 * Create Flink edge object from raw database result set.
 */
public class FlinkEdgeCreator implements MapFunction<Tuple2<Integer, Integer>, Edge<Long, NullValue>> {

  private final Edge<Long, NullValue> reuseEdge;

  public FlinkEdgeCreator() {
    reuseEdge = new Edge<>();
  }

  public Edge<Long, NullValue> map(Tuple2<Integer, Integer> tuple) throws Exception {
    reuseEdge.setSource((long) tuple.f0);
    reuseEdge.setTarget((long) tuple.f1);
    reuseEdge.setValue(NullValue.getInstance());
    return reuseEdge;
  }
}
