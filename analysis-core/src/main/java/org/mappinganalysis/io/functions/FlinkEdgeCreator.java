package org.mappinganalysis.io.functions;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.utils.Utils;

/**
 * Create Flink edge object from raw database result set.
 */
public class FlinkEdgeCreator extends RichMapFunction<Tuple2<Integer, Integer>, Edge<Long, NullValue>> {
  private final Edge<Long, NullValue> reuseEdge;
  private LongCounter edgeCounter = new LongCounter();

  public FlinkEdgeCreator() {
    reuseEdge = new Edge<>();
  }

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Utils.EDGE_COUNT_ACCUMULATOR, edgeCounter);
  }

  @Override
  public Edge<Long, NullValue> map(Tuple2<Integer, Integer> tuple) throws Exception {
    reuseEdge.setSource((long) tuple.f0);
    reuseEdge.setTarget((long) tuple.f1);
    reuseEdge.setValue(NullValue.getInstance());
    edgeCounter.add(1L);
    return reuseEdge;
  }
}
