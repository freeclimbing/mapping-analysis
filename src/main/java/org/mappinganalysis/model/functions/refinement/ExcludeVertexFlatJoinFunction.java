package org.mappinganalysis.model.functions.refinement;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class ExcludeVertexFlatJoinFunction extends RichFlatJoinFunction<Tuple1<Long>,
    Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private LongCounter excludeCounter = new LongCounter();

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Utils.EXCLUDE_VERTEX_ACCUMULATOR, excludeCounter);
  }

  @Override
  public void join(Tuple1<Long> left, Vertex<Long, ObjectMap> right,
                   Collector<Vertex<Long, ObjectMap>> collector) throws Exception {
    if (left == null) {
      excludeCounter.add(1L);
      collector.collect(right);
    }
  }
}
