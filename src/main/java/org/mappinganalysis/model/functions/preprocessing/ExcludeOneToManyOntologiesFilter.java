package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * Preprocessing strategy for cleaning link set.
 */
public class ExcludeOneToManyOntologiesFilter extends RichFilterFunction<Tuple5<Edge<Long, ObjectMap>, Long, String, Integer, Double>> {
  private LongCounter filteredLinks = new LongCounter();

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Utils.FILTERED_LINKS_ACCUMULATOR, filteredLinks);
  }

  @Override
  public boolean filter(Tuple5<Edge<Long, ObjectMap>, Long, String, Integer, Double> tuple) throws Exception {
    if (tuple.f3 > 1) {
      return true;
    } else {
      filteredLinks.add(1L);
      return false;
    }
  }
}
