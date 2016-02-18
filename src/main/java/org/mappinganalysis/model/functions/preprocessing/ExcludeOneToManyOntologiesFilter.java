package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.utils.Utils;

/**
 * Preprocessing strategy for cleaning link set.
 */
public class ExcludeOneToManyOntologiesFilter extends RichFilterFunction<Tuple4<Edge<Long, NullValue>, Long, String, Integer>> {
  private static final Logger LOG = Logger.getLogger(ExcludeOneToManyOntologiesFilter.class);

  private LongCounter filterMatches = new LongCounter();
  private ListAccumulator<Edge<Long, NullValue>> filteredLinks = new ListAccumulator<>();

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Utils.LINK_FILTER_ACCUMULATOR, filterMatches);
    getRuntimeContext().addAccumulator(Utils.FILTERED_LINKS_ACCUMULATOR, filteredLinks);
  }

  @Override
  public boolean filter(Tuple4<Edge<Long, NullValue>, Long, String, Integer> tuple) throws Exception {
    if (tuple.f3 < 2) {
      return true;
    }
    else {
      filteredLinks.add(tuple.f0);
      filterMatches.add(1L);
      return false;
    }
  }
}
