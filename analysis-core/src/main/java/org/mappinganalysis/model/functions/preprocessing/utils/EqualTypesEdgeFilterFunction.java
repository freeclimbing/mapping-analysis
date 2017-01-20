package org.mappinganalysis.model.functions.preprocessing.utils;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

/**
 * Get all edges where type or type shading is equal.
 */
public class EqualTypesEdgeFilterFunction
    extends RichFilterFunction<Tuple4<Long, Long, String, String>> {
  private LongCounter edgeCounter = new LongCounter();

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Constants.EDGE_EXCLUDE_ACCUMULATOR, edgeCounter);
  }

  @Override
  public boolean filter(Tuple4<Long, Long, String, String> tuple) throws Exception {
    boolean result = (
        tuple.f2.equals(Constants.NO_TYPE)
            || tuple.f2.equals("")
            || tuple.f3.equals(Constants.NO_TYPE)
            || tuple.f3.equals("")
        )
        || Utils.getShadingType(tuple.f2).equals(Utils.getShadingType(tuple.f3));

    if (!result) {
      edgeCounter.add(1L);
    }
    return result;
  }
}
