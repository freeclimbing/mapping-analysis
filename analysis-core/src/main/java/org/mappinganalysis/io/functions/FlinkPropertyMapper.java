package org.mappinganalysis.io.functions;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.mappinganalysis.model.FlinkProperty;
import org.mappinganalysis.utils.Utils;

/**
 * Create a FlinkProperty from raw database result set.
 */
public class FlinkPropertyMapper extends RichMapFunction<Tuple4<Integer, String, String, String>, FlinkProperty> {
  private LongCounter propertyCount = new LongCounter();

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Utils.PROP_COUNT_ACCUMULATOR, propertyCount);
  }

  @Override
  public FlinkProperty map(Tuple4<Integer, String, String, String> in) throws Exception {
    FlinkProperty property = new FlinkProperty();
    property.f0 = (long) in.f0;
    property.f1 = in.f1;
    property.f2 = in.f2;
    property.f3 = in.f3;
    propertyCount.add(1L);

    return property;
  }
}
