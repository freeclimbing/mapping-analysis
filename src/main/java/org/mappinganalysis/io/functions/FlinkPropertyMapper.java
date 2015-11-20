package org.mappinganalysis.io.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.mappinganalysis.model.FlinkProperty;

/**
 * Create a FlinkProperty from raw database result set.
 */
public class FlinkPropertyMapper implements MapFunction<Tuple4<Integer, String, String, String>, FlinkProperty> {
  @Override
  public FlinkProperty map(Tuple4<Integer, String, String, String> in) throws Exception {
    FlinkProperty property = new FlinkProperty();
    property.f0 = (long) in.f0;
    property.f1 = in.f1;
    property.f2 = in.f2;
    property.f3 = in.f3;
    return property;
  }
}
