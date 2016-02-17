package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.mappinganalysis.utils.Utils;

public class FilterNotEqualTypeEdges implements FilterFunction<Tuple4<Long, Long, String, String>> {

  @Override
  public boolean filter(Tuple4<Long, Long, String, String> tuple) throws Exception {
    return !(tuple.f2.equals(Utils.NO_TYPE_AVAILABLE)
        || tuple.f2.equals(Utils.NO_TYPE_FOUND)
        || tuple.f3.equals(Utils.NO_TYPE_AVAILABLE)
        || tuple.f3.equals(Utils.NO_TYPE_FOUND))
        && !tuple.f2.equals(tuple.f3);
  }
}
