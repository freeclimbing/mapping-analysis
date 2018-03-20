package org.mappinganalysis.model.functions.preprocessing.utils;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

/**
 * Get all edges where type or type shading is equal.
 */
public class EqualTypesEdgeFilterFunction
    implements FilterFunction<Tuple4<Long, Long, String, String>> {

  @Override
  public boolean filter(Tuple4<Long, Long, String, String> tuple) throws Exception {
    return (
        tuple.f2.equals(Constants.NO_TYPE)
            || tuple.f2.equals("")
            || tuple.f3.equals(Constants.NO_TYPE)
            || tuple.f3.equals("")
        )
        || Utils.getShadingType(tuple.f2).equals(Utils.getShadingType(tuple.f3));
  }
}
