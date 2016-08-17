package org.mappinganalysis.model.functions.simcomputation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * Filter coordinates where both latitude and longitude are 0 for either source or target resource.
 */
public class EmptyGeoCodeFilter implements FilterFunction<Triplet<Long, ObjectMap, NullValue>> {
  @Override
  public boolean filter(Triplet<Long, ObjectMap, NullValue> triplet) throws Exception {
    return triplet.getSrcVertex().getValue().hasGeoPropertiesValid()
        && triplet.getTrgVertex().getValue().hasGeoPropertiesValid();

  }
}
