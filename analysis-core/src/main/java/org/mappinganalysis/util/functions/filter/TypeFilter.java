package org.mappinganalysis.util.functions.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Triplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * Filter resulting triplets based on type equality.
 */
public class TypeFilter implements FilterFunction<Triplet<Long, ObjectMap, ObjectMap>> {
  @Override
  public boolean filter(Triplet<Long, ObjectMap, ObjectMap> weightedTriplet) throws Exception {
    ObjectMap props = weightedTriplet.getEdge().getValue();
    return props.containsKey(Constants.SIM_TYPE) && (float) props.get(Constants.SIM_TYPE) == 1f;
  }
}
