package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Triplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * Filter resulting triplets based on type equality.
 */
public class TypeFilter implements FilterFunction<Triplet<Long, ObjectMap, ObjectMap>> {
  @Override
  public boolean filter(Triplet<Long, ObjectMap, ObjectMap> weightedTriplet) throws Exception {
    ObjectMap props = weightedTriplet.getEdge().getValue();
    return props.containsKey(Utils.SIM_TYPE) && (float) props.get(Utils.SIM_TYPE) == 1f;
  }
}
