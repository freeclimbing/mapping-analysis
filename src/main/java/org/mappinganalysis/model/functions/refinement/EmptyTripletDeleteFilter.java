package org.mappinganalysis.model.functions.refinement;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;

public class EmptyTripletDeleteFilter implements FilterFunction<Triplet<Long, ObjectMap, NullValue>> {
  @Override
  public boolean filter(Triplet<Long, ObjectMap, NullValue> triplet) throws Exception {
    return triplet.getSrcVertex().getId() != 0L && triplet.getTrgVertex().getId() != 0L;
  }
}
