package org.mappinganalysis.util.functions.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Triplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

public class RefineIdFilterFunction
    implements FilterFunction<Triplet<Long, ObjectMap, ObjectMap>> {
  @Override
  public boolean filter(Triplet<Long, ObjectMap, ObjectMap> triplet) throws Exception {
    return triplet.getSrcVertex().getValue().containsKey(Constants.REFINE_ID)
        || triplet.getTrgVertex().getValue().containsKey(Constants.REFINE_ID);
  }
}
