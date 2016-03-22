package org.mappinganalysis.utils.functions.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Triplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class RefineIdExcludeFilterFunction
    implements FilterFunction<Triplet<Long, ObjectMap, ObjectMap>> {
  @Override
  public boolean filter(Triplet<Long, ObjectMap, ObjectMap> triplet) throws Exception {
    return !triplet.getSrcVertex().getValue().containsKey(Utils.REFINE_ID)
        && !triplet.getTrgVertex().getValue().containsKey(Utils.REFINE_ID);
  }
}
