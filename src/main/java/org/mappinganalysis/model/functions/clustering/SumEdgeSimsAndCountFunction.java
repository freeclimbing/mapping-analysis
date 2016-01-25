package org.mappinganalysis.model.functions.clustering;

import org.apache.flink.graph.gsa.SumFunction;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class SumEdgeSimsAndCountFunction extends SumFunction<ObjectMap, ObjectMap, ObjectMap> {
  @Override
  public ObjectMap sum(ObjectMap newMap, ObjectMap currentMap) {
    ObjectMap sumResult = new ObjectMap();
    String eSimVal = Utils.AGGREGATED_SIM_VALUE;

    sumResult.put(eSimVal, (double) newMap.get(eSimVal)
        + (double) currentMap.get(eSimVal));
    sumResult.put(Utils.AGG_VALUE_COUNT,
        (long) newMap.get(Utils.AGG_VALUE_COUNT) +
            (long) currentMap.get(Utils.AGG_VALUE_COUNT));

    return sumResult;
  }
}
