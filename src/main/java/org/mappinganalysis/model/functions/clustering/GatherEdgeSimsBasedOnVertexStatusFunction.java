package org.mappinganalysis.model.functions.clustering;

import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class GatherEdgeSimsBasedOnVertexStatusFunction extends GatherFunction<ObjectMap, ObjectMap, ObjectMap> {
  @Override
  public ObjectMap gather(Neighbor<ObjectMap, ObjectMap> neighbor) {
    ObjectMap result = new ObjectMap();
    result.put(Utils.AGG_VALUE_COUNT, 1L);
    String eSimVal = Utils.AGGREGATED_SIM_VALUE;

    if (neighbor.getNeighborValue().containsKey(Utils.VERTEX_STATUS) &&
        (boolean) neighbor.getNeighborValue().get(Utils.VERTEX_STATUS)) {
      if (neighbor.getEdgeValue().containsKey(eSimVal)) {
        result.put(eSimVal,
            neighbor.getEdgeValue().get(eSimVal));
      } else {
        result.put(eSimVal, 0.0);
      }
    } else {
      result.put(eSimVal, 0.0);
    }
    return result;
  }
}
