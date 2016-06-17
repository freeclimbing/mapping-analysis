package org.mappinganalysis.model.functions;

import org.apache.flink.graph.VertexJoinFunction;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * Add cc_id as an additional property in an existing vertex property set.
 */
public class CcIdVertexJoinFunction implements VertexJoinFunction<ObjectMap, Long> {
  @Override
  public ObjectMap vertexJoin(ObjectMap map, Long ccId) throws Exception {
    map.put(Utils.CC_ID, ccId);

    return map;
  }
}
