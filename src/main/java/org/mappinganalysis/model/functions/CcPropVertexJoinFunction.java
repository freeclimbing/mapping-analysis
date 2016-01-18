package org.mappinganalysis.model.functions;

import org.apache.flink.graph.VertexJoinFunction;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * Add cc_id as an additional property in an existing vertex property set.
 */
public class CcPropVertexJoinFunction implements VertexJoinFunction<ObjectMap, Long> {
  @Override
  public ObjectMap vertexJoin(ObjectMap objectMap, Long aLong) throws Exception {
    objectMap.put(Utils.CC_ID, aLong);
    return objectMap;
  }
}
