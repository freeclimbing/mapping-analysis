package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.FlinkVertex;
import org.mappinganalysis.utils.Utils;

/**
 * select strategy cc_id (e.g., for group by)
 */
public class CcIdKeySelector implements KeySelector<Vertex<Long, FlinkVertex>, Long> {
  @Override
  public Long getKey(Vertex<Long, FlinkVertex> vertex) throws Exception {
    return (long) vertex.getValue().getProperties().get(Utils.CC_ID);
  }
}
