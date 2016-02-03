package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * Key Selector to filter by component id and resource type.
 */
public class CcIdAndTypeKeySelector implements KeySelector<Vertex<Long, ObjectMap>, Tuple2<Long, String>> {
  @Override
  public Tuple2<Long, String> getKey(Vertex<Long, ObjectMap> vertex) throws Exception {
    return new Tuple2<>((long) vertex.getValue().get(Utils.CC_ID),
        vertex.getValue().get(Utils.TYPE_INTERN).toString());
  }
}
