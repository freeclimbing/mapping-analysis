package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * Check if type "" exists.TODO
 */
public class VertexIdTypeTupleMapper implements MapFunction<Vertex<Long, ObjectMap>, Tuple2<Long, String>> {
  @Override
  public Tuple2<Long, String> map(Vertex<Long, ObjectMap> vertex) throws Exception {
    String type = "";
    if (vertex.getValue().containsKey(Utils.TYPE_INTERN)) {
      type = vertex.getValue().get(Utils.TYPE_INTERN).toString();
    }

    return new Tuple2<>(vertex.getId(), type);
  }
}
