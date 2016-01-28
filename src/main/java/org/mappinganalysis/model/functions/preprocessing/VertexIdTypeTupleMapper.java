package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class VertexIdTypeTupleMapper implements MapFunction<Vertex<Long, ObjectMap>, Tuple2<Long, String>> {
  @Override
  public Tuple2<Long, String> map(Vertex<Long, ObjectMap> vertex) throws Exception {
    return new Tuple2<>(vertex.getId(), vertex.getValue().get(Utils.TYPE_INTERN).toString());
  }
}
