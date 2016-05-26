package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * For each type contained in a vertex, emit a Tupl2 for further processing.
 */
public class VertexIdTypeTupleMapper implements FlatMapFunction<Vertex<Long, ObjectMap>, Tuple2<Long, String>> {
  @Override
  public void flatMap(Vertex<Long, ObjectMap> vertex, Collector<Tuple2<Long, String>> out) throws Exception {
    if (vertex.getValue().containsKey(Utils.TYPE_INTERN)) {
      for (String value : vertex.getValue().getTypes(Utils.TYPE_INTERN)) {
        out.collect(new Tuple2<>(vertex.getId(), value));
      }
    }
  }
}
