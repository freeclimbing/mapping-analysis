package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.IdTypeTuple;
import org.mappinganalysis.util.Constants;

/**
 * For each type contained in a vertex, emit a Tuple2 for further processing.
 */
public class VertexIdTypeTupleMapper
    implements FlatMapFunction<Vertex<Long, ObjectMap>, IdTypeTuple> {
  @Override
  public void flatMap(Vertex<Long, ObjectMap> vertex, Collector<IdTypeTuple> out) throws Exception {
    if (vertex.getValue().containsKey(Constants.TYPE_INTERN)) {
      for (String value : vertex.getValue().getTypes(Constants.TYPE_INTERN)) {
        out.collect(new IdTypeTuple(vertex.getId(), value));
      }
    }
  }
}
