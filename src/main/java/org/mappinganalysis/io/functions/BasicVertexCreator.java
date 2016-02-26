package org.mappinganalysis.io.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;

/**
 * Create FlinkVertex object from raw database result set.
 */
public class BasicVertexCreator implements MapFunction<Tuple3<Integer, String, String>, Vertex<Long, ObjectMap>> {
  private final Vertex<Long, ObjectMap> reuseVertex;

  public BasicVertexCreator() {
    reuseVertex = new Vertex<>();
  }

  public Vertex<Long, ObjectMap> map(Tuple3<Integer, String, String> tuple) throws Exception {
    reuseVertex.setId((long) tuple.f0);
    ObjectMap propMap = new ObjectMap();
    propMap.put("url", tuple.f1);
    propMap.put("ontology", tuple.f2);
    reuseVertex.setValue(propMap);
    return reuseVertex;
  }
}
