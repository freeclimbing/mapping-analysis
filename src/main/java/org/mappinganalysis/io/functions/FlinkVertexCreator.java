package org.mappinganalysis.io.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.mappinganalysis.model.FlinkVertex;

import java.util.Map;

/**
 * Create FlinkVertex object from raw database result set.
 */
public class FlinkVertexCreator implements MapFunction<Tuple3<Integer, String, String>, FlinkVertex> {

  private final FlinkVertex reuseVertex;

  public FlinkVertexCreator() {
    reuseVertex = new FlinkVertex();
  }

  public FlinkVertex map(Tuple3<Integer, String, String> tuple) throws Exception {
    reuseVertex.setId((long) tuple.f0);
    Map<String, Object> propMap = Maps.newHashMap();
    propMap.put("url", tuple.f1);
    propMap.put("ontology", tuple.f2);
    reuseVertex.setValue(propMap);
    return reuseVertex;
  }
}
