package org.mappinganalysis.io.functions;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * Create Flink Vertex object from raw data input.
 */
public class BasicVertexCreator
    extends RichMapFunction<Tuple3<Integer, String, String>, Vertex<Long, ObjectMap>> {
  private final Vertex<Long, ObjectMap> reuseVertex;
  private LongCounter vertexCounter = new LongCounter();

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Constants.BASE_VERTEX_COUNT_ACCUMULATOR, vertexCounter);
  }

  public BasicVertexCreator() {
    reuseVertex = new Vertex<>();
    reuseVertex.setValue(new ObjectMap(Constants.GEO));
  }

  public Vertex<Long, ObjectMap> map(Tuple3<Integer, String, String> tuple) throws Exception {
    reuseVertex.setId((long) tuple.f0);
    reuseVertex.getValue().setDataSource(tuple.f2);

    vertexCounter.add(1L);
    return reuseVertex;
  }
}
