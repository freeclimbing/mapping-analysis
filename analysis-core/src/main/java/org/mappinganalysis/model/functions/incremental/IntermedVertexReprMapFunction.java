package org.mappinganalysis.model.functions.incremental;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;

/**
 * Map Vertex value to intermediate representative representation.
 */
class IntermedVertexReprMapFunction
    implements MapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(IntermedVertexReprMapFunction.class);
  private final Vertex<Long, ObjectMap> reuseVertex;

  private RepresentativeCreator representativeCreator;
  private DataDomain domain;
  private BlockingStrategy blockingStrategy;

  /**
   * Map Vertex value to intermediate representative representation.
   */
  public IntermedVertexReprMapFunction(DataDomain domain, BlockingStrategy blockingStrategy) {
    this.domain = domain;
    this.blockingStrategy = blockingStrategy;
    this.reuseVertex = new Vertex<>();
  }

  @Override
  public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
    reuseVertex.setId(vertex.getId());
    reuseVertex.setValue(new ObjectMap(vertex.getValue(), domain));
    reuseVertex.getValue().setBlockingKey(blockingStrategy);

//    LOG.info("out: " + reuseVertex.toString());
    return reuseVertex;
  }
}
