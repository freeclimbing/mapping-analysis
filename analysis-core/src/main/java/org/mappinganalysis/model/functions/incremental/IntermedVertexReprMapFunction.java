package org.mappinganalysis.model.functions.incremental;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;

/**
 * Map Vertex value to intermediate representative representation.
 */
class IntermedVertexReprMapFunction
    implements MapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(IntermedVertexReprMapFunction.class);
  private final Vertex<Long, ObjectMap> reuseVertex;
  private BlockingStrategy blockingStrategy;

  /**
   * Map Vertex value to intermediate representative representation.
   */
  public IntermedVertexReprMapFunction(BlockingStrategy blockingStrategy) {
    this.blockingStrategy = blockingStrategy;
    this.reuseVertex = new Vertex<>();
  }

  @Override
  public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
    reuseVertex.setId(vertex.getId());
    ObjectMap properties = vertex.getValue();
    properties.setBlockingKey(blockingStrategy);
    if (!properties.hasClusterDataSources()) {
      properties.setClusterDataSources(Sets.newHashSet(properties.getDataSource()));
    }
    if (!properties.hasClusterVertices()) {
      properties.setClusterVertices(Sets.newHashSet(vertex.getId()));
    }

    reuseVertex.setValue(properties);

//    LOG.info("out: " + reuseVertex.toString());
    return reuseVertex;
  }
}
