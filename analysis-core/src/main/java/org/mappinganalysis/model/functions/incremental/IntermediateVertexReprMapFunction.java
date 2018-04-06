package org.mappinganalysis.model.functions.incremental;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.util.Constants;

/**
 * Map single vertex value to intermediate representative representation.
 */
class IntermediateVertexReprMapFunction
    implements MapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private final Vertex<Long, ObjectMap> reuseVertex;
  private DataDomain domain;
  private BlockingStrategy blockingStrategy;

  /**
   * Map single vertex value to intermediate representative representation.
   */
  IntermediateVertexReprMapFunction(DataDomain domain, BlockingStrategy blockingStrategy) {
    this.domain = domain;
    this.blockingStrategy = blockingStrategy;
    this.reuseVertex = new Vertex<>();
  }

  @Override
  public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
    reuseVertex.setId(vertex.getId());
    ObjectMap properties = vertex.getValue();
    properties.setMode(domain);
    properties.setMode(DataDomain.GEOGRAPHY);
    properties.setBlockingKey(blockingStrategy);

    if (!properties.hasClusterDataSources()) {
      properties.setClusterDataSources(Sets.newHashSet(properties.getDataSource()));
      properties.remove(Constants.DATA_SOURCE);
    }
    if (!properties.hasClusterVertices()) {
      properties.setClusterVertices(Sets.newHashSet(vertex.getId()));
    }

    reuseVertex.setValue(properties);
    return reuseVertex;
  }
}
