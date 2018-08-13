package org.mappinganalysis.model.functions.incremental;

import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
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
  private static final Logger LOG = Logger.getLogger(IntermediateVertexReprMapFunction.class);

  private final Vertex<Long, ObjectMap> reuseVertex;
  private DataDomain domain;
  private BlockingStrategy blockingStrategy;
  private int blockingLength;

  /**
   * Map single vertex value to intermediate representative representation.
   */
  IntermediateVertexReprMapFunction(
      DataDomain domain,
      BlockingStrategy blockingStrategy,
      int blockingLength) {
    this.domain = domain;
    this.blockingStrategy = blockingStrategy;
    this.blockingLength = blockingLength;
    this.reuseVertex = new Vertex<>();
  }

  @Override
  public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
    reuseVertex.setId(vertex.getId());
    ObjectMap properties = vertex.getValue();
    properties.setMode(domain);
    properties.setBlockingKey(blockingStrategy, blockingLength);

    if (!properties.hasClusterDataSources()) {
      properties.setClusterDataSources(Sets.newHashSet(properties.getDataSource()));
      properties.remove(Constants.DATA_SOURCE);
    }
    if (!properties.hasClusterVertices()) {
      properties.setClusterVertices(Sets.newHashSet(vertex.getId()));
    }

    // todo +10% with source-based still not working
//    if (domain == DataDomain.GEOGRAPHY && vertex.getValue().containsKey(Constants.ALBUM)
//        && vertex.getValue().containsKey(Constants.ARTIST)) {
//      vertex.getValue().setGeoProperties(
//          Doubles.tryParse(vertex.getValue().getAlbum()),
//          Doubles.tryParse(vertex.getValue().getArtist())
//      );
//        vertex.getValue().remove(Constants.ARTIST);
//        vertex.getValue().remove(Constants.ALBUM);
//    }


    reuseVertex.setValue(properties);
    return reuseVertex;
  }
}
