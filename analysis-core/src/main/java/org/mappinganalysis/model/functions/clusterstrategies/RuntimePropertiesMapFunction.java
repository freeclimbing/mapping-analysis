package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.config.Config;

/**
 * Add meta properties to vertices for incremental use case:
 * mode, artistTitleAlbum, blockingLabel
 */
class RuntimePropertiesMapFunction
    implements MapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private Config config;

  /**
   * Default constructor
   * @param config read properties from config
   */
  public RuntimePropertiesMapFunction(Config config) {
    this.config = config;
  }

  @Override
  public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
    ObjectMap properties = vertex.getValue();
    properties.setMode(config.getMode());
    properties.setArtistTitleAlbum(
        Utils.createSimpleArtistTitleAlbum(properties));
    properties.setBlockingKey(BlockingStrategy.STANDARD_BLOCKING);

    return vertex;
  }
}