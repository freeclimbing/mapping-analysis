package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.config.IncrementalConfig;

/**
 * Add meta properties to vertices for incremental use case:
 * mode, artistTitleAlbum, blockingLabel
 */
public class RuntimePropertiesMapFunction
    implements MapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private IncrementalConfig config;

  /**
   * Default constructor
   * @param config read properties from config
   */
  public RuntimePropertiesMapFunction(IncrementalConfig config) {
    this.config = config;
  }

  @Override
  public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
    ObjectMap properties = vertex.getValue();
    properties.setMode(config.getMode());
    if (config.getDataDomain() == DataDomain.MUSIC) {
      properties.setArtistTitleAlbum(
          Utils.createSimpleArtistTitleAlbum(properties));
    }

    properties.setBlockingKey(
        config.getBlockingStrategy(),
        config.getBlockingLength());

    return vertex;
  }
}