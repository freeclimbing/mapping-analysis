package org.mappinganalysis.integration;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;

import java.util.HashSet;

/**
 * Filter vertices containing at least one of the given data sources.
 */
public class SourcesFilterFunction implements FilterFunction<Vertex<Long, ObjectMap>> {
  private HashSet<String> sources;

  public SourcesFilterFunction(HashSet<String> sources) {
    this.sources = sources;
  }

  @Override
  public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
    if (!vertex.getValue().getDataSource().equals(Constants.NULL)) {
      return sources.contains(vertex.getValue().getDataSource());
    } else if (!vertex.getValue().getDataSourcesList().isEmpty()) {
      return AbstractionUtils.hasOverlap(
          AbstractionUtils.getSourcesInt(
              vertex.getValue().getMode(),
              vertex.getValue().getDataSourcesList()),
          AbstractionUtils.getSourcesInt(
              vertex.getValue().getMode(),
              sources));
    } else {
      return false;
    }
  }
}
