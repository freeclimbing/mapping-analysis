package org.mappinganalysis.util.functions.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * Filter vertices based on data source.
 */
public class SourceFilterFunction implements FilterFunction<Vertex<Long, ObjectMap>> {
  private final String source;

  public SourceFilterFunction(String source) {
    this.source = source;
  }

  @Override
  public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
    if (!vertex.getValue().getDataSource().equals(Constants.NULL)) {
      return vertex.getValue().getDataSource().equals(source);
    } else if (!vertex.getValue().getDataSourcesList().isEmpty()) {
      return vertex.getValue().getDataSourcesList().contains(source);
    } else {
      return false;
    }
  }
}
