package org.mappinganalysis.util.functions.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;

public class SourceFilterFunction implements FilterFunction<Vertex<Long, ObjectMap>> {
  private final String source;

  public SourceFilterFunction(String source) {
    this.source = source;
  }

  @Override
  public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
    // TODO unclear if this should be done here issue #143
//    if (vertex.getValue().getDataSourcesList() != null) {
//      return AbstractionUtils.containsSrc(
//          Constants.GEO,
//          vertex.getValue().getIntDataSources(),
//          source);
//    } else
    if (vertex.getValue().getDataSource() != null) {
      return vertex.getValue().getDataSource().equals(source);
    } else {
      return false;
    }
  }
}
