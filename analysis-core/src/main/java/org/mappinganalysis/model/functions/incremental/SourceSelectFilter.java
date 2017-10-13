package org.mappinganalysis.model.functions.incremental;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;

public class SourceSelectFilter implements FilterFunction<Vertex<Long, ObjectMap>> {
  private String dataSource;

  public SourceSelectFilter(String dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
    return vertex.getValue().getDataSource().equals(dataSource);
  }
}
