package org.mappinganalysis.util.functions.filter;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;

import java.util.Set;

/**
 * Filter vertices based on a set of vertex ids
 */
public class VertexFilterFunction implements FilterFunction<Vertex<Long, ObjectMap>> {
  private Set<Long> containSet;

  public VertexFilterFunction(Set<Long> containSet) {
    this.containSet = containSet;
  }

  @Override
  public boolean filter(Vertex<Long, ObjectMap> value) throws Exception {
    return containSet.contains(value.getId());
  }
}
