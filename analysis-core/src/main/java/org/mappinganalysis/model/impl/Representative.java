package org.mappinganalysis.model.impl;

import org.apache.flink.graph.Vertex;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;

public class Representative extends Vertex<Long, RepresentativeMap> {

  public Representative() {
  }

  public Representative(Vertex<Long, ObjectMap> vertex, DataDomain domain) {
    super(vertex.getId(),
        new RepresentativeMap(vertex.getValue(), domain));
  }

  public void setBlockingKey(BlockingStrategy strategy) {
//    getValue().setBlockingKey(strategy);
  }
}
