package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

/**
 * Created by markus on 5/1/16.
 */
public class ClusterVertex extends Vertex<Long, Tuple2<Long, Long>> {
  public ClusterVertex(Long id, Long cId) {
    f0 = id;
    f1.f1 = cId;
  }
}
