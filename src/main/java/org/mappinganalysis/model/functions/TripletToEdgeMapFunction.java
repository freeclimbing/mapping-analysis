package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Triplet;
import org.mappinganalysis.model.ObjectMap;

public class TripletToEdgeMapFunction implements MapFunction<Triplet<Long, ObjectMap, ObjectMap>, Edge<Long, ObjectMap>> {
  @Override
  public Edge<Long, ObjectMap> map(Triplet<Long, ObjectMap, ObjectMap> triplet) throws Exception {
    return new Edge<>(triplet.f0, triplet.f1, triplet.f4);
  }
}
