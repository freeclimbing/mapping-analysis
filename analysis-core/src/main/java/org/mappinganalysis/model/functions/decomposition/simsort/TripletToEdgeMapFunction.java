package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Triplet;
import org.mappinganalysis.model.ObjectMap;

public class TripletToEdgeMapFunction implements MapFunction<Triplet<Long, ObjectMap, ObjectMap>, Edge<Long, ObjectMap>> {
  Edge<Long, ObjectMap> reuseEdge;
  public TripletToEdgeMapFunction() {
    reuseEdge = new Edge<>();
  }

  @Override
  public Edge<Long, ObjectMap> map(Triplet<Long, ObjectMap, ObjectMap> triplet) throws Exception {
    reuseEdge.setFields(triplet.f0, triplet.f1, triplet.f4);
    return reuseEdge;
  }
}