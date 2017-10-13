package org.mappinganalysis.model.functions.incremental;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;

public class CandidateTripletCreator
    implements GroupReduceFunction<Vertex<Long,ObjectMap>, Triplet<Long, ObjectMap, ObjectMap>> {
  @Override
  public void reduce(Iterable<Vertex<Long, ObjectMap>> values, Collector<Triplet<Long, ObjectMap, ObjectMap>> out) throws Exception {

    out.collect(null);
  }
}
