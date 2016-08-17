package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Triplet;
import org.mappinganalysis.model.ObjectMap;


class CreateNoticableTripletMapFunction
    implements MapFunction<Triplet<Long, ObjectMap, ObjectMap>, Tuple4<Long, Long, Double, Integer>> {
  @Override
  public Tuple4<Long, Long, Double, Integer> map(Triplet<Long, ObjectMap, ObjectMap> triplet) throws Exception {

    return new Tuple4<>(triplet.getSrcVertex().getId(),
        triplet.getTrgVertex().getId(),
        triplet.getEdge().getValue().getEdgeSimilarity(),
        1);
  }
}
