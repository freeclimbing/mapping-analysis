package org.mappinganalysis.model.functions.refinement;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Triplet;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class ExcludeDuplicateOntologyTripletFlatJoinFunction
    implements FlatJoinFunction<Triplet<Long, ObjectMap, ObjectMap>, Tuple3<Long, Long, Long>,
    Triplet<Long, ObjectMap, ObjectMap>> {
  @Override
  public void join(Triplet<Long, ObjectMap, ObjectMap> left, Tuple3<Long, Long, Long> right,
                   Collector<Triplet<Long, ObjectMap, ObjectMap>> collector) throws Exception {
    if (right == null) {
      collector.collect(left);
    } else if (right.f2 != Long.MIN_VALUE) { // big cluster
      left.getSrcVertex().getValue().put(Utils.REFINE_ID, right.f2);
      left.getTrgVertex().getValue().put(Utils.REFINE_ID, right.f2);
      collector.collect(left);
    }
  }
}
