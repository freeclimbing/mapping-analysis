package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Triplet;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

public class ExcludeDuplicateOntologyTripletFlatJoinFunction
    implements FlatJoinFunction<Triplet<Long, ObjectMap, ObjectMap>, Tuple4<Long, Long, Long, Double>,
    Triplet<Long, ObjectMap, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(ExcludeDuplicateOntologyTripletFlatJoinFunction.class);

  @Override
  public void join(Triplet<Long, ObjectMap, ObjectMap> left, Tuple4<Long, Long, Long, Double> right,
                   Collector<Triplet<Long, ObjectMap, ObjectMap>> collector) throws Exception {
    if (right == null) {
      collector.collect(left);
    } else if (right.f2 != Long.MIN_VALUE) { // exclude big cluster
      left.getSrcVertex().getValue().put(Constants.REFINE_ID, right.f2);
      left.getTrgVertex().getValue().put(Constants.REFINE_ID, right.f2);

      collector.collect(left);
    }
  }
}
