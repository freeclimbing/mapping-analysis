package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Creates Tuple2 with start and target vertex as well as the mean value of similarities.
 */
public class MeanValueMapFunction implements MapFunction<Tuple3<Long, Double, Integer>, Tuple2<Long, Double>> {
  @Override
  public Tuple2<Long, Double> map(Tuple3<Long, Double, Integer> tuple) throws Exception {
    return new Tuple2<>(tuple.f0, tuple.f1 / tuple.f2);
  }
}
