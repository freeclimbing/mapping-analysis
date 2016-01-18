package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class MeanValueMapFunction implements MapFunction<Tuple4<Long, String, Double, Integer>, Tuple3<Long, String, Double>> {
  @Override
  public Tuple3<Long, String, Double> map(Tuple4<Long, String, Double, Integer> tuple) throws Exception {
    return new Tuple3<>(tuple.f0, tuple.f1, tuple.f2 / tuple.f3);
  }
}
