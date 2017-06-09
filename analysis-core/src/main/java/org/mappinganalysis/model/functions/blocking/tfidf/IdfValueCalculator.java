package org.mappinganalysis.model.functions.blocking.tfidf;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class IdfValueCalculator
    implements MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>> {

  // The amount of documents is need to calculate the IDF-Value
  @Override
  public Tuple2<String, Double> map(Tuple3<String, Integer, Integer> input) throws Exception {
    return new Tuple2<>(input.f0, Math.log10(input.f2 / input.f1));
  }

}