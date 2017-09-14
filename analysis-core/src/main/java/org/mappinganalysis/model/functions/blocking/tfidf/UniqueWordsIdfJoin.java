package org.mappinganalysis.model.functions.blocking.tfidf;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class UniqueWordsIdfJoin
    implements JoinFunction<Tuple3<String, String, Integer>,
    Tuple2<String, Double>, Tuple2<String, String>> {

  @Override
  public Tuple2<String, String> join(
      Tuple3<String, String, Integer> uniqueWords,
      Tuple2<String, Double> idfWords) {
    // multiply the points and rating and construct a new output tuple

    return new Tuple2<>(uniqueWords.f0, uniqueWords.f1);
  }
}