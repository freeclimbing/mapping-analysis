package org.mappinganalysis.model.functions.blocking.tfidf;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

public class IdfValueCalculator implements MapFunction<Tuple2<String, Integer>, Tuple2<String, Double>> {
  private static final long serialVersionUID = 1L;

  // The amount of documents is need to calculate the IDF-Value
  double amountOfDocuments;

  // The constructor sets the amount of documents
  public IdfValueCalculator(double amountOfDocuments) {
    this.amountOfDocuments = amountOfDocuments;
  }

  public IdfValueCalculator(DataSet<Tuple1<Integer>> first) throws Exception {
    this.amountOfDocuments = first.collect().iterator().next().f0;
  }

  @Override
  public Tuple2<String, Double> map(Tuple2<String, Integer> input) throws Exception {
    return new Tuple2<>(input.f0, Math.log10(amountOfDocuments / input.f1));
  }

}