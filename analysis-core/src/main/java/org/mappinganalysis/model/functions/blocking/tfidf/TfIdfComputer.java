package org.mappinganalysis.model.functions.blocking.tfidf;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Compute the TF-IDF score for a word combining TF, DF, and total count.
 */
public class TfIdfComputer
    implements CustomUnaryOperation<Tuple2<Long, String>, Tuple2<String, Double>> {

  /**
   * default (music setting) stop words list, can be overwritten
   */
  private String[] stopWords = {
      "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is",
      "there", "it", "this", "that", "on", "was", "by", "of", "to", "in",
      "to", "not", "be", "with", "you", "have", "as", "can"
      // 2
      , "me", "my", "la", "all",
      "love", "de", "no", "best", "music", "live", "hits", "from", "collection", "your"
  };
  private DataSet<Tuple1<String>> inputData;

  public TfIdfComputer(String[] stopWords) {
    this.stopWords = stopWords;
  }

  @Override
  public void setInput(DataSet<Tuple2<Long, String>> inputData) {
    this.inputData = inputData
        .map(tuple -> new Tuple1<>(tuple.f1))
        .returns(new TypeHint<Tuple1<String>>() {});
  }

  @Override
  public DataSet<Tuple2<String, Double>> createResult() {
    DataSet<Tuple3<String, String, Integer>> uniqueWords = inputData
        .flatMap(new UniqueWordExtractor(stopWords));

    DataSet<Tuple2<String, Integer>> wordFrequency = uniqueWords
        .map(new MapFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>>() {
          @Override
          public Tuple2<String, Integer> map(Tuple3<String, String, Integer> input) throws Exception {
            String uniqueWord = input.f1;
            Integer count = input.f2;
            return new Tuple2<>(uniqueWord, count);
          }
        })
        .groupBy(0)
        .sum(1);
//    try {
//      wordFrequency.collect()
//          .stream()
//          .sorted((left, right) -> Integer.compare(right.f1, left.f1))
//          .forEach(System.out::println);
//    } catch (Exception e) {
//      e.printStackTrace();
//    }

    return wordFrequency.crossWithTiny(wordFrequency.sum(1))
        .with((left, right) -> new Tuple3<>(left.f0, left.f1, right.f1))
        .returns(new TypeHint<Tuple3<String, Integer, Integer>>() {})
        .map(new IdfValueCalculator());
  }
}