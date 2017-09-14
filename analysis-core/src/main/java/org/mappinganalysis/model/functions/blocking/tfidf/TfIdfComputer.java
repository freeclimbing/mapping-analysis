package org.mappinganalysis.model.functions.blocking.tfidf;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * Compute the TF-IDF score for a word combining TF, DF, and total count.
 */
public class TfIdfComputer
    implements CustomUnaryOperation<Vertex<Long, ObjectMap>, Tuple2<String, Double>> {

  /**
   * default stop words list, can be overwritten
   */
  private String[] stopWords = {
      "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is",
      "there", "it", "this", "that", "on", "was", "by", "of", "to", "in",
      "to", "not", "be", "with", "you", "have", "as", "can"
  };
  private DataSet<Tuple1<String>> inputData;

  public TfIdfComputer(String[] stopWords) {
    this.stopWords = stopWords;
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> inputData) {
    this.inputData = inputData
        .map(new MapFunction<Vertex<Long, ObjectMap>, Tuple1<String>>() {
          @Override
          public Tuple1<String> map(Vertex<Long, ObjectMap> value) throws Exception {
            String artistTitleAlbum = Constants.EMPTY_STRING;
            String artist = value.getValue().getArtist();
            if (!artist.equals(Constants.CSV_NO_VALUE)) {
              artistTitleAlbum = artist;
            }
            String label = value.getValue().getLabel();
            if (!label.equals(Constants.CSV_NO_VALUE)) {
              if (artistTitleAlbum.equals(Constants.EMPTY_STRING)) {
                artistTitleAlbum = label;
              } else {
                artistTitleAlbum = artistTitleAlbum.concat(Constants.DEVIDER).concat(label);
              }
            }
            String album = value.getValue().getAlbum();
            if (!album.equals(Constants.CSV_NO_VALUE)) {
              if (artistTitleAlbum.equals(Constants.EMPTY_STRING)) {
                artistTitleAlbum = album;
              } else {
                artistTitleAlbum = artistTitleAlbum.concat(Constants.DEVIDER).concat(album);
              }
            }
            return new Tuple1<>(artistTitleAlbum);
          }
        });
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
        }).groupBy(0).sum(1);

    DataSet<Tuple1<Integer>> inputAgg = inputData
        .map(new MapFunction<Tuple1<String>, Tuple1<Integer>>() {
          @Override
          public Tuple1<Integer> map(Tuple1<String> value) throws Exception {
            return new Tuple1<>(1);
          }
        })
        .aggregate(Aggregations.SUM, 0);

    try {
      return wordFrequency.map(new IdfValueCalculator(inputAgg.first(1)));
    } catch (Exception e) {
      e.printStackTrace();
    }
//    idfValues.collect()
//        .stream()
//        .sorted((left, right) -> Double.compare(right.f1, left.f1))
//        .forEach(System.out::println);

    return null;

  }
}