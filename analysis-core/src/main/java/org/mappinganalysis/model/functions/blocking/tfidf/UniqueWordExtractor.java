package org.mappinganalysis.model.functions.blocking.tfidf;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Copied in parts from
 * https://github.com/dataArtisans/flink-training-exercises/blob/master/src/main/java/com/dataartisans/flinktraining/exercises/dataset_java/tf_idf/MailTFIDF.java
 */
public class UniqueWordExtractor
    extends RichFlatMapFunction<Tuple1<String>, Tuple3<String, String, Integer>> {

  private Set<String> stopWords;
  private transient Set<String> emittedWords;
  private transient Pattern wordPattern;

  public UniqueWordExtractor() {
    this.stopWords = new HashSet<>();
  }

  public UniqueWordExtractor(String[] stopWords) {
    this.stopWords = new HashSet<>();
    Collections.addAll(this.stopWords, stopWords);
  }

  @Override
  public void open(Configuration config) {
    this.emittedWords = new HashSet<>();
    this.wordPattern = Pattern.compile("(\\p{Alpha})+");
  }

  @Override
  public void flatMap(Tuple1<String> input,
                      Collector<Tuple3<String, String, Integer>> out) throws Exception {
    this.emittedWords.clear();
    // split body along whitespaces into tokens
    StringTokenizer st = new StringTokenizer(input.f0);

    // for each word candidate
    while(st.hasMoreTokens()) {
      String word = st.nextToken().toLowerCase();
      if (word.length() == 1) {
        continue;
      }

      Matcher m = this.wordPattern.matcher(word);
      if(m.matches() && !this.stopWords.contains(word)
          && !this.emittedWords.contains(word)) {
        // candidate matches word pattern, is not a stop word, and was not emitted before
        out.collect(new Tuple3<>(input.f0, word, 1));
        this.emittedWords.add(word);
      }
    }
  }
}