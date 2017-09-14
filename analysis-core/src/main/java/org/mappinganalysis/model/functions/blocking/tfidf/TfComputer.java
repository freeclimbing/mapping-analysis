package org.mappinganalysis.model.functions.blocking.tfidf;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Computes the frequency of words in a mails body.
 * Words consist only of alphabetical characters. Frequent words (stop words) are filtered out.
 *
 * copied from
 * https://github.com/dataArtisans/flink-training-exercises/blob/master/src/main/java/com/dataartisans/flinktraining/exercises/dataset_java/tf_idf/MailTFIDF.java
 */
public class TfComputer extends RichFlatMapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {

  // set of stop words
  private Set<String> stopWords;
  // map to count the frequency of words
  private transient Map<String, Integer> wordCounts;
  // pattern to match against words
  private transient Pattern wordPattern;

  public TfComputer() {
    this.stopWords = new HashSet<>();
  }

  public TfComputer(String[] stopWords) {
    // initialize stop words
    this.stopWords = new HashSet<>();
    Collections.addAll(this.stopWords, stopWords);
  }

  @Override
  public void open(Configuration config) {
    // initialized map and pattern
//    this.wordPattern = Pattern.compile("(\\p{Alpha})+");
    this.wordCounts = new HashMap<>();
  }

  @Override
  public void flatMap(Tuple2<String, String> mail, Collector<Tuple3<String, String, Integer>> out) throws Exception {

    // clear count map
    this.wordCounts.clear();

    // tokenize mail body along whitespaces
    StringTokenizer st = new StringTokenizer(mail.f1);
    // for each candidate word
    while(st.hasMoreTokens()) {
      // normalize to lower case
      String word = st.nextToken().toLowerCase();
      Matcher m = this.wordPattern.matcher(word);
      if(m.matches() && !this.stopWords.contains(word)) {
        // word matches pattern and is not a stop word -> increase word count
        int count = 0;
        if(wordCounts.containsKey(word)) {
          count = wordCounts.get(word);
        }
        wordCounts.put(word, count + 1);
      }
    }

    // emit all counted words
    for(String word : this.wordCounts.keySet()) {
      out.collect(new Tuple3<>(mail.f0, word, this.wordCounts.get(word)));
    }
  }
}