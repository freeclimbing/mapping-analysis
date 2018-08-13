package org.mappinganalysis.model.functions.blocking.tfidf;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.MergeTuple;

import java.util.*;

/**
 * Get best TF/IDF values for each input (single idf computation)
 */
public class SingleIdfHighMapper
    extends RichFlatMapFunction<MergeTuple, Tuple2<Long, String>> {
  private HashMap<String, Double> valueMap = Maps.newHashMap();
  private Set<String> stopWords;

  public SingleIdfHighMapper(String[] stopWords) {
    this.stopWords = Sets.newHashSet(stopWords);
  }

  @Override
  public void open(Configuration parameters) {
    List<Tuple2<String, Double>> idfList = getRuntimeContext().getBroadcastVariable("idf");

    for (Tuple2<String, Double> tuple : idfList) {
      valueMap.put(tuple.f0, tuple.f1); // string, double
    }
  }

  @Override
  public void flatMap(MergeTuple tuple, Collector<Tuple2<Long, String>> out) throws Exception {
    StringTokenizer st = new StringTokenizer(tuple.getArtistTitleAlbum());
    HashMap<String, Double> tmpResult = Maps.newHashMap();

    while (st.hasMoreTokens()) {
      String word = st.nextToken().toLowerCase();
      if (word.length() == 1 || stopWords.contains(word) || valueMap.get(word) == null) {
        continue;
      }

      tmpResult.put(word, valueMap.get(word));
    }
//        LOG.info("tmpResult size: " + tmpResult.size());

    int count = 0;
    if (tmpResult.size() > 2) {
      while (count < 2) {
        double minValue = tmpResult.values().stream().min(Double::compare).get();

        for(Iterator<Map.Entry<String, Double>> it = tmpResult.entrySet().iterator(); it.hasNext(); ) {
          Map.Entry<String, Double> entry = it.next();

          if (entry.getValue() == minValue) {
            out.collect(new Tuple2<>(tuple.f0, entry.getKey()));
            it.remove();
            ++count;
          }
        }
      }
    } else if (tmpResult.size() > 0) {
      for (String s : tmpResult.keySet()) {
        out.collect(new Tuple2<>(tuple.f0, s));
      }
    }
  }
}
