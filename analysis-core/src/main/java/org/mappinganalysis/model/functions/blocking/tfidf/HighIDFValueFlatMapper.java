package org.mappinganalysis.model.functions.blocking.tfidf;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.MergeMusicTuple;

import java.util.*;

/**
 * Get best TF/IDF values for each input.
 */
public class HighIDFValueFlatMapper
    extends RichFlatMapFunction<MergeMusicTuple, Tuple2<Long, String>> {
  private HashMap<String, Double> valueMap = Maps.newHashMap();
  private Set<String> stopWords;

  @Override
  public void open(Configuration parameters) {
    List<Tuple2<String, Double>> idfList = getRuntimeContext().getBroadcastVariable("idf");

    for (Tuple2<String, Double> tuple : idfList) {
      valueMap.put(tuple.f0, tuple.f1);
    }
  }

  public HighIDFValueFlatMapper(String[] stopWords) {
    this.stopWords = Sets.newHashSet(stopWords);
  }
  @Override
  public void flatMap(MergeMusicTuple tuple,
                      Collector<Tuple2<Long, String>> out) throws Exception {
    StringTokenizer st = new StringTokenizer(tuple.getArtistTitleAlbum());
    HashMap<String, Double> tmpResult = Maps.newHashMap();

    while (st.hasMoreTokens()) {
      String word = st.nextToken().toLowerCase();
      if (word.length() == 1 || stopWords.contains(word) || valueMap.get(word) == null) {
        continue;
      }

      tmpResult.put(word, valueMap.get(word));
    }
//    System.out.println(tmpResult.toString());

    int count = 0;
    if (tmpResult.size() > 2) {
      while (count < 2) {
        double minValue = tmpResult.values().stream().min(Double::compare).get();

        for(Iterator<Map.Entry<String, Double>> it = tmpResult.entrySet().iterator(); it.hasNext(); ) {
          Map.Entry<String, Double> entry = it.next();

          if (entry.getValue() == minValue) {
//            System.out.println(new Tuple2<>(entry.getValue().longValue(), entry.getKey()).toString());
            out.collect(new Tuple2<>(tuple.f0, entry.getKey()));
            it.remove();
            ++count;
          }
        }

      }
    } else if (tmpResult.size() > 0) {
      for (String s : tmpResult.keySet()) {
//        System.out.println(new Tuple2<>(tmpResult.get(s).longValue(), s).toString());
        out.collect(new Tuple2<>(tuple.f0, s));
      }
    }
  }
}
