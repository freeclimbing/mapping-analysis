package org.mappinganalysis.model.functions.blocking.tfidf;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Get best TF/IDF values for each input.
 */
public class HighIDFValueFlatMapper
    extends RichFlatMapFunction<Tuple2<Long, String>, Tuple2<Long, Long>> {
  private HashMap<String, Double> valueMap = Maps.newHashMap();
  private HashMap<String, Long> dictMap = Maps.newHashMap();

  private Set<String> stopWords;

  @Override
  public void open(Configuration parameters) {
    List<Tuple2<String, Double>> idfList = getRuntimeContext().getBroadcastVariable("idf");

    for (Tuple2<String, Double> listTuple : idfList) {
      valueMap.put(listTuple.f0, listTuple.f1);
    }

    List<Tuple2<Long, String>> idfDict = getRuntimeContext().getBroadcastVariable("idfDict");

    for (Tuple2<Long, String> dictTuple : idfDict) {
      dictMap.put(dictTuple.f1, dictTuple.f0);
    }
  }

  public HighIDFValueFlatMapper(String[] stopWords) {
    this.stopWords = Sets.newHashSet(stopWords);
  }

  /**
   * out returns vertex id / string dict long value tuples
   */
  @Override
  public void flatMap(Tuple2<Long, String> tuple,
                      Collector<Tuple2<Long, Long>> out) throws Exception {
    StringTokenizer st = new StringTokenizer(tuple.f1);
    HashMap<String, Double> tmpResult = Maps.newHashMap();

    while (st.hasMoreTokens()) {
      String word = st.nextToken().toLowerCase();
      if (word.length() == 1 || stopWords.contains(word) ||
          valueMap.get(word) == null) {
        continue;
      }

      tmpResult.put(word, valueMap.get(word));
    }
//    System.out.println(tmpResult.toString());

    int limit = (int) Math.ceil((double) tmpResult.size() / 2);
    limit = limit < 2 ? 2 : limit;

    int count = 0;
    if (tmpResult.size() > limit) {
      while (count < limit) {
        double maxValue = tmpResult.values().stream().max(Double::compare).get();

        for(Iterator<Map.Entry<String, Double>> it = tmpResult.entrySet().iterator(); it.hasNext(); ) {
          Map.Entry<String, Double> entry = it.next();

          if (entry.getValue() == maxValue) {
//            System.out.println(new Tuple2<>(entry.getValue().longValue(), entry.getKey()).toString());
            out.collect(new Tuple2<>(tuple.f0, dictMap.get(entry.getKey())));
//            if (tuple.f0 == 887L || tuple.f0 == 17706L) {
//              System.out.println("high idf: " + tuple.toString() + " entry: " + entry.getKey());
//            }
            it.remove();
            ++count;
          }
        }

      }
    } else if (tmpResult.size() > 0) {
      for (String s : tmpResult.keySet()) {
//        System.out.println(new Tuple2<>(tmpResult.get(s).longValue(), s).toString());
//        if (tuple.f0 == 887L || tuple.f0 == 17706L) {
//          System.out.println("high idf: " + tuple.toString() + " s: " + s);
//        }
        out.collect(new Tuple2<>(tuple.f0, dictMap.get(s)));
      }
    }
    else if (tmpResult.size() == 0) {
//      System.out.println("tmpresult 0 " + tuple.toString());
//      if (tuple.f0 == 12960L || tuple.f0 == 12507) {
//        System.out.println("high idf: " + tuple.toString());
//      }
    }
  }
}
