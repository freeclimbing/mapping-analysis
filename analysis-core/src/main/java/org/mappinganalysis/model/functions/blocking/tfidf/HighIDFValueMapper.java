package org.mappinganalysis.model.functions.blocking.tfidf;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.ObjectMap;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Created by markus on 6/9/17.
 */
public class HighIDFValueMapper
    extends RichMapFunction<MergeMusicTuple, Tuple2<Long, ObjectMap>> {
  private HashMap<String, Double> valueMap = Maps.newHashMap();
  private Set<String> stopWords;

  public HighIDFValueMapper(String[] stopWords) {
    this.stopWords = Sets.newHashSet(stopWords);
  }

  @Override
  public void open(Configuration parameters) {
    List<Tuple2<String, Double>> idfList = getRuntimeContext().getBroadcastVariable("idf");

    for (Tuple2<String, Double> tuple : idfList) {
      valueMap.put(tuple.f0, tuple.f1);
    }
  }

  @Override
  public Tuple2<Long, ObjectMap> map(MergeMusicTuple tuple) throws Exception {
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

    ObjectMap result = new ObjectMap();
    if (tmpResult.size() > 2) {
//      while (result.getIDFs().isEmpty() || result.getIDFs().size() < 2) {
        while (result.isEmpty() || result.size() < 2) {
//          System.out.println("preResultSize: " + result.size());
//          try {
        result.addMinValueToResult(tmpResult);
//          } catch (NoSuchElementException e) {
//            System.err.println("result: " + result.toString());
//            System.err.println("tmp: " + tmpResult.toString());
//            e.printStackTrace();
//          }
//          System.out.println("postResultSize: " + result.size());
      }
    } else if (tmpResult.size() > 0) {
//        for (String value : tmpResult.keySet()) {
      result.putAll(tmpResult);
//        }
    } else {
//        HashMap<String, Double> foo = Maps.newHashMap();
//        foo.put(tuple.getArtistTitleAlbum(), 10d);
//        result.setIDFs(foo);
    }

    /**
     * TODO handle small size / no size vertices
     */
//    else {
//        LOG.info("tmpresult size = 0");
//        LOG.info(vertex.getValue().getArtistTitleAlbum());
//      }
//      if (result.size() > 3) {
////        LOG.info("result size: " + result.size());
//        for (Map.Entry<String, Double> stringDoubleEntry : result.entrySet()) {
////          LOG.info(stringDoubleEntry.toString());
//        }
//      }

    return new Tuple2<>(tuple.f0, result);
  }
}
