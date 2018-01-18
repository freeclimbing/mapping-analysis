package org.mappinganalysis.model.functions.blocking.lsh.utils;

import com.google.common.base.CharMatcher;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.log4j.Logger;
import org.mappinganalysis.util.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Too much special cases
 */
public class VertexIdfWeightsFunction
    extends RichMapFunction<Tuple2<Long, String>, Tuple2<Long, String>> {
  private static final Logger LOG = Logger.getLogger(VertexIdfWeightsFunction.class);

  private HashMap<String, Double> valueMap = Maps.newHashMap();
  private List<String> stopWords;
  @Override
  public void open(Configuration parameters) {
    List<Tuple2<String, Double>> idfList = getRuntimeContext().getBroadcastVariable("idf");
    stopWords = getRuntimeContext().getBroadcastVariable("stop");

    for (Tuple2<String, Double> listTuple : idfList) {
      valueMap.put(listTuple.f0, listTuple.f1);
    }
  }

  @Override
  public Tuple2<Long, String> map(Tuple2<Long, String> idLabelTuple) throws Exception {
    String label = idLabelTuple.f1;
    String original = label;
    StringTokenizer st = new StringTokenizer(label);
//    StringBuilder builder = new StringBuilder();

    // TODO handle non words
    while (st.hasMoreTokens()) {
      String word = st.nextToken().toLowerCase();
//      Double value = valueMap.get(word);
      if (stopWords.contains(word)) {
        label = label.replace(word, "");
      }

//      if (value != null) {
//        if (value > 2.8) { // TODO REMOVE FIXED
//          if (builder.toString().isEmpty()) {
//            builder.append(word);
//          } else {
//            builder.append(" ").append(word); // TODO ineffective, only remove word if needed!?
//          }
//        }
//      }
    }

    label = CharMatcher.WHITESPACE
        .trimAndCollapseFrom(label, ' ');

    if (!label.isEmpty()) {
      if (Utils.getUnsortedTrigrams(label).isEmpty()) {
        idLabelTuple.f1 = original;
      } else if (label.length() < 6) { // TODO remove fixed length
        idLabelTuple.f1 = original;
      } else {
        idLabelTuple.f1 = label;
      }
    } else {
      idLabelTuple.f1 = original; // TODO temp solution - if all terms are frequent, we take the original string again
    }

//    if (idLabelTuple.f0 == 0L||
//        idLabelTuple.f0 == 4279L
//        || idLabelTuple.f0 == 4316L
//        || idLabelTuple.f0 == 7350L) {
//      LOG.info("weight: " + idLabelTuple.f0 + " " + idLabelTuple.f1);
//    }

    return idLabelTuple;
  }
}
