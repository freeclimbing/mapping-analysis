package org.mappinganalysis.model.functions.blocking.lsh.utils;

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
 * Too mcuh special cases
 */
public class VertexIdfWeightsFunction
    extends RichMapFunction<Tuple2<Long, String>, Tuple2<Long, String>> {
  private static final Logger LOG = Logger.getLogger(VertexIdfWeightsFunction.class);

  private HashMap<String, Double> valueMap = Maps.newHashMap();

  @Override
  public void open(Configuration parameters) {
    List<Tuple2<String, Double>> idfList = getRuntimeContext().getBroadcastVariable("idf");

    for (Tuple2<String, Double> listTuple : idfList) {
      valueMap.put(listTuple.f0, listTuple.f1);
    }
  }

  @Override
  public Tuple2<Long, String> map(Tuple2<Long, String> labelTuple) throws Exception {
    String label = labelTuple.f1;
    StringTokenizer st = new StringTokenizer(label);
    StringBuilder builder = new StringBuilder();

    // TODO handle non words
    while (st.hasMoreTokens()) {
      String word = st.nextToken().toLowerCase();
      Double value = valueMap.get(word);
      if (value != null) {
        if (value > 2.8) { // TODO REMOVE FIXED
          if (builder.toString().isEmpty()) {
            builder.append(word);
          } else {
            builder.append(" ").append(word); // TODO ineffective, only remove word if needed!?
          }
        }
      }
    }

    if (!builder.toString().isEmpty()) {
      if (Utils.getUnsortedTrigrams(builder.toString()).isEmpty()) {
        labelTuple.f1 = label;
      } else if (builder.toString().length() < 6) { // TODO remove fixed length
        labelTuple.f1 = label;
      } else {
        labelTuple.f1 = builder.toString();
      }
    } else {
      labelTuple.f1 = label; // TODO temp solution - if all terms are frequent, we take the original string again
    }

//    if (labelTuple.f0 == 0L||
//        labelTuple.f0 == 4279L
//        || labelTuple.f0 == 4316L
//        || labelTuple.f0 == 7350L) {
//      LOG.info("weight: " + labelTuple.f0 + " " + labelTuple.f1);
//    }

    return labelTuple;
  }
}
