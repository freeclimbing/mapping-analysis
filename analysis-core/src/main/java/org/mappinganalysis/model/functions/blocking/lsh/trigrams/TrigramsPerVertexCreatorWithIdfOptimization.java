package org.mappinganalysis.model.functions.blocking.lsh.trigrams;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.functions.CharSet;
import org.mappinganalysis.model.functions.blocking.lsh.utils.VertexIdfWeightsFunction;
import org.mappinganalysis.model.functions.blocking.tfidf.TfIdfComputer;
import org.mappinganalysis.util.Utils;

import static org.apache.lucene.analysis.cn.ChineseFilter.STOP_WORDS;

/**
 * Vertex labels are mapped to a CharSet containing all (label) trigrams. Optionally,
 * a fixed percentage of frequent words from the whole text corpus are removed in a
 * preliminary step.
 */
public class TrigramsPerVertexCreatorWithIdfOptimization
    implements CustomUnaryOperation<Tuple2<Long, String>, Tuple2<Long, CharSet>> {
  private static final Logger LOG = Logger.getLogger(TrigramsPerVertexCreatorWithIdfOptimization.class);

  private DataSet<Tuple2<Long, String>> idLabelTuples;
  private boolean isIdfOptimizeEnabled;

  public TrigramsPerVertexCreatorWithIdfOptimization(boolean isIdfOptimizeEnabled) {
    this.isIdfOptimizeEnabled = isIdfOptimizeEnabled;
  }

  @Override
  public void setInput(DataSet<Tuple2<Long, String>> inputData) {
    this.idLabelTuples = inputData;
  }

  @Override
  public DataSet<Tuple2<Long, CharSet>> createResult() {
    if (isIdfOptimizeEnabled) {
      DataSet<Tuple2<String, Double>> idfValues = idLabelTuples
          .runOperation(new TfIdfComputer(STOP_WORDS));

      DataSet<String> stopWords = idfValues.sortPartition(1, Order.ASCENDING)
          .setParallelism(1)
          .first(20)
          .map(x -> {
//            LOG.info("exclude: " + x.toString());
            return x.f0;
          })
          .returns(new TypeHint<String>() {});

      idLabelTuples = idLabelTuples
          .map(new VertexIdfWeightsFunction())
          .withBroadcastSet(idfValues, "idf")
          .withBroadcastSet(stopWords, "stop");
    }

    return idLabelTuples
        .map(tuple -> new Tuple2<>(
            tuple.f0,
            Utils.getUnsortedTrigrams(tuple.f1)))
        .returns(new TypeHint<Tuple2<Long, CharSet>>() {});
  }
}
