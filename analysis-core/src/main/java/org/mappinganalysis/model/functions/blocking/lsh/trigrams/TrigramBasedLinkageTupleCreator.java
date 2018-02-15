package org.mappinganalysis.model.functions.blocking.lsh.trigrams;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.mappinganalysis.model.functions.blocking.lsh.structure.LinkageTuple;

/**
 * Based on trigrams per vertex create a trigram to long value dictionary.
 * Dictionary used to create LinkageTuple objects containing BloomFilter
 * with the BitSets (reflecting the trigrams).
 */
public class TrigramBasedLinkageTupleCreator
    implements CustomUnaryOperation<Tuple2<Long, String>, LinkageTuple> {
  private boolean isIdfOptimizeEnabled;
  private DataSet<Tuple2<Long, String>> tuples;

  public TrigramBasedLinkageTupleCreator(boolean isIdfOptimizeEnabled) {
    this.isIdfOptimizeEnabled = isIdfOptimizeEnabled;
  }

  @Override
  public void setInput(DataSet<Tuple2<Long, String>> inputData) {
    this.tuples = inputData;
  }

  @Override
  public DataSet<LinkageTuple> createResult() {

    return tuples
        .runOperation(new TrigramsPerVertexCreatorWithIdfOptimization(isIdfOptimizeEnabled))
        .runOperation(new LongValueTrigramDictionaryCreator())
        .groupBy(0)
        .reduceGroup(new TrigramPositionsToBitSetReducer());
  }

}
