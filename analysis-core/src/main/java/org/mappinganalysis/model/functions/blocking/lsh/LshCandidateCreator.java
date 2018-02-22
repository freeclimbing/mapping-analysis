package org.mappinganalysis.model.functions.blocking.lsh;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.functions.blocking.lsh.trigrams.TrigramBasedLinkageTupleCreator;
import org.mappinganalysis.model.functions.blocking.lsh.utils.LshComputation;

/**
 * Based on LSH blocking, create candidates for the following match process.
 */
public class LshCandidateCreator
    implements CustomUnaryOperation<Tuple2<Long, String>, Tuple2<Long, Long>> {
  private static final Logger LOG = Logger.getLogger(LshCandidateCreator.class);

  private DataSet<Tuple2<Long, String>> tuples;
  private boolean isIdfOptimizeEnabled;
  private int valueRangeLsh;
  private int numberOfFamilies;
  private int numberOfHashesPerFamily;

  public LshCandidateCreator(boolean isIdfOptimizeEnabled) {
    this.isIdfOptimizeEnabled = isIdfOptimizeEnabled;
    // good in tests: 3200 55 75
    valueRangeLsh = 3200;
    numberOfFamilies = 15;
    numberOfHashesPerFamily = 15;
  }

  public LshCandidateCreator(boolean isIdfOptimizeEnabled,
                             int valueRangeLsh,
                             int numberOfFamilies,
                             int numberOfHashesPerFamily) {
    this.isIdfOptimizeEnabled = isIdfOptimizeEnabled;
    this.valueRangeLsh = valueRangeLsh;
    this.numberOfFamilies = numberOfFamilies;
    this.numberOfHashesPerFamily = numberOfHashesPerFamily;
  }

  @Override
  public void setInput(DataSet<Tuple2<Long, String>> inputData) {
    this.tuples = inputData;
  }

  @Override
  public DataSet<Tuple2<Long, Long>> createResult() {
    return tuples
        .runOperation(new TrigramBasedLinkageTupleCreator(isIdfOptimizeEnabled))
        .runOperation(new LshComputation(
            valueRangeLsh,
            numberOfFamilies, // 55
            numberOfHashesPerFamily)); // 75
  }
}