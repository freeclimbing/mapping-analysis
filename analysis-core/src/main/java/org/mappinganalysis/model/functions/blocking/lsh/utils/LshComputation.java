package org.mappinganalysis.model.functions.blocking.lsh.utils;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.functions.blocking.lsh.BlockReducer;
import org.mappinganalysis.model.functions.blocking.lsh.structure.*;

public class LshComputation implements CustomUnaryOperation<LinkageTuple, Tuple2<Long, Long>> {
  private static final Logger LOG = Logger.getLogger(LshComputation.class);
  private DataSet<LinkageTuple> verticesWithTrigramBitSet;
  private final int valueRangeLsh;
  private final int numberOfFamilies;
  private final int numberOfHashesPerFamily;

  public LshComputation(int valueRangeLsh, int numberOfFamilies, int numberOfHashesPerFamily) {
    this.valueRangeLsh = valueRangeLsh;
    this.numberOfFamilies = numberOfFamilies;
    this.numberOfHashesPerFamily = numberOfHashesPerFamily;
  }

  @Override
  public void setInput(DataSet<LinkageTuple> inputData) {
    this.verticesWithTrigramBitSet = inputData;
  }

  @Override
  public DataSet<Tuple2<Long, Long>> createResult() {
//    int valueRangeLsh = 3200;
//    55,
//        75,
    final Integer[][] lshKeyPositions = HashFamilyGroup.selectRandomPositions(
        numberOfFamilies,
        numberOfHashesPerFamily,
        valueRangeLsh);

    // TODO get frequent bit positions, min value is default setting
    final BitFrequencyCounter bfc = new BitFrequencyCounter(
        valueRangeLsh,  10);

    DataSet<Integer> nonFrequentBitPositions = null;
    try {
      nonFrequentBitPositions = bfc
          .getNonFrequentBitPositions(verticesWithTrigramBitSet);
    } catch (Exception e) {
      e.printStackTrace();
    }

    assert nonFrequentBitPositions != null;
    DataSet<Tuple2<LshKey, LinkageTupleWithLshKeys>> keyBloomFilterPairs =
        verticesWithTrigramBitSet
            .flatMap(new BloomFilterLshBlocker(lshKeyPositions))
            .withBroadcastSet(nonFrequentBitPositions, "infrequentBits");

    DataSet<Tuple2<LshKey, CandidateLinkageTupleWithLshKeys>> keysWithCandidatePair =
        keyBloomFilterPairs
            .groupBy("f0.id", "f0.bits")
            .reduceGroup(new BlockReducer());

    return keysWithCandidatePair
        .map(pair -> {
          Long left = pair.f1.getCandidateOne().getId();
          Long right = pair.f1.getCandidateTwo().getId();

          if (left < right) {
            return new Tuple2<>(left, right);
          } else {
            return new Tuple2<>(right, left);
          }
        })
        .returns(new TypeHint<Tuple2<Long, Long>>() {})
        .distinct();
  }
}
