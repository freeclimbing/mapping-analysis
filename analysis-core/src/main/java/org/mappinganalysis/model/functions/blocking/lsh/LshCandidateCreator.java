package org.mappinganalysis.model.functions.blocking.lsh;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.lsh.structure.*;
import org.mappinganalysis.model.functions.blocking.lsh.trigrams.TrigramBasedLinkageTupleCreator;
import org.mappinganalysis.model.functions.blocking.lsh.utils.BitFrequencyCounter;
import org.mappinganalysis.model.functions.blocking.lsh.utils.BloomFilterLshBlocker;

/**
 * Based on LSH blocking, create candidates for the following match process.
 */
public class LshCandidateCreator
    implements CustomUnaryOperation<Vertex<Long, ObjectMap>, Tuple2<Long, Long>> {
  private DataSet<Vertex<Long, ObjectMap>> vertices;
  private boolean isIdfOptimizeEnabled;

  public LshCandidateCreator(boolean isIdfOptimizeEnabled) {
    this.isIdfOptimizeEnabled = isIdfOptimizeEnabled;
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> inputData) {
    this.vertices = inputData;
  }

  @Override
  public DataSet<Tuple2<Long, Long>> createResult() {
    DataSet<LinkageTuple> verticesWithTrigramBitSet = vertices
        .runOperation(new TrigramBasedLinkageTupleCreator(isIdfOptimizeEnabled));

    int valueRangeLsh = 3200;
    final Integer[][] lshKeyPositions = HashFamilyGroup.selectRandomPositions(
        55,
        75,
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
// env.fromCollection(Collections.singletonList(Integer.MIN_VALUE));

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