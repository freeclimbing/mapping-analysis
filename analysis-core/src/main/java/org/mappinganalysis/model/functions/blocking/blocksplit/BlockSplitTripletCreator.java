package org.mappinganalysis.model.functions.blocking.blocksplit;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.functions.stats.StatisticsCountElementsRichMapFunction;
import org.mappinganalysis.util.Constants;

public class BlockSplitTripletCreator
    implements CustomUnaryOperation<MergeMusicTuple, MergeMusicTriplet> {
  private static final Logger LOG = Logger.getLogger(BlockSplitTripletCreator.class);
  private DataSet<MergeMusicTuple> inputTuples;
  private DataDomain dataDomain;
  private String newSource = Constants.EMPTY_STRING;

  /**
   * Source based addition constructor
   */
  public BlockSplitTripletCreator(DataDomain dataDomain, String newSource) {
    this.dataDomain = dataDomain;
    this.newSource = newSource;
  }

  /**
   * Default constructor
   */
  public BlockSplitTripletCreator() {
  }

  @Override
  public void setInput(DataSet<MergeMusicTuple> inputData) {
    inputTuples = inputData;
  }

  /**
   * partition id == pid
   * blocking key == bkey
   */
  @Override
  public DataSet<MergeMusicTriplet> createResult() {
    DataSet<Tuple3<Long, String, Integer>> bkeyPid = inputTuples
        .map(new TuplePartitionIdMapper());

    UnsortedGrouping<Tuple3<String, Integer, Long>> bKeyPidCountGroupingByBkey = bkeyPid
        .groupBy(2, 1)
        .combineGroup(new EnumeratePartitionEntities())
        .groupBy(0);

    DataSet<Tuple4<Long, String, Integer, Long>> tupleKeyPartIdStartPoint
        = bKeyPidCountGroupingByBkey
        .sortGroup(1, Order.ASCENDING)
        .reduceGroup(new ComputePartitionEnumerationStartPoint()) /* Generate vertex index */
        .join(bkeyPid)
        .where(0, 1).equalTo(1, 2)
        .with((JoinFunction<Tuple3<String, Integer, Long>,
            Tuple3<Long, String, Integer>,
            Tuple4<Long, String, Integer, Long>>) (startPoint, tuple)
            -> Tuple4.of(tuple.f0, tuple.f1, tuple.f2, startPoint.f2))
        .returns(new TypeHint<Tuple4<Long, String, Integer, Long>>() {});


    DataSet<Tuple3<Long, String, Long>> tupleBkeyTupleId = tupleKeyPartIdStartPoint
        .groupBy(1, 2)
        .reduceGroup(new AssignVertexIndex());

    /* Prepare key (block) information (size, index, no. of pairs in prev blocks, no. of all pairs) */
    DataSet <Tuple3<String, Long, Long>> bkeySizeIndex = DataSetUtils
        .zipWithUniqueId(bKeyPidCountGroupingByBkey
            .reduceGroup(new ComputeBlockSize()))
        .map(new AssignBlockIndex());

    /* Provide information (BlockIndex, BlockSize, PrevBlockPairs, allPairs) for each vertex         */
    DataSet<Tuple6<Long, String, Long, Long, Long, Long>> tupleBkeyVindexBlockSizePrevBlockPairsAllPairs
        = tupleBkeyTupleId
        .join(bkeySizeIndex
            .reduceGroup(new ComputePrevBlocksPairNoAllPairs()))
        .where(1).equalTo(0)
        .with(new ConcatAllInfoToVertex());

    /* Load Balancing */
    DataSet<Tuple5<Long, String, Long, Boolean, Integer>> tupleBkeyVindexIsLastReducerId
        = tupleBkeyVindexBlockSizePrevBlockPairsAllPairs
        .flatMap(new ReplicateAndAssignReducerId())
        .partitionCustom(new PartitionVertices(), 4);

    /* Make pairs */
    DataSet<Tuple2<Long, Long>> tripletCandidates
        = tupleBkeyVindexIsLastReducerId
        .groupBy(1)
        .sortGroup(2, Order.ASCENDING)
        .combineGroup(new CreatePairedVertices())
        .map(new StatisticsCountElementsRichMapFunction<>(
            Constants.BLOCK_SPLIT_TRIPLET_ACCUMULATOR));

    return tripletCandidates.join(inputTuples)
        .where(0).equalTo(0)
        .with(new JoinFunction<Tuple2<Long,Long>, MergeMusicTuple,
            Tuple2<MergeMusicTuple, Long>>() {
          @Override
          public Tuple2<MergeMusicTuple, Long> join(
              Tuple2<Long, Long> left, MergeMusicTuple right) throws Exception {
            return new Tuple2<>(right, left.f1);
          }
        })
        .join(inputTuples)
        .where(1).equalTo(0)
        .with(new TripletCandidateRestrictor(dataDomain, newSource));
  }
}