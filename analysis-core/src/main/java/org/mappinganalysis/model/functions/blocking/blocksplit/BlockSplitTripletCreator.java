package org.mappinganalysis.model.functions.blocking.blocksplit;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.util.Constants;

public class BlockSplitTripletCreator
    implements CustomUnaryOperation<MergeMusicTuple, MergeMusicTriplet> {
  private static final Logger LOG = Logger.getLogger(BlockSplitTripletCreator.class);
  private DataSet<MergeMusicTuple> inputTuples;
  private DataDomain dataDomain;
  private String newSource = Constants.EMPTY_STRING;

  /**
   * Source based addition constructor
   * @param dataDomain
   * @param newSource
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
    DataSet<Tuple3<MergeMusicTuple, String, Integer>> bkeyPid = inputTuples
        .map(new TuplePartitionIdMapper());

    UnsortedGrouping<Tuple3<String, Integer, Long>> bKeyPidCountGroupingByBkey = bkeyPid
        .groupBy(2,1)
        .combineGroup(new EnumeratePartitionEntities())
        .groupBy(0);

    DataSet<Tuple4<MergeMusicTuple, String, Integer, Long>> tupleKeyPartIdStartPoint
        = bKeyPidCountGroupingByBkey
        .sortGroup(1, Order.ASCENDING)
        .reduceGroup(new ComputePartitionEnumerationStartPoint()) /* Generate vertex index */
        .join(bkeyPid)
        .where(0,1).equalTo(1,2)
        .with((JoinFunction<Tuple3<String, Integer, Long>,
            Tuple3<MergeMusicTuple, String, Integer>,
            Tuple4<MergeMusicTuple, String, Integer, Long>>) (startPoint, tuple)
            -> Tuple4.of(tuple.f0, tuple.f1, tuple.f2, startPoint.f2))
        .returns(new TypeHint<Tuple4<MergeMusicTuple, String, Integer, Long>>() {});


    DataSet<Tuple3<MergeMusicTuple, String, Long>> tupleBkeyTupleId = tupleKeyPartIdStartPoint
        .groupBy(1,2)
        .reduceGroup(new AssignVertexIndex());

    /* Prepare key (block) information (size, index, no. of pairs in prev blocks, no. of all pairs) */
    DataSet <Tuple3<String, Long, Long>> bkeySizeIndex = DataSetUtils
        .zipWithUniqueId(bKeyPidCountGroupingByBkey
            .reduceGroup(new ComputeBlockSize()))
        .map(new AssignBlockIndex());

    /* Provide information (BlockIndex, BlockSize, PrevBlockPairs, allPairs) for each vertex         */
    DataSet<Tuple6<MergeMusicTuple, String, Long, Long, Long, Long>> tupleBkeyVindexBlockSizePrevBlockPairsAllPairs
        = tupleBkeyTupleId
        .join(bkeySizeIndex
            .reduceGroup(new ComputePrevBlocksPairNoAllPairs()))
        .where(1).equalTo(0)
        .with(new ConcatAllInfoToVertex());

    /* Load Balancing */
    DataSet<Tuple5<MergeMusicTuple, String, Long, Boolean, Integer>> tupleBkeyVindexIsLastReducerId
        = tupleBkeyVindexBlockSizePrevBlockPairsAllPairs
        .flatMap(new ReplicateAndAssignReducerId())
        .partitionCustom(new PartitionVertices(), 4)
        .map(x -> {
//          if (x.f0.f0 == 16889L || x.f0.f0 == 9919L)
//          LOG.info("PartitionVertices: " + x.toString());
          return x;
        })
        .returns(new TypeHint<Tuple5<MergeMusicTuple, String, Long, Boolean, Integer>>() {});

    /* Make pairs */
    return tupleBkeyVindexIsLastReducerId
        .groupBy(1)
        .sortGroup(2, Order.ASCENDING)
        .combineGroup(new CreatePairedVertices(dataDomain, newSource))
        .map(x -> {
//          LOG.info("CreatePairedVertices: " + x.toString());
          return x;
        })
        .returns(new TypeHint<MergeMusicTriplet>() {});
  }
}
