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
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;

public class BlockSplitTupleCreator
    implements CustomUnaryOperation<MergeMusicTuple, MergeMusicTriplet> {
  private static final Logger LOG = Logger.getLogger(BlockSplitTupleCreator.class);
  private DataSet<MergeMusicTuple> inputTuples;

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

    DataSet<Tuple3<String, Integer, Long>> bkeyPidCount = bkeyPid
        .groupBy(2,1)
        .combineGroup(new EnumeratePartitionEntities())
        .map(x -> {
//          LOG.info("Partitions should be equal: " + x.toString());
          return x;
        })
        .returns(new TypeHint<Tuple3<String, Integer, Long>>() {});

    UnsortedGrouping<Tuple3<String, Integer, Long>> bKeyPidCountGroupingByBkey = bkeyPidCount
        .groupBy(0);

    /* Generate vertex index */
    DataSet<Tuple3<String, Integer, Long>> bkeyPidStartPoint = bKeyPidCountGroupingByBkey
        .sortGroup(1, Order.ASCENDING)
        .reduceGroup(new ComputePartitionEnumerationStartPoint()) // TODO check total sum
        .map(x -> {
//          LOG.info("ComputePartitionEnumerationStartPoint: " + x.toString());
          return x;
        })
        .returns(new TypeHint<Tuple3<String, Integer, Long>>() {});

    DataSet<Tuple4<MergeMusicTuple, String, Integer, Long>> tupleKeyPartIdStartPoint = bkeyPidStartPoint
        .join(bkeyPid)
        .where(0,1).equalTo(1,2)
        .with((JoinFunction<Tuple3<String, Integer, Long>,
            Tuple3<MergeMusicTuple, String, Integer>,
            Tuple4<MergeMusicTuple, String, Integer, Long>>) (startPoint, tuple)
            -> Tuple4.of(tuple.f0, tuple.f1, tuple.f2, startPoint.f2))
        .returns(new TypeHint<Tuple4<MergeMusicTuple, String, Integer, Long>>() {})
        .map(x -> {
//          LOG.info("Join lambda: " + x.toString());
          return x;
        })
        .returns(new TypeHint<Tuple4<MergeMusicTuple, String, Integer, Long>>() {});


    DataSet<Tuple3<MergeMusicTuple, String, Long>> tupleBkeyTupleId = tupleKeyPartIdStartPoint
        .groupBy(1,2)
        .reduceGroup(new AssignVertexIndex())
        .map(x -> {
//          LOG.info("AssignVertexIndex: " + x.toString());
          return x;
        })
        .returns(new TypeHint<Tuple3<MergeMusicTuple, String, Long>>() {});

    /* Prepare key (block) information (size, index, no. of pairs in prev blocks, no. of all pairs) */
    DataSet <Tuple3<String, Long, Long>> bkeySizeIndex = DataSetUtils
        .zipWithUniqueId(bKeyPidCountGroupingByBkey
            .reduceGroup(new ComputeBlockSize()))
        .map(new AssignBlockIndex())
        .map(x -> {
//          LOG.info("AssignVertexIndex: " + x.toString());
          return x;
        })
        .returns(new TypeHint<Tuple3<String, Long, Long>>() {});

    // todo refactor
    DataSet<Tuple5<String, Long, Long, Long, Long>> bkeySizeIndexPrevBlocksPairsAllPairs = bkeySizeIndex
        .reduceGroup(new ComputePrevBlocksPairNoAllPairs())
        .map(x -> {
//          LOG.info("ComputePrevBlocksPairNoAllPairs: " + x.toString());
          return x;
        })
        .returns(new TypeHint<Tuple5<String, Long, Long, Long, Long>>() {});

    /* Provide information (BlockIndex, BlockSize, PrevBlockPairs, allPairs) for each vertex         */
    
    DataSet<Tuple6<MergeMusicTuple, String, Long, Long, Long, Long>> tupleBkeyVindexBlockSizePrevBlockPairsAllPairs
        = tupleBkeyTupleId
        .join(bkeySizeIndexPrevBlocksPairsAllPairs)
        .where(1).equalTo(0)
        .with(new ConcatAllInfoToVertex())
        .map(x -> {
//          LOG.info("ConcatAllInfoToVertex: " + x.toString());
          return x;
        })
        .returns(new TypeHint<Tuple6<MergeMusicTuple, String, Long, Long, Long, Long>>() {});

    /* Load Balancing */
    DataSet<Tuple5<MergeMusicTuple, String, Long, Boolean, Integer>> tupleBkeyVindexIsLastReducerId
        = tupleBkeyVindexBlockSizePrevBlockPairsAllPairs
        .flatMap(new ReplicateAndAssignReducerId())
        .partitionCustom(new PartitionVertices(), 4)
        .map(x -> {
//          LOG.info("PartitionVertices: " + x.toString());
          return x;
        })
        .returns(new TypeHint<Tuple5<MergeMusicTuple, String, Long, Boolean, Integer>>() {});

    /* Make pairs */
    return tupleBkeyVindexIsLastReducerId
        .groupBy(1)
        .sortGroup(2, Order.ASCENDING)
        .combineGroup(new CreatePairedVertices())
        .map(x -> {
//          LOG.info("CreatePairedVertices: " + x.toString());
          return x;
        })
        .returns(new TypeHint<MergeMusicTriplet>() {});
  }
}
