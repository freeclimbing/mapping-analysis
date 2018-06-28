package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Triplet;
import org.mappinganalysis.model.ObjectMap;

/**
 * For a set of triplets with similarities (from a bipartite graph based on
 * data sources), select all links where the link is the maximum from right
 * as well as left side.
 */
class MaxBothSelection
    implements CustomUnaryOperation<
    Triplet<Long, ObjectMap, ObjectMap>,
    Triplet<Long, ObjectMap, ObjectMap>> {
  private DataSet<Triplet<Long, ObjectMap, ObjectMap>> input;

  @Override
  public void setInput(DataSet<Triplet<Long, ObjectMap, ObjectMap>> dataSet) {
    input = dataSet;
  }

  @Override
  public DataSet<Triplet<Long, ObjectMap, ObjectMap>> createResult() {
    DataSet<Tuple3<Long, Long, Double>> inputTuples = input
        .map(triplet -> new Tuple3<>(
            triplet.f0,
            triplet.f1,
            triplet.f4.getEdgeSimilarity()))
        .returns(new TypeHint<Tuple3<Long, Long, Double>>() {
    });

    // considered: only one entity with max similarity available
//    ReduceOperator<Tuple3<Long, Long, Double>> left = inputTuples.groupBy(0).maxBy(2);
//    ReduceOperator<Tuple3<Long, Long, Double>> right = inputTuples.groupBy(1).maxBy(2);
//
//    DataSet<Tuple3<Long, Long, Double>> resultTuple = left
//        .join(right)
//        .where(0, 1).equalTo(0, 1)
//        .with((l, r) -> l)
//        .returns(new TypeHint<Tuple3<Long, Long, Double>>() {
//    });

    // realistic: multiple entities may have same similarity, decision currently based on min id
    DataSet<Tuple3<Long, Long, Double>> leftSide = inputTuples
        .groupBy(0)
        .max(2)
        .join(inputTuples)
        .where(0, 2).equalTo(0, 2)
        .with((left, right) -> right)
        .returns(new TypeHint<Tuple3<Long, Long, Double>>() {
        })
        .groupBy(0)
        .min(1);

    DataSet<Tuple3<Long, Long, Double>> rightSide = inputTuples
        .groupBy(1)
        .max(2)
        .join(inputTuples)
        .where(1, 2).equalTo(1, 2)
        .with((left, right) -> right)
        .returns(new TypeHint<Tuple3<Long, Long, Double>>() {
        })
        .groupBy(1)
        .min(0);

    DataSet<Tuple3<Long, Long, Double>> resultTuple = leftSide
        .join(rightSide)
        .where(0, 1).equalTo(0, 1)
        .with((left, right) -> left)
        .returns(new TypeHint<Tuple3<Long, Long, Double>>() {
    });

    return input.join(resultTuple)
        .where(0,1)
        .equalTo(0,1)
        .with((inputTriplet, tuple) -> inputTriplet)
        .returns(new TypeHint<Triplet<Long, ObjectMap, ObjectMap>>() {});

//    return input.leftOuterJoin(resultTuple)
//        .where(0,1)
//        .equalTo(0,1)
//        .with(new MaxBothResultExtractJoinFunction());
  }
}
