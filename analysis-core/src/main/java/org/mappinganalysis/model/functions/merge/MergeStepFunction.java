package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.functions.LeftMinusRightSideJoinFunction;

/**
 * Merge step function logic.
 */
public class MergeStepFunction {
  private static final Logger LOG = Logger.getLogger(MergeStepFunction.class);
  private DataSet<MergeTriplet> workset;
  private SimilarityComputation<MergeTriplet, MergeTriplet> similarityComputation;
  private int sourcesCount;
  private DataSet<MergeTuple> delta;

  public MergeStepFunction(DataSet<MergeTriplet> workset,
                           SimilarityComputation<MergeTriplet, MergeTriplet> similarityComputation,
                           int sourcesCount) {
    this.workset = workset;
    this.similarityComputation = similarityComputation;
    this.sourcesCount = sourcesCount;

    this.compute();
  }

  public void compute() {
    DataSet<MergeTriplet> maxTriplets = getIterationMaxTriplets(workset);
    delta = maxTriplets.flatMap(new MergeMapFunction());

    // remove max triplets from workset, they are getting merged anyway
    workset = workset.leftOuterJoin(maxTriplets)
        .where(0,1)
        .equalTo(0,1)
        .with(new LeftMinusRightSideJoinFunction<>());

    DataSet<Tuple2<Long, Long>> transitions = getTransitionElements(maxTriplets);

    DataSet<MergeTriplet> changes = getChangedTriplets(
        workset,
        delta,
        transitions);
    DataSet<MergeTriplet> changesWithNewSimilarities = computeSimilarities(
        sourcesCount,
        similarityComputation,
        changes);

    // throw out everything with transition elements
    DataSet<MergeTriplet> nonChangedWorksetPart = workset.leftOuterJoin(transitions)
        .where(0)
        .equalTo(0)
        .with(new LeftMinusRightSideJoinFunction<>())
        .leftOuterJoin(transitions)
        .where(1)
        .equalTo(0)
        .with(new LeftMinusRightSideJoinFunction<>());

    workset = nonChangedWorksetPart.union(changesWithNewSimilarities);
  }

  public DataSet<MergeTriplet> getWorkset() {
    return workset;
  }

  public DataSet<MergeTuple> getDelta() {
    return delta;
  }

  /**
   * Compute similarities within an iteration step and check side conditions
   * @param sourcesCount count of different data sources
   * @param similarityComputation type of similarity computation
   * @param changes input values
   * @return changed input values
   */
  private static DataSet<MergeTriplet> computeSimilarities(
      int sourcesCount,
      SimilarityComputation<MergeTriplet, MergeTriplet> similarityComputation,
      DataSet<MergeTriplet> changes) {
    return changes
        .map(triplet -> {
          if (triplet.getSrcId() > triplet.getTrgId()) {
            MergeTuple tmp = triplet.getSrcTuple();
            triplet.setSrcId(triplet.getTrgId());
            triplet.setSrcTuple(triplet.getTrgTuple());
            triplet.setTrgId(tmp.getId());
            triplet.setTrgTuple(tmp);
          }
          return triplet;
        })
        .distinct(0,1) // is needed
        .filter(triplet -> {
//          LOG.info("CHANGED AND GETS NEW SIM " + triplet.toString());
          boolean hasSourceOverlap = AbstractionUtils.hasOverlap(
              triplet.getSrcTuple().getIntSources(),
              triplet.getTrgTuple().getIntSources());
          boolean isSourceCountChecked = sourcesCount >=
              (AbstractionUtils.getSourceCount(triplet.getSrcTuple().getIntSources())
                  + AbstractionUtils.getSourceCount(triplet.getTrgTuple().getIntSources()));
          return !hasSourceOverlap && isSourceCountChecked;
        })
        .runOperation(similarityComputation);
  }

  private static FlatMapOperator<MergeTriplet, Tuple2<Long, Long>> getTransitionElements(
      DataSet<MergeTriplet> maxTriplets) {
    return maxTriplets
        .flatMap(new FlatMapFunction<MergeTriplet, Tuple2<Long, Long>>() {
          @Override
          public void flatMap(MergeTriplet triplet, Collector<Tuple2<Long, Long>> out)
              throws Exception {
            Long min = triplet.getSrcId() < triplet.getTrgId()
                ? triplet.getSrcId() : triplet.getTrgId();
//            LOG.info(triplet.getSrcId() + " " + triplet.getTrgId() + " " + min);
            out.collect(new Tuple2<>(triplet.getSrcId(), min));
            out.collect(new Tuple2<>(triplet.getTrgId(), min));
          }
        });
  }

  private static DataSet<MergeTriplet> getChangedTriplets(
      DataSet<MergeTriplet> workset,
      DataSet<MergeTuple> delta,
      DataSet<Tuple2<Long, Long>> transitions) {
    DataSet<MergeTriplet> leftChanges = workset.join(transitions)
        .where(0)
        .equalTo(0)
        .with((triplet, transition) -> {
          triplet.setSrcId(transition.f1);
          return triplet;
        })
        .returns(new TypeHint<MergeTriplet>() {})
        .distinct(0,1)
        .join(delta.filter(MergeTuple::isActive))
        .where(0)
        .equalTo(0)
        .with((triplet, newTuple) -> {
          triplet.setSrcTuple(newTuple);
//          LOG.info("LEFT DELTA JOIN " + triplet.toString());
          return triplet;
        })
        .returns(new TypeHint<MergeTriplet>() {});

    return workset.join(transitions)
        .where(1)
        .equalTo(0)
        .with((triplet, transition) -> {
          triplet.setTrgId(transition.f1);
          return triplet;
        })
        .returns(new TypeHint<MergeTriplet>() {})
        .distinct(0,1)
        .join(delta.filter(MergeTuple::isActive))
        .where(1)
        .equalTo(0)
        .with((triplet, newTuple) -> {
          triplet.setTrgTuple(newTuple);
//          LOG.info("RIGHT DELTA JOIN " + triplet.toString());
          return triplet;
        })
        .returns(new TypeHint<MergeTriplet>() {})
        .union(leftChanges);
  }

  /**
   * In each iteration, get the highest triplet similarity for each blocking key. If
   * more than one triplet has highest similarity, take lowest entity id.
   * @return only maximal similarity triplet for each blocking key
   */
  private static DataSet<MergeTriplet> getIterationMaxTriplets(DataSet<MergeTriplet> workset) {
//    DataSet<Tuple2<Double, String>> maxFilter =
    return workset
        .groupBy(5)
        .reduce(new MaxSimMinIdReducer());
//        .max(4)
//        .map(triplet -> {
//          LOG.info("##############  " + triplet.toString());
//          return new Tuple2<>(triplet.getSimilarity(),
//              triplet.getSrcTuple().getBlockingLabel());
//        })
//        .returns(new TypeHint<Tuple2<Double, String>>() {})
//        .distinct(0,1);

//    return workset.join(maxFilter)
//        .where(4)
//        .equalTo(0)
//        .with((triplet, tuple) ->  triplet)
//        .returns(new TypeHint<MergeTriplet>() {})
//        .distinct(0,1)
//        .groupBy(5)
//        .maxBy(4)
//        .returns(new TypeHint<MergeTriplet>() {});
  }


}
