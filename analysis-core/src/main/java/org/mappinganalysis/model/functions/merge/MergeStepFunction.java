package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.util.functions.LeftMinusRightSideJoinFunction;

/**
 * Merge step function logic.
 */
public class MergeStepFunction<T> {
  private static final Logger LOG = Logger.getLogger(MergeStepFunction.class);
//  private final Class<T> clazz;
  private DataDomain domain;
  private DataSet<MergeTriplet<T>> workset;
  private SimilarityComputation<MergeTriplet<T>, MergeTriplet<T>> similarityComputation;
  private int sourcesCount;
  private DataSet<T> delta;

  public MergeStepFunction(DataSet<MergeTriplet<T>> workset,
                           SimilarityComputation<MergeTriplet<T>, MergeTriplet<T>> similarityComputation,
                           int sourcesCount,
                           Class<T> clazz,
                           DataDomain domain) {
    this.workset = workset;
    this.similarityComputation = similarityComputation;
    this.sourcesCount = sourcesCount;
//    this.clazz = clazz;
    this.domain = domain;

    this.compute(); // TODO REMOVE
  }

  public void compute() {
    DataSet<MergeTriplet<T>> maxTriplets = getIterationMaxTriplets(workset);
    if (domain == DataDomain.GEOGRAPHY) {
      delta = maxTriplets.flatMap(new MergeGeoMapFunction<>());
    } else {
      delta = maxTriplets.flatMap(new MergeMusicMapFunction<>()); // TODO
    }

    // remove max triplets from workset, they are getting merged anyway
    workset = workset.leftOuterJoin(maxTriplets)
        .where(0,1)
        .equalTo(0,1)
        .with(new LeftMinusRightSideJoinFunction<>());

    DataSet<Tuple2<Long, Long>> transitions = maxTriplets
        .flatMap(new TransitionElementsFlatMapFunction<>());

    workset = workset
        .runOperation(new NonChangedWorksetPartOperation<>(transitions))
        .union(workset
            .runOperation(new ChangesOperation<>(delta, transitions, domain))
            .runOperation(new ComputePrepareOperation<>(domain, sourcesCount))
            .runOperation(similarityComputation));
  }

  public DataSet<MergeTriplet<T>> getWorkset() {
    return workset;
  }

  public DataSet<T> getDelta() {
    return delta;
  }

  /**
   * In each iteration, get the highest triplet similarity for each blocking key. If
   * more than one triplet has highest similarity, take lowest entity id.
   * @return only maximal similatrity riplet for each blocking key
   */
  private static <T> DataSet<MergeTriplet<T>> getIterationMaxTriplets(DataSet<MergeTriplet<T>> workset) {
//    DataSet<Tuple2<Double, String>> maxFilter =
    return workset
        .groupBy(5)
        .reduce(new MaxSimMinIdReducer<>());
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


  private static class TransitionElementsFlatMapFunction<T>
      implements FlatMapFunction<MergeTriplet<T>, Tuple2<Long, Long>> {
    @Override
    public void flatMap(
        MergeTriplet<T> triplet,
        Collector<Tuple2<Long, Long>> out) throws Exception {
      Long min = triplet.getSrcId() < triplet.getTrgId()
          ? triplet.getSrcId() : triplet.getTrgId();
//            LOG.info(triplet.getSrcId() + " " + triplet.getTrgId() + " " + min);
      out.collect(new Tuple2<>(triplet.getSrcId(), min));
      out.collect(new Tuple2<>(triplet.getTrgId(), min));
    }
  }
}
