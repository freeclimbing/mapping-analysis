package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.util.functions.LeftMinusRightSideJoinFunction;

/**
 * Merge step function logic.
 */
public class MergeStepFunction {
  private static final Logger LOG = Logger.getLogger(MergeStepFunction.class);
  private DataDomain domain;
  private DataSet<MergeGeoTriplet> workset;
  private SimilarityComputation<MergeGeoTriplet, MergeGeoTriplet> similarityComputation;
  private int sourcesCount;
  private DataSet<MergeGeoTuple> delta;

  public MergeStepFunction(DataSet<MergeGeoTriplet> workset,
                           SimilarityComputation<MergeGeoTriplet, MergeGeoTriplet> similarityComputation,
                           int sourcesCount,
                           DataDomain domain) {
    this.workset = workset;
    this.similarityComputation = similarityComputation;
    this.sourcesCount = sourcesCount;
//    this.clazz = clazz;
    this.domain = domain;

    this.compute(); // TODO REMOVE
  }

  public void compute() {
    DataSet<MergeGeoTriplet> maxTriplets = getIterationMaxTriplets(workset);
//    if (domain == DataDomain.GEOGRAPHY) {
      delta = maxTriplets.flatMap(new MergeGeoMapFunction());
//    } else {
//      delta = maxTriplets.flatMap(new MergeMusicMapFunction<>()); // TODO
//    }

    // remove max triplets from workset, they are getting merged anyway
    workset = workset.leftOuterJoin(maxTriplets)
        .where(0,1)
        .equalTo(0,1)
        .with(new LeftMinusRightSideJoinFunction<>());

    DataSet<Tuple2<Long, Long>> transitions = maxTriplets
        .flatMap(new TransitionElementsFlatMapFunction());

    workset = workset
        .runOperation(new NonChangedWorksetPartOperation<>(transitions))
        .union(workset
            .runOperation(new ChangesOperation(delta, transitions, domain))
            .runOperation(new ComputePrepareOperation(domain, sourcesCount))
            .runOperation(similarityComputation));
  }

  public DataSet<MergeGeoTriplet> getWorkset() {
    return workset;
  }

  public DataSet<MergeGeoTuple> getDelta() {
    return delta;
  }

  /**
   * In each iteration, get the highest triplet similarity for each blocking key. If
   * more than one triplet has highest similarity, take lowest entity id.
   * @return only maximal similatrity riplet for each blocking key
   */
  private static DataSet<MergeGeoTriplet> getIterationMaxTriplets(DataSet<MergeGeoTriplet> workset) {
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


  private static class TransitionElementsFlatMapFunction
      implements FlatMapFunction<MergeGeoTriplet, Tuple2<Long, Long>> {
    @Override
    public void flatMap(
        MergeGeoTriplet triplet,
        Collector<Tuple2<Long, Long>> out) throws Exception {
      Long min = triplet.getSrcId() < triplet.getTrgId()
          ? triplet.getSrcId() : triplet.getTrgId();
//            LOG.info(triplet.getSrcId() + " " + triplet.getTrgId() + " " + min);
      out.collect(new Tuple2<>(triplet.getSrcId(), min));
      out.collect(new Tuple2<>(triplet.getTrgId(), min));
    }
  }
}
