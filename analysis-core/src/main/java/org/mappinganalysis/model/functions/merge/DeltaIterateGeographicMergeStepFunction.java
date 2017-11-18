package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.util.functions.LeftMinusRightSideJoinFunction;

/**
 * Merge step function logic.
 */
public class DeltaIterateGeographicMergeStepFunction {
  private static final Logger LOG = Logger.getLogger(DeltaIterateGeographicMergeStepFunction.class);
  private DataDomain domain;
  private DataSet<MergeGeoTriplet> workset;
  private SimilarityComputation<MergeGeoTriplet, MergeGeoTriplet> similarityComputation;
  private int sourcesCount;
  private DataSet<MergeGeoTuple> delta;

  public DeltaIterateGeographicMergeStepFunction(DataSet<MergeGeoTriplet> workset,
                                                 SimilarityComputation<MergeGeoTriplet, MergeGeoTriplet> similarityComputation,
                                                 int sourcesCount,
                                                 DataDomain domain) {
    this.workset = workset;
    this.similarityComputation = similarityComputation;
    this.sourcesCount = sourcesCount;
    this.domain = domain;

    this.compute(); // TODO REMOVE
  }

  public void compute() {
    DataSet<MergeGeoTriplet> maxTriplets = getIterationMaxTriplets(workset);
    delta = maxTriplets.flatMap(new DualMergeGeographyMapper());

    // remove max triplets from workset, they are getting merged anyway
    workset = workset.leftOuterJoin(maxTriplets)
        .where(0,1)
        .equalTo(0,1)
        .with(new LeftMinusRightSideJoinFunction<>());

    DataSet<Tuple2<Long, Long>> transitions = maxTriplets
        .flatMap(new TransitionElementsFlatMapFunction<>(domain));

    workset = workset
        .runOperation(new WorksetNewClusterRemoveOperation<>(transitions))
        .union(workset
            .runOperation(new ChangesGeoOperation(delta, transitions, domain))
            .runOperation(new ComputePrepareGeoOperation(domain, sourcesCount))
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
        .reduce(new MaxSimMinIdGeoReducer());
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
