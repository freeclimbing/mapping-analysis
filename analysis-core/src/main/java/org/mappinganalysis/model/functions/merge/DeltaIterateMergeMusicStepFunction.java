package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.util.functions.LeftMinusRightSideJoinFunction;

/**
 * Flink delta iteration step function for music domain.
 */
public class DeltaIterateMergeMusicStepFunction {
  private static final Logger LOG = Logger.getLogger(DeltaIterateMergeMusicStepFunction.class);
  private DataDomain domain;
  private DataSet<MergeMusicTriplet> workset;
  private SimilarityComputation<MergeMusicTriplet, MergeMusicTriplet> similarityComputation;
  private int sourcesCount;
  private DataSet<MergeMusicTuple> delta;

  public DeltaIterateMergeMusicStepFunction(
      DataSet<MergeMusicTriplet> workset,
      SimilarityComputation<MergeMusicTriplet, MergeMusicTriplet> similarityComputation,
      int sourcesCount,
      DataDomain domain) {
    this.workset = workset;
    this.similarityComputation = similarityComputation;
    this.sourcesCount = sourcesCount;
    this.domain = domain;

    this.compute(); // TODO REMOVE
  }

  public void compute() {
    DataSet<MergeMusicTriplet> maxTriplets = getIterationMaxTriplets(workset);
    /*
      delta is the solution set which is changed over the iterations
      contains the resulting clusters per iteration
     */
    delta = maxTriplets.flatMap(new DualMergeMusicMapper());

    workset = printSuperstep(workset);

    // remove max triplets from workset, they are getting merged anyway
    // duplicate of WorksetNewClusterRemoveOperation
    workset = workset.leftOuterJoin(maxTriplets)
        .where(0,1)
        .equalTo(0,1)
        .with(new LeftMinusRightSideJoinFunction<>());

    DataSet<Tuple2<Long, Long>> transitions = maxTriplets
        .flatMap(new TransitionElementsFlatMapFunction<>(domain));

    DataSet<MergeMusicTriplet> removeTmp = workset
        .runOperation(new WorksetNewClusterRemoveOperation<>(transitions));

    workset
        = workset
        .runOperation(new ChangesMusicOperation(delta, transitions, domain))
        .runOperation(new ComputePrepareMusicOperation(domain, sourcesCount))
        .runOperation(similarityComputation)
        .union(removeTmp)
    ;
  }

  public DataSet<MergeMusicTriplet> getWorkset() {
    return workset;
  }

  public DataSet<MergeMusicTuple> getDelta() {
    return delta;
  }

  /**
   * In each iteration, get the highest triplet similarity for each blocking key. If
   * more than one triplet has highest similarity, take lowest entity id.
   * @return only maximal similarity triplet for each blocking key
   */
  private static DataSet<MergeMusicTriplet> getIterationMaxTriplets(
      DataSet<MergeMusicTriplet> workset) {
    return workset
        .groupBy(5)
        .reduce(new MaxSimMinIdMusicReducer());
  }

  /**
   * optional Helper method to write the current iteration superstep to the log.
   */
  private static <T> DataSet<T> printSuperstep(DataSet<T> iteration) {
    DataSet<T> superstepPrinter = iteration
        .first(1)
        .filter(new SuperStepFilter<>());

    return iteration.union(superstepPrinter);
  }

  private static class SuperStepFilter<T> extends RichFilterFunction<T> {
    private Integer superstep = null;

    @Override
    public void open(Configuration parameters) throws Exception {
      this.superstep = getIterationRuntimeContext().getSuperstepNumber();
    }

    @Override
    public boolean filter(T vertex) throws Exception {
      LOG.info("Superstep: " + superstep);
      return false;
    }
  }
}