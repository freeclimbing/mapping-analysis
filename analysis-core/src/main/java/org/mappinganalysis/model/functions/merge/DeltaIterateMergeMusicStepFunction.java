package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.util.functions.LeftMinusRightSideJoinFunction;

import java.util.HashSet;

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

  DeltaIterateMergeMusicStepFunction(
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
    DataSet<MergeMusicTriplet> maxTriplets = getIterationMaxTriplets(workset)
        .map(x -> {
//          LOG.info("max: " + x.toString());
          return x;
        })
        .returns(new TypeHint<MergeMusicTriplet>() {});

    /*
      delta is the solution set which is changed over the iterations
      contains the resulting clusters per iteration
     */
    delta = maxTriplets.flatMap(new DualMergeMusicMapper());
    // merge + fake LOG

    workset = printSuperstep(workset);

    // remove max triplets from workset, they are getting merged anyway
    // duplicate of WorksetNewClusterRemoveOperation

    workset = workset.leftOuterJoin(maxTriplets)
        .where(0,1)
        .equalTo(0,1)
        .with(new LeftMinusRightSideJoinFunction<>());

    // HOLD EXCLUDE 1 potentially useless

    DataSet<Tuple2<Long, Long>> transitions = maxTriplets
        .flatMap(new TransitionElementsFlatMapFunction<>(domain));

    // remove workset triples containing max element src or trg
    DataSet<MergeMusicTriplet> nextUnchangedWorkset = workset
        .runOperation(new WorksetNewClusterRemoveOperation<>(transitions))
        .map(x -> {
//          LOG.info("nextUnchanged: " + x.toString());
          return x;
        })
        .returns(new TypeHint<MergeMusicTriplet>() {});
    // HOLD EXCLUDE 2

    workset = workset
        .runOperation(new ChangesMusicOperation(delta, transitions, domain))
        .runOperation(new ComputePrepareMusicOperation(domain, sourcesCount))
        .runOperation(similarityComputation)
        .union(nextUnchangedWorkset)
    ;
  }

  DataSet<MergeMusicTriplet> getWorkset() {
    return workset;
  }

  DataSet<MergeMusicTuple> getDelta() {
    delta = delta.map(x -> {
//      LOG.info(x.toString());
      return x;
    })
        .returns(new TypeHint<MergeMusicTuple>() {});
    return delta;
  }

  /**
   * In each iteration, get the highest triplet similarity for each blocking key. If
   * more than one triplet has highest similarity, take lowest entity id.
   * @return only maximal similarity triplet for each blocking key
   */
  private static DataSet<MergeMusicTriplet> getIterationMaxTriplets(
      DataSet<MergeMusicTriplet> workset) {

    // max sim, blocking key
    return workset.join(workset.groupBy(5).max(4))
        .where(4,5)
        .equalTo(4,5)
        .with((first, second) -> first)
        .returns(new TypeHint<MergeMusicTriplet>() {})
        .groupBy(5)
        .sortGroup(0, Order.ASCENDING)
        .sortGroup(1, Order.ASCENDING)
        .reduceGroup(new GroupReduceFunction<MergeMusicTriplet, MergeMusicTriplet>() {
          @Override
          public void reduce(Iterable<MergeMusicTriplet> values,
                             Collector<MergeMusicTriplet> out) throws Exception {
            HashSet<Long> processedSet = Sets.newHashSet();
            for (MergeMusicTriplet value : values) {
              if (!processedSet.contains(value.getSrcId())
                  && !processedSet.contains(value.getTrgId())) {
                processedSet.add(value.getTrgId());
                processedSet.add(value.getSrcId());

                out.collect(value);
              }
            }
          }
        });

//
//    return workset
//        .groupBy(5)
//        .reduce(new MaxSimMinIdMusicReducer());
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
