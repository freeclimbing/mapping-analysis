package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.*;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.blocking.tfidf.IdfBlockingOperation;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.impl.SimilarityStrategy;

/**
 * Merge process, iteratively collate similar clusters in compliance with restrictions
 * according to types and data sources.
 */
public class MergeExecution
    implements CustomUnaryOperation<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(MergeExecution.class);
  private DataDomain domain;
  private int sourcesCount;
  private ExecutionEnvironment env;
  private DataSet<Vertex<Long, ObjectMap>> baseClusters;

  public MergeExecution(DataDomain domain, int sourcesCount, ExecutionEnvironment env) {
    this.domain = domain;
    this.sourcesCount = sourcesCount;
    this.env = env;
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> vertices) {
    this.baseClusters = vertices;
  }

  /**
   * Execute the refinement step - compare clusters with each other and combine similar clusters.
   * @return refined dataset
   */
  @Override
  public DataSet<Vertex<Long, ObjectMap>> createResult() {
    /*
      ########### GEOGRAPHY ##############
     */
    if (domain == DataDomain.GEOGRAPHY) {
      // initial solution set
      DataSet<MergeGeoTuple> clusters = baseClusters
          .map(new AddShadingTypeMapFunction())
          .map(new MergeGeoTupleCreator()); // TODO add custom blocking strategy option

      SimilarityFunction<MergeGeoTriplet, MergeGeoTriplet> simFunction =
          new MergeGeoSimilarity();

      SimilarityComputation<MergeGeoTriplet,
          MergeGeoTriplet> similarityComputation
          = new SimilarityComputation
          .SimilarityComputationBuilder<MergeGeoTriplet,
          MergeGeoTriplet>()
          .setSimilarityFunction(simFunction)
          .setStrategy(SimilarityStrategy.MERGE)
          .setThreshold(0.5)
          .build();

      // initial working set
      DataSet<MergeGeoTriplet> initialWorkingSet = clusters
          .filter(new SourceCountRestrictionFilter<>(domain, sourcesCount))
          .groupBy(7) // MergeGeoTuple::getBlockingLabel not working
          .reduceGroup(new MergeGeoTripletCreator(sourcesCount))
          .runOperation(similarityComputation);

      // initialize the iteration
      DeltaIteration<MergeGeoTuple, MergeGeoTriplet> iteration = clusters
          .iterateDelta(initialWorkingSet, Integer.MAX_VALUE, 0);

      // log superstep
//    DataSet<MergeTriplet> workset = printSuperstep(iteration.getWorkset())
//        .map(x->x); // why do we need this line, not working without

      // start step function
      DeltaIterateGeographicMergeStepFunction stepFunction = new DeltaIterateGeographicMergeStepFunction(
          iteration.getWorkset(),
          similarityComputation,
          sourcesCount,
          domain);

      return iteration
          .closeWith(stepFunction.getDelta(), stepFunction.getWorkset())
          .leftOuterJoin(baseClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator());
    } else
    /*
      ########## MUSIC ##############
     */
    if (domain == DataDomain.MUSIC) {
      BlockingStrategy blockingStrategy = BlockingStrategy.STANDARD_BLOCKING;

      // initial solution set
      DataSet<MergeMusicTuple> clusters = baseClusters
          .map(new MergeMusicTupleCreator(blockingStrategy));

      SimilarityFunction<MergeMusicTriplet, MergeMusicTriplet> simFunction =
          new MergeMusicSimilarity();

      SimilarityComputation<MergeMusicTriplet,
          MergeMusicTriplet> similarityComputation
          = new SimilarityComputation
          .SimilarityComputationBuilder<MergeMusicTriplet,
          MergeMusicTriplet>()
          .setSimilarityFunction(simFunction)
          .setStrategy(SimilarityStrategy.MERGE)
          .setThreshold(0.5)
          .build();

      // prep phase initial working set
      DataSet<MergeMusicTuple> preBlockingClusters = clusters
          .filter(new SourceCountRestrictionFilter<>(DataDomain.MUSIC, sourcesCount));

      // initial working set
      DataSet<MergeMusicTriplet> initialWorkingSet;

      /*
        Blocking (MUSIC only)
       */
      if (blockingStrategy.equals(BlockingStrategy.STANDARD_BLOCKING)) {
        initialWorkingSet = preBlockingClusters
            .groupBy(10) // blocking key
            .reduceGroup(new MergeMusicTripletCreator(sourcesCount))
            .runOperation(similarityComputation)
            .rebalance();
      } else if (blockingStrategy.equals(BlockingStrategy.IDF_BLOCKING)) {
        DataSet<MergeMusicTriplet> idfPartTriplets = preBlockingClusters
            .runOperation(new IdfBlockingOperation(2, env)) // TODO define support globally
            .runOperation(similarityComputation)
            .rebalance();

        DataSet<MergeMusicTuple> simpleTuples = idfPartTriplets.<Tuple2<Long, Long>>project(0, 1)
            .flatMap((Tuple2<Long, Long> tuple, Collector<Tuple1<Long>> out) -> {
              out.collect(new Tuple1<>(tuple.f0));
              out.collect(new Tuple1<>(tuple.f1));
            })
            .returns(new TypeHint<Tuple1<Long>>() {
            })
            .rightOuterJoin(preBlockingClusters)
            .where(0)
            .equalTo(0)
            .with(new FlatJoinFunction<Tuple1<Long>, MergeMusicTuple, MergeMusicTuple>() {
              @Override
              public void join(Tuple1<Long> idfIds, MergeMusicTuple unmatchedTuple,
                               Collector<MergeMusicTuple> out) throws Exception {
                if (idfIds == null) {
                  out.collect(unmatchedTuple);
                }
              }
            });

        DataSet<MergeMusicTriplet> simpleTriplets = simpleTuples.groupBy(10) // blocking key
            .reduceGroup(new MergeMusicTripletCreator(sourcesCount))
            .runOperation(similarityComputation);

        initialWorkingSet = idfPartTriplets.union(simpleTriplets);
      } else  {
        throw new IllegalArgumentException("Unsupported strategy: " + blockingStrategy);
      }

      // initialize the iteration
      DeltaIteration<MergeMusicTuple, MergeMusicTriplet> iteration = clusters
          .iterateDelta(initialWorkingSet, Integer.MAX_VALUE, 0);

      // log superstep
//    DataSet<MergeTriplet> workset = printSuperstep(iteration.getWorkset())
//        .map(x->x); // why do we need this line, not working without

      /*
        start step function - creates changed vertices and merges clusters
       */
      DeltaIterateMergeMusicStepFunction stepFunction = new DeltaIterateMergeMusicStepFunction(
          iteration.getWorkset(),
          similarityComputation,
          sourcesCount,
          domain);

      return iteration
          .closeWith(stepFunction.getDelta(), stepFunction.getWorkset())
          .leftOuterJoin(baseClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeMusicVertexCreator());
    } else {
      throw new IllegalArgumentException("Unsupported domain: " + domain.toString());
    }
  }
}
