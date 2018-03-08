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
import org.mappinganalysis.model.functions.NcLshCandidateTupleCreator;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.blocking.blocksplit.BlockSplitTupleCreator;
import org.mappinganalysis.model.functions.blocking.tfidf.IdfBlockingOperation;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.impl.SimilarityStrategy;
import org.mappinganalysis.util.Constants;

/**
 * Merge process, iteratively collate similar clusters in compliance with restrictions
 * according to types and data sources.
 */
public class MergeExecution
    implements CustomUnaryOperation<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(MergeExecution.class);
  private final String metric;
  private DataDomain domain;
  private BlockingStrategy blockingStrategy;
  private double mergeThreshold;
  private int sourcesCount;
  private ExecutionEnvironment env;

  private DataSet<Vertex<Long, ObjectMap>> baseClusters;

  private int valueRangeLsh;
  private int numberOfFamilies;
  private int numberOfHashesPerFamily;

  public MergeExecution(DataDomain domain,
                        String metric,
                        double mergeThreshold,
                        int sourcesCount,
                        ExecutionEnvironment env) {
    this(domain,
        metric,
        BlockingStrategy.STANDARD_BLOCKING,
        mergeThreshold,
        sourcesCount,
        env);
  }

  public MergeExecution(DataDomain domain,
                        String metric,
                        BlockingStrategy blockingStrategy,
                        double mergeThreshold,
                        int sourcesCount,
                        ExecutionEnvironment env) {
    this.domain = domain;
    this.metric = metric;
    this.blockingStrategy = blockingStrategy;
    this.mergeThreshold = mergeThreshold;
    this.sourcesCount = sourcesCount;
    this.env = env;
  }

  // only LSH
  public MergeExecution(DataDomain domain,
                        BlockingStrategy blockingStrategy,
                        double mergeThreshold,
                        int sourcesCount,
                        int valueRangeLsh,
                        int numberOfFamilies,
                        int numberOfHashesPerFamily,
                        ExecutionEnvironment env) {
    this.metric = Constants.COSINE_TRIGRAM;
    this.domain = domain;
    this.blockingStrategy = blockingStrategy;
    this.mergeThreshold = mergeThreshold;
    this.sourcesCount = sourcesCount;
    this.valueRangeLsh = valueRangeLsh;
    this.numberOfFamilies = numberOfFamilies;
    this.numberOfHashesPerFamily = numberOfHashesPerFamily;
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
          new MergeGeoSimilarity(metric);

      SimilarityComputation<MergeGeoTriplet,
          MergeGeoTriplet> similarityComputation
          = new SimilarityComputation
          .SimilarityComputationBuilder<MergeGeoTriplet,
          MergeGeoTriplet>()
          .setSimilarityFunction(simFunction)
          .setStrategy(SimilarityStrategy.MERGE)
          .setThreshold(mergeThreshold)
          .build();

      // initial working set GEO
      DataSet<MergeGeoTriplet> initialWorkingSet = clusters
          .filter(new SourceCountRestrictionFilter<>(domain, sourcesCount))
          .groupBy(7) // MergeGeoTuple::getBlockingLabel not working
          .reduceGroup(new MergeGeoTripletCreator(sourcesCount))
          .runOperation(similarityComputation);

      // initialize the iteration GEO
      DeltaIteration<MergeGeoTuple, MergeGeoTriplet> iteration = clusters
          .iterateDelta(initialWorkingSet, Integer.MAX_VALUE, 0);

      // start step function GEO
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
      ########## MUSIC/NC ##############
     */
    if (domain == DataDomain.MUSIC || domain == DataDomain.NC) {
      DataSet<MergeMusicTuple> initialSolutionSet = baseClusters
            .map(new MergeMusicTupleCreator(BlockingStrategy.STANDARD_BLOCKING, domain));
//      } else {
//        initialSolutionSet = baseClusters
//            .map(new MergeMusicTupleCreator(BlockingStrategy.NO_BLOCKING, domain)); // TODO check LSH
//      }

      // prep phase initial working set
      DataSet<MergeMusicTuple> preBlockingClusters = initialSolutionSet
          .filter(new SourceCountRestrictionFilter<>(domain, sourcesCount));
      SimilarityFunction<MergeMusicTriplet, MergeMusicTriplet> simFunction;
      if (domain == DataDomain.MUSIC) {
        simFunction = new MergeMusicSimilarity(metric);
      } else if (domain == DataDomain.NC) {
        simFunction = new MergeNcSimilarity(metric);
      } else {
        throw new IllegalArgumentException("simfunction: Unsupported domain: " + domain);
      }

      SimilarityComputation<MergeMusicTriplet,
          MergeMusicTriplet> similarityComputation
          = new SimilarityComputation
          .SimilarityComputationBuilder<MergeMusicTriplet,
          MergeMusicTriplet>()
          .setSimilarityFunction(simFunction)
          .setStrategy(SimilarityStrategy.MERGE)
          .setThreshold(mergeThreshold)
          .build();

      // initial working set
      DataSet<MergeMusicTriplet> initialWorkingSet;

      /*
        Blocking (MUSIC/NC only)
       */
      if (blockingStrategy == BlockingStrategy.STANDARD_BLOCKING) {
        initialWorkingSet = preBlockingClusters
            .groupBy(10) // blocking key
            .reduceGroup(new MergeMusicTripletCreator(sourcesCount))
            .runOperation(similarityComputation);
      } else if (blockingStrategy == BlockingStrategy.BLOCK_SPLIT) {
        initialWorkingSet = preBlockingClusters.runOperation(
            new BlockSplitTupleCreator())
            .runOperation(similarityComputation);
      } else if (blockingStrategy == BlockingStrategy.LSH_BLOCKING) {
        initialWorkingSet = preBlockingClusters.runOperation(
            new NcLshCandidateTupleCreator(
                similarityComputation,
                valueRangeLsh,
                numberOfFamilies,
                numberOfHashesPerFamily,
                env));
      } else if (blockingStrategy == BlockingStrategy.IDF_BLOCKING) {
        DataSet<MergeMusicTriplet> idfPartTriplets = preBlockingClusters
            .runOperation(new IdfBlockingOperation(2, env)) // TODO define support globally
            .runOperation(similarityComputation);

        DataSet<MergeMusicTuple> simpleTuples = idfPartTriplets
            .<Tuple2<Long, Long>>project(0, 1)
            .flatMap((Tuple2<Long, Long> tuple, Collector<Tuple1<Long>> out) -> {
              out.collect(new Tuple1<>(tuple.f0));
              out.collect(new Tuple1<>(tuple.f1));
            })
            .returns(new TypeHint<Tuple1<Long>>() {})
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

        DataSet<MergeMusicTriplet> simpleTriplets = simpleTuples
            .groupBy(10) // blocking key
            .reduceGroup(new MergeMusicTripletCreator(sourcesCount))
            .runOperation(similarityComputation);

        initialWorkingSet = idfPartTriplets.union(simpleTriplets);
      } else  {
        throw new IllegalArgumentException("Unsupported strategy: " + blockingStrategy);
      }

      // initialize the iteration
      DeltaIteration<MergeMusicTuple, MergeMusicTriplet> iteration = initialSolutionSet
          .iterateDelta(initialWorkingSet, Integer.MAX_VALUE, 0);

      /*
        start step function - creates changed vertices and merges clusters
       */
      DeltaIterateMergeMusicStepFunction stepFunction = new DeltaIterateMergeMusicStepFunction(
          iteration.getWorkset(),
          similarityComputation,
          sourcesCount,
          domain);

      return iteration//.parallelism() // TODO CHECK THIS OUT
          // low iteration paralellism???? and high other one
          .closeWith(stepFunction.getDelta(), stepFunction.getWorkset())
          .leftOuterJoin(baseClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeMusicVertexCreator(domain));
    } else {
      throw new IllegalArgumentException("Unsupported domain: " + domain.toString());
    }
  }
}
