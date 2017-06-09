package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.graph.Vertex;
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
  private DataSet<Vertex<Long, ObjectMap>> baseClusters;

  public MergeExecution(DataDomain domain, int sourcesCount) {
    this.domain = domain;
    this.sourcesCount = sourcesCount;
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
    if (domain == DataDomain.GEOGRAPHY) {
      // initial solution set
      DataSet<MergeGeoTuple> clusters = baseClusters
          .map(new AddShadingTypeMapFunction())
          .map(new MergeGeoTupleCreator());

      SimilarityFunction<MergeGeoTriplet, MergeGeoTriplet> simFunction =
          new MergeGeoSimilarity(new MeanAggregationMode());

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
      MergeGeoStep stepFunction = new MergeGeoStep(
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
    /**
     * MUSIC
     */
    if (domain == DataDomain.MUSIC) {
      // initial solution set
      DataSet<MergeMusicTuple> clusters = baseClusters
          .map(new MergeMusicTupleCreator()); // added artist title album
//          .map(x -> {
//            LOG.info(x.toString());
//            return x;
//          })
//          .returns(new TypeHint<MergeMusicTuple>() {});

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

      BlockingStrategy blockingStrategy = BlockingStrategy.IDF_BLOCKING;
      // initial working set
      DataSet<MergeMusicTriplet> initialWorkingSet = clusters
          .filter(new SourceCountRestrictionFilter<>(DataDomain.MUSIC, sourcesCount))
          .groupBy(10)
          .reduceGroup(new MergeMusicTripletCreator(sourcesCount))
          .runOperation(similarityComputation);
      DataSet<MergeMusicTriplet> initialWorkingSet;

      if (blockingStrategy.equals(BlockingStrategy.STANDARD_BLOCKING)) {
        initialWorkingSet = preBlockingClusters
            .groupBy(10) // blocking key
            .reduceGroup(new MergeMusicTripletCreator(sourcesCount))
            .runOperation(similarityComputation);
//          .map(x -> {
//            LOG.info(x.toString());
//            return x;
//          })
//          .returns(new TypeHint<MergeMusicTriplet>() {});
      } else if (blockingStrategy.equals(BlockingStrategy.IDF_BLOCKING)) {
        initialWorkingSet = preBlockingClusters
            .runOperation(new IdfBlockingOperation(2)) // TODO define support globally
            .runOperation(similarityComputation);
      } else  {
        throw new IllegalArgumentException("Unsupported strategy: " + blockingStrategy);
      }

      // initialize the iteration
      DeltaIteration<MergeMusicTuple, MergeMusicTriplet> iteration = clusters
          .iterateDelta(initialWorkingSet, Integer.MAX_VALUE, 0);

      // log superstep
//    DataSet<MergeTriplet> workset = printSuperstep(iteration.getWorkset())
//        .map(x->x); // why do we need this line, not working without

      // start step function
      MergeMusicStep stepFunction = new MergeMusicStep(
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
    } else
    {
      throw new IllegalArgumentException("Unsupported domain: " + domain.toString());
    }
  }
}
