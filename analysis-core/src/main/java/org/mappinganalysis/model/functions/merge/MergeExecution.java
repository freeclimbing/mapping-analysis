package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.*;
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

  /**
   * @param vertices base clusters - prepared dataset
   */
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
          .map(new MergeTupleCreator(domain));

      SimilarityFunction<MergeTriplet<MergeGeoTuple>, MergeTriplet<MergeGeoTuple>> simFunction;
      if (domain == DataDomain.GEOGRAPHY) {
        simFunction = new MergeTripletGeoLabelSimilarity<>(new MeanAggregationMode<>());
//      } else if (domain == DataDomain.MUSIC) {
//        simFunction = new GeoMergeSimilarity(new MeanAggregationMode<>());// TODO CHECK
      } else {
        throw new IllegalArgumentException("Unsupported domain: " + domain.toString());
      }

      SimilarityComputation<MergeTriplet<MergeGeoTuple>,
          MergeTriplet<MergeGeoTuple>> similarityComputation
          = new SimilarityComputation
          .SimilarityComputationBuilder<MergeTriplet<MergeGeoTuple>,
          MergeTriplet<MergeGeoTuple>>()
          .setSimilarityFunction(simFunction)
          .setStrategy(SimilarityStrategy.MERGE) // TODO check
          .setThreshold(0.5) // TODO CHECK
          .build();

      // initial working set
      DataSet<MergeTriplet<MergeGeoTuple>> initialWorkingSet = clusters
          .filter(new SourceCountRestrictionFilter<>(domain, sourcesCount))
          .groupBy(7) // TODO
          .reduceGroup(new MergeTripletCreator<>(domain, sourcesCount))
          .runOperation(similarityComputation);

      // initialize the iteration
      DeltaIteration<MergeGeoTuple, MergeTriplet<MergeGeoTuple>> iteration = clusters
          .iterateDelta(initialWorkingSet, Integer.MAX_VALUE, 0);

      // log superstep
//    DataSet<MergeTriplet> workset = printSuperstep(iteration.getWorkset())
//        .map(x->x); // why do we need this line, not working without

      // start step function
      MergeStepFunction<MergeGeoTuple> stepFunction = new MergeStepFunction<>(
          iteration.getWorkset(),
          similarityComputation,
          sourcesCount,
          MergeGeoTuple.class,
          domain);

      return iteration
          .closeWith(stepFunction.getDelta(), stepFunction.getWorkset())
          .leftOuterJoin(baseClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeVertexCreator());
    } else
    /**
     * MUSIC
     */
//    if (domain == DataDomain.MUSIC) {
//      // initial solution set
//      DataSet<MergeMusicTuple> clusters = baseClusters
//          .map(new MergeTupleCreator<>(domain));
//
//      // initial working set
//      DataSet<MergeTriplet> initialWorkingSet = clusters
//          .filter(new SourceCountRestrictionFilter<>(sourcesCount))
//          .groupBy(MergeMusicTuple::getBlockingLabel)
//          .reduceGroup(new MergeTripletCreator<>(domain, sourcesCount))
//          .runOperation(similarityComputation);
//
//      // initialize the iteration
//      DeltaIteration<MergeMusicTuple, MergeTriplet> iteration = clusters
//          .iterateDelta(initialWorkingSet, Integer.MAX_VALUE, 0);
//
//      // log superstep
////    DataSet<MergeTriplet> workset = printSuperstep(iteration.getWorkset())
////        .map(x->x); // why do we need this line, not working without
//
//      // start step function
//      MergeStepFunction<MergeMusicTuple> stepFunction = new MergeStepFunction<>(
//          iteration.getWorkset(),
//          similarityComputation,
//          sourcesCount,
//          MergeMusicTuple.class,
//          domain);
//
//      return iteration
//          .closeWith(stepFunction.getDelta(), stepFunction.getWorkset())
//          .leftOuterJoin(baseClusters)
//          .where(0)
//          .equalTo(0)
//          .with(new FinalMergeVertexCreator());
//    } else
    {
      throw new IllegalArgumentException("Unsupported domain: " + domain.toString());
    }
  }

  /**
   * optional Helper method to write the current iteration superstep to the log.
   */
  private static DataSet<MergeTriplet> printSuperstep(DataSet<MergeTriplet> iteration) {
    DataSet<MergeTriplet> superstepPrinter = iteration
        .first(1)
        .filter(new RichFilterFunction<MergeTriplet>() {
          private Integer superstep = null;
          @Override
          public void open(Configuration parameters) throws Exception {
            this.superstep = getIterationRuntimeContext().getSuperstepNumber();
          }
          @Override
          public boolean filter(MergeTriplet vertex) throws Exception {
            LOG.info("Superstep: " + superstep);
            return false;
          }
        });

    return iteration.union(superstepPrinter);
  }
}
