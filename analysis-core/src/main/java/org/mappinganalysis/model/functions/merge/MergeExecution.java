package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.model.ObjectMap;
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
  private int sourcesCount;
  private DataSet<Vertex<Long, ObjectMap>> baseClusters;

  public MergeExecution(int sourcesCount) {
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
    MergeTripletGeoLabelSimilarity simFunction =
        new MergeTripletGeoLabelSimilarity(new MeanAggregationMode());

    SimilarityComputation<MergeTriplet, MergeTriplet> similarityComputation = new SimilarityComputation
        .SimilarityComputationBuilder<MergeTriplet, MergeTriplet>()
        .setSimilarityFunction(simFunction)
        .setStrategy(SimilarityStrategy.MERGE)
        .setThreshold(0.5)
        .build();

    // initial solution set
    DataSet<MergeTuple> clusters = baseClusters
        .map(new AddShadingTypeMapFunction())
        .map(new MergeTupleCreator());

    // initial working set
    DataSet<MergeTriplet> initialWorkingSet = clusters
        .filter(new SourceCountRestrictionFilter(sourcesCount))
        .groupBy(7)
        .reduceGroup(new MergeTripletCreator(sourcesCount));
    initialWorkingSet = initialWorkingSet
        .runOperation(similarityComputation);

    // initialize the iteration
    DeltaIteration<MergeTuple, MergeTriplet> iteration = clusters
        .iterateDelta(initialWorkingSet, 1000, 0);

    // log superstep
//    DataSet<MergeTriplet> workset = printSuperstep(iteration.getWorkset())
//        .map(x->x); // why do we need this line, not working without

    // start step function
    MergeStepFunction stepFunction = new MergeStepFunction(
        iteration.getWorkset(),
        similarityComputation,
        sourcesCount);

//    DataSet<MergeTriplet> workset = stepFunction.getWorkset();

    return iteration.closeWith(stepFunction.getDelta(), stepFunction.getWorkset())
        .leftOuterJoin(baseClusters)
        .where(0)
        .equalTo(0)
        .with(new FinalMergeVertexCreator());
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
