package org.mappinganalysis.benchmark.nc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.blocking.blocksplit.BlockSplitTripletCreator;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreatorMultiMerge;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.decomposition.typegroupby.TypeGroupBy;
import org.mappinganalysis.model.functions.merge.*;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.impl.SimilarityStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.config.IncrementalConfig;

import java.util.ArrayList;
import java.util.List;

public class NCBenchmarkParts implements ProgramDescription {
  private static final Logger LOG = Logger.getLogger(NCBenchmarkParts.class);

  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  private static final String DECOMPOSITION = "nc-decomposition-representatives";
  private static final String MERGE = "nc-merged-clusters";
  private static final String DEC_JOB = "NC Decomposition + Representatives";
  private static final String MER_JOB = "NC Merge";

  /**
   * Main class for test nc benchmark
   */
  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 1,
        "args[0]: input dir");
    final int sourcesCount = 10;
    final String INPUT_PATH = args[0];
//    final double simSortThreshold = 0.7;
    IncrementalConfig config = new IncrementalConfig(DataDomain.NC, env);

    List<String> sourceList = Lists.newArrayList(
        "1/"//, "3/"
    );

    for (String dataset : sourceList) {
      final String roundInputPath = INPUT_PATH.concat(dataset);
      final String datasetNumber = dataset.substring(0,1);
      // Read input graph
      LogicalGraph logicalGraph = Utils
          .getGradoopGraph(roundInputPath, env);
      Graph<Long, ObjectMap, NullValue> graph = Utils
          .getInputGraph(logicalGraph, Constants.NC, env);

      /*
        metrics
       */
      ArrayList<String> metrics = Lists.newArrayList(
          Constants.JARO_WINKLER, Constants.COSINE_TRIGRAM
      );
      for (String metric : metrics) {
        config.setMetric(metric);

        Graph<Long, ObjectMap, ObjectMap> preprocGraph = graph
            .run(new DefaultPreprocessing(config));

        // Decomposition + Representative
        DataSet<Vertex<Long, ObjectMap>> representatives = preprocGraph
            .run(new TypeGroupBy(env))
            .run(new SimSort(config))
            .getVertices()
            .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.NC));

        String decompositionSuffix = "-parts-" + metric;
        String decompositionStep = DECOMPOSITION.concat(decompositionSuffix);
        new JSONDataSink(roundInputPath, decompositionStep)
            .writeVertices(representatives);
        env.execute(datasetNumber.concat(DEC_JOB.concat(decompositionSuffix)));

      /*
        blocking
       */
        ArrayList<BlockingStrategy> blockingStrategies = Lists.newArrayList(
//            BlockingStrategy.STANDARD_BLOCKING,
            BlockingStrategy.BLOCK_SPLIT
        );
        for (BlockingStrategy blockingStrategy : blockingStrategies) {
          for (int run = 0; run < 1; run++) {
            // Read graph from disk and merge
            representatives = new org.mappinganalysis.io.impl.json.JSONDataSource(
                roundInputPath, decompositionStep, env)
                .getVertices();

            for (int mergeFor = 95; mergeFor >= 90; mergeFor -= 5) {
              double mergeThreshold = (double) mergeFor / 100;

              DataSet<MergeMusicTuple> preBlockingClusters = representatives
                  .runOperation(new MergeInitialization(DataDomain.NC))
                  .map(new MergeMusicTupleCreator(BlockingStrategy.STANDARD_BLOCKING, DataDomain.NC))
                  .filter(new SourceCountRestrictionFilter<>(DataDomain.NC, sourcesCount));

              SimilarityComputation<MergeMusicTriplet,
                  MergeMusicTriplet> similarityComputation
                  = new SimilarityComputation
                  .SimilarityComputationBuilder<MergeMusicTriplet,
                  MergeMusicTriplet>()
                  .setSimilarityFunction(new MergeNcSimilarity(metric))
                  .setStrategy(SimilarityStrategy.MERGE)
                  .setThreshold(mergeThreshold)
                  .build();

              DataSet<MergeMusicTriplet> initialWorkingSet = null;

            /* Blocking (MUSIC/NC only) */
              if (blockingStrategy == BlockingStrategy.STANDARD_BLOCKING) {
                initialWorkingSet = preBlockingClusters
                    .groupBy(10) // blocking key
                    .reduceGroup(new MergeMusicTripletCreator(sourcesCount))
                    .runOperation(similarityComputation);
              } else if (blockingStrategy == BlockingStrategy.BLOCK_SPLIT) {
                initialWorkingSet = preBlockingClusters.runOperation(
                    new BlockSplitTripletCreator())
                    .runOperation(similarityComputation);
              }

              String mergeSuffix = "-96-run" + run + blockingStrategy.toString()
                  + "-m" + Utils.getOutputSuffix(mergeThreshold)
                  .concat(decompositionSuffix);

              new JSONDataSink(roundInputPath, MERGE.concat(mergeSuffix))
                  .writeTuples(initialWorkingSet);
              env.execute(datasetNumber.concat(MER_JOB.concat(mergeSuffix)));
            }
          }
        }
      }
    }
  }

  @Override
  public String getDescription() {
    return null;
  }
}