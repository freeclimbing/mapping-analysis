package org.mappinganalysis.benchmark.nc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreatorMultiMerge;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.decomposition.typegroupby.TypeGroupBy;
import org.mappinganalysis.model.functions.merge.MergeExecution;
import org.mappinganalysis.model.functions.merge.MergeInitialization;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.ArrayList;
import java.util.List;

public class NCBenchmark implements ProgramDescription {
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
  private static final String DECOMPOSITION = "nc-decomposition-representatives";
  private static final String MERGE = "nc-merged-clusters";
  private static final String DEC_JOB = "NC Decomposition + Representatives";
  private static final String MER_JOB = "NC Merge";

  /**
   * Main class for NC benchmark
   */
  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 2,
        "args[0]: input dir, args[1]: sources count");
    final int sourcesCount = Integer.parseInt(args[1]);
    final String INPUT_PATH = args[0];
    double simSortThreshold = 0.7;

    /*
      METRICS
     */
    ArrayList<String> metrics = Lists.newArrayList(
//        Constants.COSINE_TRIGRAM//,
        Constants.JARO_WINKLER
    );
    for (String metric : metrics) {

      /*
        DATA SET SOURCES LIST
       */
      List<String> sourceList = Lists.newArrayList(
          "1/"//,
//          "2/",
//          "3/"//, "5/"
      );
      for (String dataset : sourceList) {
        final String roundInputPath = INPUT_PATH.concat(dataset);
        final String datasetNumber = dataset.substring(0, 1);
        // Read input graph
        LogicalGraph logicalGraph = Utils
            .getGradoopGraph(roundInputPath, env);
        Graph<Long, ObjectMap, NullValue> graph = Utils
            .getInputGraph(logicalGraph, Constants.NC, env);
        Graph<Long, ObjectMap, ObjectMap> preprocGraph = graph
            .run(new DefaultPreprocessing(metric, DataDomain.NC, env));

        for (int simFor = 90; simFor <= 90; simFor += 10) {
          simSortThreshold = (double) simFor / 100;

          // Decomposition + Representative
          DataSet<Vertex<Long, ObjectMap>> representatives = preprocGraph
              .run(new TypeGroupBy(env))
              .run(new SimSort(DataDomain.NC, metric, simSortThreshold, env))
              .getVertices()
              .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.NC));

          String decompositionSuffix = "-96-s" + Utils.getOutputSuffix(simSortThreshold)
              + "-" + metric;
          String decompositionStep = DECOMPOSITION.concat(decompositionSuffix);
          new JSONDataSink(roundInputPath, decompositionStep)
              .writeVertices(representatives);
          env.execute(datasetNumber.concat(DEC_JOB.concat(decompositionSuffix)));

        /*
          BLOCKING STRATEGIES
         */
          ArrayList<BlockingStrategy> blockingStrategies = Lists.newArrayList(
              BlockingStrategy.STANDARD_BLOCKING
//            ,
//            BlockingStrategy.BLOCK_SPLIT
          );
          for (BlockingStrategy blockingStrategy : blockingStrategies) {

            // Read graph from disk and merge
            representatives = new org.mappinganalysis.io.impl.json.JSONDataSource(
                roundInputPath, decompositionStep, env)
                .getVertices();

          /*
            MERGE FOR QUEUE
           */
            for (int mergeFor = 95; mergeFor >= 95; mergeFor -= 5) {
              double mergeThreshold = (double) mergeFor / 100;
              DataSet<Vertex<Long, ObjectMap>> merged = representatives
                  .runOperation(new MergeInitialization(DataDomain.NC))
                  .runOperation(new MergeExecution(
                      DataDomain.NC,
                      metric,
                      blockingStrategy,
                      mergeThreshold,
                      sourcesCount,
                      env));

              String mergeSuffix = "-m" + Utils.getOutputSuffix(mergeThreshold)
                  + "-" + Utils.getShortBlockingStrategy(blockingStrategy)
                  .concat(decompositionSuffix);

              new JSONDataSink(roundInputPath, MERGE.concat(mergeSuffix))
                  .writeVertices(merged);
              env.execute(datasetNumber.concat(MER_JOB.concat(mergeSuffix)));
            }
          }
        }
      }

    }

//     /*
//      METRICS
//     */
//    ArrayList<String> metrics = Lists.newArrayList(
//        Constants.COSINE_TRIGRAM//,
////        Constants.JARO_WINKLER
//    );
//    for (String metric : metrics) {
//
//      /*
//        DATA SET SOURCES LIST
//       */
//      List<String> sourceList = Lists.newArrayList(
//          "1/",
//          "2/",
//          "3/"//, "5/"
//      );
//      for (String dataset : sourceList) {
//        final String roundInputPath = INPUT_PATH.concat(dataset);
//        final String datasetNumber = dataset.substring(0, 1);
//        // Read input graph
//        LogicalGraph logicalGraph = Utils
//            .getGradoopGraph(roundInputPath, env);
//        Graph<Long, ObjectMap, NullValue> graph = Utils
//            .getInputGraph(logicalGraph, Constants.NC, env);
//        Graph<Long, ObjectMap, ObjectMap> preprocGraph = graph
//            .run(new DefaultPreprocessing(metric, DataDomain.NC, env));
//
//        for (int simFor = 65; simFor <= 65; simFor += 10) {
//          simSortThreshold = (double) simFor / 100;
//
//          // Decomposition + Representative
//          DataSet<Vertex<Long, ObjectMap>> representatives = preprocGraph
//              .run(new TypeGroupBy(env))
//              .run(new SimSort(DataDomain.NC, metric, simSortThreshold, env))
//              .getVertices()
//              .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.NC));
//
//          String decompositionSuffix = "-96-s" + Utils.getOutputSuffix(simSortThreshold)
//              + "-" + metric;
//          String decompositionStep = DECOMPOSITION.concat(decompositionSuffix);
//          new JSONDataSink(roundInputPath, decompositionStep)
//              .writeVertices(representatives);
//          env.execute(datasetNumber.concat(DEC_JOB.concat(decompositionSuffix)));
//
//        /*
//          BLOCKING STRATEGIES
//         */
//          ArrayList<BlockingStrategy> blockingStrategies = Lists.newArrayList(
//              BlockingStrategy.STANDARD_BLOCKING
////            ,
////            BlockingStrategy.BLOCK_SPLIT
//          );
//          for (BlockingStrategy blockingStrategy : blockingStrategies) {
//
//            // Read graph from disk and merge
//            representatives = new org.mappinganalysis.io.impl.json.JSONDataSource(
//                roundInputPath, decompositionStep, env)
//                .getVertices();
//
//          /*
//            MERGE FOR QUEUE
//           */
//            for (int mergeFor = 85; mergeFor >= 70; mergeFor -= 5) {
//              double mergeThreshold = (double) mergeFor / 100;
//              DataSet<Vertex<Long, ObjectMap>> merged = representatives
//                  .runOperation(new MergeInitialization(DataDomain.NC))
//                  .runOperation(new MergeExecution(
//                      DataDomain.NC,
//                      metric,
//                      blockingStrategy,
//                      mergeThreshold,
//                      sourcesCount,
//                      env));
//
//              String mergeSuffix = "-m" + Utils.getOutputSuffix(mergeThreshold)
//                  + "-" + Utils.getShortBlockingStrategy(blockingStrategy)
//                  .concat(decompositionSuffix);
//
//              new JSONDataSink(roundInputPath, MERGE.concat(mergeSuffix))
//                  .writeVertices(merged);
//              env.execute(datasetNumber.concat(MER_JOB.concat(mergeSuffix)));
//            }
//          }
//        }
//      }
//
//    }
  }

  @Override
  public String getDescription() {
    return null;
  }
}

/* Inner for loop lsh blocking, not working efficient */
//  int valueRangeLsh = 3200;
//// numbersOfFamilies 55
//// numbersOfHashesPerFamily 75
//        for (int pre = 3; pre <= 11; pre++) {
//            int numbersOfFamilies = pre * 5;
//
//            for (int numbersOfHashesPerFamily = numbersOfFamilies + 5;
//            numbersOfHashesPerFamily <= numbersOfFamilies + 15; numbersOfHashesPerFamily += 5) {
//
//            DataSet<Vertex<Long, ObjectMap>> merged = representatives
//    .runOperation(new MergeInitialization(DataDomain.NC))
//    .runOperation(new MergeExecution(
//    DataDomain.NC,
//    BlockingStrategy.LSH_BLOCKING,
//    mergeThreshold,
//    sourcesCount,
//    valueRangeLsh,
//    numbersOfFamilies,
//    numbersOfHashesPerFamily,
//    env));
//
//    String mergeSuffix = "-96-JW-LSHB-" + valueRangeLsh + "" +
//    "-" + numbersOfFamilies + "-" + numbersOfHashesPerFamily +
//    "-m" + Utils.getOutputSuffix(mergeThreshold)
//    .concat(decompositionSuffix);
//
//    new JSONDataSink(roundInputPath, MERGE.concat(mergeSuffix))
//    .writeVertices(merged);
//    env.execute(datasetNumber.concat(MER_JOB.concat(mergeSuffix)));
//    }
//    }
