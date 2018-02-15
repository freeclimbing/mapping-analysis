package org.mappinganalysis.benchmark.nc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
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
import org.mappinganalysis.util.Utils;

import java.util.List;

public class NCBenchmark {
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
  private static final String DECOMPOSITION = "nc-decomposition-representatives";
  private static final String MERGE = "nc-merged-clusters";
  private static final String DEC_JOB = "NC Decomposition + Representatives";
  private static final String MER_JOB = "NC Merge";

  /**
   * Main class for Settlement benchmark
   */
  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 1,
        "args[0]: input dir");
    final int sourcesCount = 10;
    final String INPUT_PATH = args[0];
    final double simSortThreshold = 0.7;

    List<String> sourceList = Lists.newArrayList(
        "1/", "2/", "3/"//, "5/"
    );
    for (String dataset : sourceList) {
      final String roundInputPath = INPUT_PATH.concat(dataset);
      final String datasetNumber = dataset.substring(0,1);
      // Read input graph
      LogicalGraph logicalGraph = Utils
          .getGradoopGraph(roundInputPath, env);
      Graph<Long, ObjectMap, NullValue> graph = Utils
          .getInputGraph(logicalGraph, env);
      Graph<Long, ObjectMap, ObjectMap> preprocGraph = graph
          .run(new DefaultPreprocessing(DataDomain.NC, env));

      // Decomposition + Representative
      DataSet<Vertex<Long, ObjectMap>> representatives = preprocGraph
          .run(new TypeGroupBy(env))
          .run(new SimSort(DataDomain.NC, simSortThreshold, env))
          .getVertices()
          .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.NC));

      String decompositionSuffix = "-JW-s" + Utils.getOutputSuffix(simSortThreshold);
      String decompositionStep = DECOMPOSITION.concat(decompositionSuffix);
      new JSONDataSink(roundInputPath, decompositionStep)
          .writeVertices(representatives);
      env.execute(datasetNumber.concat(DEC_JOB.concat(decompositionSuffix)));

      // Read graph from disk and merge
      representatives = new org.mappinganalysis.io.impl.json.JSONDataSource(
          roundInputPath, decompositionStep, env)
          .getVertices();

      for (int mergeFor = 95; mergeFor >= 70; mergeFor -= 5) {
        double mergeThreshold = (double) mergeFor / 100;

        DataSet<Vertex<Long, ObjectMap>> merged = representatives
            .runOperation(new MergeInitialization(DataDomain.NC))
            .runOperation(new MergeExecution(
                DataDomain.NC,
                BlockingStrategy.STANDARD_BLOCKING,
                mergeThreshold,
                sourcesCount,
                env));

        String mergeSuffix = "-JW-m" + Utils.getOutputSuffix(mergeThreshold)
            .concat(decompositionSuffix);

        new JSONDataSink(roundInputPath, MERGE.concat(mergeSuffix))
            .writeVertices(merged);
        env.execute(datasetNumber.concat(MER_JOB.concat(mergeSuffix)));
      }
    }
  }
}
