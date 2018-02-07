package org.mappinganalysis.benchmark.nc;

import com.google.common.base.Preconditions;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreatorMultiMerge;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.decomposition.typegroupby.TypeGroupBy;
import org.mappinganalysis.model.functions.merge.MergeExecution;
import org.mappinganalysis.model.functions.merge.MergeInitialization;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.util.Utils;

public class NCBenchmark {
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  private static final String PREPROCESSING = "nc-preprocessing";
  private static final String DECOMPOSITION = "nc-decomposition-representatives";
  private static final String MERGE = "nc-merged-clusters";
  private static final String PRE_JOB = "NC Preprocessing";
  private static final String DEC_JOB = "NC Decomposition + Representatives";
  private static final String MER_JOB = "NC Merge";

  /**
   * Main class for Settlement benchmark
   */
  public static void main(String[] args) throws Exception {
    // TODO check arguments
    Preconditions.checkArgument(args.length == 1,
        "args[0]: input dir");
    final int sourcesCount = 10;
    final String INPUT_PATH = args[0];
    final double simSortThreshold = 0.7;
    final double mergeThreshold = 0.7;
//    final String suffix = simSortThreshold*10

    // Read input graph
    LogicalGraph logicalGraph = Utils.getGradoopGraph(INPUT_PATH, env);
    Graph<Long, ObjectMap, NullValue> graph = Utils
        .getInputGraph(logicalGraph, env);

    //TODO  check times with reading graph from disk
    Graph<Long, ObjectMap, ObjectMap> preprocGraph = graph
        .run(new DefaultPreprocessing(DataDomain.NC, env));

    // Decomposition + Representative
    DataSet<Vertex<Long, ObjectMap>> representatives = preprocGraph
        .run(new TypeGroupBy(env))
        .run(new SimSort(DataDomain.NC, simSortThreshold, env))
        .getVertices()
        .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.NC));
    
    String stepSuffix = "m" + Utils.getOutputSuffix(mergeThreshold)
        .concat("s")
        + Utils.getOutputSuffix(simSortThreshold);
    String decompositionStep = DECOMPOSITION.concat(stepSuffix);
    new JSONDataSink(INPUT_PATH, decompositionStep)
        .writeVertices(representatives);
    env.execute(DEC_JOB);

    // Read graph from disk and merge
    representatives = new org.mappinganalysis.io.impl.json.JSONDataSource(
            INPUT_PATH, decompositionStep, env)
            .getVertices();

    DataSet<Vertex<Long, ObjectMap>> merged = representatives
        .runOperation(new MergeInitialization(DataDomain.NC))
        .runOperation(new MergeExecution(
            DataDomain.NC,
            mergeThreshold,
            sourcesCount,
            env));

//    graphPath.concat("/output/m") + mergeFor
//        + "s" + simSortThreshold + "/", "test"
    new JSONDataSink(INPUT_PATH, MERGE.concat(stepSuffix))
        .writeVertices(merged);
    env.execute(MER_JOB);

  }
}
