package org.mappinganalysis.benchmark.musicbrainz;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.mappinganalysis.graph.utils.EdgeComputationStrategy;
import org.mappinganalysis.graph.utils.EdgeComputationOnVerticesForKeySelector;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.csv.CSVDataSource;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.clusterstrategies.ClusteringStep;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClusteringStrategy;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreatorMultiMerge;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.decomposition.typegroupby.TypeGroupBy;
import org.mappinganalysis.model.functions.merge.MergeExecution;
import org.mappinganalysis.model.functions.merge.MergeInitialization;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.QualityUtils;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.config.Config;
import org.mappinganalysis.util.config.IncrementalConfig;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

import java.util.concurrent.TimeUnit;

/**
 * benchmark musicbrainz dataset https://vsis-www.informatik.uni-hamburg.de/download/info.txt
 */
public class MusicbrainzBenchmark implements ProgramDescription {
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  private static final String INPUT_STEP = "musicbrainz-input";
  private static final String PREPROCESSING_STEP = "musicbrainz-preprocessing";
  private static final String DECOMPOSITION_STEP = "musicbrainz-decomposition-representatives";
  private static final String MERGE_STEP = "musicbrainz-merged-clusters";
  private static final String INP_JOB = "Musicbrainz Input";
  private static final String PRE_JOB = "Musicbrainz Preprocessing";
  private static final String DEC_JOB = "Musicbrainz Decomposition + Representatives";
  private static final String MER_JOB = "Musicbrainz Merge";

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 5,
        "args[0]: input dir, " +
            "args[1]: file name, " +
            "args[2]: all/merge mode selection, " +
            "args[3]: inputOnly" +
            "args[4]: metric");
    final String inputPath = args[0];
    final String vertexFileName = args[1];
    final boolean runInputOnly = args[3].equals("inputOnly");
    final String mode = args[2];
    final String metric = args[4];

    JobExecutionResult result;

    IncrementalConfig config = new IncrementalConfig(DataDomain.MUSIC, env);
    config.setBlockingStrategy(BlockingStrategy.BLOCK_SPLIT);
    config.setMetric(metric);
    config.setSimSortSimilarity(0.5);
    config.setMinResultSimilarity(0.8); // MERGE SIM
    config.setExistingSourcesCount(5);
    config.setBlockingLength(4);

    DataSet<Tuple2<Long, Long>> evalResultList = null;

    if (!mode.equals("merge")) {
      /*
        process input csv data, create basic clean graph
       */
      if (runInputOnly) {
        DataSet<Vertex<Long, ObjectMap>> inputVertices =
            new CSVDataSource(inputPath, vertexFileName, env)
                .getVertices();
        DataSet<Edge<Long, NullValue>> inputEdges = inputVertices
            .runOperation(new EdgeComputationOnVerticesForKeySelector(
                new CcIdKeySelector(), EdgeComputationStrategy.SIMPLE));

        new JSONDataSink(inputPath, INPUT_STEP)
            .writeGraph(Graph.fromDataSet(inputVertices, inputEdges, env));
        result = env.execute(INP_JOB);
        System.out.println(INP_JOB + " needed " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds.");

        return;
      }

    /*
      preprocessing
     */

    /*
     input for incremental paper
     */
      LogicalGraph logicalGraph = Utils
          .getGradoopGraph(inputPath, env);
      Graph<Long, ObjectMap, NullValue> graph = Utils
          .getInputGraph(logicalGraph, Constants.MUSIC, env);
      evalResultList = graph.getVertices()
          .map(vertex -> new Tuple2<>(vertex.getId(), (long) vertex.getValue().get("clsId")))
          .returns(new TypeHint<Tuple2<Long, Long>>() {});
      /*
       normal (old) graph input
       */
//      Graph<Long, ObjectMap, NullValue> graph =
//          new JSONDataSource(inputPath, INPUT_STEP, env)
//              .getGraph(ObjectMap.class, NullValue.class);

      new JSONDataSink(inputPath, PREPROCESSING_STEP)
          .writeGraph(graph.run(new DefaultPreprocessing(config)));
      result = env.execute(PRE_JOB);
      System.out.println(PRE_JOB + " needed " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds.");

    /*
      decomposition with representative creation
     */
      DataSet<Vertex<Long, ObjectMap>> vertices =
          new JSONDataSource(inputPath, PREPROCESSING_STEP, env)
              .getGraph()
              .run(new TypeGroupBy(env))
              .run(new SimSort(config))
              .getVertices()
              .runOperation(new RepresentativeCreatorMultiMerge(config.getDataDomain()));

      new JSONDataSink(inputPath, DECOMPOSITION_STEP)
          .writeVertices(vertices);
      result = env.execute(DEC_JOB);
      System.out.println(DEC_JOB + " needed " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds.");
    }

    /*
      merge
     */
    DataSet<Vertex<Long, ObjectMap>> mergedVertices =
        new JSONDataSource(inputPath, DECOMPOSITION_STEP, env)
            .getVertices()
            .runOperation(new MergeExecution(config));

    new JSONDataSink(inputPath, MERGE_STEP)
        .writeVertices(mergedVertices);
    result = env.execute(MER_JOB);
    System.out.println(MER_JOB + " needed " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds.");

    DataSet<Vertex<Long, ObjectMap>> clusters = new JSONDataSource(inputPath, MERGE_STEP, env)
        .getGraph()
        .getVertices();

    QualityUtils.printNewMusicQuality(clusters,
        config,
        inputPath,
        evalResultList,
        vertexFileName,
        "cluster");
  }

  @Override
  public String getDescription() {
    return null;
  }
}
