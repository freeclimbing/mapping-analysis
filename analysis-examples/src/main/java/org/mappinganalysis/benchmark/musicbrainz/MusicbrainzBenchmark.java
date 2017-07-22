package org.mappinganalysis.benchmark.musicbrainz;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.graph.utils.EdgeComputationStrategy;
import org.mappinganalysis.graph.utils.EdgeComputationVertexCcSet;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.csv.CSVDataSource;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreator;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.decomposition.typegroupby.TypeGroupBy;
import org.mappinganalysis.model.functions.merge.MergeExecution;
import org.mappinganalysis.model.functions.merge.MergeInitialization;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

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

  private static String INPUT_PATH;
  private static String VERTEX_FILE_NAME;
  private static String MODE;

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 4, "args[0]: input dir, " +
        "args[1]: file name, args[2]: all/merge mode selection, args[3]: inputOnly");
    INPUT_PATH = args[0];
    VERTEX_FILE_NAME = args[1];
    boolean runInputOnly = args[3].equals("inputOnly");
    MODE = args[2];

    Constants.SOURCE_COUNT = 5;
    DataDomain domain = DataDomain.MUSIC;

    if (!MODE.equals("merge")) {
      /*
        process input csv data, create basic clean graph
       */
      if (runInputOnly) {
        DataSet<Vertex<Long, ObjectMap>> inputVertices =
          new CSVDataSource(INPUT_PATH, VERTEX_FILE_NAME, env)
              .getVertices();
      DataSet<Edge<Long, NullValue>> inputEdges = inputVertices
          .runOperation(new EdgeComputationVertexCcSet(
              new CcIdKeySelector(), EdgeComputationStrategy.SIMPLE));

      new JSONDataSink(INPUT_PATH, INPUT_STEP)
          .writeGraph(Graph.fromDataSet(inputVertices, inputEdges, env));
      env.execute(INP_JOB);

        return;
      }

      /*
        preprocessing
       */
      Graph<Long, ObjectMap, NullValue> graph =
          new JSONDataSource(INPUT_PATH, INPUT_STEP, env)
              .getGraph(ObjectMap.class, NullValue.class);
    /*
      preprocessing
     */
    Graph<Long, ObjectMap, NullValue> graph =
        new JSONDataSource(INPUT_PATH, INPUT_STEP, env)
            .getGraph(ObjectMap.class, NullValue.class);

      new JSONDataSink(INPUT_PATH, PREPROCESSING_STEP)
          .writeGraph(graph.run(new DefaultPreprocessing(domain, env)));
      env.execute(PRE_JOB);

    /*
      decomposition with representative creation
     */
    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(INPUT_PATH, PREPROCESSING_STEP, env)
            .getGraph()
            .run(new TypeGroupBy(env))
            .run(new SimSort(domain, 0.5, env))
            .getVertices()
            .runOperation(new RepresentativeCreator(domain));

      new JSONDataSink(INPUT_PATH, DECOMPOSITION_STEP)
          .writeVertices(vertices);
      env.execute(DEC_JOB);
    }

    /*
      merge
     */
    DataSet<Vertex<Long, ObjectMap>> mergedVertices =
        new JSONDataSource(INPUT_PATH, DECOMPOSITION_STEP, env)
            .getVertices()
            .runOperation(new MergeInitialization(DataDomain.MUSIC))
            .runOperation(new MergeExecution(DataDomain.MUSIC, Constants.SOURCE_COUNT, env));

    new JSONDataSink(INPUT_PATH, MERGE_STEP)
        .writeVertices(mergedVertices);
    env.execute(MER_JOB);
  }

  @Override
  public String getDescription() {
    return null;
  }
}
