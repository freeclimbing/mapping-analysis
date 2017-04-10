package org.mappinganalysis.benchmark.musikbrainz;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.mappinganalysis.graph.utils.EdgeComputationVertexCcSet;
import org.mappinganalysis.io.impl.csv.CSVDataSource;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;
import org.mappinganalysis.util.functions.keyselector.HashCcIdKeySelector;

/**
 * benchmark musicbrainz dataset https://vsis-www.informatik.uni-hamburg.de/download/info.txt
 *
 * TODO check utf-8 csv input on server
 */
public class MusicbrainzBenchmark implements ProgramDescription {
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  public static final String INPUT = "musicbrainz-input";
  public static final String PREPROCESSING = "musicbrainz-preprocessing";
  public static final String DECOMPOSITION = "musicbrainz-decomposition-representatives";
  public static final String MERGE = "musicbrainz-merged-clusters";
  public static final String INP_JOB = "Musicbrainz Preprocessing";
  public static final String PRE_JOB = "Musicbrainz Preprocessing";
  public static final String DEC_JOB = "Musicbrainz Decomposition + Representatives";
  public static final String MER_JOB = "Musicbrainz Merge";

  public static String INPUT_PATH;
  public static String VERTEX_FILE_NAME;


  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 2, "args[0]: input dir" +
        "args[1]: processing mode (input, preproc, analysis, eval)");
    INPUT_PATH = args[0];
    VERTEX_FILE_NAME = args[1];

    DataSet<Vertex<Long, ObjectMap>> vertices =
        new CSVDataSource(INPUT_PATH, VERTEX_FILE_NAME, env)
            .getVertices();

    DataSet<Edge<Long, NullValue>> edges = vertices
        .runOperation(new EdgeComputationVertexCcSet(new CcIdKeySelector(), false));


    new JSONDataSink(INPUT_PATH, INPUT)
        .writeGraph(Graph.fromDataSet(vertices, edges, env));
    env.execute(INP_JOB);
  }
  @Override
  public String getDescription() {
    return null;
  }
}
