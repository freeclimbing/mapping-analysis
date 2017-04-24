package org.mappinganalysis.benchmark.musikbrainz;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAnalytic;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.mappinganalysis.graph.utils.EdgeComputationVertexCcSet;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.csv.CSVDataSource;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreator;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.decomposition.typegroupby.TypeGroupBy;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.model.functions.simcomputation.BasicEdgeSimilarityComputation;
import org.mappinganalysis.util.Constants;
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
    DataDomain domain = DataDomain.MUSIC;

    /**
     * process input csv data, create basic clean graph
     */
    DataSet<Vertex<Long, ObjectMap>> inputVertices =
        new CSVDataSource(INPUT_PATH, VERTEX_FILE_NAME, env)
            .getVertices();
    DataSet<Edge<Long, NullValue>> inputEdges = inputVertices
        .runOperation(new EdgeComputationVertexCcSet(new CcIdKeySelector(), false));

    new JSONDataSink(INPUT_PATH, INPUT)
        .writeGraph(Graph.fromDataSet(inputVertices, inputEdges, env));
    env.execute(INP_JOB);

    /**
     * preprocessing
     */
    Graph<Long, ObjectMap, NullValue> graph =
        new JSONDataSource(INPUT_PATH, Constants.INPUT, env)
            .getGraph(ObjectMap.class, NullValue.class);

    new JSONDataSink(INPUT_PATH, PREPROCESSING)
        .writeGraph(graph.run(new DefaultPreprocessing(domain, env)));
    env.execute(PRE_JOB);

    /**
     * decomposition with representative creation
     */
    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(INPUT_PATH, PREPROCESSING, env)
            .getGraph()
            .run(new TypeGroupBy(env)) // not needed? TODO
            .run(new SimSort(domain, 0.5, env))
            .getVertices()
            .runOperation(new RepresentativeCreator(domain));

    new JSONDataSink(INPUT_PATH, DECOMPOSITION)
        .writeVertices(vertices);
    env.execute(DEC_JOB);

  }

  @Override
  public String getDescription() {
    return null;
  }
}
