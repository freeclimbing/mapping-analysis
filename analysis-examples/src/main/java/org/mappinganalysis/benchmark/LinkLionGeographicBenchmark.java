package org.mappinganalysis.benchmark;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.io.impl.DataDomain;
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

/**
 * TODO fix paths
 * TODO fix constant variables
 */
public class LinkLionGeographicBenchmark implements ProgramDescription {
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  public static final String PREPROCESSING = "settlement-preprocessing";
  public static final String CORRUPTED = "settlement-corrupted";
  public static final String DECOMPOSITION = "settlement-decomposition-representatives";
  public static final String MERGE = "settlement-merged-clusters";
  public static final String PRE_JOB = "Settlement Preprocessing";
  public static final String DEC_JOB = "Settlement Decomposition + Representatives";
  public static final String MER_JOB = "Settlement Merge";

  public static String INPUT_PATH;

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 2, "args[0]: input dir, " +
            "args[1]: min SimSort similarity (e.g., 0.7)");

    Double minSimilarity = Doubles.tryParse(args[1]);
    Constants.INPUT_DIR = args[0];
    Constants.SOURCE_COUNT = 5;

    Graph<Long, ObjectMap, ObjectMap> graph = new JSONDataSource(
        Constants.INPUT_DIR,
        Constants.LL_MODE.concat(Constants.INPUT_GRAPH),
        env)
        .getGraph(ObjectMap.class, NullValue.class)
        .run(new DefaultPreprocessing(true, env));;

    graph = graph
        .run(new TypeGroupBy(env))
        .run(new SimSort(minSimilarity, env));

    DataSet<Vertex<Long, ObjectMap>> representatives = graph.getVertices()
        .runOperation(new RepresentativeCreator(DataDomain.GEOGRAPHY));

    representatives = representatives
        .runOperation(new MergeInitialization(DataDomain.GEOGRAPHY))
        .runOperation(new MergeExecution(DataDomain.GEOGRAPHY, Constants.SOURCE_COUNT, env));

    new JSONDataSink(Constants.INPUT_DIR, "6-merged-clusters-json")
        .writeVertices(representatives);
//    Utils.writeVerticesToJSONFile(representatives, "6-merged-clusters-json");

    env.execute("Representatives and Merge");
  }

  @Override
  public String getDescription() {
    return LinkLionGeographicBenchmark.class.getName();
  }
}
