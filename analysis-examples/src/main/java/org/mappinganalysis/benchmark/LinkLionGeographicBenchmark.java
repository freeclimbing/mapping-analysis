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
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreatorMultiMerge;
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

  private static final String PREPROCESSING = "geographic-preprocessing";
  private static final String DECOMPOSITION = "geographic-decomposition-representatives";
  private static final String MERGE = "geographic-merged-clusters";
  private static final String PRE_JOB = "Geographic Preprocessing";
  private static final String DEC_JOB = "Geographic Decomposition + Representatives";
  private static final String MER_JOB = "Geographic Merge";

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 2,
        "args[0]: input dir, " +
            "args[1]: min SimSort similarity (e.g., 0.7)");

    Double minSimilarity = Doubles.tryParse(args[1]);
    final String inputPath = args[0];
    Constants.SOURCE_COUNT = 5;

    Graph<Long, ObjectMap, ObjectMap> graph = new JSONDataSource(
        // TODO check path
        Constants.INPUT_PATH,
        Constants.LL_MODE.concat(Constants.INPUT_GRAPH),
        env)
        .getGraph(ObjectMap.class, NullValue.class)
        .run(new DefaultPreprocessing(true, env));

    new JSONDataSink(inputPath, PREPROCESSING)
        .writeGraph(graph);
    env.execute(PRE_JOB);

    /*
      Decomposition
     */
    DataSet<Vertex<Long, ObjectMap>> representatives =
        new JSONDataSource(inputPath, PREPROCESSING, env)
            .getGraph()
            .run(new TypeGroupBy(env))
            .run(new SimSort(DataDomain.GEOGRAPHY, minSimilarity, env))
            .getVertices()
            .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.GEOGRAPHY));

    new JSONDataSink(inputPath, DECOMPOSITION)
        .writeVertices(representatives);
    env.execute(DEC_JOB);

    /*
      Merge
     */
    DataSet<Vertex<Long, ObjectMap>> mergedVertices =
        new JSONDataSource(inputPath, DECOMPOSITION, env)
            .getVertices()
            .runOperation(new MergeInitialization(DataDomain.GEOGRAPHY))
            .runOperation(new MergeExecution(DataDomain.GEOGRAPHY, 0.5, Constants.SOURCE_COUNT, env));

    new JSONDataSink(inputPath, MERGE)
        .writeVertices(mergedVertices);
    env.execute(MER_JOB);
  }

  @Override
  public String getDescription() {
    return LinkLionGeographicBenchmark.class.getName();
  }
}
