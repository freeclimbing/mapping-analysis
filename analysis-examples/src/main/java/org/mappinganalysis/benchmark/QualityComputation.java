package org.mappinganalysis.benchmark;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.utils.EdgeComputationStrategy;
import org.mappinganalysis.graph.utils.EdgeComputationOnVerticesForKeySelector;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.SmallEdgeIdFirstMapFunction;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

/**
 * Not tested, most probably not working anymore.
 */
public class QualityComputation implements ProgramDescription {
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
  private static final Logger LOG = Logger.getLogger(QualityComputation.class);


  public static final String INPUT_STEP = "musicbrainz-input";
  public static final String MERGE_STEP = "musicbrainz-merged-clusters";
  public static final String QUALITY_JOB = "Quality";

  public static String INPUT_PATH;
  public static String VERTEX_FILE_NAME;
  public static String MODE;

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 3, "args[0]: input dir, " +
        "args[1]: file name, args[2]: all/merge mode selection" );
    INPUT_PATH = args[0];
    VERTEX_FILE_NAME = args[1];
    MODE = args[2];
    DataDomain domain = DataDomain.MUSIC;

    DataSet<Vertex<Long, ObjectMap>> inputVertices =
        new JSONDataSource(INPUT_PATH, INPUT_STEP, env)
            .getVertices();

      DataSet<Vertex<Long, ObjectMap>> clusters =
          new JSONDataSource(INPUT_PATH, MERGE_STEP, env)
          .getVertices();

    // result set of edges which is to be checked against perfect result
    DataSet<Edge<Long, NullValue>> edgeResultSet = clusters
        .runOperation(new EdgeComputationOnVerticesForKeySelector())
        .map(new SmallEdgeIdFirstMapFunction());

    long checkCount = edgeResultSet.count();
    LOG.info("checkcount: " + checkCount);


    DataSet<Edge<Long, NullValue>> goldEdges = inputVertices
        .runOperation(new EdgeComputationOnVerticesForKeySelector(
            new CcIdKeySelector(),
            EdgeComputationStrategy.ALL,
            true));
    long goldCount = goldEdges.count();
//    goldEdges.print();

    DataSet<Edge<Long, NullValue>> truePositives = goldEdges.join(edgeResultSet)
        .where(0, 1).equalTo(0, 1)
        .with(new JoinFunction<Edge<Long, NullValue>, Edge<Long, NullValue>, Edge<Long, NullValue>>() {
          @Override
          public Edge<Long, NullValue> join(Edge<Long, NullValue> left, Edge<Long, NullValue> right) throws Exception {
            return left;
          }
        })
        .distinct();

    long tpCount = truePositives.count();

    double precision = (double) tpCount / checkCount;
    double recall = (double) tpCount / goldCount;
    LOG.info("###############");
    LOG.info("Precision = tp count / check count = " + tpCount + " / " + checkCount + " = " + precision);
    LOG.info("###############");
    LOG.info("Recall = tp count / gold count = " + tpCount + " / " + goldCount + " = " + recall);
    LOG.info("###############");
    LOG.info("f1 = 2 * precision * recall / (precision + recall) = "
        + 2 * precision * recall / (precision + recall));
    LOG.info("###############");


    env.execute(QUALITY_JOB);
  }

  @Override
  public String getDescription() {
    return null;
  }
}
