package org.mappinganalysis.model;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.junit.Test;
import org.mappinganalysis.MappingAnalysisExample;
import org.mappinganalysis.util.Utils;

public class EvalTest {
  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  /**
   * todo write test ;)
   */
  public void testExecuteEval() throws Exception {

    String graphPath = EvalTest.class
        .getResource("/data/preprocessing/general/").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph = Utils.readFromJSONFile(graphPath, env, true);

    MappingAnalysisExample.printEdgeSizes(graph);
  }
}
