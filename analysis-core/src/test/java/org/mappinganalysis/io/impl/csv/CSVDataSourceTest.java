package org.mappinganalysis.io.impl.csv;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.benchmark.MusicbrainzBenchmarkTest;
import org.mappinganalysis.model.ObjectMap;

import static org.junit.Assert.assertEquals;

public class CSVDataSourceTest {
  private static ExecutionEnvironment env = TestBase.setupLocalEnvironment();


  @Test
  public void getGraphTest() throws Exception {
    final String path = MusicbrainzBenchmarkTest.class
        .getResource("/data/musicbrainz/").getFile();
    final String vertexFileName = "basic/music-test.csv";
    Graph<Long, ObjectMap, NullValue> baseGraph
        = new CSVDataSource(path, vertexFileName, env)
        .getGraph();

    assertEquals(0, baseGraph.getEdges().count());
  }
}