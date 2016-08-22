package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.flink.hadoop.shaded.com.google.protobuf.DescriptorProtos;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.HashMap;

import static org.junit.Assert.*;


public class MergeTest {
  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
  private static final Logger LOG = Logger.getLogger(MergeTest.class);

  @Test
  public void testInit() throws Exception {
    Constants.MIN_CLUSTER_SIM = 0.5;
    Constants.IGNORE_MISSING_PROPERTIES = true;
    Constants.MIN_LABEL_PRIORITY_SIM = 0.5;

    String graphPath = MergeTest.class
        .getResource("/data/representative/merge/").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices = Utils.readFromJSONFile(graphPath, env, true)
        .getVertices();

    vertices = Merge.init(vertices, null);
    for (Vertex<Long, ObjectMap> vertex : vertices.collect()) {
      LOG.info(vertex);
    }
  }

  @Test
  public void testExecute() throws Exception {

  }

  @Test
  public void testGetFinalValue() throws Exception {
    HashMap<String, Integer> map = Maps.newHashMap();
    map.put("Leipzig, Sachsen", 1);
    map.put("Leipzig Saxonia Germany", 1);
    map.put("Leipzig (Sachsen)", 1);

    String finalValue = Merge.getFinalValue(map, Constants.LABEL);
    assertTrue("Leipzig Saxonia Germany".equals(finalValue));

    map.put("Leipzig", 3);
    map.put("Lipsia Test", 2);
    finalValue = Merge.getFinalValue(map, Constants.LABEL);

    assertTrue("Leipzig".equals(finalValue));
  }
}