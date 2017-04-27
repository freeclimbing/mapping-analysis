package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;

import static org.junit.Assert.*;

public class MergeTupleCreatorTest {
  private static ExecutionEnvironment env;
  private static final Logger LOG = Logger.getLogger(MergeTest.class);

  /**
   * 1. part
   * @throws Exception
   */
  @Test
  public void testMergeTupleCreator() throws Exception {
    env = TestBase.setupLocalEnvironment();
    TestBase.setupConstants();

    String graphPath = MergeTest.class
        .getResource("/data/representative/mergeExec/").getFile();

    DataSet<MergeGeoTuple> result = new JSONDataSource(graphPath, true, env)
        .getVertices()
        .map(new AddShadingTypeMapFunction())
        .map(new MergeTupleCreator(DataDomain.GEOGRAPHY));

    assertTrue(11 == result.collect().size());
  }

  /**
   * 2. part
   * @throws Exception
   */
  @Test
  public void testMergeTripletCreator() throws Exception {
    env = TestBase.setupLocalEnvironment();
    TestBase.setupConstants();

    String graphPath = MergeTest.class
        .getResource("/data/representative/mergeExec/").getFile();

    DataSet<MergeGeoTuple> result = new JSONDataSource(graphPath, true, env)
        .getVertices()
        .map(new AddShadingTypeMapFunction())
        .map(new MergeTupleCreator(DataDomain.GEOGRAPHY));

    DataSet<MergeTriplet<MergeGeoTuple>> initialWorkingSet = result
        .filter(new SourceCountRestrictionFilter<>(DataDomain.GEOGRAPHY, 5))
        .groupBy(7) // TODO
        .reduceGroup(new MergeTripletCreator<>(DataDomain.GEOGRAPHY, 5));

//    initialWorkingSet.print();
  }


}