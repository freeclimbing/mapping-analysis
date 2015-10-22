package org.mappinganalysis.io;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.junit.Assert;
import org.mappinganalysis.model.FlinkEdge;
import org.mappinganalysis.model.FlinkVertex;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 * Flink Test - not working
 */
public class FlinkJDBCExampleTest {

//  protected void performTest(ExecutionEnvironment env, LDBCToFlink ldbcToFlink)
//      throws Exception {
//
//    List<FlinkVertex> vertexList = Lists.newArrayList();
//    List<FlinkEdge> edgeList = Lists.newArrayList();
//
//    ldbcToFlink.getVertices().output(
//        new LocalCollectionOutputFormat<>(vertexList));
//    ldbcToFlink.getEdges().output(
//        new LocalCollectionOutputFormat<>(edgeList));
//
//    env.execute();
//
//    Assert.assertEquals(80, vertexList.size());
//    Assert.assertEquals(230, edgeList.size());
//  }
}
