package org.mappinganalysis.io;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.flink.graph.Edge;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.FlinkProperty;
import org.mappinganalysis.model.FlinkVertex;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * JDBC Data Loader for Flink
 */
public class JDBCDataLoader {
  private static final Logger LOG = Logger.getLogger(JDBCDataLoader.class);
  private final ExecutionEnvironment env;
  private final ResourceBundle prop;

  public JDBCDataLoader(ExecutionEnvironment env) {
    this.env = env;
    this.prop = ResourceBundle.getBundle("db", Locale.getDefault(), Thread.currentThread().getContextClassLoader());
  }

  /**
   * Parses and transforms the LDBC vertex files to {@link FlinkVertex} tuples.
   *
   * @return DataSet containing all vertices in the LDBC graph
   */
  @SuppressWarnings("unchecked")
  public DataSet<FlinkVertex> getVertices() throws Exception {
    LOG.info("Reading vertices");

    DataSet<Tuple3<Integer, String, String>> input
        = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
            .setDrivername("com.mysql.jdbc.Driver")
            .setDBUrl(prop.getString("dbURLfull"))
            .setUsername(prop.getString("user"))
            .setPassword(prop.getString("pw"))
            .setQuery("select id, url, ontID_fk from concept")
            .finish(),
        new TupleTypeInfo(Tuple3.class, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
    );

    DataSet<FlinkVertex> vertices = input.map(new FlinkVertexCreator());

    vertices = vertices
        .coGroup(getProperties())
        .where(0)
        .equalTo(0)
        .with(new CoGroupFunction<FlinkVertex, FlinkProperty, FlinkVertex>() {
          public void coGroup(Iterable<FlinkVertex> vertices,
                              Iterable<FlinkProperty> properties,
                              Collector<FlinkVertex> out) throws Exception {
            FlinkVertex result = vertices.iterator().next();
            Map<String, Object> resultingProperties = result.getProperties();

            for (FlinkProperty property : properties) {
              resultingProperties.put(property.getPropertyKey(), property.getPropertyValue());
            }

            result.setProperties(resultingProperties);
            out.collect(result);
          }
        });

    return vertices;
  }

  /**
   * Parses and transforms the properties to {@link FlinkProperty} tuples.
   *
   * @return DataSet containing all properties from database.
   */
  @SuppressWarnings("unchecked")
  public DataSet<FlinkProperty> getProperties() {
    LOG.info("Reading properties");
    return env.createInput(JDBCInputFormat.buildJDBCInputFormat()
            .setDrivername("com.mysql.jdbc.Driver")
            .setDBUrl(prop.getString("dbURLfull"))
            .setUsername(prop.getString("user"))
            .setPassword(prop.getString("pw"))
            .setQuery("select id, attName, attValue from concept_attributes")
            .finish(),
        new TupleTypeInfo(FlinkProperty.class, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
    );
  }

  /**
   * Parses and transforms the db edges to {@link Edge} tuples.
   *
   * @return DataSet containing all edges from the database table.
   */
  @SuppressWarnings("unchecked")
  public DataSet<Edge<Integer, NullValue>> getEdges() {
    LOG.info("Reading edges");
    DataSet<Tuple2<Integer, Integer>> input
        = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
            .setDrivername("com.mysql.jdbc.Driver")
            .setDBUrl(prop.getString("dbURLfull"))
            .setUsername(prop.getString("user"))
            .setPassword(prop.getString("pw"))
            .setQuery("select srcID, trgID from linksWithIDs")
            .finish(),
        new TupleTypeInfo(Tuple2.class, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO)
    );

    return input.map(new FlinkEdgeCreator());
  }

  private static class FlinkEdgeCreator implements MapFunction<Tuple2<Integer, Integer>, Edge<Integer, NullValue>> {

    private final Edge<Integer, NullValue> reuseEdge;

    public FlinkEdgeCreator() {
      reuseEdge = new Edge<>();
    }

    public Edge<Integer, NullValue> map(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
      reuseEdge.setSource(integerIntegerTuple2.f0);
      reuseEdge.setTarget(integerIntegerTuple2.f1);
      reuseEdge.setValue(NullValue.getInstance());
      return reuseEdge;
    }
  }

  private static class FlinkVertexCreator implements MapFunction<Tuple3<Integer, String, String>, FlinkVertex> {

    private final FlinkVertex reuseVertex;

    private FlinkVertexCreator() {
      reuseVertex = new FlinkVertex();
    }

    public FlinkVertex map(Tuple3<Integer, String, String> tuple3) throws Exception {
      reuseVertex.setVertexId(tuple3.f0);
      reuseVertex.setLabel(tuple3.f1);
      HashMap<String, Object> propMap = new HashMap<>();
      propMap.put("ontology", tuple3.f2);
      reuseVertex.setProperties(propMap);
      return reuseVertex;
    }
  }
}
