package org.mappinganalysis.io.impl.jdbc;

import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.functions.BasicVertexCreator;
import org.mappinganalysis.io.functions.FlinkEdgeCreator;
import org.mappinganalysis.io.functions.FlinkPropertyMapper;
import org.mappinganalysis.model.FlinkProperty;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

import java.util.Locale;
import java.util.ResourceBundle;

/**
 * Needs rework, not currently used. Stopped working with JDBC
 * because it's no parallel implementation for JDBC.
 */
@Deprecated
public class JDBCDataSource {
  private static final Logger LOG = Logger.getLogger(JDBCDataSource.class);

  private ExecutionEnvironment env;
  private final ResourceBundle prop;

  public JDBCDataSource(ExecutionEnvironment env) {
    this.env = env;
    this.prop = ResourceBundle.getBundle(Constants.DB_PROPERY_FILE_NAME,
        Locale.getDefault(),
        Thread.currentThread().getContextClassLoader());
  }


  /**
   * Parses and transforms database entities to tuples. DB data retrieval no longer recommended, it's too slow.
   *
   * @return DataSet containing all vertices in the graph
   */
  @SuppressWarnings("unchecked")
  @Deprecated
  public DataSet<Vertex<Long, ObjectMap>> getVerticesFromDb(String fullDbName) throws Exception {

    DataSet<Tuple3<Integer, String, String>> input
        = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
            .setDrivername("com.mysql.jdbc.Driver")
            .setDBUrl(prop.getString(fullDbName))
            .setUsername(prop.getString("user"))
            .setPassword(prop.getString("pw"))
            .setQuery("select id, url, ontID_fk from concept where ontID_fk in " +
                " ('http://dbpedia.org/', 'http://sws.geonames.org/', 'http://linkedgeodata.org/', " +
                " 'http://data.nytimes.com/', 'http://rdf.freebase.com/')")
            .finish(),
        new TupleTypeInfo(Tuple3.class,
            BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO)
    );

    return getProperties(fullDbName)
        .groupBy(0)
        .reduceGroup(new PropertyVertexCreator())
        .leftOuterJoin(input.map(new BasicVertexCreator()))
        .where(0)
        .equalTo(0)
        .with(new FlatJoinFunction<Vertex<Long,ObjectMap>,
            Vertex<Long, ObjectMap>,
            Vertex<Long, ObjectMap>>() {
          @Override
          public void join(Vertex<Long, ObjectMap> left,
                           Vertex<Long, ObjectMap> right,
                           Collector<Vertex<Long, ObjectMap>> collector) throws Exception {
            left.getValue().addProperty(Constants.DATA_SOURCE,
                right.getValue().get(Constants.DATA_SOURCE));

            collector.collect(left);
          }
        });
  }

  /**
   * Parses and transforms the properties to {@link FlinkProperty} tuples.
   * DB data retrieval no longer recommended, it's too slow.
   *
   * @return DataSet containing all properties from database.
   */
  @SuppressWarnings("unchecked")
  @Deprecated
  public DataSet<FlinkProperty> getProperties(String fullDbName) {

    return env.createInput(JDBCInputFormat.buildJDBCInputFormat()
            .setDrivername("com.mysql.jdbc.Driver")
            .setDBUrl(prop.getString(fullDbName))
            .setUsername(prop.getString("user"))
            .setPassword(prop.getString("pw"))
            .setQuery("select id, attName, attValue, attValueType from concept_attributes")
            .finish(),
        new TupleTypeInfo(Tuple4.class, BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO)
    ).map(new FlinkPropertyMapper()).withForwardedFields("f1;f2;f3");
  }

  /**
   * Parses and transforms the db edges to {@link Edge} tuples. DB data retrieval no longer recommended, it's too slow.
   *
   * @return DataSet containing all edges from the database table.
   */
  @SuppressWarnings("unchecked")
  @Deprecated
  public DataSet<Edge<Long, NullValue>> getEdges(String fullDbName) {
    DataSet<Tuple2<Integer, Integer>> input
        = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
            .setDrivername("com.mysql.jdbc.Driver")
            .setDBUrl(prop.getString(fullDbName))
            .setUsername(prop.getString("user"))
            .setPassword(prop.getString("pw"))
            .setQuery("select srcID, trgID from linksWithIDs")
            .finish(),
        new TupleTypeInfo(Tuple2.class,
            BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.INT_TYPE_INFO)
    );

    return input.map(new FlinkEdgeCreator());
  }

  private static class PropertyVertexCreator
      implements GroupReduceFunction<FlinkProperty, Vertex<Long, ObjectMap>> {
    private final Vertex<Long, ObjectMap> reuseVertex;

    public PropertyVertexCreator() {
      reuseVertex = new Vertex<>();
      reuseVertex.setValue(new ObjectMap(Constants.GEO));
    }
    @Override
    public void reduce(Iterable<FlinkProperty> properties,
                       Collector<Vertex<Long, ObjectMap>> collector) throws Exception {

      boolean isIdSet = false;
      for (FlinkProperty property : properties) {
        if (!isIdSet) {
          reuseVertex.setId(property.getVertexId());
          isIdSet = true;
        }
        Object value = property.getPropertyValue();
        String key = property.getPropertyKey();
        if (reuseVertex.getValue().containsKey(Constants.LAT)
            || reuseVertex.getValue().containsKey(Constants.LON)) {
          continue;
        }

        if (property.getPropertyType().equals("double")) {
          value = Doubles.tryParse(value.toString());
        } else if (property.getPropertyType().equals("string")) {
          value = value.toString();
        }

        reuseVertex.getValue().addProperty(key, value);
      }

      if (reuseVertex.getValue().containsKey(Constants.LABEL)) {
        collector.collect(reuseVertex);
      }
    }
  }

}
