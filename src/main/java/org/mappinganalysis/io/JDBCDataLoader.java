package org.mappinganalysis.io;import org.apache.flink.api.common.typeinfo.BasicTypeInfo;import org.apache.flink.api.java.DataSet;import org.apache.flink.api.java.ExecutionEnvironment;import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;import org.apache.flink.api.java.tuple.Tuple2;import org.apache.flink.api.java.tuple.Tuple3;import org.apache.flink.api.java.tuple.Tuple4;import org.apache.flink.api.java.typeutils.TupleTypeInfo;import org.apache.flink.graph.Edge;import org.apache.flink.graph.Vertex;import org.apache.flink.types.NullValue;import org.apache.log4j.Logger;import org.mappinganalysis.io.functions.BasicVertexCreator;import org.mappinganalysis.io.functions.FlinkEdgeCreator;import org.mappinganalysis.io.functions.FlinkPropertyMapper;import org.mappinganalysis.io.functions.PropertyCoGroupFunction;import org.mappinganalysis.model.FlinkProperty;import org.mappinganalysis.model.ObjectMap;import org.mappinganalysis.utils.Utils;import java.util.Locale;import java.util.ResourceBundle;/** * JDBC Data Loader for Flink */public class JDBCDataLoader {  private static final Logger LOG = Logger.getLogger(JDBCDataLoader.class);  private final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();  private final ResourceBundle prop;  public JDBCDataLoader() {    this.prop = ResourceBundle.getBundle(Utils.DB_PROPERY_FILE_NAME,        Locale.getDefault(),        Thread.currentThread().getContextClassLoader());  }  /**   * Parses and transforms database entities to tuples.   *   * @return DataSet containing all vertices in the graph   */  @SuppressWarnings("unchecked")  public DataSet<Vertex<Long, ObjectMap>> getVertices(String fullDbName) throws Exception {    LOG.info("Reading vertices");    DataSet<Tuple3<Integer, String, String>> input        = env.createInput(JDBCInputFormat.buildJDBCInputFormat()            .setDrivername("com.mysql.jdbc.Driver")            .setDBUrl(prop.getString(fullDbName))            .setUsername(prop.getString("user"))            .setPassword(prop.getString("pw"))            .setQuery("select id, url, ontID_fk from concept where ontID_fk in " +                " ('http://dbpedia.org/', 'http://sws.geonames.org/', 'http://linkedgeodata.org/', " +                " 'http://data.nytimes.com/', 'http://rdf.freebase.com/')")            .finish(),        new TupleTypeInfo(Tuple3.class,            BasicTypeInfo.INT_TYPE_INFO,            BasicTypeInfo.STRING_TYPE_INFO,            BasicTypeInfo.STRING_TYPE_INFO)    );    return input.map(new BasicVertexCreator())        .coGroup(getProperties(fullDbName))        .where(0)        .equalTo(0)        .with(new PropertyCoGroupFunction());  }  /**   * Parses and transforms the properties to {@link FlinkProperty} tuples.   *   * @return DataSet containing all properties from database.   */  @SuppressWarnings("unchecked")  public DataSet<FlinkProperty> getProperties(String fullDbName) {    LOG.info("Reading properties");    return env.createInput(JDBCInputFormat.buildJDBCInputFormat()            .setDrivername("com.mysql.jdbc.Driver")            .setDBUrl(prop.getString(fullDbName))            .setUsername(prop.getString("user"))            .setPassword(prop.getString("pw"))            .setQuery("select id, attName, attValue, attValueType from concept_attributes")            .finish(),        new TupleTypeInfo(Tuple4.class, BasicTypeInfo.INT_TYPE_INFO,            BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,            BasicTypeInfo.STRING_TYPE_INFO)    ).map(new FlinkPropertyMapper()).withForwardedFields("f1;f2;f3");  }  /**   * Parses and transforms the db edges to {@link Edge} tuples.   *   * @return DataSet containing all edges from the database table.   */  @SuppressWarnings("unchecked")  public DataSet<Edge<Long, NullValue>> getEdges(String fullDbName) {    LOG.info("Reading edges");    DataSet<Tuple2<Integer, Integer>> input        = env.createInput(JDBCInputFormat.buildJDBCInputFormat()            .setDrivername("com.mysql.jdbc.Driver")            .setDBUrl(prop.getString(fullDbName))            .setUsername(prop.getString("user"))            .setPassword(prop.getString("pw"))            .setQuery("select srcID, trgID from linksWithIDs")            .finish(),        new TupleTypeInfo(Tuple2.class,            BasicTypeInfo.INT_TYPE_INFO,            BasicTypeInfo.INT_TYPE_INFO)    );    return input.map(new FlinkEdgeCreator());  }}