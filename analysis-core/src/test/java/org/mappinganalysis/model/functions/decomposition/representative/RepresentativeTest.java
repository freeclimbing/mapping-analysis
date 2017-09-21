package org.mappinganalysis.model.functions.decomposition.representative;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.MappingAnalysisExampleTest;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.functions.keyselector.HashCcIdKeySelector;
import org.s1ck.gdl.GDLHandler;

import static org.junit.Assert.assertTrue;

public class RepresentativeTest {
  private static final Logger LOG = Logger.getLogger(RepresentativeTest.class);
  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  // todo wrong notype
  private static final String REPRESENTIVE = "g[" +
      "(v1 {label = \"Santa Barbara (Calif)\", typeIntern = \"no_type_available\", hashCc = 6551576449116476652L, " +
      "ontology = \"http://data.nytimes.com/\", lon = -119.698D, lat = 34.4208D})" +
      "(v2 {label = \"Ibiza\", typeIntern = \"Island\", hashCc = 6090427849451902653L, " +
      "ontology = \"http://sws.geonames.org/\", lat = 38.9744D, lon = 1.40007D})" +
      "(v3 {label = \"ibiza\", typeIntern = \"Island\", hashCc = 6090427849451902653L, " +
      "ontology = \"http://rdf.freebase.com/\", lat = 38.98D, lon = 1.43D})" +
      "(v4 {label = \"Ibiza\", typeIntern = \"Island\", hashCc = 6090427849451902653L, ontology = \"http://dbpedia.org/\", " +
      "lat = 38.98D, lon = 1.43D})" +
      "(v5 {label = \"Ibiza (Spain)\", typeIntern = \"Island\", hashCc = 6090427849451902653L, " +
      "ontology = \"http://data.nytimes.com/\", lat = 38.9744D, lon = 1.40007D})" +
      "(v5)-[e1:sameAs {aggSimValue = 0.9428090453147888D}]->(v2)" +
      "(v5)-[e1:sameAs {aggSimValue = 0.9428090453147888D}]->(v3)" +
      "(v5)-[e2:sameAs {aggSimValue = 0.9428090453147888D}]->(v4)]";



//  @Test
//  public void repTest() throws Exception {
//    String graphPath = RepresentativeTest.class
//        .getResource("/data/preprocessing/general/").getFile();
//    Graph<Long, ObjectMap, ObjectMap> graph = Utils.readFromJSONFile(graphPath, env, true);
//
//    DataSet<Vertex<Long, ObjectMap>> mergedClusterVertices = graph.getVertices()
//        .groupBy(new HashCcIdKeySelector())
//        .reduceGroup(new MajorityPropertiesGroupReduceFunction());
//
//    for (Vertex<Long, ObjectMap> vertex : mergedClusterVertices.collect()) {
//      LOG.info("result: " + vertex);
//    }
//  }


  // todo use representative haiti test files
  @Test
  public void repTest() throws Exception {
    GDLHandler handler = new GDLHandler.Builder().buildFromString(REPRESENTIVE);
    Graph<Long, ObjectMap, ObjectMap> graph = MappingAnalysisExampleTest.createTestGraph(handler);

    DataSet<Vertex<Long, ObjectMap>> mergedClusterVertices = graph.getVertices()
        .groupBy(new HashCcIdKeySelector())
        .reduceGroup(new GeographicMajorityPropertiesGroupReduceFunction());

    for (Vertex<Long, ObjectMap> vertex : mergedClusterVertices.collect()) {
      LOG.info("result: " + vertex);
    }
  }

  @Test
  public void geoEntityValidTest() throws Exception {
    String graphPath = RepresentativeTest.class.getResource("/data/simsort/").getFile();

    Graph<Long, ObjectMap, ObjectMap> graph = new JSONDataSource(graphPath, true, env).getGraph();

    graph.filterOnVertices(new FilterFunction<Vertex<Long, ObjectMap>>() {
      @Override
      public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
        if (vertex.getValue().hasGeoPropertiesValid()) {
          Double latitude = vertex.getValue().getLatitude();
          Double longitude = vertex.getValue().getLongitude();
          if (!Utils.isValidLatitude(latitude)) {
            LOG.info("bad latitude: " + latitude);
          }
          assertTrue(Utils.isValidLatitude(latitude) || latitude == null);

          if (!Utils.isValidLongitude(longitude)) {
            LOG.info("bad longitude: " + longitude);
          }
          assertTrue(Utils.isValidLongitude(longitude) || longitude == null);

          return true;
        } else {
          return false;
        }
      }
    }).getVertices().first(1).collect();
  }

}
