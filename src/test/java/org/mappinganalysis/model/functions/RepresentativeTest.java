package org.mappinganalysis.model.functions;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.MappingAnalysisExampleTest;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.representative.MajorityPropertiesGroupReduceFunction;
import org.s1ck.gdl.GDLHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * work in progress
 */
public class RepresentativeTest {
  private static final Logger LOG = Logger.getLogger(RepresentativeTest.class);


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

  @Test
  public void repTest() throws Exception {
    GDLHandler handler = new GDLHandler.Builder().buildFromString(REPRESENTIVE);
    Graph<Long, ObjectMap, ObjectMap> graph = MappingAnalysisExampleTest.createTestGraph(handler);

    DataSet<Vertex<Long, ObjectMap>> mergedClusterVertices = graph.getVertices()
        .groupBy(new HashCcIdKeySelector())
        .reduceGroup(new MajorityPropertiesGroupReduceFunction());

    for (Vertex<Long, ObjectMap> vertex : mergedClusterVertices.collect()) {
      LOG.info("result: " + vertex);
    }
  }
}