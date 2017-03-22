package org.mappinganalysis.model.functions.decomposition.representative;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Utils;

import static org.junit.Assert.assertTrue;

public class RepresentativeTest {
  private static final Logger LOG = Logger.getLogger(RepresentativeTest.class);
  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


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
