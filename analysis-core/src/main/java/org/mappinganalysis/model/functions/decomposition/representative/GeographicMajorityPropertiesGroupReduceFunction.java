package org.mappinganalysis.model.functions.decomposition.representative;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.GeoCode;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.HashMap;
import java.util.Set;

/**
 * Merge properties for representative.
 */
public class GeographicMajorityPropertiesGroupReduceFunction
    extends RichGroupReduceFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(GeographicMajorityPropertiesGroupReduceFunction.class);
  private LongCounter representativeCount = new LongCounter();

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Constants.REPRESENTATIVE_ACCUMULATOR, representativeCount);
  }

  @Override
  public void reduce(
      Iterable<Vertex<Long, ObjectMap>> vertices,
      Collector<Vertex<Long, ObjectMap>> collector) throws Exception {
    Vertex<Long, ObjectMap> resultVertex = new Vertex<>(); // don't use reuseVertex here, not every property is rewritten
    ObjectMap resultProps = new ObjectMap(Constants.GEO);
    Set<Long> clusterVertices = Sets.newHashSet();
    Set<String> clusterDataSources = Sets.newHashSet();
    HashMap<String, Integer> labelMap = Maps.newHashMap();
    HashMap<String, GeoCode> geoMap = Maps.newHashMap();

    for (Vertex<Long, ObjectMap> vertex : vertices) {
      updateVertexId(resultVertex, vertex);
      updateClusterVertexIds(clusterVertices, vertex);
      updateClusterOntologies(clusterDataSources, vertex);

      addLabelToMap(labelMap, vertex);

      resultProps.addTypes(Constants.TYPE_INTERN, vertex.getValue().getTypes(Constants.TYPE_INTERN));
      addGeoToMap(geoMap, vertex);

      if (vertex.getValue().containsKey(Constants.OLD_HASH_CC)) {
        resultProps.setOldHashCcId(vertex.getValue().getOldHashCcId());
      }
    }

    resultProps.setGeoProperties(geoMap);
    resultProps.setLabel(Utils.getFinalValue(labelMap));
    resultProps.setBlockingKey(BlockingStrategy.STANDARD_BLOCKING);

    resultProps.setClusterDataSources(clusterDataSources);
    resultProps.setClusterVertices(clusterVertices);

    resultVertex.setValue(resultProps);
    representativeCount.add(1L);
    collector.collect(resultVertex);
  }

  private void updateClusterOntologies(
      Set<String> clusterOntologies,
      Vertex<Long, ObjectMap> currentVertex) {
    if (currentVertex.getValue().containsKey(Constants.DATA_SOURCE)) {
      clusterOntologies.add(currentVertex.getValue().getDataSource());
    }
    if (currentVertex.getValue().containsKey(Constants.DATA_SOURCES)) {
      clusterOntologies.addAll(currentVertex.getValue().getDataSourcesList());
    }
  }

  private void updateClusterVertexIds(
      Set<Long> clusterVertices,
      Vertex<Long, ObjectMap> currentVertex) {
    clusterVertices.add(currentVertex.getId());
    if (currentVertex.getValue().containsKey(Constants.CL_VERTICES)) {
      clusterVertices.addAll(currentVertex.getValue().getVerticesList());
    }
  }

  private void updateVertexId(Vertex<Long, ObjectMap> resultVertex, Vertex<Long, ObjectMap> currentVertex) {
    if (resultVertex.getId() == null || currentVertex.getId() < resultVertex.getId()) {
      resultVertex.setId(currentVertex.getId());
    }
  }

  private void addGeoToMap(
      HashMap<String, GeoCode> geoMap,
      Vertex<Long, ObjectMap> vertex) {
    if (vertex.getValue().hasGeoPropertiesValid()) {
      Double latitude = vertex.getValue().getLatitude();
      Double longitude = vertex.getValue().getLongitude();

      if (vertex.getValue().containsKey(Constants.DATA_SOURCE)) {
        geoMap.put(vertex.getValue().getDataSource(),
            new GeoCode(latitude, longitude));
      } else if (vertex.getValue().containsKey(Constants.DATA_SOURCES)) {
        for (String value : vertex.getValue().getDataSourcesList()) {
          geoMap.put(value, new GeoCode(latitude, longitude));
        }
      }
    }
  }

  private void addLabelToMap(
      HashMap<String, Integer> labelMap,
      Vertex<Long, ObjectMap> currentVertex) {
    if (currentVertex.getValue().containsKey(Constants.LABEL)) {
      String label = Utils.simplify(currentVertex.getValue().getLabel());
      if (labelMap.containsKey(label)) {
        int labelCount = labelMap.get(label);
        labelMap.put(label, labelCount + 1);
      } else {
        labelMap.put(label, 1);
      }
    }
  }
}
