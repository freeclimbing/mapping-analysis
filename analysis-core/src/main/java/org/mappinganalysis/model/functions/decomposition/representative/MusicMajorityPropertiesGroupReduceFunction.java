package org.mappinganalysis.model.functions.decomposition.representative;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.GeoCode;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.HashMap;
import java.util.Set;

/**
 * actual implementation for music data set
 */
public class MusicMajorityPropertiesGroupReduceFunction
    implements GroupReduceFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(MajorityPropertiesGroupReduceFunction.class);

  @Override
  public void reduce(
      Iterable<Vertex<Long, ObjectMap>> vertices,
      Collector<Vertex<Long, ObjectMap>> collector) throws Exception {
    Vertex<Long, ObjectMap> resultVertex = new Vertex<>(); // don't use reuseVertex here
    ObjectMap resultProps = new ObjectMap(Constants.MUSIC);
    Set<Long> clusterVertices = Sets.newHashSet();
    Set<String> clusterOntologies = Sets.newHashSet();
    HashMap<String, Integer> labelMap = Maps.newHashMap();
    HashMap<String, Integer> artistMap = Maps.newHashMap();
    HashMap<String, Integer> albumMap = Maps.newHashMap();
    HashMap<String, Integer> numberMap = Maps.newHashMap();
    HashMap<Integer, Integer> lengthMap = Maps.newHashMap();
    HashMap<Integer, Integer> yearMap = Maps.newHashMap();

    // get properties for all vertices
    for (Vertex<Long, ObjectMap> vertex : vertices) {
      updateVertexId(resultVertex, vertex);
      updateClusterVertexIds(clusterVertices, vertex);
      updateClusterOntologies(clusterOntologies, vertex);

      addAttributeToMap(Constants.LABEL, labelMap, vertex);
      addAttributeToMap(Constants.ARTIST, artistMap, vertex);
      addAttributeToMap(Constants.ALBUM, albumMap, vertex);
      addAttributeToMap(Constants.NUMBER, numberMap, vertex);
      addIntAttributeToMap(Constants.LENGTH, lengthMap, vertex);
      addIntAttributeToMap(Constants.YEAR, yearMap, vertex);


//      resultProps.addTypes(Constants.TYPE_INTERN, vertex.getValue().getTypes(Constants.TYPE_INTERN));

      if (vertex.getValue().containsKey(Constants.OLD_HASH_CC)) {
        resultProps.put(Constants.OLD_HASH_CC,
            vertex.getValue().get(Constants.OLD_HASH_CC));
      }
    }

    // decide for best one
    if (!labelMap.isEmpty()) {
      resultProps.put(Constants.LABEL, Utils.getFinalValue(labelMap, Constants.LABEL));
    }
    if (!artistMap.isEmpty()) {
      resultProps.put(Constants.ARTIST, Utils.getFinalValue(labelMap, Constants.ARTIST));
    }
    if (!albumMap.isEmpty()) {
      resultProps.put(Constants.ALBUM, Utils.getFinalValue(labelMap, Constants.ALBUM));
    }
    if (!numberMap.isEmpty()) {
      resultProps.put(Constants.NUMBER, Utils.getFinalValue(labelMap, Constants.NUMBER));
    }
    if (!lengthMap.isEmpty()) {
      resultProps.put(Constants.LENGTH, Utils.getFinalValue(labelMap, Constants.LENGTH));
    }
    if (!yearMap.isEmpty()) {
      resultProps.put(Constants.YEAR, Utils.getFinalValue(labelMap, Constants.YEAR));
    }

    resultProps.setClusterDataSources(clusterOntologies);
    resultProps.setClusterVertices(clusterVertices);

    resultVertex.setValue(resultProps);

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
//      if (!vertex.getValue().containsKey(Constants.DATA_SOURCE)
//          && !vertex.getValue().containsKey(Constants.DATA_SOURCES)) {
//        LOG.info("no/more ont but geo: " + vertex);
//      }

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

  private void addIntAttributeToMap(
      String attrName,
      HashMap<Integer, Integer> lengthMap,
      Vertex<Long, ObjectMap> currentVertex) {
    if (currentVertex.getValue().containsKey(attrName)) {
      int length = (int) currentVertex.getValue().get(attrName);
      if (lengthMap.containsKey(length)) {
        int lengthCount = lengthMap.get(length);
        lengthMap.put(length, lengthCount + 1);
      } else {
        lengthMap.put(length, 1);
      }
    }
  }

  private void addAttributeToMap(
      String attrName,
      HashMap<String, Integer> map,
      Vertex<Long, ObjectMap> currentVertex) {
    if (currentVertex.getValue().containsKey(attrName)) {
      String value = Utils.simplify(currentVertex.getValue().get(attrName).toString());
      if (map.containsKey(value)) {
        int attrCount = map.get(value);
        map.put(value, attrCount + 1);
      } else {
        map.put(value, 1);
      }
    }
  }
}