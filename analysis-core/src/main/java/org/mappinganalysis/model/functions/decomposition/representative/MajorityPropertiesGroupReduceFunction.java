package org.mappinganalysis.model.functions.decomposition.representative;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.GeoCode;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Merge properties for representative.
 */
public class MajorityPropertiesGroupReduceFunction
    extends RichGroupReduceFunction<Vertex<Long, ObjectMap>,
    Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(MajorityPropertiesGroupReduceFunction.class);
  private LongCounter representativeCount = new LongCounter();

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Constants.REPRESENTATIVE_ACCUMULATOR, representativeCount);
  }

  @Override
  public void reduce(Iterable<Vertex<Long, ObjectMap>> vertices,
                     Collector<Vertex<Long, ObjectMap>> collector) throws Exception {
    Vertex<Long, ObjectMap> resultVertex = new Vertex<>(); // don't use reuseVertex here
    ObjectMap resultProps = new ObjectMap();
    Set<Long> clusterVertices = Sets.newHashSet();
    Set<String> clusterOntologies = Sets.newHashSet();
    HashMap<String, Integer> labelMap = Maps.newHashMap();
    Set<String> clusterTypeSet = Sets.newHashSet();
    HashMap<String, GeoCode> geoMap = Maps.newHashMap();

    for (Vertex<Long, ObjectMap> vertex : vertices) {
      updateVertexId(resultVertex, vertex);
      updateClusterVertexIds(clusterVertices, vertex);
      updateClusterOntologies(clusterOntologies, vertex);

      addLabelToMap(labelMap, vertex);
      addTypesToSet(clusterTypeSet, vertex);
      addGeoToMap(geoMap, vertex);

      if (vertex.getValue().containsKey(Constants.OLD_HASH_CC)) {
        resultProps.put(Constants.OLD_HASH_CC,
            vertex.getValue().get(Constants.OLD_HASH_CC));
      }
    }

    if (!geoMap.isEmpty()) {
      resultProps.setGeoProperties(geoMap);
    }
    if (!labelMap.isEmpty()) {
      resultProps.put(Constants.LABEL, getFinalValue(labelMap, Constants.LABEL));
    }
    if (!clusterTypeSet.isEmpty()) {
//      Set<String> finalValue = getFinalValue(typeSet, Constants.TYPE_INTERN);
      resultProps.put(Constants.TYPE_INTERN, clusterTypeSet);
    }
    resultProps.put(Constants.ONTOLOGIES, clusterOntologies);
    resultProps.put(Constants.CL_VERTICES, clusterVertices);

    resultVertex.setValue(resultProps);
    representativeCount.add(1L);

    collector.collect(resultVertex);
  }

  private void updateClusterOntologies(Set<String> clusterOntologies, Vertex<Long, ObjectMap> currentVertex) {
    if (currentVertex.getValue().containsKey(Constants.ONTOLOGY)) {
      clusterOntologies.add(currentVertex.getValue().getOntology());
    }
    if (currentVertex.getValue().containsKey(Constants.ONTOLOGIES)) {
      clusterOntologies.addAll(currentVertex.getValue().getOntologiesList());
    }
  }

  private void updateClusterVertexIds(Set<Long> clusterVertices, Vertex<Long, ObjectMap> currentVertex) {
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

  private void addGeoToMap(HashMap<String, GeoCode> geoMap, Vertex<Long, ObjectMap> vertex) {
    if (vertex.getValue().hasGeoPropertiesValid()) {
      if (!vertex.getValue().containsKey(Constants.ONTOLOGY)
          && !vertex.getValue().containsKey(Constants.ONTOLOGIES)) {
        LOG.info("no/more ont but geo: " + vertex);
      }

      Double latitude = vertex.getValue().getLatitude();
      Double longitude = vertex.getValue().getLongitude();
      if (vertex.getValue().containsKey(Constants.ONTOLOGY)) {
        geoMap.put(vertex.getValue().getOntology(),
            new GeoCode(latitude, longitude));
      } else if (vertex.getValue().containsKey(Constants.ONTOLOGIES)) {
        for (String value : vertex.getValue().getOntologiesList()) {
          geoMap.put(value, new GeoCode(latitude, longitude));
        }
      }
    }
  }

  private void addLabelToMap(HashMap<String, Integer> labelMap, Vertex<Long, ObjectMap> currentVertex) {
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

  private void addTypesToSet(Set<String> cTypeSet,
                             Vertex<Long, ObjectMap> vertex) {
    if (!vertex.getValue().hasTypeNoType(Constants.TYPE_INTERN)) {
      cTypeSet.addAll(vertex.getValue().getTypes(Constants.TYPE_INTERN));
    }
  }

  /**
   * Get the hash map value having the highest count of occurrence.
   * For label property, if count is equal, a longer string is preferred.
   * @param map containing value options with count of occurrence
   * @param propertyName special behavior if label
   * @return resulting value
   */
  private <T> T getFinalValue(HashMap<T, Integer> map, String propertyName) {
    Map.Entry<T, Integer> finalEntry = null;
    for (Map.Entry<T, Integer> entry : map.entrySet()) {
      if (finalEntry == null || Ints.compare(entry.getValue(), finalEntry.getValue()) > 0) {
        finalEntry = entry;
      } else if (entry.getKey() instanceof String && propertyName.equals(Constants.LABEL)) {
        String labelKey = entry.getKey().toString();
        if (labelKey.length() > finalEntry.getKey().toString().length()) {
          finalEntry = entry;
        }
      }
    }

    checkArgument(finalEntry != null, "Entry must not be null");
    return finalEntry.getKey();
  }
}
