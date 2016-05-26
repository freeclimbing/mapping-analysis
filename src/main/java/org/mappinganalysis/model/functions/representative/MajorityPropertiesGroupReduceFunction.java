package org.mappinganalysis.model.functions.representative;

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
import org.mappinganalysis.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Merge properties for representative.
 */
public class MajorityPropertiesGroupReduceFunction extends RichGroupReduceFunction<Vertex<Long, ObjectMap>,
    Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(MajorityPropertiesGroupReduceFunction.class);
  private LongCounter representativeCount = new LongCounter();

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Utils.REPRESENTATIVE_ACCUMULATOR, representativeCount);
  }

  @Override
  public void reduce(Iterable<Vertex<Long, ObjectMap>> vertices,
                     Collector<Vertex<Long, ObjectMap>> collector) throws Exception {
    Vertex<Long, ObjectMap> resultVertex = new Vertex<>(); // don't use reuseVertex here
    ObjectMap resultProps = new ObjectMap();
    Set<Long> clusterVertices = Sets.newHashSet();
    Set<String> clusterOntologies = Sets.newHashSet();
    HashMap<String, Integer> labelMap = Maps.newHashMap();
    HashMap<Set<String>, Integer> typeMap = Maps.newHashMap();
    HashMap<String, GeoCode> geoMap = Maps.newHashMap();

    for (Vertex<Long, ObjectMap> currentVertex : vertices) {
      updateVertexId(resultVertex, currentVertex);
      updateClusterVertexIds(clusterVertices, currentVertex);
      updateClusterOntologies(clusterOntologies, currentVertex);

      addLabelToMap(labelMap, currentVertex);
      addTypeToMap(typeMap, currentVertex);
      addGeoToMap(geoMap, currentVertex);

      if (currentVertex.getValue().containsKey(Utils.OLD_HASH_CC)) {
        resultProps.put(Utils.OLD_HASH_CC, currentVertex.getValue().get(Utils.OLD_HASH_CC));
      }
    }

    if (!geoMap.isEmpty()) {
      resultProps.setGeoProperties(geoMap);
    }
    if (!labelMap.isEmpty()) {
      resultProps.put(Utils.LABEL, getFinalValue(labelMap, Utils.LABEL));
    }
    if (!typeMap.isEmpty()) {
      Set<String> finalValue = getFinalValue(typeMap, Utils.TYPE_INTERN);
      resultProps.put(Utils.TYPE_INTERN, finalValue);
    }
    resultProps.put(Utils.ONTOLOGIES, clusterOntologies);
    resultProps.put(Utils.CL_VERTICES, clusterVertices);

    resultVertex.setValue(resultProps);
    representativeCount.add(1L);
    collector.collect(resultVertex);
  }

  private void updateClusterOntologies(Set<String> clusterOntologies, Vertex<Long, ObjectMap> currentVertex) {
    if (currentVertex.getValue().containsKey(Utils.ONTOLOGY)) {
      clusterOntologies.add(currentVertex.getValue().get(Utils.ONTOLOGY).toString());
    }
    if (currentVertex.getValue().containsKey(Utils.ONTOLOGIES)) {
      clusterOntologies.addAll((Set<String>) currentVertex.getValue().get(Utils.ONTOLOGIES));
    }
  }

  private void updateClusterVertexIds(Set<Long> clusterVertices, Vertex<Long, ObjectMap> currentVertex) {
    clusterVertices.add(currentVertex.getId());
    if (currentVertex.getValue().containsKey(Utils.CL_VERTICES)) {
      clusterVertices.addAll((Set<Long>) currentVertex.getValue().get(Utils.CL_VERTICES));
    }
  }

  private void updateVertexId(Vertex<Long, ObjectMap> resultVertex, Vertex<Long, ObjectMap> currentVertex) {
    if (resultVertex.getId() == null || currentVertex.getId() < resultVertex.getId()) {
      resultVertex.setId(currentVertex.getId());
    }
  }

  private void addGeoToMap(HashMap<String, GeoCode> geoMap, Vertex<Long, ObjectMap> vertex) {
    if (vertex.getValue().hasGeoProperties()) {
      if (!vertex.getValue().containsKey(Utils.ONTOLOGY) && !vertex.getValue().containsKey(Utils.ONTOLOGIES)) {
        LOG.info("no/more ont but geo: " + vertex);
      }

      if (vertex.getValue().containsKey(Utils.ONTOLOGY)) {
        geoMap.put(vertex.getValue().get(Utils.ONTOLOGY).toString(),
            new GeoCode(vertex.getValue().getLatitude(), vertex.getValue().getLongitude()));
      } else if (vertex.getValue().containsKey(Utils.ONTOLOGIES)) {
        for (String value : (Set<String>) vertex.getValue().get(Utils.ONTOLOGIES)) {
          geoMap.put(value, new GeoCode(vertex.getValue().getLatitude(), vertex.getValue().getLongitude()));
        }
      }
    }
  }

  private void addLabelToMap(HashMap<String, Integer> labelMap, Vertex<Long, ObjectMap> currentVertex) {
    if (currentVertex.getValue().containsKey(Utils.LABEL)) {
      String label = Utils.simplify(currentVertex.getValue().get(Utils.LABEL).toString());
      if (labelMap.containsKey(label)) {
        int labelCount = labelMap.get(label);
        labelMap.put(label, labelCount + 1);
      } else {
        labelMap.put(label, 1);
      }
    }
  }

  private void addTypeToMap(HashMap<Set<String>, Integer> typeMap, Vertex<Long, ObjectMap> currentVertex) {
    if (!currentVertex.getValue().hasTypeNoType(Utils.TYPE_INTERN)) {
      Set<String> types = currentVertex.getValue().getTypes(Utils.TYPE_INTERN);

      if (typeMap.containsKey(types)) {
        int labelCount = typeMap.get(types);
        typeMap.put(types, labelCount + 1);
      } else {
        typeMap.put(types, 1);
      }
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
      } else if (entry.getKey() instanceof String && propertyName.equals(Utils.LABEL)) {
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
