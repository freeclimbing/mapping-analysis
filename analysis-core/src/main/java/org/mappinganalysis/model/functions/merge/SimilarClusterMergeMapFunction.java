package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

import java.util.Set;

/**
 * Merge similar triplets into a vertex containing information of
 * both source and target triplet vertex.
 */
@Deprecated
public class SimilarClusterMergeMapFunction
    implements MapFunction<Triplet<Long, ObjectMap, ObjectMap>, Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(SimilarClusterMergeMapFunction.class);
  Vertex<Long, ObjectMap> reuseVertex;

  public SimilarClusterMergeMapFunction() {
    reuseVertex = new Vertex<>();
  }

  @Override
  public Vertex<Long, ObjectMap> map(Triplet<Long, ObjectMap, ObjectMap> triplet) throws Exception {
    ObjectMap srcVal = triplet.getSrcVertex().getValue();
    ObjectMap trgVal = triplet.getTrgVertex().getValue();
    Set<Long> trgVertices = trgVal.getVerticesList();
    Set<Long> srcVertices = srcVal.getVerticesList();

//    if (triplet.getSrcVertex().getId() == 1981L || triplet.getSrcVertex().getId() == 1982L
//        && triplet.getTrgVertex().getId() == 1981L || triplet.getTrgVertex().getId() == 1982L) {
//      LOG.info("###wei: " + triplet.toString());
//    }

    if (srcVertices.size() >= trgVertices.size()) {
      reuseVertex.setFields(triplet.getSrcVertex().getId(), compareAndReturnBest(srcVal, trgVal));
    } else {
      reuseVertex.setFields(triplet.getSrcVertex().getId(), compareAndReturnBest(trgVal, srcVal));
    }
    srcVertices.addAll(trgVertices);
    reuseVertex.getValue().put(Constants.CL_VERTICES, srcVertices);

    Set<String> resultOnts = Sets.newHashSet();
    resultOnts.addAll(srcVal.getOntologiesList());
    resultOnts.addAll(trgVal.getOntologiesList());
    reuseVertex.getValue().put(Constants.ONTOLOGIES, resultOnts);

    LOG.info("############new cluster: " + reuseVertex.toString());
    return reuseVertex;
  }

  private ObjectMap compareAndReturnBest(ObjectMap priority, ObjectMap minor) {
    if (!priority.containsKey(Constants.LABEL)
        && minor.containsKey(Constants.LABEL)) {
      priority.put(Constants.LABEL, minor.getLabel());
    }
    if (priority.hasTypeNoType(Constants.TYPE_INTERN)
        && !minor.hasTypeNoType(Constants.TYPE_INTERN)) {
      priority.put(Constants.TYPE_INTERN, minor.getTypes(Constants.TYPE_INTERN));
    }
    if (!priority.hasGeoPropertiesValid()
        && minor.hasGeoPropertiesValid()) {
      priority.put(Constants.LAT, minor.getLatitude());
      priority.put(Constants.LON, minor.getLongitude());
    }
    return priority;
  }
}
