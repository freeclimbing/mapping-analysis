package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
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
public class SimilarClusterMergeMapFunction extends RichMapFunction<Triplet<Long, ObjectMap, ObjectMap>,
    Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(SimilarClusterMergeMapFunction.class);

  Vertex<Long, ObjectMap> reuseVertex;
  private LongCounter mergedClusterCount = new LongCounter();

  public SimilarClusterMergeMapFunction() {
    reuseVertex = new Vertex<>();
  }

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Constants.REFINEMENT_MERGE_ACCUMULATOR, mergedClusterCount);
  }

  @Override
  public Vertex<Long, ObjectMap> map(Triplet<Long, ObjectMap, ObjectMap> triplet) throws Exception {
    ObjectMap srcVal = triplet.getSrcVertex().getValue();
    ObjectMap trgVal = triplet.getTrgVertex().getValue();
    Set<Long> trgVertices = trgVal.getVerticesList();
    Set<Long> srcVertices = srcVal.getVerticesList();

    if (srcVertices.size() >= trgVertices.size()) {
      reuseVertex.setFields(triplet.getSrcVertex().getId(), compareAndReturnBest(srcVal, trgVal));
    } else {
      reuseVertex.setFields(triplet.getSrcVertex().getId(), compareAndReturnBest(trgVal, srcVal));
    }
    srcVertices.addAll(trgVertices);
    reuseVertex.getValue().put(Constants.CL_VERTICES, srcVertices);

    Set<String> resultOnts = Sets.newHashSet();
    resultOnts.addAll((Set<String>) srcVal.get(Constants.ONTOLOGIES));
    resultOnts.addAll((Set<String>) trgVal.get(Constants.ONTOLOGIES));
    reuseVertex.getValue().put(Constants.ONTOLOGIES, resultOnts);
    mergedClusterCount.add(1L);

    LOG.info("new cluster: " + reuseVertex.toString());
    return reuseVertex;
  }



  private ObjectMap compareAndReturnBest(ObjectMap priority, ObjectMap minor) {
    if (!priority.containsKey(Constants.LABEL) && minor.containsKey(Constants.LABEL)) {
      priority.put(Constants.LABEL, minor.get(Constants.LABEL));
    }
    if (!priority.hasTypeNoType(Constants.TYPE_INTERN) && minor.hasTypeNoType(Constants.TYPE_INTERN)) {
      priority.put(Constants.TYPE_INTERN, minor.get(Constants.TYPE_INTERN));
    }
    if (!priority.hasGeoPropertiesValid() && minor.hasGeoPropertiesValid()) {
      priority.put(Constants.LAT, minor.getLatitude());
      priority.put(Constants.LON, minor.getLongitude());
    }
    return priority;
  }
}
