package org.mappinganalysis.model.functions.representative;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

import java.util.Set;

/**
 * Merge similar triplets into a vertex containing information of
 * both source and target triplet vertex.
 */
public class SimilarClusterMergeMapFunction extends RichMapFunction<Triplet<Long, ObjectMap, ObjectMap>,
    Vertex<Long, ObjectMap>> {
  private LongCounter mergedClusterCount = new LongCounter();

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Utils.REFINEMENT_MERGE_ACCUMULATOR, mergedClusterCount);
  }

  @Override
  public Vertex<Long, ObjectMap> map(Triplet<Long, ObjectMap, ObjectMap> triplet) throws Exception {
    ObjectMap srcVal = triplet.getSrcVertex().getValue();
    ObjectMap trgVal = triplet.getTrgVertex().getValue();
    Set<Long> trgVertices = trgVal.getVerticesList();
    Set<Long> srcVertices = srcVal.getVerticesList();

    Vertex<Long, ObjectMap> resultVertex;
    if (srcVertices.size() >= trgVertices.size()) {
      resultVertex = new Vertex<>(triplet.getSrcVertex().getId(), compareAndReturnBest(srcVal, trgVal));
    } else {
      resultVertex = new Vertex<>(triplet.getSrcVertex().getId(), compareAndReturnBest(trgVal, srcVal));
    }
    srcVertices.addAll(trgVertices);
    resultVertex.getValue().put(Utils.CL_VERTICES, srcVertices);
    mergedClusterCount.add(1L);

    return resultVertex;
  }



  private ObjectMap compareAndReturnBest(ObjectMap priority, ObjectMap minor) {
    if (!priority.containsKey(Utils.LABEL) && minor.containsKey(Utils.LABEL)) {
      priority.put(Utils.LABEL, minor.get(Utils.LABEL));
    }
    if (!priority.hasNoType(Utils.TYPE_INTERN) && minor.hasNoType(Utils.TYPE_INTERN)) {
      priority.put(Utils.TYPE_INTERN, minor.get(Utils.TYPE_INTERN));
    }
    if (!priority.hasGeoProperties() && minor.hasGeoProperties()) {
      priority.put(Utils.LAT, minor.getLatitude());
      priority.put(Utils.LON, minor.getLongitude());
    }
    return priority;
  }
}
