package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;

/**
 * Create final vertices from merge tuples.
 * Only active merge tuples are translated to default Gelly vertices.
 */
public class FinalMergeVertexCreator
    extends RichFlatJoinFunction<MergeGeoTuple, Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  @Override
  public void join(MergeGeoTuple left,
                   Vertex<Long, ObjectMap> second,
                   Collector<Vertex<Long, ObjectMap>> out) throws Exception {
    if (left.isActive()) {
      ObjectMap map = new ObjectMap(Constants.GEO);
      map.setLabel(left.getLabel());
      map.setGeoProperties(left.getLatitude(), left.getLongitude());
      map.setClusterDataSources(AbstractionUtils.getSourcesStringSet(Constants.GEO, left.getIntSources()));
      map.setTypes(Constants.TYPE_INTERN, AbstractionUtils.getTypesStringSet(left.getIntTypes()));
      map.setClusterVertices(Sets.newHashSet(left.getClusteredElements()));

      out.collect(new Vertex<>(left.getId(), map));
    }
  }
}
