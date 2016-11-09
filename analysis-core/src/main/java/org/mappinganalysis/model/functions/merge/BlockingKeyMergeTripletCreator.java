package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Utils;

public class BlockingKeyMergeTripletCreator
    implements FlatMapFunction<Vertex<Long, ObjectMap>, MergeTriplet> {
  private static final Logger LOG = Logger.getLogger(BlockingKeyMergeTripletCreator.class);

  MergeTriplet reuseTriplet;
  int sourcesCount;

  /**
   * Temporary dataset containing vertex id, type (as int), size, sources (as int) and label.
   */
  public BlockingKeyMergeTripletCreator(int sourcesCount) {
    this.reuseTriplet = new MergeTriplet();
    this.sourcesCount = sourcesCount;
  }
  @Override
  public void flatMap(Vertex<Long, ObjectMap> vertex, Collector<MergeTriplet> out) throws Exception {
    ObjectMap properties = vertex.getValue();
    if (AbstractionUtils.getSourceCount(properties.getIntSources()) < sourcesCount) {
      reuseTriplet.setId(vertex.getId());
      reuseTriplet.setLatitude(properties.getLatitude());
      reuseTriplet.setLongitude(properties.getLongitude());
      reuseTriplet.setIntTypes(properties.getIntTypes());
      reuseTriplet.setIntSources(properties.getIntSources());
      reuseTriplet.setLabel(properties.getLabel());
      reuseTriplet.setBlockingLabel(Utils.getBlockingLabel(properties.getLabel()));
      reuseTriplet.addClusteredElements(properties.getVerticesList());

      LOG.info("reuseTriplet: " + reuseTriplet.toString());
      out.collect(reuseTriplet);
    }
  }
}
