package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.TypeDictionary;
import org.mappinganalysis.util.Utils;


public class MergeTupleMapper
    implements FlatMapFunction<Vertex<Long, ObjectMap>, MergeTuple> {
  private static final Logger LOG = Logger.getLogger(MergeTupleMapper.class);

  MergeTuple reuseTuple;
  int sourcesCount;

  /**
   * Temporary dataset containing vertex id, type (as int), size, sources (as int) and label.
   */
  public MergeTupleMapper(int sourcesCount) {
    this.reuseTuple = new MergeTuple();
    this.sourcesCount = sourcesCount;
  }
  @Override
  public void flatMap(Vertex<Long, ObjectMap> vertex, Collector<MergeTuple> out) throws Exception {
    ObjectMap properties = vertex.getValue();
    if (AbstractionUtils.getSourceCount(properties.getSourcesAsInt()) < sourcesCount) {
      reuseTuple.setVertexId(vertex.getId());
      reuseTuple.setType(properties.getTypesAsInt());
      reuseTuple.setSize(properties.getVerticesCount());
      reuseTuple.setIntSources(properties.getSourcesAsInt());
      reuseTuple.setLabel(Utils.getBlockingLabel(properties.getLabel()));

      LOG.info("reuseTuple: " + reuseTuple.toString());
      out.collect(reuseTuple);
    }
  }
}
