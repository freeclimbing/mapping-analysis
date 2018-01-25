package org.mappinganalysis.model.functions.preprocessing.utils;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.util.AbstractionUtils;

import java.util.HashMap;
import java.util.List;

/**
 * Add links to result set, check if datasource is already contained in current cluster.
 * If contained, remove link.
 *
 * TODO To reduce complexity to ccid groups, ccid is still used.
 * todo Test version with sort partition.
 */
public class LinkSelectionWithCcIdFunction
    implements GroupReduceFunction<EdgeSourceSimTuple, Tuple2<Long, Long>> {
  private static final Logger LOG = Logger.getLogger(LinkSelectionWithCcIdFunction.class);
  private HashMap<String, Integer> sourcesMap;

  public LinkSelectionWithCcIdFunction(List<String> sources) {
    this.sourcesMap = AbstractionUtils.getSourcesMap(sources);
  }

  @Override
  public void reduce(Iterable<EdgeSourceSimTuple> values,
                     Collector<Tuple2<Long, Long>> out) throws Exception {
    HashMap<Long, ComponentSourceTuple> entitySourceMap = Maps.newHashMap();

    // ccid, e.src, e.trg, v.src, e.src, sim
    for (EdgeSourceSimTuple edge : values) {
      ComponentSourceTuple src = entitySourceMap.get(edge.getSrcId());
      ComponentSourceTuple trg = entitySourceMap.get(edge.getTrgId());
      // preparation
      if (src == null) {
        src = new ComponentSourceTuple(edge.getSrcId(), sourcesMap);
      }
      if (trg == null) {
        trg = new ComponentSourceTuple(edge.getTrgId(), sourcesMap);
      }

      // selection
      if (!src.contains(edge.getTrgDataSource())
          && !trg.contains(edge.getSrcDataSource())) {
        src.addSource(edge.getTrgDataSource());
        entitySourceMap.put(edge.getSrcId(), src);

        trg.addSource(edge.getSrcDataSource());
        entitySourceMap.put(edge.getTrgId(), trg);

        out.collect(new Tuple2<>(edge.getSrcId(), edge.getTrgId()));
      }
    }
  }
}
