package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * Add links to result set, check if datasource is already contained in current cluster.
 * If contained, sort link out.
 *
 * To reduce complexity to ccid groups, ccid is still used.
 * todo Test version with sort partition.
 */
public class LinkSelectionWithCcIdFunction implements GroupReduceFunction<EdgeSourceSimTuple, Tuple2<Long, Long>> {
  @Override
  public void reduce(Iterable<EdgeSourceSimTuple> values,
                     Collector<Tuple2<Long, Long>> out) throws Exception {
    HashMap<Long, ComponentSourceTuple> entitySourceMap = Maps.newHashMap();
    // ccid, e.src, e.trg, v.src, e.src, sim
    for (EdgeSourceSimTuple link : values) {
//              LOG.info("###nnof: " + link.toString());
      ComponentSourceTuple src = entitySourceMap.get(link.getSrcId());
      ComponentSourceTuple trg = entitySourceMap.get(link.getTrgId());
      if (src == null) {
        src = new ComponentSourceTuple(link.getSrcId());
      }
      if (trg == null) {
        trg = new ComponentSourceTuple(link.getTrgId());
      }

      if (!src.contains(link.getTrgOntology())
          && !trg.contains(link.getSrcOntology())) {
        src.addSource(link.getTrgOntology());
        entitySourceMap.put(link.getSrcId(), src);

        trg.addSource(link.getSrcOntology());
        entitySourceMap.put(link.getTrgId(), trg);

//                LOG.info("###sec: result: " + tmpResult.toString());
        out.collect(new Tuple2<>(link.getSrcId(), link.getTrgId()));
      }
    }
  }
}
