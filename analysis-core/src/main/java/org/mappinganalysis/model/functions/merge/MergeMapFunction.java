package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.Set;

/**
 * Merge function, for the "losing" tuple, add a dummy result tuple for solution set.
 *
 * Do not use reuse tuple.
 */
public class MergeMapFunction
    implements FlatMapFunction<MergeTriplet, MergeTuple> {
  private static final Logger LOG = Logger.getLogger(MergeMapFunction.class);

  @Override
  public void flatMap(MergeTriplet triplet, Collector<MergeTuple> out) throws Exception {
    // prepare
    MergeTuple priority = triplet.getSrcTuple();
    MergeTuple minor = triplet.getTrgTuple();

    if (priority.getId() == 252016L) {
      LOG.info(priority + " " + minor);
      LOG.info("P "+ priority.getIntSources() + " " + minor.getIntSources());
    }
    if (minor.getId() == 252016L) {
      LOG.info(priority + " " + minor);
      LOG.info("M "+ minor.getIntSources() + " " + priority.getIntSources());
    }


    Set<Long> trgElements = minor.getClusteredElements();
    Set<Long> srcElements = priority.getClusteredElements();
    if (srcElements.size() < trgElements.size()) {
      MergeTuple tmp = minor;
      minor = priority;
      priority = tmp;
    }

    MergeTuple valueTuple = new MergeTuple();
    // set tuple properties
    valueTuple.setId(priority.getId() > minor.getId() ? minor.getId() : priority.getId());
    // is there a case where minor label should be taken?
    valueTuple.setLabel(priority.getLabel());

    // geo coordinates
    MergeTuple geoTuple = Utils.isOnlyOneValidGeoObject(priority, minor);
    if (geoTuple != null) {
      valueTuple.setGeoProperties(geoTuple);
    } else if (AbstractionUtils.containsSrc(priority.getIntSources(), Constants.GN_NS)) {
      valueTuple.setGeoProperties(priority);
    } else if (AbstractionUtils.containsSrc(minor.getIntSources(), Constants.GN_NS)) {
      valueTuple.setGeoProperties(minor);
    } else if (AbstractionUtils.containsSrc(priority.getIntSources(), Constants.DBP_NS)) {
      valueTuple.setGeoProperties(priority);
    } else if (AbstractionUtils.containsSrc(priority.getIntSources(), Constants.DBP_NS)) {
      valueTuple.setGeoProperties(minor);
    }

    srcElements.addAll(trgElements);
    valueTuple.addClusteredElements(srcElements);
    valueTuple.setIntSources(AbstractionUtils.mergeIntValues(
        priority.getIntSources(),
        minor.getIntSources()));
    valueTuple.setIntTypes(AbstractionUtils.mergeIntValues(
        priority.getIntTypes(),
        minor.getIntTypes()));
    valueTuple.setBlockingLabel(priority.getBlockingLabel());

    LOG.info("### new cluster: " + valueTuple.toString());
    MergeTuple tmp = new MergeTuple(
        priority.getId() > minor.getId() ? priority.getId() : minor.getId(),
        false);
    LOG.info("### fake cluster: " + tmp.toString());

    if (priority.getId() == 252016L) {
      LOG.info("P "+ priority.getIntSources() + " " + minor.getIntSources());
    }
    if (minor.getId() == 252016L) {
      LOG.info("M " + minor.getIntSources() + " " + priority.getIntSources());
    }
    out.collect(tmp);
    out.collect(valueTuple);
  }
}
