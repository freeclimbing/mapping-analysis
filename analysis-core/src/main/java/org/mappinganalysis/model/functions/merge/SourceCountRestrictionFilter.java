package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.util.AbstractionUtils;

/**
 * Check cluster for contained element count, if lower than max source count, return true.
 */
public class SourceCountRestrictionFilter<T> implements FilterFunction<T> {
  private static final Logger LOG = Logger.getLogger(SourceCountRestrictionFilter.class);

  private DataDomain domain;
  private int sourcesCount;

  public SourceCountRestrictionFilter(DataDomain domain, int sourcesCount) {
    this.domain = domain;
    this.sourcesCount = sourcesCount;
  }

  /**
   * Check cluster for contained element count and restrict if >= sourcesCount.
   * @param tuple MergeTuple to check
   * @return if contained element count lower than max source count, return true.
   */
  @Override
  public boolean filter(T tuple) throws Exception {
    if (domain == DataDomain.GEOGRAPHY) {
      MergeGeoTuple tmp = (MergeGeoTuple) tuple;
      return AbstractionUtils.getSourceCount(tmp.getIntSources()) < sourcesCount;
    } else {
      MergeTuple tmp = (MergeTuple) tuple;
      return AbstractionUtils.getSourceCount(tmp.getIntSources()) < sourcesCount;

    }
  }
}
