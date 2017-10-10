package org.mappinganalysis.model.impl;

import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.Set;

public class RepresentativeMap extends ObjectMap {
  private static final Logger LOG = Logger.getLogger(RepresentativeMap.class);

  public RepresentativeMap(ObjectMap properties, DataDomain domain) {
    super(properties, domain);
  }

  public void setBlockingKey(BlockingStrategy strategy) {
    LOG.info("set blocking key for " + getLabel());

    if (strategy.equals(BlockingStrategy.STANDARD_BLOCKING)) {
      if (getMode().equals(Constants.GEO)) {
        LOG.info("sbs gL: " + getLabel());
        LOG.info("sbs map: " + getMap());
        getMap().put(Constants.BLOCKING_LABEL, Utils.getGeoBlockingLabel(getLabel()));
      } else if (getMode().equals(Constants.MUSIC)) {
        LOG.info("music put blocking label");
        getMap().put(Constants.BLOCKING_LABEL, Utils.getMusicBlockingLabel(getLabel()));
      } else {
        throw new IllegalArgumentException("Unsupported strategy: " + strategy);
      }
    }
  }

  /**
   * TODO Blocking key string for IDF?
   */
  public String getBlockingKey() {
    if (this.get(Constants.BLOCKING_LABEL) == null) {
      return Constants.EMPTY_STRING;
    } else {
      return this.get(Constants.BLOCKING_LABEL).toString();
    }
  }

  @Deprecated
  public void setClusterDataSources(Set<String> sources) {
    if (!sources.isEmpty()) {
      this.put(Constants.DATA_SOURCES, sources);
    }
  }

  /**
   * Get internal representation of all data sources in a cluster.
   */
  @Deprecated
  public Integer getIntDataSources() {
    Object dataSources = this.get(Constants.DATA_SOURCES);
    Set<String> sources;
    if (dataSources instanceof Set) {
      sources = (Set<String>) dataSources;
    } else if (dataSources == null) {
      sources = Sets.newHashSet(getDataSource());
    } else {
      sources = Sets.newHashSet(dataSources.toString());
    }

    return AbstractionUtils.getSourcesInt(this.getMode(), sources);
  }
}
