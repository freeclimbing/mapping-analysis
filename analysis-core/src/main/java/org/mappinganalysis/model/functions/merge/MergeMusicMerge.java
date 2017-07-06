package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.Set;

/**
 * Merge implementation for music dataset.
 */
public class MergeMusicMerge
    implements FlatMapFunction<MergeMusicTriplet, MergeMusicTuple> {
  private static final Logger LOG = Logger.getLogger(MergeMusicMerge.class);

  @Override
  public void flatMap(MergeMusicTriplet triplet, Collector<MergeMusicTuple> out) throws Exception {
    MergeMusicTuple priority = triplet.getSrcTuple();
    MergeMusicTuple minor = triplet.getTrgTuple();

    Set<Long> trgElements = minor.getClusteredElements();
    Set<Long> srcElements = priority.getClusteredElements();
    if (srcElements.size() < trgElements.size()) {
      MergeMusicTuple tmp = minor;
      minor = priority;
      priority = tmp;
    }

    MergeMusicTuple mergedCluster = new MergeMusicTuple();
    mergedCluster.setId(priority.getId() > minor.getId() ? minor.getId() : priority.getId());

    srcElements.addAll(trgElements);
    mergedCluster.addClusteredElements(srcElements);

    mergedCluster.setIntSources(AbstractionUtils.mergeIntValues(
        priority.getIntSources(),
        minor.getIntSources()));

    // attributes
    //Constants.BLOCKING_LABEL, priority, minor);
    if (Utils.isSane(priority.getBlockingLabel())) {
      mergedCluster.setBlockingLabel(priority.getBlockingLabel());
    } else if (Utils.isSane(minor.getBlockingLabel())) {
      mergedCluster.setBlockingLabel(minor.getBlockingLabel());
    } else {
      mergedCluster.setBlockingLabel(Constants.EMPTY_STRING);
    }

    //  Constants.LABEL
    if (Utils.isSane(priority.getLabel())) {
      mergedCluster.setLabel(priority.getLabel());
    } else if (Utils.isSane(minor.getLabel())) {
      mergedCluster.setLabel(minor.getLabel());
    } else {
      mergedCluster.setLabel(Constants.EMPTY_STRING);
    }

    //Constants.ALBUM
    if (Utils.isSane(priority.getAlbum())) {
      mergedCluster.setAlbum(priority.getAlbum());
    } else if (Utils.isSane(minor.getAlbum())) {
      mergedCluster.setAlbum(minor.getAlbum());
    } else {
      mergedCluster.setAlbum(Constants.EMPTY_STRING);
    }

    //Constants.ARTIST, priority, minor);
    if (Utils.isSane(priority.getArtist())) {
      mergedCluster.setArtist(priority.getArtist());
    } else if (Utils.isSane(minor.getArtist())) {
      mergedCluster.setArtist(minor.getArtist());
    } else {
      mergedCluster.setArtist(Constants.EMPTY_STRING);
    }

    //Constants.NUMBER, priority, minor);
    if (Utils.isSane(priority.getNumber())) {
      mergedCluster.setNumber(priority.getNumber());
    } else if (Utils.isSane(minor.getNumber())) {
      mergedCluster.setNumber(minor.getNumber());
    } else {
      mergedCluster.setNumber(Constants.EMPTY_STRING);
    }

//    mergedCluster.setAttribute(Constants.LANGUAGE, priority, minor);
    mergedCluster.setLang(Constants.EMPTY_STRING); // TODO check

    //Constants.YEAR, priority, minor);
    if (Utils.isSaneInt(priority.getYear())) {
      mergedCluster.setYear(priority.getYear());
    } else if (Utils.isSaneInt(minor.getYear())) {
      mergedCluster.setYear(minor.getYear());
    } else {
      mergedCluster.setYear(Constants.EMPTY_INT);
    }

    //Constants.LENGTH, priority, minor);
    if (Utils.isSaneInt(priority.getLength())) {
      mergedCluster.setLength(priority.getLength());
    } else if (Utils.isSaneInt(minor.getLength())) {
      mergedCluster.setLength(minor.getLength());
    } else {
      mergedCluster.setLength(Constants.EMPTY_INT);
    }

    if (Utils.isSane(priority.getArtistTitleAlbum())) {
      mergedCluster.setArtistTitleAlbum(priority.getArtistTitleAlbum());
    } else if (Utils.isSane(minor.getArtistTitleAlbum())) {
      mergedCluster.setArtistTitleAlbum(minor.getArtistTitleAlbum());
    } else {
      mergedCluster.setArtistTitleAlbum(Constants.EMPTY_STRING);
    }

    MergeMusicTuple fakeCluster = new MergeMusicTuple(
        priority.getId() > minor.getId() ? priority.getId() : minor.getId());

//    LOG.info("fake: " + fakeCluster.toString());
//    LOG.info("merged: " + mergedCluster.toString());

    out.collect(fakeCluster);
//    LOG.info("fake: " + fakeCluster.toString());
//    LOG.info("merge: " + mergedCluster.toString());
    out.collect(mergedCluster);
  }
}
