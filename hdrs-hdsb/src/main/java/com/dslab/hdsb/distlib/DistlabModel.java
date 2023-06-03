package com.dslab.hdsb.distlib;

import com.dslab.hdsb.core.Distribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;

public final class DistlabModel {

  public static DistGenerator<Integer> withFileSize(int FileMinSize,
          int fileMaxSize, Distribution dist, double zipfExponent, double normalSD) {
    switch (dist) {
      case SIMPLE:
        return () -> fileMaxSize;
      case ZIPF:
        return () -> FileMinSize + Math.abs((int) createZipfDist(fileMaxSize, zipfExponent));
      case UNIFORM:
        return () -> Math.abs(createUniformDist(FileMinSize, fileMaxSize));
      case NORMAL:
        return () -> Math.abs((int) createNormalDist(fileMaxSize, normalSD));
      default:
        throw createException(dist);
    }
  }

  public static DistGenerator<Integer> withTreadReqNum(Distribution dist,
          int threadReqNumLower, int threadReqNumUpper, double zipfExponent, double normalSD) {
    switch (dist) {
      case SIMPLE:
        return () -> threadReqNumUpper;
      case ZIPF:
        return () -> threadReqNumLower + (int) createZipfDist(threadReqNumUpper, zipfExponent);
      case UNIFORM:
        return () -> (int) DistlabModel.createUniformDist(threadReqNumLower, threadReqNumUpper);
      case NORMAL:
        return () -> (int) DistlabModel.createNormalDist(threadReqNumUpper, normalSD);
      default:
        throw createException(dist);
    }
  }

  public static DistGenerator<Integer> withConcurrentTreadNum(Distribution dist,
          int threadNumLower, int threadNumUpper, double zipfExponent, double normalSD) {
    switch (dist) {
      case SIMPLE:
        return () -> threadNumUpper;
      case ZIPF:
        return () -> threadNumLower + (int) createZipfDist(threadNumUpper, zipfExponent);
      case UNIFORM:
        return () -> (int) createUniformDist(threadNumLower, threadNumUpper);
      case NORMAL:
        return () -> (int) createNormalDist(threadNumUpper, normalSD);
      default:
        throw createException(dist);
    }
  }

  public static DistGenerator<String> withSource(Distribution dist, String source,
          double zipfExponent, double normalSD) {
    return distByIndex(dist, source.split(","), zipfExponent, normalSD);
  }

  public static DistGenerator<String> withDestination(Distribution dist, String destination,
          double zipfExponent, double normalSD) {
    return distByIndex(dist, destination.split(","), zipfExponent, normalSD);
  }

  public static DistGenerator<Integer> withFrequency(Distribution dist,
          int frequencyLower, int frequencyUpper, double zipfExponent, double normalSD) {
    switch (dist) {
      case SIMPLE:
        return () -> frequencyUpper;
      case ZIPF:
        return () -> frequencyLower + (int) createZipfDist(frequencyUpper, zipfExponent);
      case UNIFORM:
        return () -> (int) createUniformDist(frequencyLower, frequencyUpper);
      case NORMAL:
        return () -> (int) createNormalDist(frequencyUpper, normalSD);
      default:
        throw createException(dist);
    }
  }

  private static DistGenerator<String> distByIndex(Distribution dist, String[] data,
          double zipfExponent, double normalSD) {
    assert data.length >= 1 : "The number of destinations should be bigger than zero";
    // it is the chosen one.
    if (data.length == 1) {
      return () -> data[0];
    }
    int index;
    switch (dist) {
      case ZIPF:
        index = Math.max(0, Math.min(data.length - 1, (int) createZipfDist(data.length, zipfExponent)));
        break;
      case UNIFORM:
        index = Math.max(0, Math.min(data.length - 1, (int) createUniformDist(0, data.length)));
        break;
      case NORMAL:
        index = Math.max(0, Math.min(data.length - 1, (int) createNormalDist(data.length / 2, normalSD)));
        break;
      default:
        throw createException(dist);
    }
    return () -> data[index];
  }

  private static IllegalArgumentException createException(Distribution dist) {
    return new IllegalArgumentException("Unsupported dist:" + dist);
  }

  private static int createUniformDist(int min, int max) {
    UniformIntegerDistribution u = new UniformIntegerDistribution(min, max);
    return u.sample();
  }

  private static double createZipfDist(int numberOfElements, double exponent) {
    ZipfDistribution z = new ZipfDistribution(numberOfElements, exponent);
    return z.sample();
  }

  private static double createNormalDist(double mu, double sigma) {
    NormalDistribution n = new NormalDistribution(mu, sigma);
    return n.sample();
  }

  private DistlabModel() {
  }
}
