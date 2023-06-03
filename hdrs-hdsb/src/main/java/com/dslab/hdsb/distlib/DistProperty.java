package com.dslab.hdsb.distlib;

import com.dslab.hdsb.core.Distribution;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class DistProperty {

  public static Builder newBuilder(int initialCapacity) {
    return new Builder(initialCapacity);
  }

  public static class Builder {
    private static final int DEFAULT_SIZE = 16;
    private static final int DEFAULT_PARALLEL_BUILDER = 10;
    private final List<DistGenerator<Integer>> fileSizeGen;
    private final List<DistGenerator<String>> sourceGen;
    private final List<DistGenerator<String>> destinationGen;
    private final List<DistGenerator<Integer>> reqFreqGen;
    private final List<DistGenerator<Integer>> threadNumberGen;
    private final List<DistGenerator<Integer>> threadRequestNumberGen;
    private final List<String> localFilePaths;
    private final int initialCapacity;

    private Builder(int initialCapacity) {
      this.initialCapacity = Math.max(initialCapacity, DEFAULT_SIZE);
      fileSizeGen = new ArrayList<>(this.initialCapacity);
      sourceGen = new ArrayList<>(this.initialCapacity);
      destinationGen = new ArrayList<>(this.initialCapacity);
      reqFreqGen = new ArrayList<>(this.initialCapacity);
      threadNumberGen = new ArrayList<>(this.initialCapacity);
      threadRequestNumberGen = new ArrayList<>(this.initialCapacity);
      localFilePaths = new ArrayList<>(this.initialCapacity);
    }

    public Builder addFileSize(int minSize, int maxSize, Distribution dist,
            double zipfExponent, double normalSD) {
      fileSizeGen.add(DistlabModel.withFileSize(minSize, maxSize, dist, zipfExponent, normalSD));
      return this;
    }

    public Builder addSource(Distribution dist, String source,
            double zipfExponent, double normalSD) {
      sourceGen.add(DistlabModel.withSource(dist, source, zipfExponent, normalSD));
      return this;
    }

    public Builder addDestination(Distribution dist, String destination,
            double zipfExponent, double normalSD) {
      destinationGen.add(DistlabModel.withDestination(dist, destination, zipfExponent, normalSD));
      return this;
    }

    public Builder addReqFrequency(Distribution dist,
            int frequencyLower, int frequencyUpper, double zipfExponent, double normalSD) {
      reqFreqGen.add(DistlabModel.withFrequency(dist, frequencyLower,
              frequencyUpper, zipfExponent, normalSD));
      return this;
    }

    public Builder addConcurrentTreadNum(Distribution dist,
            int threadNumLower, int threadNumUpper, double zipfExponent, double normalSD) {
      threadNumberGen.add(DistlabModel.withConcurrentTreadNum(dist,
              threadNumLower, threadNumUpper, zipfExponent, normalSD));
      return this;
    }

    public Builder addTreadReqNum(Distribution dist,
            int threadReqNumLower, int threadReqNumUpper, double zipfExponent, double normalSD) {
      threadRequestNumberGen.add(DistlabModel.withTreadReqNum(dist, threadReqNumLower,
              threadReqNumUpper, zipfExponent, normalSD));
      return this;
    }

    public Builder addLocalSource(String local) {
      localFilePaths.add(local);
      return this;
    }

    public DistProperty build() {
      return new DistProperty(
              convert(fileSizeGen),
              convert(sourceGen),
              convert(sourceGen),
              convert(destinationGen),
              convert(reqFreqGen),
              convert(threadNumberGen),
              convert(threadRequestNumberGen),
              new ArrayList<>(localFilePaths)
      );
    }

    public DistProperty parallelBuild() {
      ExecutorService pool = Executors.newFixedThreadPool(DEFAULT_PARALLEL_BUILDER);
      CompletableFuture<List<Integer>> fileSizeThread = convertAsync(fileSizeGen, pool);
      CompletableFuture<List<String>> sourceThread = convertAsync(sourceGen, pool);
      CompletableFuture<List<String>> destinationThread = convertAsync(destinationGen, pool);
      CompletableFuture<List<Integer>> reqFreqThread = convertAsync(reqFreqGen, pool);
      CompletableFuture<List<Integer>> threadNumberThread = convertAsync(threadNumberGen, pool);
      CompletableFuture<List<Integer>> threadRequestNumberThread = convertAsync(threadRequestNumberGen, pool);
      try {
        return new DistProperty(
                fileSizeThread.join(),
                sourceThread.join(),
                sourceThread.join(),
                destinationThread.join(),
                reqFreqThread.join(),
                threadNumberThread.join(),
                threadRequestNumberThread.join(),
                new ArrayList<>(localFilePaths)
        );
      } finally {
        pool.shutdown();
      }
    }

    private static <T> List<T> convert(List<DistGenerator<T>> data) {
      return data.stream().map(v -> v.generate()).collect(Collectors.toList());
    }

    private static <T> CompletableFuture<List<T>> convertAsync(
            List<DistGenerator<T>> data, Executor pool) {
      return CompletableFuture.supplyAsync(() -> data.stream()
              .map(v -> v.generate()).collect(Collectors.toList()), pool);
    }
  }

  @VisibleForTesting
  DistProperty(List<Integer> fileSizes,
          List<String> uploadTargets,
          List<String> copyUploadTargets,
          List<String> destinations,
          List<Integer> frequencys,
          List<Integer> threadNumbers,
          List<Integer> threadRequestNumbers,
          List<String> localFilePaths) {
    this.fileSizes = fileSizes;
    this.uploadTargets = uploadTargets;
    this.copyUploadTargets = copyUploadTargets;
    this.destinations = destinations;
    this.frequencys = frequencys;
    this.threadNumbers = threadNumbers;
    this.threadRequestNumbers = threadRequestNumbers;
    this.localFilePaths = localFilePaths;
  }
  private final List<Integer> fileSizes;
  private final List<String> uploadTargets;
  private final List<String> copyUploadTargets;
  private final List<String> destinations;
  private final List<Integer> frequencys;
  private final List<Integer> threadNumbers;
  private final List<Integer> threadRequestNumbers;
  private final List<String> localFilePaths;

  public Iterator<String> getDestinations() {
    return this.destinations.iterator();
  }

  public Iterator<Integer> getFileSizes() {
    return this.fileSizes.iterator();
  }

  public Iterator<Integer> getFrequencies(int from, int to) {
    return this.frequencys.subList(from, to).iterator();
  }

  public Iterator<String> getLocals() {
    return this.localFilePaths.iterator();
  }

  public Iterator<String> getUploadTargets() {
    return this.uploadTargets.iterator();
  }

  public Iterator<String> getCopyUploadTargets() {
    return this.copyUploadTargets.iterator();
  }

  public Iterator<Integer> getThreadNumbers() {
    return this.threadNumbers.iterator();
  }

  public Iterator<Integer> getThreadRequestNumbers() {
    return this.threadRequestNumbers.iterator();
  }
}
