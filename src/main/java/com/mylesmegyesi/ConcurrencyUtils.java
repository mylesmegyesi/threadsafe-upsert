package com.mylesmegyesi;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

class ConcurrencyUtils {

  static <T> List<T> executeNTimesInParallel(int numThreads, int times, IntFunction<T> func) {
    var threadPool = Executors.newFixedThreadPool(numThreads);
    try {
      return IntStream.range(0, times)
          .mapToObj(i -> threadPool.submit(() -> func.apply(i)))
          .map(
              future -> {
                try {
                  return future.get();
                } catch (InterruptedException | ExecutionException exception) {
                  throw new RuntimeException(exception);
                }
              })
          .collect(toList());
    } finally {
      threadPool.shutdown();
    }
  }
}
