package com.mylesmegyesi;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

class ConcurrencyUtils {
    static <T> List<T> executeNTimesInParallel(int numThreads, int times, IntFunction<T> f) {
        ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
        try {
            return IntStream.range(0, times)
                    .mapToObj(i -> threadPool.submit(() -> f.apply(i)))
                    .map(future -> {
                        try {
                            return future.get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(toList());
        } finally {
            threadPool.shutdown();
        }
    }
}
