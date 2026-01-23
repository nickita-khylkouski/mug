/*****************************************************************************
 * ------------------------------------------------------------------------- *
 * Licensed under the Apache License, Version 2.0 (the "License");           *
 * you may not use this file except in compliance with the License.          *
 * You may obtain a copy of the License at                                   *
 *                                                                           *
 * http://www.apache.org/licenses/LICENSE-2.0                                *
 *                                                                           *
 * Unless required by applicable law or agreed to in writing, software       *
 * distributed under the License is distributed on an "AS IS" BASIS,         *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 * See the License for the specific language governing permissions and       *
 * limitations under the License.                                            *
 *****************************************************************************/
package com.google.mu.collect;

import static com.google.common.truth.Truth.assertThat;
import static com.google.mu.util.stream.MoreStreams.indexesFrom;

import com.google.mu.util.concurrent.Parallelizer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

/**
 * Tests for concurrent access to Chain focusing on correct values.
 *
 * <p>These tests verify that concurrent readers always see correct data. Since Chain is
 * immutable, all threads should see the same correct values.
 *
 * <p>Parallelizer automatically propagates exceptions from worker threads to the main thread,
 * enabling immediate test failure with clear error messages.
 */
public class ChainConcurrentAccessTest {

  /** Verifies multiple threads calling get() all get correct values. */
  @Test
  public void testConcurrentGet_returnsCorrectValues() throws Exception {
    Chain<String> chain =
        Chain.concat(Chain.of("alpha", "beta"), Chain.of("gamma", "delta", "epsilon"));

    List<String> results = new CopyOnWriteArrayList<>();
    List<String> expectedValues = List.of("alpha", "beta", "gamma", "delta", "epsilon");

    runConcurrently(16, i -> {
      int index = i % 5; // Access different indices
      String result = chain.get(index);
      String expected = expectedValues.get(index);
      if (!result.equals(expected)) {
        throw new AssertionError(
            "Expected '" + expected + "' at index " + index + " but got '" + result + "'");
      }
      results.add(result);
    });

    assertThat(results).containsAtLeastElementsIn(expectedValues);
    assertThat(results).hasSize(16);
  }

  /** Verifies multiple threads iterating all see correct elements. */
  @Test
  public void testConcurrentIteration_allSeeCorrectElements() throws Exception {
    Chain<Integer> chain =
        Chain.concat(
            Chain.of(1, 2, 3), Chain.concat(Chain.of(4, 5), Chain.of(6, 7, 8, 9, 10)));

    List<List<Integer>> collectedResults = new CopyOnWriteArrayList<>();

    runConcurrently(12, i -> collectedResults.add(chain.stream().collect(Collectors.toList())));

    // Every thread should see exactly [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    List<Integer> expected = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    for (List<Integer> threadResult : collectedResults) {
      assertThat(threadResult).containsExactlyElementsIn(expected).inOrder();
    }
    assertThat(collectedResults).hasSize(12);
  }

  /** Verifies multiple threads calling size() all see the same size. */
  @Test
  public void testConcurrentSize_allSeeSameSize() throws Exception {
    Chain<Integer> chain =
        Chain.concat(
            Chain.of(1, 2, 3, 4, 5),
            Chain.concat(Chain.of(6, 7, 8), Chain.of(9, 10)));

    List<Integer> sizes = new CopyOnWriteArrayList<>();

    runConcurrently(20, i -> sizes.add(chain.size()));

    // All threads should see size = 10
    assertThat(sizes).hasSize(20);
    for (int size : sizes) {
      assertThat(size).isEqualTo(10);
    }
  }

  /** Verifies multiple threads calling stream() all see correct elements. */
  @Test
  public void testConcurrentStream_allSeeCorrectElements() throws Exception {
    Chain<String> chain =
        Chain.concat(
            Chain.of("one", "two"),
            Chain.concat(Chain.of("three"), Chain.of("four", "five")));

    List<List<String>> collectedResults = new CopyOnWriteArrayList<>();

    runConcurrently(
        15, i -> collectedResults.add(chain.stream().collect(Collectors.toList())));

    // Every thread should see exactly ["one", "two", "three", "four", "five"]
    List<String> expected = List.of("one", "two", "three", "four", "five");
    for (List<String> threadResult : collectedResults) {
      assertThat(threadResult).containsExactlyElementsIn(expected).inOrder();
    }
    assertThat(collectedResults).hasSize(15);
  }

  /** Verifies mixed concurrent operations all return correct values. */
  @Test
  public void testConcurrentMixedOperations_allReturnCorrectValues() throws Exception {
    Chain<Integer> chain =
        Chain.concat(
            Chain.of(10, 20, 30, 40, 50), Chain.of(60, 70, 80, 90, 100));

    // Track results from different operation types
    List<Integer> getSizeResults = new CopyOnWriteArrayList<>();
    List<Integer> getResults = new CopyOnWriteArrayList<>();
    List<List<Integer>> streamResults = new CopyOnWriteArrayList<>();

    List<Integer> validValues = List.of(10, 20, 30, 40, 50, 60, 70, 80, 90, 100);

    runConcurrently(
        24,
        threadId -> {
          // Different threads perform different operations
          switch (threadId % 3) {
            case 0: // size()
              int size = chain.size();
              if (size != 10) {
                throw new AssertionError("Expected size=10 but got " + size);
              }
              getSizeResults.add(size);
              break;
            case 1: // get()
              Integer value = chain.get(threadId % 10);
              if (!validValues.contains(value)) {
                throw new AssertionError(
                    "Invalid value: " + value + " (threadId=" + threadId + ")");
              }
              getResults.add(value);
              break;
            case 2: // stream()
              List<Integer> streamResult = chain.stream().collect(Collectors.toList());
              streamResults.add(streamResult);
              break;
          }
        });

    // All size() calls should return 10
    assertThat(getSizeResults).isNotEmpty();
    for (int size : getSizeResults) {
      assertThat(size).isEqualTo(10);
    }

    // All get() calls should return correct values
    assertThat(getResults).isNotEmpty();
    for (Integer value : getResults) {
      assertThat(value).isIn(validValues);
    }

    // All stream() calls should return complete correct sequence
    assertThat(streamResults).isNotEmpty();
    List<Integer> expected = List.of(10, 20, 30, 40, 50, 60, 70, 80, 90, 100);
    for (List<Integer> streamResult : streamResults) {
      assertThat(streamResult).containsExactlyElementsIn(expected).inOrder();
    }
  }

  /**
   * Helper method to run a task concurrently across multiple threads.
   *
   * <p>Uses Parallelizer which automatically propagates exceptions from worker threads
   * to the main thread for immediate test failure.
   *
   * @param numThreads number of threads to run the task on
   * @param task the task to run, receives thread index as parameter
   */
  private static void runConcurrently(int numThreads, java.util.function.Consumer<Integer> task)
      throws Exception {
    java.util.concurrent.ExecutorService executor =
        java.util.concurrent.Executors.newFixedThreadPool(numThreads);
    try {
      new Parallelizer(executor, numThreads)
          .parallelize(indexesFrom(0).limit(numThreads), task);
    } finally {
      executor.shutdown();
      executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
    }
  }
}
