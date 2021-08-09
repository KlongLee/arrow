/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.driver.jdbc.utils;

import static java.lang.String.format;
import static java.util.Collections.synchronizedSet;
import static org.apache.arrow.util.Preconditions.checkNotNull;
import static org.apache.arrow.util.Preconditions.checkState;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Collection;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.flight.FlightStream;
import org.apache.calcite.avatica.AvaticaConnection;

/**
 * Auxiliary class used to handle consuming of multiple {@link FlightStream}.
 * <p>
 * The usage follows this routine:
 * <ol>
 *   <li>Create a <code>FlightStreamQueue</code>;</li>
 *   <li>Call <code>enqueue(FlightStream)</code> for all streams to be consumed;</li>
 *   <li>Call <code>next()</code> to get a FlightStream that is ready to consume</li>
 *   <li>Consume the given FlightStream and add it back to the queue - call <code>enqueue(FlightStream)</code></li>
 *   <li>Repeat from (3) until <code>next()</code> returns null.</li>
 * </ol>
 */
public class FlightStreamQueue implements AutoCloseable {
  private final CompletionService<FlightStream> completionService;
  private final Set<Future<FlightStream>> futures = synchronizedSet(new HashSet<>());
  private final Set<FlightStream> unpreparedStreams = synchronizedSet(new HashSet<>());
  private final AtomicBoolean closed = new AtomicBoolean();

  /**
   * Instantiate a new FlightStreamQueue.
   */
  protected FlightStreamQueue(final CompletionService<FlightStream> executorService) {
    completionService = checkNotNull(executorService);
  }

  /**
   * Creates a new {@link FlightStreamQueue} from the provided {@link ExecutorService}.
   *
   * @param service the service from which to create a new queue.
   * @return a new queue.
   */
  public static FlightStreamQueue createNewQueue(final ExecutorService service) {
    return new FlightStreamQueue(new ExecutorCompletionService<>(service));
  }

  /**
   * Gets whether this queue is closed.
   *
   * @return a boolean indicating whether this resource is closed.
   */
  public boolean isClosed() {
    return closed.get();
  }

  /**
   * Blocking request with timeout to get the next ready FlightStream in queue.
   *
   * @param timeoutValue the amount of time to be waited
   * @param timeoutUnit  the timeoutValue time unit
   * @return a FlightStream that is ready to consume or null if all FlightStreams are ended.
   */
  public FlightStream next(long timeoutValue, TimeUnit timeoutUnit) throws SQLException {
    checkOpen();
    while (!futures.isEmpty()) {
      Optional<FlightStream> loadedStream;
      try {
        final Future<FlightStream> future = completionService.poll(timeoutValue, timeoutUnit);
        if (future == null) {
          // The poll method with timeout values returns null if it didn't get anything until the time is out.
          throw new TimeoutException();
        }
        futures.remove(future);
        loadedStream = Optional.ofNullable(future.get());
        final FlightStream stream = loadedStream.orElseThrow(NoSuchElementException::new);
        if (stream.getRoot().getRowCount() > 0) {
          return stream;
        }
      } catch (final TimeoutException e) {
        throw new SQLTimeoutException(
            String.format("Query failed to be retrieved after %d %s", timeoutValue, timeoutUnit));
      } catch (final ExecutionException | InterruptedException | CancellationException e) {
        throw AvaticaConnection.HELPER.wrap("Query canceled", e);
      }
    }
    return null;
  }

  /**
   * Blocking request to get the next ready FlightStream in queue.
   *
   * @return a FlightStream that is ready to consume or null if all FlightStreams are ended.
   */
  public FlightStream next() throws SQLException {
    checkOpen();
    FlightStream result = null; // If empty.
    while (!futures.isEmpty()) {
      Optional<FlightStream> loadedStream;
      try {
        final Future<FlightStream> future = completionService.take();
        futures.remove(future);
        loadedStream = Optional.ofNullable(future.get());
        final FlightStream stream = loadedStream.orElseThrow(NoSuchElementException::new);
        if (stream.getRoot().getRowCount() > 0) {
          result = stream;
          break;
        }
      } catch (final ExecutionException | InterruptedException | CancellationException e) {
        throw AvaticaConnection.HELPER.wrap("Query canceled", e);
      }
    }
    return result;
  }

  /**
   * Checks if this queue is open.
   */
  public synchronized void checkOpen() {
    checkState(!isClosed(), format("%s closed", this.getClass().getSimpleName()));
  }

  /**
   * Readily adds given {@link FlightStream}s to the queue.
   */
  public void enqueue(final Collection<FlightStream> flightStreams) {
    flightStreams.forEach(this::enqueue);
  }

  /**
   * Adds given {@link FlightStream} to the queue.
   */
  public synchronized void enqueue(final FlightStream flightStream) {
    checkNotNull(flightStream);
    checkOpen();
    unpreparedStreams.add(flightStream);
    futures.add(completionService.submit(() -> {
      // `FlightStream#next` will block until new data can be read or stream is over.
      flightStream.next();
      unpreparedStreams.remove(flightStream);
      return flightStream;
    }));
  }

  @Override
  public synchronized void close() throws Exception {
    if (isClosed()) {
      return;
    }
    try {
      futures.forEach(future -> future.cancel(true));
      unpreparedStreams.forEach(flightStream -> flightStream.cancel("Query canceled", null));
    } finally {
      unpreparedStreams.clear();
      futures.clear();
      closed.set(true);
    }
  }
}
