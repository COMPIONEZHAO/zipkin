/*
 * Copyright 2015-2018 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import zipkin2.Endpoint;
import zipkin2.Span;

public class Trace {
  /*
   * Spans can be sent in multiple parts. Also client and server spans can share the same ID. This
   * merges both scenarios.
   */
  public static List<Span> merge(List<Span> spans) {
    int length = spans.size();
    if (length <= 1) return spans;
    List<Span> result = new ArrayList<>(spans);
    Collections.sort(result, CLEANUP_COMPARATOR);

    // let's see if there are mixed trace IDs
    String traceId = spans.get(0).traceId();
    boolean fixNeeded = false;

    // We have to loop twice (in addition to the sorting). This loop figures out what the best trace
    // ID is and detects if we need to do anything else.
    for (int i = 0; i < length; i++) {
      Span previous = result.get(i);
      String previousTraceId = previous.traceId();
      String previousId = previous.id();
      boolean previousShared = Boolean.TRUE.equals(previous.shared());

      if (previousTraceId.length() != traceId.length()) fixNeeded = true;
      if (traceId.length() != 32) traceId = previousTraceId;

      if (previousId.equals(previous.parentId())) fixNeeded = true; // self-referencing

      while (i + 1 < length) {
        Span next = result.get(i + 1);
        String nextId = next.id();
        if (!nextId.equals(previousId)) break;

        boolean nextShared = Boolean.TRUE.equals(next.shared());
        if (previousShared == nextShared &&
          equals(previous.localEndpoint(), next.localEndpoint())) {
          fixNeeded = true; // multiple parts
          i++;
          continue;
        }
        if (nextShared && next.parentId() == null && previous.parentId() != null) {
          fixNeeded = true; // child server span missing parent ID
        }
        break;
      }
    }

    if (!fixNeeded) return spans; // nothing to do so return original input

    for (int i = 0; i < length; i++) {
      Span previous = result.get(i);
      String previousId = previous.id();
      boolean previousShared = Boolean.TRUE.equals(previous.shared());

      Span.Builder replacement = null;
      if (previous.traceId().length() != traceId.length()) {
        replacement = previous.toBuilder().traceId(traceId);
      }

      if (previousId.equals(previous.parentId())) {
        if (replacement == null) replacement = previous.toBuilder();
        replacement.parentId(null);
      }

      while (i + 1 < length) {
        Span next = result.get(i + 1);
        String nextId = next.id();
        if (!nextId.equals(previousId)) break;

        boolean nextShared = Boolean.TRUE.equals(next.shared());
        if (previousShared == nextShared &&
          equals(previous.localEndpoint(), next.localEndpoint())) {
          if (replacement == null) replacement = previous.toBuilder();
          replacement.merge(next);

          // remove the redundant one
          length--;
          result.remove(i + 1);
          continue;
        }
        if (nextShared && next.parentId() == null && previous.parentId() != null) {
          // handle a shared RPC server span that wasn't propagated its parent span ID
          result.set(i + 1, next.toBuilder().parentId(previous.parentId()).build());
        }
        break;
      }

      if (replacement != null) result.set(i, replacement.build());
    }

    return result;
  }

  static final Comparator<Span> CLEANUP_COMPARATOR = new Comparator<Span>() {
    @Override public int compare(Span left, Span right) {
      if (left == right) return 0;
      int bySpanId = left.id().compareTo(right.id());
      if (bySpanId != 0) return bySpanId;
      int byShared = nullSafeCompareTo(left.shared(), right.shared(), true);
      if (byShared != 0) return byShared;
      return compareEndpoint(left.localEndpoint(), right.localEndpoint());
    }
  };

  static boolean equals(Endpoint left, Endpoint right) {
    if (left == right) return true;
    if (left == null || right == null) return false;
    return left.equals(right);
  }

  /**
   * It is possible that a server can get the same request on a different port. Not addressing
   * this.
   */
  static int compareEndpoint(Endpoint left, Endpoint right) {
    int byService = nullSafeCompareTo(left.serviceName(), right.serviceName(), false);
    if (byService != 0) return byService;
    int byIpV4 = nullSafeCompareTo(left.ipv4(), right.ipv4(), false);
    if (byIpV4 != 0) return byIpV4;
    return nullSafeCompareTo(left.ipv6(), right.ipv6(), false);
  }

  static <T extends Comparable<T>> int nullSafeCompareTo(T left, T right, boolean nullFirst) {
    if (left == null) {
      return (right == null) ? 0 : (nullFirst ? -1 : 1);
    } else if (right == null) {
      return nullFirst ? 1 : -1;
    } else {
      return left.compareTo(right);
    }
  }

  Trace() {
  }
}
