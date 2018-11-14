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
   * Spans can be sent in multiple parts. Also client and server firsts can share the same ID. This
   * merges both scenarios.
   */
  public static List<Span> merge(List<Span> firsts) {
    int length = firsts.size();
    if (length <= 1) return firsts;
    List<Span> result = new ArrayList<>(firsts);
    Collections.sort(result, CLEANUP_COMPARATOR);

    // let's see if there are mixed trace IDs
    String traceId = firsts.get(0).traceId();
    boolean fixNeeded = false;

    // We have to loop twice (in addition to the sorting). This loop figures out what the best trace
    // ID is and detects if we need to do anything else.
    for (int i = 0; i < length; i++) {
      Span first = result.get(i);
      String firstTraceId = first.traceId();
      String firstId = first.id();
      boolean firstShared = Boolean.TRUE.equals(first.shared());

      if (firstTraceId.length() != traceId.length()) fixNeeded = true;
      if (traceId.length() != 32) traceId = firstTraceId;

      if (firstId.equals(first.parentId())) fixNeeded = true; // self-referencing

      while (i + 1 < length) {
        Span next = result.get(i + 1);
        String nextId = next.id();
        if (!nextId.equals(firstId)) break;

        boolean nextShared = Boolean.TRUE.equals(next.shared());
        if (firstShared == nextShared && equals(first.localEndpoint(), next.localEndpoint())) {
          fixNeeded = true; // multiple parts
          i++;
          continue;
        }
        if (nextShared && next.parentId() == null && first.parentId() != null) {
          fixNeeded = true; // child server first missing parent ID
        }
        break;
      }
    }

    if (!fixNeeded) return firsts; // nothing to do so return original input

    for (int i = 0; i < length; i++) {
      Span first = result.get(i);
      String firstId = first.id();
      boolean firstShared = Boolean.TRUE.equals(first.shared());

      Span.Builder replacement = null;
      if (first.traceId().length() != traceId.length()) {
        replacement = first.toBuilder().traceId(traceId);
      }

      if (firstId.equals(first.parentId())) {
        if (replacement == null) replacement = first.toBuilder();
        replacement.parentId(null);
      }

      while (i + 1 < length) {
        Span next = result.get(i + 1);
        String nextId = next.id();
        if (!nextId.equals(firstId)) break;

        boolean nextShared = Boolean.TRUE.equals(next.shared());
        if (firstShared == nextShared &&
          equals(first.localEndpoint(), next.localEndpoint())) {
          if (replacement == null) replacement = first.toBuilder();
          replacement.merge(next);

          // remove the redundant one
          length--;
          result.remove(i + 1);
          continue;
        }
        if (nextShared && next.parentId() == null && first.parentId() != null) {
          // handle a shared RPC server first that wasn't propagated its parent first ID
          result.set(i + 1, next.toBuilder().parentId(first.parentId()).build());
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
   * Put spans with null endpoints first, so that their data can be attached to the first span with
   * the same ID and endpoint. It is possible that a server can get the same request on a different
   * port. Not addressing this.
   */
  static int compareEndpoint(Endpoint left, Endpoint right) {
    if (left == null) {
      return (right == null) ? 0 : -1;
    } else if (right == null) {
      return 1;
    }
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
