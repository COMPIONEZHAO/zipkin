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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.logging.Level.FINE;

/**
 * Convenience type representing a tree. This is here because multiple facets in zipkin require
 * traversing the trace tree. For example, looking at network boundaries to correct clock skew, or
 * counting requests imply visiting the tree.
 *
 * @param <V> the node's value. Ex a full span or a tuple like {@code (serviceName, isLocal)}
 */
public final class Node<V> {
  /** Set via {@link #addChild(Node)} */
  Node<V> parent;
  V value;
  /** mutable to avoid allocating lists for childless nodes */
  List<Node<V>> children = Collections.emptyList();

  Node(@Nullable V value) {
    this.value = value;
  }

  /** Returns the parent, or null if root */
  @Nullable public Node<V> parent() {
    return parent;
  }

  /** Returns the value, or null if a synthetic root node */
  @Nullable public V value() {
    return value;
  }

  /** Mutable as some transformations, such as clock skew, adjust the current node in the tree. */
  public Node<V> setValue(V newValue) {
    if (newValue == null) throw new NullPointerException("newValue == null");
    this.value = newValue;
    return this;
  }

  public Node<V> addChild(Node<V> child) {
    if (child == this) throw new IllegalArgumentException("circular dependency on " + this);
    child.parent = this;
    if (children.equals(Collections.emptyList())) children = new ArrayList<>();
    children.add(child);
    return this;
  }

  /** Returns the children of this node. */
  public Collection<Node<V>> children() {
    return children;
  }

  /** Traverses the tree, breadth-first. */
  public Iterator<Node<V>> traverse() {
    return new BreadthFirstIterator<>(this);
  }

  static final class BreadthFirstIterator<V> implements Iterator<Node<V>> {
    private final Queue<Node<V>> queue = new ArrayDeque<>();

    BreadthFirstIterator(Node<V> root) {
      queue.add(root);
    }

    @Override
    public boolean hasNext() {
      return !queue.isEmpty();
    }

    @Override
    public Node<V> next() {
      if (!hasNext()) throw new NoSuchElementException();
      Node<V> result = queue.remove();
      queue.addAll(result.children);
      return result;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove");
    }
  }

  interface MergeFunction<V> {
    V merge(@Nullable V existing, @Nullable V update);
  }

  static final MergeFunction FIRST_NOT_NULL = new MergeFunction() {
    @Override public Object merge(@Nullable Object existing, @Nullable Object update) {
      return existing != null ? existing : update;
    }
  };

  /**
   * Some operations do not require the entire span object. This creates a tree given (parent id,
   * id) pairs.
   *
   * @param <V> same type as {@link Node#value}
   */
  public static final class TreeBuilder<V> {
    final Logger logger;
    final MergeFunction<V> mergeFunction;
    final String traceId;

    public TreeBuilder(Logger logger, String traceId) {
      this(logger, FIRST_NOT_NULL, traceId);
    }

    TreeBuilder(Logger logger, MergeFunction<V> mergeFunction, String traceId) {
      this.logger = logger;
      this.mergeFunction = mergeFunction;
      this.traceId = traceId;
    }

    Key rootKey = null;
    Node<V> rootNode = null;
    List<Entry<V>> entries = new ArrayList<>();
    // Nodes representing the trace tree
    Map<Key, Node<V>> keyToNode = new LinkedHashMap<>();
    // Collect the parent-child relationships between all spans.
    Map<Key, Key> keyToParent = new LinkedHashMap<>(keyToNode.size());

    /**
     * This is a variant of a normal parent/child graph specialized for Zipkin. In a Zipkin tree, a
     * parent and child can share the same ID if in an RPC. This variant treats a {@code shared}
     * node as a child of any node matching the same ID.
     *
     * @return false after logging to FINE if the value couldn't be added
     */
    public boolean addNode(@Nullable String parentId, String id, @Nullable Boolean shared,
      V value) {
      if (parentId != null) {
        if (parentId.equals(id)) {
          if (logger.isLoggable(FINE)) {
            logger.fine(
              format("skipping circular dependency: traceId=%s, spanId=%s", traceId, id));
          }
          return false;
        }
      }
      boolean sharedV = Boolean.TRUE.equals(shared);

      // ensure that keyToParent has a key for all entries as this is used to iterate later
      Key idKey = new Key(id, sharedV);
      Key parentKey = null;
      if (sharedV) {
        parentKey = new Key(id, false);
      } else if (parentId != null) {
        parentKey = new Key(parentId, false);
      }
      keyToParent.put(idKey, parentKey);

      entries.add(new Entry<>(parentId, id, sharedV, value));
      return true;
    }

    void processNode(Entry<V> entry) {
      Key key = new Key(entry.id, entry.shared);
      V value = entry.value;

      Key parentKey = null;
      if (key.shared) {
        parentKey = new Key(entry.id, false);
      } else if (entry.parentId != null) {
        // Check to see if a shared parent exists, and prefer that. This means the current entry is
        // a child of a shared node.
        parentKey = new Key(entry.parentId, true);
        if (keyToParent.containsKey(parentKey)) {
          keyToParent.put(key, parentKey);
        } else {
          parentKey = new Key(entry.parentId, false);
        }
      }

      if (parentKey == null) {
        if (rootKey != null) {
          if (logger.isLoggable(FINE)) {
            logger.fine(format(
              "attributing span missing parent to root: traceId=%s, rootSpanId=%s, spanId=%s",
              traceId, rootKey.id, key.id));
          }
        } else {
          rootKey = key;
        }
      }

      Node<V> node = new Node<>(value);
      // special-case root, and attribute missing parents to it. In
      // other words, assume that the first root is the "real" root.
      if (parentKey == null && rootNode == null) {
        rootNode = node;
        rootKey = key;
        keyToParent.remove(key);
      } else if (parentKey == null && rootKey.equals(key)) {
        rootNode.setValue(mergeFunction.merge(rootNode.value, node.value));
      } else {
        Node<V> previous = keyToNode.put(key, node);
        if (previous != null) node.setValue(mergeFunction.merge(previous.value, node.value));
      }
    }

    /** Builds a tree from calls to {@link #addNode}, or returns an empty tree. */
    public Node<V> build() {
      for (int i = 0, length = entries.size(); i < length; i++) {
        processNode(entries.get(i));
      }

      if (rootNode == null) {
        if (logger.isLoggable(FINE)) {
          logger.fine("substituting dummy node for missing root span: traceId=" + traceId);
        }
        rootNode = new Node<>(null);
      }

      // Materialize the tree using parent - child relationships
      for (Map.Entry<Key, Key> entry : keyToParent.entrySet()) {
        if (entry.getKey() == rootKey) continue; // don't re-process root

        Node<V> node = keyToNode.get(entry.getKey());
        Node<V> parent = keyToNode.get(entry.getValue());
        if (parent == null) { // handle headless
          rootNode.addChild(node);
        } else {
          parent.addChild(node);
        }
      }
      return rootNode;
    }
  }

  /** A node in the tree is not always unique on ID. Sharing is allowed once per ID (Ex: in RPC). */
  static final class Key {
    final String id;
    boolean shared;

    Key(String id, boolean shared) {
      this.id = id;
      this.shared = shared;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof Key)) return false;
      Key that = (Key) o;
      return id.equals(that.id) && shared == that.shared;
    }

    @Override public int hashCode() {
      int result = 1;
      result *= 1000003;
      result ^= id.hashCode();
      result *= 1000003;
      result ^= shared ? 1231 : 1237;
      return result;
    }
  }

  static final class Entry<V> {
    @Nullable final String parentId;
    final String id;
    final boolean shared;
    final V value;

    Entry(@Nullable String parentId, String id, boolean shared, V value) {
      if (id == null) throw new NullPointerException("id == null");
      if (value == null) throw new NullPointerException("value == null");
      this.parentId = parentId;
      this.id = id;
      this.shared = shared;
      this.value = value;
    }

    @Override public String toString() {
      return "Entry{parentId="
        + parentId
        + ", id="
        + id
        + ", shared="
        + shared
        + ", value="
        + value
        + "}";
    }
  }

  @Override public String toString() {
    return "Node{parent=" + parent + ", value=" + value + ", children=" + children + "}";
  }
}
