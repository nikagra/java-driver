/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.core.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IsolatedTests.class)
public class MockResolverIT {

  private static final Logger LOG = LoggerFactory.getLogger(MockResolverIT.class);

  private static final int CLUSTER_WAIT_SECONDS =
      20; // Maximal wait time for cluster nodes to get up

  /**
   * Generous on purpose: a lookup racing the resolver re-point can re-cache the dead addresses for
   * one {@code networkaddress.cache.ttl} (30 s by default), and each reconnection round first fails
   * on the three dead nodes of the previous cluster.
   */
  private static final int RECOVERY_WAIT_SECONDS = 120;

  /**
   * The mock resolver's entries and the JVM's cache of the answers it gave are both process-global,
   * and every test in this class shares one JVM. Clearing both around each test keeps a name one
   * test re-pointed from being served to the next.
   */
  @Before
  @After
  public void clearResolverState() {
    MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
    MultimapHostResolverProvider.clearJvmCache();
  }

  private static void waitForAllNodesUp(CqlSession session, int expectedNodes) {
    Awaitility.await()
        .atMost(CLUSTER_WAIT_SECONDS, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .until(
            () -> {
              Collection<Node> nodes = session.getMetadata().getNodes().values();
              long upCount = nodes.stream().filter(n -> n.getUpSinceMillis() > 0).count();
              return upCount == expectedNodes;
            });
  }

  @Test
  public void should_connect_with_mocked_hostname() {
    CcmBridge.Builder ccmBridgeBuilder = CcmBridge.builder().withNodes(1);
    try (CcmBridge ccmBridge = ccmBridgeBuilder.build()) {
      MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(1));
      ccmBridge.create();
      ccmBridge.start();

      DriverConfigLoader loader =
          new DefaultProgrammaticDriverConfigLoaderBuilder()
              .withBoolean(TypedDriverOption.RESOLVE_CONTACT_POINTS.getRawOption(), false)
              .withBoolean(TypedDriverOption.RECONNECT_ON_INIT.getRawOption(), true)
              .withStringList(
                  TypedDriverOption.CONTACT_POINTS.getRawOption(),
                  Collections.singletonList("test.cluster.fake:9042"))
              .build();

      CqlSessionBuilder builder = new CqlSessionBuilder().withConfigLoader(loader);
      try (CqlSession session = builder.build()) {
        ResultSet rs = session.execute("select * from system.local where key='local'");
        List<Row> rows = rs.all();
        assertThat(rows).hasSize(1);
        LOG.trace("system.local contents: {}", rows.get(0).getFormattedContents());
        Collection<Node> nodes = session.getMetadata().getNodes().values();
        for (Node node : nodes) {
          LOG.trace("Found metadata node: {}", node);
        }
        // The node the control connection came up on is identified by the address it reached, not
        // by the contact point it was reached through, so select it by that address.
        String reachedIp = ccmBridge.getNodeIpAddress(1);
        Set<Node> filteredNodes =
            nodes.stream()
                .filter(x -> reachedIp.equals(hostAddressOf(x)))
                .collect(Collectors.toSet());
        assertThat(filteredNodes).hasSize(1);
        Node controlNode = filteredNodes.iterator().next();
        InetSocketAddress address = (InetSocketAddress) controlNode.getEndPoint().resolve();
        assertFalse(address.isUnresolved());
        // Bytes and port only: no label, so the metric prefix and toString() read like a peer's.
        assertThat(address.getHostString()).isEqualTo(reachedIp);
        assertThat(controlNode.getEndPoint().asMetricPrefix())
            .isEqualTo(reachedIp.replace('.', '_') + ":9042");
        assertThat(nodesNamingTheContactPoint(nodes)).isEmpty();
      }
    }
  }

  @Test
  public void replace_cluster_test() {
    final int numberOfNodes = 3;
    DriverConfigLoader loader =
        new DefaultProgrammaticDriverConfigLoaderBuilder()
            .withBoolean(TypedDriverOption.RESOLVE_CONTACT_POINTS.getRawOption(), false)
            .withBoolean(TypedDriverOption.RECONNECT_ON_INIT.getRawOption(), true)
            .withStringList(
                TypedDriverOption.CONTACT_POINTS.getRawOption(),
                Collections.singletonList("test.cluster.fake:9042"))
            .build();

    CqlSessionBuilder builder = new CqlSessionBuilder().withConfigLoader(loader);
    CqlSession session;

    try (CcmBridge ccmBridge =
        CcmBridge.builder().withNodes(numberOfNodes).withIpPrefix("127.0.1.").build()) {
      MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(1));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(2));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(3));
      ccmBridge.create();
      ccmBridge.start();
      session = builder.build();
      waitForAllNodesUp(session, numberOfNodes);
      ResultSet rs = session.execute("select * from system.local where key='local'");
      assertThat(rs).isNotNull();
      Row row = rs.one();
      assertThat(row).isNotNull();
      Collection<Node> nodes = session.getMetadata().getNodes().values();
      assertThat(nodes).hasSize(numberOfNodes);
      Iterator<Node> iterator = nodes.iterator();
      while (iterator.hasNext()) {
        LOG.trace("Metadata node: " + iterator.next().toString());
      }
      // Every node, the control node included, is identified by its own address.
      assertThat(nodesNamingTheContactPoint(nodes)).isEmpty();
    }
    try (CcmBridge ccmBridge =
        CcmBridge.builder().withNodes(numberOfNodes).withIpPrefix("127.0.1.").build()) {
      ccmBridge.create();
      ccmBridge.start();
      waitForAllNodesUp(session, numberOfNodes);
      ResultSet rs = session.execute("select * from system.local where key='local'");
      assertThat(rs).isNotNull();
      Row row = rs.one();
      assertThat(row).isNotNull();

      Collection<Node> nodes = session.getMetadata().getNodes().values();
      assertThat(nodes).hasSize(numberOfNodes);
      Iterator<Node> iterator = nodes.iterator();
      while (iterator.hasNext()) {
        LOG.trace("Metadata node: " + iterator.next().toString());
      }
      assertThat(nodesNamingTheContactPoint(nodes)).isEmpty();
    }
    session.close();
  }

  @Test
  public void should_recover_when_the_cluster_moves_to_new_addresses() {
    // replace_cluster_test brings the cluster back on the same addresses, so it never shows whether
    // a session finds one that came back on *different* ones. That takes the contact-point
    // reconnection fallback re-resolving the hostname once every live node is gone: every metadata
    // node, the control node included, holds a resolved address that is never re-resolved.
    final int numberOfNodes = 3;
    DriverConfigLoader loader =
        new DefaultProgrammaticDriverConfigLoaderBuilder()
            // Pinned, like every other test here: the fallback only re-resolves a contact point
            // that was kept unresolved. Relying on the fallback's own new default is deliberate;
            // relying on this one would make the test silently stop covering anything.
            .withBoolean(TypedDriverOption.RESOLVE_CONTACT_POINTS.getRawOption(), false)
            .withBoolean(TypedDriverOption.RECONNECT_ON_INIT.getRawOption(), true)
            .withDuration(
                TypedDriverOption.RECONNECTION_BASE_DELAY.getRawOption(), Duration.ofSeconds(1))
            .withDuration(
                TypedDriverOption.RECONNECTION_MAX_DELAY.getRawOption(), Duration.ofSeconds(1))
            .withDuration(
                TypedDriverOption.CONNECTION_CONNECT_TIMEOUT.getRawOption(), Duration.ofSeconds(2))
            .withStringList(
                TypedDriverOption.CONTACT_POINTS.getRawOption(),
                Collections.singletonList("test.cluster.fake:9042"))
            .build();
    CqlSessionBuilder builder = new CqlSessionBuilder().withConfigLoader(loader);
    CqlSession session = null;

    try {
      try (CcmBridge ccmBridge =
          CcmBridge.builder().withNodes(numberOfNodes).withIpPrefix("127.0.1.").build()) {
        pointContactPointAt(ccmBridge, numberOfNodes);
        ccmBridge.create();
        ccmBridge.start();
        session = builder.build();
        waitForAllNodesUp(session, numberOfNodes);
        assertThat(nodesOnPrefix(session, "127.0.1.")).hasSize(numberOfNodes);
        // The recovery below is the fallback's, and the loader leaves the option at its default on
        // purpose. Check the default is still what this test needs: with the fallback off there is
        // nothing to re-resolve the name, and the phase below would report a two-minute Awaitility
        // timeout rather than the reason for it.
        assertThat(
                session
                    .getContext()
                    .getConfig()
                    .getDefaultProfile()
                    .getBoolean(
                        TypedDriverOption.CONTROL_CONNECTION_RECONNECT_CONTACT_POINTS
                            .getRawOption()))
            .isTrue();
      }
      // The old cluster is gone. Before the new one exists, re-point the name and drop what the JVM
      // cached from the earlier lookups, or the fallback would be served the dead addresses for one
      // networkaddress.cache.ttl.
      try (CcmBridge ccmBridge =
          CcmBridge.builder().withNodes(numberOfNodes).withIpPrefix("127.0.2.").build()) {
        pointContactPointAt(ccmBridge, numberOfNodes);
        MultimapHostResolverProvider.clearJvmCache();
        ccmBridge.create();
        ccmBridge.start();
        awaitAllNodesUpOnPrefix(session, "127.0.2.", numberOfNodes);
        // Implied by the wait above, and stated because it is the point of the test: the session
        // followed the name to the new cluster and let go of the old one.
        Collection<Node> nodes = session.getMetadata().getNodes().values();
        assertThat(nodesOnPrefix(session, "127.0.1.")).isEmpty();
        assertThat(nodesNamingTheContactPoint(nodes)).isEmpty();
        ResultSet rs = session.execute("select * from system.local where key='local'");
        assertThat(rs.one()).isNotNull();
      }
    } finally {
      // A recovery timeout, or any failed assertion, must not leave a session reconnecting once a
      // second against torn-down clusters for the rest of this IsolatedTests JVM.
      if (session != null) {
        session.close();
      }
    }
  }

  private static void pointContactPointAt(CcmBridge ccmBridge, int numberOfNodes) {
    MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
    for (int i = 1; i <= numberOfNodes; i++) {
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(i));
    }
  }

  /**
   * Waits until every node the session knows sits on {@code ipPrefix} and is up. A parameter, not a
   * captured local, so the session may be reassigned by its caller's cleanup handling.
   */
  private static void awaitAllNodesUpOnPrefix(
      CqlSession session, String ipPrefix, int numberOfNodes) {
    Awaitility.await()
        .atMost(RECOVERY_WAIT_SECONDS, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        // Asserted rather than tested: a plain until() on the conjunction would report only that
        // the wait expired. Up-ness is counted on the prefix -- counting UP across the whole
        // metadata would be unsatisfiable while a node of the previous cluster is still recorded
        // UP -- but the total is counted across it, because the previous cluster's nodes going
        // away is part of what this waits for. Assert that after the wait instead and it races:
        // the new nodes can all be up while an old one is still listed.
        .untilAsserted(
            () -> {
              Set<Node> onPrefix = nodesOnPrefix(session, ipPrefix);
              assertThat(onPrefix).hasSize(numberOfNodes);
              assertThat(upNodes(onPrefix)).hasSize(numberOfNodes);
              assertThat(session.getMetadata().getNodes()).hasSize(numberOfNodes);
            });
  }

  /** The metadata nodes whose endpoint resolves to an address in {@code ipPrefix}. */
  private static Set<Node> nodesOnPrefix(CqlSession session, String ipPrefix) {
    return session.getMetadata().getNodes().values().stream()
        .filter(
            node -> {
              String ip = hostAddressOf(node);
              return ip != null && ip.startsWith(ipPrefix);
            })
        .collect(Collectors.toSet());
  }

  private static Set<Node> upNodes(Set<Node> nodes) {
    return nodes.stream().filter(n -> n.getUpSinceMillis() > 0).collect(Collectors.toSet());
  }

  /** The IP literal a node's endpoint resolves to, or {@code null} if it is not resolved. */
  private static String hostAddressOf(Node node) {
    SocketAddress resolved = node.getEndPoint().resolve();
    if (resolved instanceof InetSocketAddress && !((InetSocketAddress) resolved).isUnresolved()) {
      return ((InetSocketAddress) resolved).getAddress().getHostAddress();
    }
    return null;
  }

  /**
   * The nodes whose endpoint carries the contact-point name as its host string. Expected empty: the
   * node the control connection reached is identified by its own address, like every peer.
   */
  private static Set<Node> nodesNamingTheContactPoint(Collection<Node> nodes) {
    return nodes.stream()
        .filter(
            node -> {
              SocketAddress resolved = node.getEndPoint().resolve();
              return resolved instanceof InetSocketAddress
                  && "test.cluster.fake".equals(((InetSocketAddress) resolved).getHostString());
            })
        .collect(Collectors.toSet());
  }

  @SuppressWarnings("unused")
  public void run_replace_test_20_times() {
    for (int i = 1; i <= 20; i++) {
      LOG.info(
          "Running ({}/20}) {}", i, MockResolverIT.class.toString() + "#replace_cluster_test()");
      replace_cluster_test();
    }
  }

  // This is too long to run during CI, but is useful for manual investigations.
  @SuppressWarnings("unused")
  public void cannot_reconnect_with_resolved_socket() {
    DriverConfigLoader loader =
        new DefaultProgrammaticDriverConfigLoaderBuilder()
            .withBoolean(TypedDriverOption.RESOLVE_CONTACT_POINTS.getRawOption(), false)
            .withBoolean(TypedDriverOption.RECONNECT_ON_INIT.getRawOption(), true)
            .withStringList(
                TypedDriverOption.CONTACT_POINTS.getRawOption(),
                Collections.singletonList("test.cluster.fake:9042"))
            .build();

    CqlSessionBuilder builder = new CqlSessionBuilder().withConfigLoader(loader);
    CqlSession session;
    Collection<Node> nodes;
    Set<Node> filteredNodes;
    try (CcmBridge ccmBridge = CcmBridge.builder().withNodes(3).build()) {
      MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(1));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(2));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(3));
      ccmBridge.create();
      ccmBridge.start();
      session = builder.build();
      waitForAllNodesUp(session, 3);
      ResultSet rs = session.execute("select * from system.local where key='local'");
      assertThat(rs).isNotNull();
      Row row = rs.one();
      assertThat(row).isNotNull();
      nodes = session.getMetadata().getNodes().values();
      assertThat(nodes).hasSize(3);
      Iterator<Node> iterator = nodes.iterator();
      while (iterator.hasNext()) {
        LOG.trace("Metadata node: " + iterator.next().toString());
      }
      filteredNodes =
          nodes.stream()
              .filter(x -> x.toString().contains("test.cluster.fake"))
              .collect(Collectors.toSet());
      assertThat(filteredNodes).hasSize(1);
    }
    int counter = 0;
    while (filteredNodes.size() == 1) {
      counter++;
      if (counter == 255) {
        LOG.error("Completed 254 runs. Breaking.");
        break;
      }
      LOG.warn(
          "Launching another cluster until we lose resolved socket from metadata (run {}).",
          counter);
      try (CcmBridge ccmBridge = CcmBridge.builder().withNodes(3).build()) {
        MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
        MultimapHostResolverProvider.addResolverEntry(
            "test.cluster.fake", ccmBridge.getNodeIpAddress(1));
        MultimapHostResolverProvider.addResolverEntry(
            "test.cluster.fake", ccmBridge.getNodeIpAddress(2));
        MultimapHostResolverProvider.addResolverEntry(
            "test.cluster.fake", ccmBridge.getNodeIpAddress(3));
        ccmBridge.create();
        ccmBridge.start();
        waitForAllNodesUp(session, 3);
        nodes = session.getMetadata().getNodes().values();
        assertThat(nodes).hasSize(3);
        Iterator<Node> iterator = nodes.iterator();
        while (iterator.hasNext()) {
          LOG.trace("Metadata node: " + iterator.next().toString());
        }
        filteredNodes =
            nodes.stream()
                .filter(x -> x.toString().contains("test.cluster.fake"))
                .collect(Collectors.toSet());
        if (filteredNodes.size() > 1) {
          fail(
              "Somehow there is more than 1 node in metadata with unresolved hostname. This should not ever happen.");
        }
      }
    }
    Iterator<Node> iterator = nodes.iterator();
    while (iterator.hasNext()) {
      InetSocketAddress address = (InetSocketAddress) iterator.next().getEndPoint().resolve();
      assertFalse(address.isUnresolved());
    }
    try (CcmBridge ccmBridge = CcmBridge.builder().withNodes(3).build()) {
      MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(1));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(2));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(3));
      // Now the driver should fail to reconnect since unresolved hostname is gone.
      ccmBridge.create();
      ccmBridge.start();
      waitForAllNodesUp(session, 3);
      session.execute("select * from system.local where key='local'");
    }
    session.close();
  }
}
