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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.AsyncAutoCloseable;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Monitors the state of the Cassandra cluster.
 *
 * <p>It can either push {@link TopologyEvent topology events} to the rest of the driver (to do
 * that, retrieve the {@link EventBus}) from the {@link InternalDriverContext}), or receive requests
 * to refresh data about the nodes.
 *
 * <p>The default implementation uses the control connection: {@code TOPOLOGY_CHANGE} and {@code
 * STATUS_CHANGE} events on the connection are converted into {@code TopologyEvent}s, and node
 * refreshes are done with queries to system tables. If you prefer to rely on an external monitoring
 * tool, this can be completely overridden.
 */
public interface TopologyMonitor extends AsyncAutoCloseable {

  /**
   * Triggers the initialization of the monitor.
   *
   * <p>The completion of the future returned by this method marks the point when the driver
   * considers itself "connected" to the cluster, and proceeds with the rest of the initialization:
   * refreshing the list of nodes and the metadata, opening connection pools, etc. By then, the
   * topology monitor should be ready to accept calls to its other methods; in particular, {@link
   * #refreshNodeList()} will be called shortly after the completion of the future, to load the
   * initial list of nodes to connect to.
   *
   * <p>If {@code advanced.reconnect-on-init = true} in the configuration, this method is
   * responsible for handling reconnection. That is, if the initial attempt to "connect" to the
   * cluster fails, it must schedule reattempts, and only complete the returned future when
   * connection eventually succeeds. If the user cancels the returned future, then the reconnection
   * attempts should stop.
   *
   * <p>If this method is called multiple times, it should trigger initialization only once, and
   * return the same future on subsequent invocations.
   */
  CompletionStage<Void> init();

  /**
   * The future returned by {@link #init()}.
   *
   * <p>Note that this method may be called before {@link #init()}; at that stage, the future should
   * already exist, but be incomplete.
   */
  CompletionStage<Void> initFuture();

  /**
   * Invoked when the driver needs to refresh the information about an existing node. This is called
   * when the node was back and comes back up.
   *
   * <p>This will be invoked directly from a driver's internal thread; if the refresh involves
   * blocking I/O or heavy computations, it should be scheduled on a separate thread.
   *
   * @param node the node to refresh.
   * @return a future that completes with the information. If the monitor can't fulfill the request
   *     at this time, it should reply with {@link Optional#empty()}, and the driver will carry on
   *     with its current information.
   */
  CompletionStage<Optional<NodeInfo>> refreshNode(Node node);

  /**
   * Invoked when the driver needs to get information about a newly discovered node.
   *
   * <p>This will be invoked directly from a driver's internal thread; if the refresh involves
   * blocking I/O or heavy computations, it should be scheduled on a separate thread.
   *
   * @param broadcastRpcAddress the node's broadcast RPC address,.
   * @return a future that completes with the information. If the monitor doesn't know any node with
   *     this address, it should reply with {@link Optional#empty()}; the new node will be ignored.
   * @see Node#getBroadcastRpcAddress()
   */
  CompletionStage<Optional<NodeInfo>> getNewNodeInfo(InetSocketAddress broadcastRpcAddress);

  /**
   * Invoked when the driver needs to refresh information about all the nodes.
   *
   * <p>This will be invoked directly from a driver's internal thread; if the refresh involves
   * blocking I/O or heavy computations, it should be scheduled on a separate thread.
   *
   * <p>The driver calls this at initialization, and uses the result to initialize the {@link
   * LoadBalancingPolicy}; successful initialization of the {@link Session} object depends on that
   * initial call succeeding.
   *
   * @return a future that completes with the information. We assume that the full node list will
   *     always be returned in a single message (no paging).
   */
  CompletionStage<Iterable<NodeInfo>> refreshNodeList();

  /**
   * Resolves the full identity and metadata of the node at the other end of the given channel by
   * querying system.local. This is used by the control connection after establishing a channel to
   * resolve the contact point's full identity (hostId, datacenter, rack, endpoint, etc.).
   *
   * @param channel the channel to query system.local on.
   * @return a future that completes with the resolved node info.
   */
  CompletionStage<NodeInfo> getChannelNodeInfo(DriverChannel channel);

  /**
   * Checks whether the nodes in the cluster agree on a common schema version.
   *
   * <p>This should typically be implemented with a few retries and a timeout, as the schema can
   * take a while to replicate across nodes.
   */
  CompletionStage<Boolean> checkSchemaAgreement();

  /**
   * Resets any cached column name sets learned from previous system table query responses.
   *
   * <p>Called by the control connection on reconnect so that the next topology refresh re-learns
   * the available columns via {@code SELECT *} instead of reusing a potentially stale projection.
   *
   * <p>The default implementation is a no-op; implementations that cache column names (such as
   * {@link DefaultTopologyMonitor}) should override this method.
   */
  default void resetColumnCaches() {}

  /**
   * Whether this monitor re-resolves node addresses on every connection attempt (for example by
   * handing out a proxy hostname to be looked up at connect time), rather than registering an
   * address resolved once.
   *
   * <p>Implementations must report {@code true} only where re-resolution can actually happen: a
   * monitor that hands out an address it looked up once, or a name it will not look up again, has
   * to answer {@code false}, or it silently suppresses the fallback for a node whose address can
   * then never change.
   *
   * <p>When {@code true}, the control connection's reconnection plan does not append the original
   * contact points as a DNS re-resolution fallback (see {@code
   * advanced.control-connection.reconnection.fallback-to-original-contact-points}) unless the
   * live-node plan is empty: the monitor keeps addresses fresh on its own, and appending raw
   * contact points could resurrect nodes it has removed.
   *
   * <p>The default is {@code false}, which is right for {@link DefaultTopologyMonitor}: peers hold
   * a resolved address from the peers table, and the node the control connection reached is
   * registered under the address it reached, so neither re-reads DNS. Proxy-based monitors override
   * this.
   */
  default boolean reresolvesNodeAddresses() {
    return false;
  }

  /**
   * The endpoint that the node at the other end of {@code channel} should be identified by, called
   * once when the channel is adopted as the control connection.
   *
   * <p>Asked only for a candidate the driver has not identified yet -- one with no host id, which
   * is to say a contact point. A node that has a host id keeps the endpoint the driver already
   * chose for it: that endpoint may be an unresolved hostname on purpose (an address translator
   * configured to resolve on every connection, {@code advanced.address-translator.resolve-addresses
   * = false}), and replacing it with the address one connection reached would freeze it there and
   * end the re-resolution it exists for.
   *
   * <p>Answered before the channel is adopted: before {@code ControlConnection} publishes it and
   * before the identity query is sent on it -- though after protocol init and the TLS handshake,
   * which ran against the configured endpoint. That ordering is what lets every later read of
   * {@code channel.getEndPoint()} -- this monitor's own {@code refreshNodeList()}, {@code
   * refreshNode()} and {@code getNewNodeInfo()} included -- see the endpoint returned here: {@code
   * getChannelNodeInfo} takes the local row's endpoint straight off the channel, so the node
   * registered for it carries this very instance. Deriving it later would leave a window in which
   * one node refresh identifies the control node by the address it was dialled at and the next one
   * by the address it answered on, and reconciling those two rewrites the node's endpoint and
   * clears its metrics ({@code NodesRefresh#copyInfos} to {@code DefaultNode#setEndPoint}).
   *
   * <p>A monitor that can only name the node from the identity row itself (the Cloud SNI proxy,
   * client routes) is the exception to that: it returns the configured endpoint here, and {@code
   * ControlConnection} upgrades the channel's endpoint once the row arrives -- at a point where the
   * node does not exist yet, so nothing has compared it.
   *
   * <p>The default returns the channel's configured endpoint, which is right for any monitor whose
   * endpoints already identify a node on their own (the Cloud SNI proxy, client routes). {@link
   * DefaultTopologyMonitor} overrides it for the one case where the configured endpoint names no
   * node in particular: an unresolved contact point.
   *
   * <p>May return {@code null} to leave the channel's configured endpoint alone. Throwing is
   * tolerated but pointless: {@code ControlConnection} closes the channel, records the error and
   * moves on to the next candidate, so a throw costs this candidate its turn in the reconnection
   * round and is reported in that round's {@code AllNodesFailedException}.
   */
  default EndPoint connectedNodeEndPoint(DriverChannel channel) {
    return channel.getEndPoint();
  }
}
