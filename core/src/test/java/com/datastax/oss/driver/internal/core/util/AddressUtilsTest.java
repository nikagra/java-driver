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
package com.datastax.oss.driver.internal.core.util;

import static com.datastax.oss.driver.Assertions.assertThat;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.junit.Test;

/**
 * Note: every assertion here reads a label that is already set, or an address literal. Nothing
 * calls {@code getHostName()} on an unlabelled address, which would be a reverse lookup.
 */
public class AddressUtilsTest {

  private static final byte[] IPV4 = {10, 0, 0, 2};
  // 2001:db8::1
  private static final byte[] IPV6 = {
    0x20, 0x01, 0x0d, (byte) 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1
  };

  @Test
  public void should_label_ipv4_address() throws Exception {
    InetAddress labelled =
        AddressUtils.withHostName("cluster.example.com", InetAddress.getByAddress(IPV4));

    assertThat(labelled.getHostName()).isEqualTo("cluster.example.com");
    assertThat(labelled.getHostAddress()).isEqualTo("10.0.0.2");
  }

  @Test
  public void should_remove_label_from_ipv4_address() throws Exception {
    InetAddress labelled = InetAddress.getByAddress("cluster.example.com", IPV4);

    InetAddress unlabelled = AddressUtils.withHostName(null, labelled);

    assertThat(unlabelled.getHostAddress()).isEqualTo("10.0.0.2");
    // InetAddress#toString() prints the label field as-is, without resolving it.
    assertThat(unlabelled.toString()).isEqualTo("/10.0.0.2");
  }

  @Test
  public void should_not_append_scope_to_unscoped_ipv6_address() throws Exception {
    Inet6Address unscoped = (Inet6Address) InetAddress.getByAddress(null, IPV6);
    assertThat(unscoped.getScopeId()).isZero();

    InetAddress relabelled = AddressUtils.withHostName("cluster.example.com", unscoped);

    // The whole point of the scopeId != 0 guard: the scoped getByAddress() overload treats any
    // scope_id >= 0 as scoped, so going through it here would report 2001:db8::1%0.
    assertThat(relabelled.getHostAddress()).isEqualTo("2001:db8:0:0:0:0:0:1");
    assertThat(relabelled.getHostAddress()).doesNotContain("%");
    assertThat(((Inet6Address) relabelled).getScopeId()).isZero();
  }

  @Test
  public void should_preserve_ipv6_scope_id() throws Exception {
    Inet6Address scoped = Inet6Address.getByAddress("cluster.example.com", IPV6, 5);

    InetAddress unlabelled = AddressUtils.withHostName(null, scoped);

    assertThat(((Inet6Address) unlabelled).getScopeId()).isEqualTo(5);
    assertThat(unlabelled.getHostAddress()).isEqualTo("2001:db8:0:0:0:0:0:1%5");
  }

  @Test
  public void should_strip_host_name_from_resolved_address() throws Exception {
    InetSocketAddress labelled =
        new InetSocketAddress(InetAddress.getByAddress("cluster.example.com", IPV4), 9042);
    assertThat(labelled.getHostString()).isEqualTo("cluster.example.com");

    InetSocketAddress stripped = AddressUtils.stripHostName(labelled);

    assertThat(stripped).isNotNull();
    assertThat(stripped.isUnresolved()).isFalse();
    assertThat(stripped.getPort()).isEqualTo(9042);
    // Bytes and port only: this is what a node's durable identity and its metric prefix key on.
    assertThat(stripped.getHostString()).isEqualTo("10.0.0.2");
    assertThat(stripped.toString()).isEqualTo("/10.0.0.2:9042");
  }

  @Test
  public void should_strip_host_name_and_keep_ipv6_scope_id() throws Exception {
    InetSocketAddress labelled =
        new InetSocketAddress(Inet6Address.getByAddress("cluster.example.com", IPV6, 5), 9042);

    InetSocketAddress stripped = AddressUtils.stripHostName(labelled);

    assertThat(stripped).isNotNull();
    assertThat(stripped.getHostString()).isEqualTo("2001:db8:0:0:0:0:0:1%5");
  }

  @Test
  public void should_not_strip_host_name_from_unresolved_address() {
    InetSocketAddress unresolved = InetSocketAddress.createUnresolved("cluster.example.com", 9042);

    // No InetAddress to strip, so there is no identity to derive: callers keep what they had.
    assertThat(AddressUtils.stripHostName(unresolved)).isNull();
  }
}
