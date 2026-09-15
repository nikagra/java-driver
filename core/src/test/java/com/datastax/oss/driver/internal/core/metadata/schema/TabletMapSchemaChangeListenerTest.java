package com.datastax.oss.driver.internal.core.metadata.schema;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.TabletMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * The integration tests mock this listener out, so they only prove the driver calls it. These cover
 * the forwarding itself.
 */
@RunWith(MockitoJUnitRunner.Strict.class)
public class TabletMapSchemaChangeListenerTest {

  private static final CqlIdentifier KS = CqlIdentifier.fromCql("ks");
  private static final CqlIdentifier PREVIOUS_KS = CqlIdentifier.fromCql("ks_previous");
  private static final CqlIdentifier TABLE = CqlIdentifier.fromCql("tab");
  private static final CqlIdentifier PREVIOUS_TABLE = CqlIdentifier.fromCql("tab_previous");

  @Mock private MetadataManager manager;
  @Mock private Metadata metadata;
  @Mock private TabletMap tabletMap;
  @Mock private KeyspaceMetadata keyspace;
  @Mock private KeyspaceMetadata previousKeyspace;
  @Mock private TableMetadata table;
  @Mock private TableMetadata previousTable;

  private TabletMapSchemaChangeListener listener;

  @Before
  public void setup() {
    when(manager.getMetadata()).thenReturn(metadata);
    listener = new TabletMapSchemaChangeListener(manager);
  }

  @Test
  public void should_evict_keyspace_when_it_is_dropped() {
    givenTabletMap();
    when(keyspace.getName()).thenReturn(KS);

    listener.onKeyspaceDropped(keyspace);

    verify(tabletMap).removeByKeyspace(KS);
  }

  /**
   * An update evicts under the PREVIOUS name, since that is what the cached tablets are keyed by.
   */
  @Test
  public void should_evict_previous_keyspace_when_it_is_updated() {
    givenTabletMap();
    when(previousKeyspace.getName()).thenReturn(PREVIOUS_KS);

    listener.onKeyspaceUpdated(keyspace, previousKeyspace);

    verify(tabletMap).removeByKeyspace(PREVIOUS_KS);
    verify(tabletMap, never()).removeByKeyspace(KS);
  }

  @Test
  public void should_evict_table_when_it_is_dropped() {
    givenTabletMap();
    when(table.getName()).thenReturn(TABLE);

    listener.onTableDropped(table);

    verify(tabletMap).removeByTable(TABLE);
  }

  @Test
  public void should_evict_previous_table_when_it_is_updated() {
    givenTabletMap();
    when(previousTable.getName()).thenReturn(PREVIOUS_TABLE);

    listener.onTableUpdated(table, previousTable);

    verify(tabletMap).removeByTable(PREVIOUS_TABLE);
    verify(tabletMap, never()).removeByTable(TABLE);
  }

  @Test
  public void should_do_nothing_when_there_is_no_tablet_map() {
    // Server without tablets: every callback must be a no-op rather than an NPE
    when(metadata.getTabletMap()).thenReturn(Optional.empty());

    listener.onKeyspaceDropped(keyspace);
    listener.onKeyspaceUpdated(keyspace, previousKeyspace);
    listener.onTableDropped(table);
    listener.onTableUpdated(table, previousTable);

    verifyNoInteractions(tabletMap, keyspace, previousKeyspace, table, previousTable);
  }

  private void givenTabletMap() {
    when(metadata.getTabletMap()).thenReturn(Optional.of(tabletMap));
  }
}
