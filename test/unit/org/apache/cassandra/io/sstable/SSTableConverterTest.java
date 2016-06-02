/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.io.sstable;

import java.util.*;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.SSTableConverter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.tools.StandaloneConverter;
import org.apache.cassandra.utils.OutputHandler;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;


public class SSTableConverterTest extends CQLTester
{
    @Test
    public void testUnsupportedVersionShouldFail() throws Throwable
    {
        initAndAssertTestEnvironment();

        StandaloneConverter converter = new StandaloneConverter();
        String[] args = new String[]{keyspace(), getCurrentColumnFamilyStore().toString(), "jb"};
        converter.main(args);
    }

    @Test
    public void testConverteLatestToMa() throws Throwable
    {
        initAndAssertTestEnvironment();

        Keyspace keyspace = Keyspace.openWithoutSSTables(KEYSPACE);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        OutputHandler handler = new OutputHandler.SystemOutput(false, false);

        Collection<SSTableReader> readers = cfs.getSSTables();

        for (SSTableReader reader : readers)
        {
            Descriptor descriptor = Descriptor.fromFilename(reader.getFilename());
            Map<MetadataType, MetadataComponent> metadata = descriptor.getMetadataSerializer().deserialize(descriptor, EnumSet.allOf(MetadataType.class));
            StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);

            assertNotSame(ReplayPosition.NONE, stats.commitLogLowerBound);
        }

        cfs.clearUnsafe();

        for (SSTableReader sstable : readers)
        {
            try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.CONVERT_SSTABLES, sstable))
            {
                SSTableConverter converter = new SSTableConverter(cfs, txn, handler, "ma");
                cfs.addSSTables(converter.convert(true));
            }
        }

        readers = cfs.getSSTables();

        for (SSTableReader reader : readers)
        {
            Descriptor descriptor = Descriptor.fromFilename(reader.getFilename());
            Map<MetadataType, MetadataComponent> metadata = descriptor.getMetadataSerializer().deserialize(descriptor, EnumSet.allOf(MetadataType.class));
            StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);

            assertEquals(ReplayPosition.NONE, stats.commitLogLowerBound);
        }
    }

    private void initAndAssertTestEnvironment() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v1 int, v2 text);");

        execute("INSERT INTO %s (k, v1, v2) values (?, ?, ?)", "first", 1, "value1");
        execute("INSERT INTO %s (k, v1, v2) values (?, ?, ?)", "second", 2, "value2");
        execute("INSERT INTO %s (k, v1, v2) values (?, ?, ?)", "third", 3, "value3");

        flush();

        assertRows(execute("SELECT * FROM %s"),
                   row("third",  3, "value3"),
                   row("second", 2, "value2"),
                   row("first",  1, "value1")
        );

    }
}