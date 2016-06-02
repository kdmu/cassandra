/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.tools;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.compaction.SSTableConverter;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.OutputHandler;

public class StandaloneUpgrader extends StandaloneConverter
{
    public static void main(String args[])
    {
        Options options = Options.parseArgs(args, true);

        if (!DatabaseDescriptor.getSSTableFormat().info.getVersion(options.version).isCompatibleForWriting())
        {
            throw new RuntimeException(String.format("Conversion to %s is not supported", options.version));
        }

        Util.initDatabaseDescriptor();

        try
        {
            // load keyspace descriptions.
            Schema.instance.loadFromDisk(false);

            if (Schema.instance.getCFMetaData(options.keyspace, options.cf) == null)
                throw new IllegalArgumentException(String.format("Unknown keyspace/table %s.%s",
                                                                 options.keyspace,
                                                                 options.cf));

            Keyspace keyspace = Keyspace.openWithoutSSTables(options.keyspace);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(options.cf);

            OutputHandler handler = new OutputHandler.SystemOutput(false, options.debug);
            Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW);
            if (options.snapshot != null)
                lister.onlyBackups(true).snapshots(options.snapshot);
            else
                lister.includeBackups(false);

            Collection<SSTableReader> readers = new ArrayList<>();

            // Upgrade sstables
            for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
            {
                Set<Component> components = entry.getValue();
                if (!components.contains(Component.DATA) || !components.contains(Component.PRIMARY_INDEX))
                    continue;

                try
                {
                    SSTableReader sstable = SSTableReader.openNoValidation(entry.getKey(), components, cfs);
                    if (sstable.descriptor.version.equals(DatabaseDescriptor.getSSTableFormat().info.getLatestVersion()))
                        continue;
                    readers.add(sstable);
                }
                catch (Exception e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    System.err.println(String.format("Error Loading %s: %s", entry.getKey(), e.getMessage()));
                    if (options.debug)
                        e.printStackTrace(System.err);

                    continue;
                }
            }

            int numSSTables = readers.size();
            handler.output("Found " + numSSTables + " sstables that need upgrade.");

            for (SSTableReader sstable : readers)
            {
                try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.UPGRADE_SSTABLES, sstable))
                {
                    SSTableConverter converter = new SSTableConverter(cfs, txn, handler, options.version);
                    converter.convert(options.keepSource);
                }
                catch (Exception e)
                {
                    System.err.println(String.format("Error upgrading %s: %s", sstable, e.getMessage()));
                    if (options.debug)
                        e.printStackTrace(System.err);
                }
                finally
                {
                    // we should have released this through commit of the LifecycleTransaction,
                    // but in case the upgrade failed (or something else went wrong) make sure we don't retain a reference
                    sstable.selfRef().ensureReleased();
                }
            }
            CompactionManager.instance.finishCompactionsAndShutdown(5, TimeUnit.MINUTES);
            LifecycleTransaction.waitForDeletions();
            System.exit(0);
        }
        catch (Exception e)
        {
            System.err.println(e.getMessage());
            if (options.debug)
                e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}

