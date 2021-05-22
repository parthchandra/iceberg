/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
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

package org.apache.iceberg.hive;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.COMMIT_NUM_STATUS_CHECKS;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_STATUS_CHECKS_DEFAULT;

/**
 * TODO we should be able to extract some more commonalities to BaseMetastoreTableOperations to
 * avoid code duplication between this class and Metacat Tables.
 */
public class HiveTableOperations extends BaseMetastoreTableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(HiveTableOperations.class);

  private static final int COMMIT_STATUS_CHECK_WAIT_MS = 1000;

  private static final String HIVE_ACQUIRE_LOCK_TIMEOUT_MS = "iceberg.hive.lock-timeout-ms";
  private static final String HIVE_LOCK_CHECK_MIN_WAIT_MS = "iceberg.hive.lock-check-min-wait-ms";
  private static final String HIVE_LOCK_CHECK_MAX_WAIT_MS = "iceberg.hive.lock-check-max-wait-ms";
  private static final String HIVE_LOCK_CHECK_BACKOFF_SCALE_FACTOR = "iceberg.hive.lock-check-backoff-scale-factor";
  private static final long HIVE_ACQUIRE_LOCK_TIMEOUT_MS_DEFAULT = 3 * 60 * 1000; // 3 minutes
  private static final long HIVE_LOCK_CHECK_MIN_WAIT_MS_DEFAULT = 50; // 50 milliseconds
  private static final long HIVE_LOCK_CHECK_MAX_WAIT_MS_DEFAULT = 5 * 1000; // 5 seconds
  private static final String HIVE_TABLE_LEVEL_LOCK_EVICT_MS = "iceberg.hive.table-level-lock-evict-ms";
  private static final double HIVE_LOCK_CHECK_BACKOFF_SCALE_FACTOR_DEFAULT = 1.5;
  private static final long HIVE_TABLE_LEVEL_LOCK_EVICT_MS_DEFAULT = TimeUnit.MINUTES.toMillis(10);
  private static final DynMethods.UnboundMethod ALTER_TABLE = DynMethods.builder("alter_table")
      .impl(HiveMetaStoreClient.class, "alter_table_with_environmentContext",
          String.class, String.class, Table.class, EnvironmentContext.class)
      .impl(HiveMetaStoreClient.class, "alter_table",
          String.class, String.class, Table.class, EnvironmentContext.class)
      .build();

  private static Cache<String, ReentrantLock> commitLockCache;

  private static synchronized void initTableLevelLockCache(long evictionTimeout) {
    if (commitLockCache == null) {
      commitLockCache = Caffeine.newBuilder()
          .expireAfterAccess(evictionTimeout, TimeUnit.MILLISECONDS)
          .build();
    }
  }

  protected static class WaitingForLockException extends RuntimeException {
    public WaitingForLockException(String message) {
      super(message);
    }
  }

  private static final WaitingForLockException WAITING_FOR_LOCK_EXCEPTION =
      new WaitingForLockException("waiting for lock");

  private enum CommitStatus {
    FAILURE,
    SUCCESS,
    UNKNOWN
  }

  private final HiveClientPool metaClients;
  private final String fullName;
  private final String database;
  private final String tableName;
  private final Configuration conf;
  private final long lockAcquireTimeout;
  private final long lockCheckMinWaitTime;
  private final long lockCheckMaxWaitTime;
  private final double lockCheckBackoffScaleFactor;

  private FileIO fileIO;

  protected HiveTableOperations(Configuration conf, HiveClientPool metaClients,
                                String catalogName, String database, String table) {
    this.conf = conf;
    this.metaClients = metaClients;
    this.fullName = catalogName + "." + database + "." + table;
    this.database = database;
    this.tableName = table;
    this.lockAcquireTimeout =
        conf.getLong(HIVE_ACQUIRE_LOCK_TIMEOUT_MS, HIVE_ACQUIRE_LOCK_TIMEOUT_MS_DEFAULT);
    this.lockCheckMinWaitTime =
        conf.getLong(HIVE_LOCK_CHECK_MIN_WAIT_MS, HIVE_LOCK_CHECK_MIN_WAIT_MS_DEFAULT);
    this.lockCheckMaxWaitTime =
        conf.getLong(HIVE_LOCK_CHECK_MAX_WAIT_MS, HIVE_LOCK_CHECK_MAX_WAIT_MS_DEFAULT);
    this.lockCheckBackoffScaleFactor =
        conf.getDouble(HIVE_LOCK_CHECK_BACKOFF_SCALE_FACTOR, HIVE_LOCK_CHECK_BACKOFF_SCALE_FACTOR_DEFAULT);
    long tableLevelLockCacheEvictionTimeout =
        conf.getLong(HIVE_TABLE_LEVEL_LOCK_EVICT_MS, HIVE_TABLE_LEVEL_LOCK_EVICT_MS_DEFAULT);
    initTableLevelLockCache(tableLevelLockCacheEvictionTimeout);
  }

  @Override
  public FileIO io() {
    if (fileIO == null) {
      fileIO = new HadoopFileIO(conf);
    }

    return fileIO;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = null;
    try {
      Table table = metaClients.run(client -> client.getTable(database, tableName));
      validateTableIsIceberg(table, fullName);

      metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);

    } catch (NoSuchObjectException e) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException("No such table: %s.%s", database, tableName);
      }

    } catch (TException e) {
      String errMsg = String.format("Failed to get table info from metastore %s.%s", database, tableName);
      throw new RuntimeException(errMsg, e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during refresh", e);
    }

    refreshFromMetadataLocation(metadataLocation);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    String newMetadataLocation = base == null && metadata.metadataFileLocation() != null ?
        metadata.metadataFileLocation() : writeNewMetadata(metadata, currentVersion() + 1);

    CommitStatus commitStatus = CommitStatus.FAILURE;
    Optional<Long> lockId = Optional.empty();
    boolean updateHiveTable = false;
    // getting a process-level lock per table to avoid concurrent commit attempts to the same table from the same
    // JVM process, which would result in unnecessary and costly HMS lock acquisition requests
    ReentrantLock tableLevelMutex = commitLockCache.get(fullName, t -> new ReentrantLock(true));
    tableLevelMutex.lock();
    try {
      lockId = Optional.of(acquireLock());
      // TODO add lock heart beating for cases where default lock timeout is too low.
      Table tbl;
      if (base != null) {
        tbl = metaClients.run(client -> client.getTable(database, tableName));
        tbl.setSd(storageDescriptor(metadata)); // set to pickup any schema changes
        updateHiveTable = true;
      } else {
        final long currentTimeMillis = System.currentTimeMillis();
        tbl = new Table(tableName,
            database,
            System.getProperty("user.name"),
            (int) currentTimeMillis / 1000,
            (int) currentTimeMillis / 1000,
            Integer.MAX_VALUE,
            storageDescriptor(metadata),
            Collections.emptyList(),
            new HashMap<>(),
            null,
            null,
            TableType.EXTERNAL_TABLE.toString());
        tbl.getParameters().put("EXTERNAL", "TRUE"); // using the external table type also requires this
      }

      String metadataLocation = tbl.getParameters().get(METADATA_LOCATION_PROP);
      String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
      if (!Objects.equals(baseMetadataLocation, metadataLocation)) {
        throw new CommitFailedException(
            "Base metadata location '%s' is not same as the current table metadata location '%s' for %s.%s",
            baseMetadataLocation, metadataLocation, database, tableName);
      }

      setPdtParameters(metadata.properties(), tbl);
      setParameters(newMetadataLocation, tbl);

      try {
        persistTable(tbl, updateHiveTable);
        commitStatus = CommitStatus.SUCCESS;
      } catch (Throwable persistFailure) {
        LOG.error("Cannot tell if commit to {}.{} succeeded, attempting to reconnect and check.",
            database, tableName, persistFailure);
        commitStatus = checkCommitStatus(newMetadataLocation, metadata);
        switch (commitStatus) {
          case SUCCESS:
            break;
          case FAILURE:
            throw persistFailure;
          case UNKNOWN:
            throw new CommitStateUnknownException(persistFailure);
        }
      }
    } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
      throw new AlreadyExistsException("Table already exists: %s.%s", database, tableName);

    } catch (TException | UnknownHostException e) {
      if (e.getMessage() != null && e.getMessage().contains("Table/View 'HIVE_LOCKS' does not exist")) {
        throw new RuntimeException("Failed to acquire locks from metastore because 'HIVE_LOCKS' doesn't " +
            "exist, this probably happened when using embedded metastore or doesn't create a " +
            "transactional meta table. To fix this, use an alternative metastore", e);
      }

      throw new RuntimeException(String.format("Metastore operation failed for %s.%s", database, tableName), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during commit", e);

    } finally {
      cleanupMetadataAndUnlock(commitStatus, newMetadataLocation, lockId);
      tableLevelMutex.unlock();
    }
  }

  /**
   * Attempt to load the table and see if any current or past metadata location matches the one we were attempting
   * to set. This is used as a last resort when we are dealing with exceptions that may indicate the commit has
   * failed but are not proof that this is the case. Past locations must also be searched on the chance that a second
   * committer was able to successfully commit on top of our commit.
   *
   * @param newMetadataLocation the path of the new commit file
   * @param config metadata to use for configuration
   * @return Commit Status of Success, Failure or Unknown
   */
  private CommitStatus checkCommitStatus(String newMetadataLocation, TableMetadata config) {
    int maxAttempts = PropertyUtil.propertyAsInt(config.properties(), COMMIT_NUM_STATUS_CHECKS,
        COMMIT_NUM_STATUS_CHECKS_DEFAULT);

    AtomicReference<CommitStatus> status = new AtomicReference<>(CommitStatus.UNKNOWN);

    Tasks.foreach(newMetadataLocation)
        .retry(maxAttempts)
        .suppressFailureWhenFinished()
        .exponentialBackoff(COMMIT_STATUS_CHECK_WAIT_MS, COMMIT_STATUS_CHECK_WAIT_MS, Long.MAX_VALUE, 2.0)
        .onFailure((location, checkException) ->
            LOG.error("Cannot check if commit to {}.{} exists.", database, tableName, checkException))
        .run(location -> {
          TableMetadata metadata = refresh();
          String currentMetadataLocation = metadata.metadataFileLocation();
          boolean commitSuccess = currentMetadataLocation.equals(newMetadataLocation) ||
              metadata.previousFiles().stream().anyMatch(log -> log.file().equals(newMetadataLocation));
          if (commitSuccess) {
            LOG.info("Commit status check: Commit to {}.{} of {} succeeded", database, tableName, newMetadataLocation);
            status.set(CommitStatus.SUCCESS);
          } else {
            LOG.info("Commit status check: Commit to {}.{} of {} failed", database, tableName, newMetadataLocation);
            status.set(CommitStatus.FAILURE);
          }
        });

    if (status.get() == CommitStatus.UNKNOWN) {
      LOG.error("Cannot determine commit state to {}.{}. Failed during checking {} times. " +
              "Treating commit state as unknown.",
          database, tableName, maxAttempts);
    }
    return status.get();
  }

  @VisibleForTesting
  void persistTable(Table hmsTable, boolean updateHiveTable) throws TException, InterruptedException {
    if (updateHiveTable) {
      metaClients.run(client -> {
        EnvironmentContext envContext = new EnvironmentContext(
            ImmutableMap.of(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE)
        );
        ALTER_TABLE.invoke(client, database, tableName, hmsTable, envContext);
        return null;
      });
    } else {
      metaClients.run(client -> {
        client.createTable(hmsTable);
        return null;
      });
    }
  }

  private void setPdtParameters(Map<String, String> tableProperties, Table tbl) {
    // We need to persist `pdt.` and `spark.sql.sources.` properties in HMS for PDT tables
    Map<String, String> parameters = tbl.getParameters();
    List<String> existingPdtParams = parameters.keySet().stream()
        .filter(key -> key.startsWith("pdt.") || key.startsWith("spark.sql.sources."))
        .collect(Collectors.toList());
    existingPdtParams.forEach(parameters::remove);
    tableProperties.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith("pdt.") || entry.getKey().startsWith("spark.sql.sources."))
        .forEach(entry -> parameters.put(entry.getKey(), entry.getValue()));
  }

  private void setParameters(String newMetadataLocation, Table tbl) {
    Map<String, String> parameters = tbl.getParameters();

    if (parameters == null) {
      parameters = new HashMap<>();
    }

    parameters.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH));
    parameters.put(METADATA_LOCATION_PROP, newMetadataLocation);

    if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
      parameters.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
    }

    tbl.setParameters(parameters);
  }

  private StorageDescriptor storageDescriptor(TableMetadata metadata) {

    final StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(columns(metadata.schema()));
    storageDescriptor.setLocation(metadata.location());
    storageDescriptor.setOutputFormat("org.apache.hadoop.mapred.FileOutputFormat");
    storageDescriptor.setInputFormat("org.apache.hadoop.mapred.FileInputFormat");
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    // we need to persist the path in serde params for compatibility with Spark tables
    Map<String, String> serDeParams = Maps.newHashMap();
    serDeParams.put("path", metadata.location());
    serDeInfo.setParameters(serDeParams);
    storageDescriptor.setSerdeInfo(serDeInfo);
    return storageDescriptor;
  }

  private List<FieldSchema> columns(Schema schema) {
    return schema.columns().stream()
        .map(col -> new FieldSchema(col.name(), HiveTypeConverter.convert(col.type()), ""))
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  long acquireLock() throws UnknownHostException, TException, InterruptedException {
    final LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, database);
    lockComponent.setTablename(tableName);
    final LockRequest lockRequest = new LockRequest(Lists.newArrayList(lockComponent),
        System.getProperty("user.name"),
        InetAddress.getLocalHost().getHostName());
    LockResponse lockResponse = metaClients.run(client -> client.lock(lockRequest));
    AtomicReference<LockState> state = new AtomicReference<>(lockResponse.getState());
    long lockId = lockResponse.getLockid();

    final long start = System.currentTimeMillis();
    long duration = 0;
    boolean timeout = false;

    try {
      if (state.get().equals(LockState.WAITING)) {
        // Retry count is the typical "upper bound of retries" for Tasks.run() function. In fact, the maximum number of
        // attempts the Tasks.run() would try is `retries + 1`. Here, for checking locks, we use timeout as the
        // upper bound of retries. So it is just reasonable to set a large retry count. However, if we set
        // Integer.MAX_VALUE, the above logic of `retries + 1` would overflow into Integer.MIN_VALUE. Hence,
        // the retry is set conservatively as `Integer.MAX_VALUE - 100` so it doesn't hit any boundary issues.
        Tasks.foreach(lockId)
            .retry(Integer.MAX_VALUE - 100) // Endless retries bound by timeouts. Tasks.retry adds 1 for "first try".
            .exponentialBackoff(
                lockCheckMinWaitTime,
                lockCheckMaxWaitTime,
                lockAcquireTimeout,
                lockCheckBackoffScaleFactor)
            .throwFailureWhenFinished()
            .onlyRetryOn(WaitingForLockException.class)
            .run(id -> {
              try {
                LockResponse response = metaClients.run(client -> client.checkLock(id));
                LockState newState = response.getState();
                state.set(newState);
                if (newState.equals(LockState.WAITING)) {
                  throw WAITING_FOR_LOCK_EXCEPTION;
                }
              } catch (InterruptedException e) {
                Thread.interrupted(); // Clear the interrupt status flag
                LOG.warn("Interrupted while waiting for lock.", e);
              }
            }, TException.class);
      }
    } catch (WaitingForLockException waitingForLockException) {
      timeout = true;
      duration = System.currentTimeMillis() - start;
    } finally {
      if (!state.get().equals(LockState.ACQUIRED)) {
        unlock(Optional.of(lockId));
      }
    }

    // timeout and do not have lock acquired
    if (timeout && !state.get().equals(LockState.ACQUIRED)) {
      throw new CommitFailedException("Timed out after %s ms waiting for lock on %s.%s",
          duration, database, tableName);
    }

    if (!state.get().equals(LockState.ACQUIRED)) {
      throw new CommitFailedException("Could not acquire the lock on %s.%s, " +
          "lock request ended in state %s", database, tableName, state);
    }
    return lockId;
  }

  private void cleanupMetadataAndUnlock(CommitStatus commitStatus, String metadataLocation, Optional<Long> lockId) {
    try {
      if (commitStatus == CommitStatus.FAILURE) {
        // If we are sure the commit failed, clean up the uncommitted metadata file
        io().deleteFile(metadataLocation);
      }
    } catch (RuntimeException e) {
      LOG.error("Fail to cleanup metadata file at {}", metadataLocation, e);
      throw e;
    } finally {
      unlock(lockId);
    }
  }

  private void unlock(Optional<Long> lockId) {
    if (lockId.isPresent()) {
      try {
        doUnlock(lockId.get());
      } catch (Exception e) {
        LOG.warn("Failed to unlock {}.{}", database, tableName, e);
      }
    }
  }

  @VisibleForTesting
  void doUnlock(long lockId) throws TException, InterruptedException {
    metaClients.run(client -> {
      client.unlock(lockId);
      return null;
    });
  }

  static void validateTableIsIceberg(Table table, String fullName) {
    String tableType = table.getParameters().get(TABLE_TYPE_PROP);
    NoSuchIcebergTableException.check(tableType != null && tableType.equalsIgnoreCase(ICEBERG_TABLE_TYPE_VALUE),
        "Not an iceberg table: %s (type=%s)", fullName, tableType);
    NoSuchIcebergTableException.check(table.getParameters().get(METADATA_LOCATION_PROP) != null,
        "Not an iceberg table: %s missing %s", fullName, METADATA_LOCATION_PROP);
  }
}
