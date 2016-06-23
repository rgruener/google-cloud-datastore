/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.datastore.v1beta3.client;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.datastore.v1beta3.AllocateIdsRequest;
import com.google.datastore.v1beta3.AllocateIdsResponse;
import com.google.datastore.v1beta3.BeginTransactionRequest;
import com.google.datastore.v1beta3.BeginTransactionResponse;
import com.google.datastore.v1beta3.CommitRequest;
import com.google.datastore.v1beta3.CommitResponse;
import com.google.datastore.v1beta3.LookupRequest;
import com.google.datastore.v1beta3.LookupResponse;
import com.google.datastore.v1beta3.RollbackRequest;
import com.google.datastore.v1beta3.RollbackResponse;
import com.google.datastore.v1beta3.RunQueryRequest;
import com.google.datastore.v1beta3.RunQueryResponse;
import com.google.rpc.Code;

import java.io.IOException;
import java.io.InputStream;

/**
 * Provides access to Cloud Datastore.
 */
public class Datastore {

  final RemoteRpc remoteRpc;

  Datastore(RemoteRpc remoteRpc) {
    this.remoteRpc = remoteRpc;
  }

  /**
   * Reset the RPC count.
   */
  public void resetRpcCount() {
    remoteRpc.resetRpcCount();
  }

  /**
   * Returns the number of RPC calls made since the client was created
   * or {@link #resetRpcCount} was called.
   */
  public int getRpcCount() {
    return remoteRpc.getRpcCount();
  }

  private DatastoreException invalidResponseException(String method, IOException exception) {
    return RemoteRpc.makeException(remoteRpc.getUrl(), method, Code.UNAVAILABLE,
        "Invalid response", exception);
  }

  public AllocateIdsResponse allocateIds(AllocateIdsRequest request) throws DatastoreException {
    try (InputStream is = remoteRpc.call("allocateIds", request)) {
      return AllocateIdsResponse.parseFrom(is);
    } catch (IOException exception) {
      throw invalidResponseException("allocateIds", exception);
    }
  }


  public ListenableFuture<AllocateIdsResponse> allocateIdsAsync(AllocateIdsRequest request) {
    final ListenableFuture<InputStream> future = remoteRpc.callAsync("allocateIdsAsync", request);
    return Futures.transform(future, new AsyncFunction<InputStream, AllocateIdsResponse>() {
      @Override
      public ListenableFuture<AllocateIdsResponse> apply(InputStream input) throws Exception {
        try {
          return Futures.immediateFuture(AllocateIdsResponse.parseFrom(input));
        } catch (IOException e) {
          return Futures.immediateFailedFuture(invalidResponseException("allocateIdsAsync", e));
        } finally {
          input.close();
        }
      }
    });
  }

  public BeginTransactionResponse beginTransaction(BeginTransactionRequest request)
      throws DatastoreException {
    try (InputStream is = remoteRpc.call("beginTransaction", request)) {
      return BeginTransactionResponse.parseFrom(is);
    } catch (IOException exception) {
      throw invalidResponseException("beginTransaction", exception);
    }
  }

  public ListenableFuture<BeginTransactionResponse> beginTransactionAsync(BeginTransactionRequest request) {
    final ListenableFuture<InputStream> future = remoteRpc.callAsync("beginTransactionAsync", request);
    return Futures.transform(future, new AsyncFunction<InputStream, BeginTransactionResponse>() {
      @Override
      public ListenableFuture<BeginTransactionResponse> apply(InputStream input) throws Exception {
        try {
          return Futures.immediateFuture(BeginTransactionResponse.parseFrom(input));
        } catch (IOException e) {
          return Futures.immediateFailedFuture(invalidResponseException("beginTransactionAsync", e));
        } finally {
          input.close();
        }
      }
    });
  }

  public CommitResponse commit(CommitRequest request) throws DatastoreException {
    try (InputStream is = remoteRpc.call("commit", request)) {
      return CommitResponse.parseFrom(is);
    } catch (IOException exception) {
      throw invalidResponseException("commit", exception);
    }
  }

  public ListenableFuture<CommitResponse> commitAsync(CommitRequest request) {
    final ListenableFuture<InputStream> future = remoteRpc.callAsync("commitAsync", request);
    return Futures.transform(future, new AsyncFunction<InputStream, CommitResponse>() {
      @Override
      public ListenableFuture<CommitResponse> apply(InputStream input) throws Exception {
        try {
          return Futures.immediateFuture(CommitResponse.parseFrom(input));
        } catch (IOException e) {
          return Futures.immediateFailedFuture(invalidResponseException("commitAsync", e));
        } finally {
          input.close();
        }
      }
    });
  }

  public LookupResponse lookup(LookupRequest request) throws DatastoreException {
    try (InputStream is = remoteRpc.call("lookup", request)) {
      return LookupResponse.parseFrom(is);
    } catch (IOException exception) {
      throw invalidResponseException("lookup", exception);
    }
  }

  public ListenableFuture<LookupResponse> lookupAsync(LookupRequest request) {
    final ListenableFuture<InputStream> future = remoteRpc.callAsync("lookupAsync", request);
    return Futures.transform(future, new AsyncFunction<InputStream, LookupResponse>() {
      @Override
      public ListenableFuture<LookupResponse> apply(InputStream input) throws Exception {
        try {
          return Futures.immediateFuture(LookupResponse.parseFrom(input));
        } catch (IOException e) {
          return Futures.immediateFailedFuture(invalidResponseException("lookupAsync", e));
        } finally {
          input.close();
        }
      }
    });
  }

  public RollbackResponse rollback(RollbackRequest request) throws DatastoreException {
    try (InputStream is = remoteRpc.call("rollback", request)) {
      return RollbackResponse.parseFrom(is);
    } catch (IOException exception) {
      throw invalidResponseException("rollback", exception);
    }
  }

  public ListenableFuture<RollbackResponse> rollbackAsync(RollbackRequest request) {
    final ListenableFuture<InputStream> future = remoteRpc.callAsync("rollbackAsync", request);
    return Futures.transform(future, new AsyncFunction<InputStream, RollbackResponse>() {
      @Override
      public ListenableFuture<RollbackResponse> apply(InputStream input) throws Exception {
        try {
          return Futures.immediateFuture(RollbackResponse.parseFrom(input));
        } catch (IOException e) {
          return Futures.immediateFailedFuture(invalidResponseException("rollbackAsync", e));
        } finally {
          input.close();
        }
      }
    });
  }

  public RunQueryResponse runQuery(RunQueryRequest request) throws DatastoreException {
    try (InputStream is = remoteRpc.call("runQuery", request)) {
      return RunQueryResponse.parseFrom(is);
    } catch (IOException exception) {
      throw invalidResponseException("runQuery", exception);
    }
  }

  public ListenableFuture<RunQueryResponse> runQueryAsync(RunQueryRequest request) {
    final ListenableFuture<InputStream> future = remoteRpc.callAsync("runQueryAsync", request);
    return Futures.transform(future, new AsyncFunction<InputStream, RunQueryResponse>() {
      @Override
      public ListenableFuture<RunQueryResponse> apply(InputStream input) throws Exception {
        try {
          return Futures.immediateFuture(RunQueryResponse.parseFrom(input));
        } catch (IOException e) {
          return Futures.immediateFailedFuture(invalidResponseException("runQueryAsync", e));
        } finally {
          input.close();
        }
      }
    });
  }
}
