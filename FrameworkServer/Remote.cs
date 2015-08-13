/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2015 ForgeRock AS. All rights reserved.
 *
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the License). You may not use this file except in
 * compliance with the License.
 *
 * You can obtain a copy of the License at
 * http://forgerock.org/license/CDDLv1.0.html
 * See the License for the specific language governing
 * permission and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL
 * Header Notice in each file and include the License file
 * at http://forgerock.org/license/CDDLv1.0.html
 * If applicable, add the following below the CDDL Header,
 * with the fields enclosed by brackets [] replaced by
 * your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.EC;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Crypto.Tls;
using Org.BouncyCastle.Security;
using Org.ForgeRock.OpenICF.Common.RPC;
using Org.IdentityConnectors.Common;
using Org.IdentityConnectors.Common.Security;
using Org.IdentityConnectors.Framework.Api.Operations;
using Org.IdentityConnectors.Framework.Common;
using Org.IdentityConnectors.Framework.Common.Exceptions;
using Org.IdentityConnectors.Framework.Common.Objects.Filters;
using Org.IdentityConnectors.Framework.Common.Serializer;
using Org.IdentityConnectors.Framework.Impl.Api;
using Org.IdentityConnectors.Framework.Impl.Api.Remote;
using Org.IdentityConnectors.Framework.Spi;
using OBJ = Org.IdentityConnectors.Framework.Common.Objects;
using API = Org.IdentityConnectors.Framework.Api;
using PRB = Org.ForgeRock.OpenICF.Common.ProtoBuf;

namespace Org.ForgeRock.OpenICF.Framework.Remote
{

    #region AsyncRemoteConnectorInfoManager

    /// <summary>
    ///     An AsyncRemoteConnectorInfoManager.
    /// </summary>
    /// <remarks>Since 1.5</remarks>
    public class AsyncRemoteConnectorInfoManager : DelegatingAsyncConnectorInfoManager
    {
        protected readonly IDisposable RemoteDisposable;

        private readonly
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
            _messageDistributor;

        public AsyncRemoteConnectorInfoManager(IDisposable remoteDisposable,
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                messageDistributor)
            : base(false)
        {
            RemoteDisposable = remoteDisposable;
            _messageDistributor = messageDistributor;
        }

        public AsyncRemoteConnectorInfoManager(IRemoteConnectorInfoManager loadBalancingAlgorithm)
            : this(loadBalancingAlgorithm, loadBalancingAlgorithm.RequestDistributor)
        {
            AddAsyncConnectorInfoManager(loadBalancingAlgorithm.AsyncConnectorInfoManager);
        }

        protected AsyncRemoteConnectorInfoManager(LoadBalancingAlgorithmFactory loadBalancingAlgorithmFactory)
            : this(
                null,
                loadBalancingAlgorithmFactory.NewInstance(
                    loadBalancingAlgorithmFactory.AsyncRemoteConnectorInfoManager.Select(
                        manager => manager.MessageDistributor)))
        {
            if (
                loadBalancingAlgorithmFactory.AsyncRemoteConnectorInfoManager.SelectMany(
                    @delegate => @delegate.Delegates).Any(am => !AddAsyncConnectorInfoManager(am)))
            {
                throw new ArgumentException("Possible circular or repeated remote in LoadBalancing tree");
            }
        }


        protected override
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
            MessageDistributor
        {
            get { return _messageDistributor; }
        }

        protected override void DoClose()
        {
            if (null != RemoteDisposable)
            {
                try
                {
                    RemoteDisposable.Dispose();
                }
                catch (Exception e)
                {
                    TraceUtil.TraceException("Failed to close underlying remote connection", e);
                }
            }
        }
    }

    #endregion

    /*
    #region AsyncRemoteLegacyConnectorInfoManager

/// <summary>
/// @since 1.5
/// </summary>
public class AsyncRemoteLegacyConnectorInfoManager : ManagedAsyncConnectorInfoManager<RemoteConnectorInfoImpl, AsyncRemoteLegacyConnectorInfoManager>
{

	protected internal readonly ConnectorEventHandler handler = new ConnectorEventHandlerAnonymousInnerClassHelper();

	private class ConnectorEventHandlerAnonymousInnerClassHelper : ConnectorEventHandler
	{
		public ConnectorEventHandlerAnonymousInnerClassHelper()
		{
		}
		public virtual void handleEvent(ConnectorEvent @event)
		{
			if (ConnectorEvent.CONNECTOR_REGISTERED.Equals(@event.Topic))
			{
				ConnectorInfo connectorInfo = outerInstance.@delegate.findConnectorInfo((ConnectorKey) @event.Source);
				addConnectorInfo((RemoteConnectorInfoImpl) connectorInfo);
			}
		}
	}
	private readonly RemoteConnectorInfoManagerImpl @delegate;

	private readonly ScheduledFuture<?> future;

//JAVA TO C# CONVERTER WARNING: 'final' parameters are not allowed in .NET:
//ORIGINAL LINE: public AsyncRemoteLegacyConnectorInfoManager(final RemoteFrameworkConnectionInfo info, final ScheduledExecutorService scheduler)
	public AsyncRemoteLegacyConnectorInfoManager(RemoteFrameworkConnectionInfo info, ScheduledExecutorService scheduler)
	{
		this.@delegate = new RemoteConnectorInfoManagerImpl(info, false);
		@delegate.addConnectorEventHandler(handler);
		long heartbeatInterval = info.HeartbeatInterval;
		if (heartbeatInterval <= 0)
		{
			heartbeatInterval = 60L;
		}
		try
		{
			future = scheduler.scheduleAtFixedRate(@delegate, 0, heartbeatInterval, TimeUnit.SECONDS);
			logger.info("Legacy ConnectorServer Heartbeat scheduled to {0} by {1} seconds", info, heartbeatInterval);
		}
		catch (RejectedExecutionException e)
		{
			throw new ConnectorException(e.Message, e);
		}
	}

	protected internal virtual void doClose()
	{
		future.cancel(true);
		@delegate.deleteConnectorEventHandler(handler);
		base.doClose();
	}

	public override IList<ConnectorInfo> ConnectorInfos
	{
		get
		{
			return @delegate.ConnectorInfos;
		}
	}

	public override ConnectorInfo FindConnectorInfo(ConnectorKey key)
	{
		return @delegate.FindConnectorInfo(key);
	}

}
    #endregion
    */

    #region ConnectionPrincipal

    public abstract class ConnectionPrincipal : GenericPrincipal, IDisposable
        , IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
    {
        public const string DefaultName = "anonymous";

        protected internal Int32 isRunning = 1;

        protected readonly ConcurrentDictionary<string, WebSocketConnectionGroup> ConnectionGroups =
            new ConcurrentDictionary<string, WebSocketConnectionGroup>();

        private readonly ConcurrentDictionary<string, WebSocketConnectionGroup> _globalConnectionGroups;

        private readonly IMessageListener<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
            _listener;

        public event EventHandler Disposed;

        protected ConnectionPrincipal(
            IMessageListener<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext> listener,
            ConcurrentDictionary<string, WebSocketConnectionGroup> globalConnectionGroups)
            : base(new GenericIdentity(DefaultName), new[] {"connector"})
        {
            _listener = listener;
            _globalConnectionGroups = globalConnectionGroups;
        }


        public virtual RemoteOperationContext Handshake(WebSocketConnectionHolder webSocketConnection,
            PRB.HandshakeMessage message)
        {
            WebSocketConnectionGroup newConnectionGroup = new WebSocketConnectionGroup(message.SessionId);
            WebSocketConnectionGroup connectionGroup = _globalConnectionGroups.GetOrAdd(message.SessionId,
                newConnectionGroup);
            if (newConnectionGroup == connectionGroup)
            {
                ConnectionGroups.TryAdd(message.SessionId, connectionGroup);
                try
                {
                    OnNewWebSocketConnectionGroup(connectionGroup);
                }
                catch (Exception ignore)
                {
#if DEBUG
                    StringBuilder sb = new StringBuilder("Failed to notify onNewWebSocketConnectionGroup - ");
                    TraceUtil.ExceptionToString(sb, ignore, String.Empty);
                    Debug.WriteLine(sb.ToString());
#endif
                }
            }
            return connectionGroup.Handshake(this, webSocketConnection, message);
        }

        protected virtual void OnNewWebSocketConnectionGroup(WebSocketConnectionGroup connectionGroup)
        {
        }

        public virtual IMessageListener<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
            OperationMessageListener
        {
            get { return _listener; }
        }

        public virtual TR TrySubmitRequest<TR, TV, TE>(
            IRemoteRequestFactory
                <TR, TV, TE, WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                requestFactory)
            where TR :
                RemoteRequest<TV, TE, WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
            where TE : Exception
        {
            return
                (from e in ConnectionGroups.Values where e.Operational select e.TrySubmitRequest(requestFactory))
                    .FirstOrDefault(result => null != result);
        }

        public virtual bool Operational
        {
            get { return isRunning == 1 && ConnectionGroups.Values.Any(e => e.Operational); }
        }

        /// <summary>
        ///     Closes this manager and releases any system resources associated with it.
        ///     If the manager is already closed then invoking this method has no effect.
        /// </summary>
        protected abstract void DoClose();

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref isRunning, 0, 1) == 1)
            {
                DoClose();
                // Notify CloseListeners
                OnDisposed();
            }
        }

        protected virtual void OnDisposed()
        {
            var handler = Disposed;
            if (handler != null) handler(this, EventArgs.Empty);
        }
    }

    #endregion

    #region FailoverLoadBalancingAlgorithmFactory

    public class FailoverLoadBalancingAlgorithmFactory : LoadBalancingAlgorithmFactory
    {
        protected override
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
            CreateLoadBalancer(
            IList<IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>>
                delegates)
        {
            return
                new FailoverLoadBalancingAlgorithm
                    <WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>(delegates);
        }
    }

    #endregion

    #region LoadBalancingAlgorithmFactory

    public abstract class LoadBalancingAlgorithmFactory
    {
        private readonly IList<AsyncRemoteConnectorInfoManager> _remoteConnectorInfoManagers =
            new List<AsyncRemoteConnectorInfoManager>();

        public virtual void AddAsyncRemoteConnectorInfoManager(
            AsyncRemoteConnectorInfoManager remoteConnectorInfoManager)
        {
            if (null != remoteConnectorInfoManager) _remoteConnectorInfoManagers.Add(remoteConnectorInfoManager);
        }

        public virtual ICollection<AsyncRemoteConnectorInfoManager> AsyncRemoteConnectorInfoManager
        {
            get { return _remoteConnectorInfoManagers; }
        }

        public virtual IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
            NewInstance(
            IEnumerable
                <IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>>
                parameter)
        {
            IList<IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>>
                delegates = parameter.Where(dist => null != dist).ToList();
            if (delegates.Count == 0)
            {
                throw new ArgumentException("The LoadBalancing delegates is empty");
            }
            return CreateLoadBalancer(delegates);
        }

        protected abstract
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
            CreateLoadBalancer(
            IList<IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>>
                delegates);
    }

    #endregion

    #region LoadBalancingConnectorFacadeContext

    /// <summary>
    ///     A LoadBalancingConnectorFacadeContext gives contextual information about the
    ///     current executing
    ///     <seealso cref="ConnectionPrincipal" />
    /// </summary>
    public interface ILoadBalancingConnectorFacadeContext
    {
        /// <summary>
        ///     Loads the <seealso cref="Connector" /> and
        ///     <seealso cref="Configuration" /> class in order
        ///     to determine the proper default configuration parameters.
        /// </summary>
        /// <returns> default APIConfiguration </returns>
        API.APIConfiguration ApiConfiguration { get; }

        /// <summary>
        ///     Gets the principal name of executing
        ///     <seealso cref="ConnectionPrincipal" />.
        /// </summary>
        /// <returns>
        ///     name of
        ///     <seealso cref="ConnectionPrincipal" />
        /// </returns>
        string PrincipalName { get; }

        /// <summary>
        ///     Get the RemoteOperationContext of executing
        ///     <seealso cref="ConnectionPrincipal" />
        /// </summary>
        /// <returns>
        ///     context of
        ///     <seealso cref="ConnectionPrincipal" />
        /// </returns>
        RemoteOperationContext RemoteOperationContext { get; }
    }

    #endregion

    #region LoadBalancingConnectorInfoManager

    /// <remarks>Since 1.5</remarks>
    public class LoadBalancingConnectorInfoManager : AsyncRemoteConnectorInfoManager
    {
        public LoadBalancingConnectorInfoManager(LoadBalancingAlgorithmFactory loadBalancingAlgorithmFactory)
            : base(loadBalancingAlgorithmFactory)
        {
        }

        public virtual Task<API.ConnectorFacade> NewInstance(API.ConnectorKey key,
            Func<API.APIConfiguration, ILoadBalancingConnectorFacadeContext> transformer)
        {
            return FindConnectorInfoAsync(key).ContinueWith((task, state) =>
            {
                if (task.Result is RemoteConnectorInfoImpl)
                {
                    //return new RemoteAsyncConnectorFacade((RemoteConnectorInfoImpl) task.Result, transformer);
                    return default(API.ConnectorFacade);
                }
                else
                {
                    throw new ArgumentException("Invalid RemoteConnectorInfoImpl");
                }
            }, transformer);
        }
    }

    #endregion

    #region ManagedAsyncConnectorInfoManager

    public class ManagedAsyncConnectorInfoManager<TV, TC> : DisposableAsyncConnectorInfoManager<TC>
        where TV : API.ConnectorInfo
        where TC : ManagedAsyncConnectorInfoManager<TV, TC>
    {
        private readonly ConcurrentDictionary<API.ConnectorKey, ConnectorEntry<TV>> _managedConnectorInfos =
            new ConcurrentDictionary<API.ConnectorKey, ConnectorEntry<TV>>();

        private class ConnectorKeyComparator : IComparer<API.ConnectorKey>
        {
            /// <summary>
            ///     Descending ConnectorKey Comparator.
            /// </summary>
            /// <param name="left">
            ///     the first object to be compared.
            /// </param>
            /// <param name="right">
            ///     the second object to be compared.
            /// </param>
            /// <returns>
            ///     a negative integer, zero, or a positive
            ///     integer as the first argument is less than,
            ///     equal to, or greater than the second.
            /// </returns>
            public virtual int Compare(API.ConnectorKey left, API.ConnectorKey right)
            {
                int result = String.Compare(left.BundleName, right.BundleName, StringComparison.Ordinal);
                if (result != 0)
                {
                    return result*-1;
                }
                result = String.Compare(left.ConnectorName, right.ConnectorName, StringComparison.Ordinal);
                if (result != 0)
                {
                    return result*-1;
                }
                return String.Compare(left.BundleVersion, right.BundleVersion, StringComparison.Ordinal)*-1;
            }
        }

        private readonly ConcurrentDictionary<Pair<ConnectorKeyRange, TaskCompletionSource<API.ConnectorInfo>>, Boolean>
            _rangePromiseCacheList =
                new ConcurrentDictionary<Pair<ConnectorKeyRange, TaskCompletionSource<API.ConnectorInfo>>, Boolean>();

        public const String ClosedExceptionMsg = "AsyncConnectorInfoManager is shut down!";


        protected override void DoClose()
        {
            foreach (ConnectorEntry<TV> entry in _managedConnectorInfos.Values)
            {
                entry.Shutdown();
            }
            _managedConnectorInfos.Clear();
            foreach (
                Pair<ConnectorKeyRange, TaskCompletionSource<API.ConnectorInfo>> entry in _rangePromiseCacheList.Keys)
            {
                entry.Second.SetException(
                    new InvalidOperationException("ManagedAsyncConnectorInfoManager is shutting down!"));
            }
            _rangePromiseCacheList.Clear();
        }

        private Int32 _revision;

        public virtual bool IsChanged(int lastRevision)
        {
            return lastRevision != _revision;
        }

        protected internal virtual void AddConnectorInfo(TV connectorInfo)
        {
            ConnectorEntry<TV> entry = _managedConnectorInfos.GetOrAdd(connectorInfo.ConnectorKey,
                new ConnectorEntry<TV>());

            Trace.TraceInformation("Add new ConnectorInfo: {0}", connectorInfo.ConnectorKey);

            entry.ConnectorInfo = connectorInfo;
            Interlocked.Increment(ref _revision);

            foreach (
                var rangeEntry in _rangePromiseCacheList.Keys.Where(x => x.First.IsInRange(connectorInfo.ConnectorKey)))
            {
                rangeEntry.Second.SetResult(connectorInfo);
            }
        }

        public override IList<API.ConnectorInfo> ConnectorInfos
        {
            get
            {
                List<API.ConnectorInfo> resultList = new List<API.ConnectorInfo>(_managedConnectorInfos.Count);
                resultList.AddRange(
                    (from entry in _managedConnectorInfos.Values
                        where null != entry.ConnectorInfo
                        select entry.ConnectorInfo).Select(dummy => (API.ConnectorInfo) dummy));
                return resultList;
            }
        }

        public override API.ConnectorInfo FindConnectorInfo(API.ConnectorKey key)
        {
            ConnectorEntry<TV> entry;
            _managedConnectorInfos.TryGetValue(key, out entry);
            if (null != entry)
            {
                return entry.ConnectorInfo;
            }
            return null;
        }

        public override Task<API.ConnectorInfo> FindConnectorInfoAsync(ConnectorKeyRange keyRange)
        {
            if (IsRunning == 0)
            {
                var result = new TaskCompletionSource<API.ConnectorInfo>();
                result.SetException(new InvalidOperationException("AsyncConnectorInfoManager is shut down!"));
                return result.Task;
            }
            else
            {
                if (keyRange.BundleVersionRange.Empty)
                {
                    var result = new TaskCompletionSource<API.ConnectorInfo>();
                    result.SetException(new ArgumentException("ConnectorBundle VersionRange is Empty"));
                    return result.Task;
                }
                else if (keyRange.BundleVersionRange.Exact)
                {
                    return FindConnectorInfoAsync(keyRange.ExactConnectorKey);
                }
                else
                {
                    Pair<ConnectorKeyRange, TaskCompletionSource<API.ConnectorInfo>> cacheEntry =
                        Pair<ConnectorKeyRange, TaskCompletionSource<API.ConnectorInfo>>.Of(keyRange,
                            new TaskCompletionSource<API.ConnectorInfo>());

                    cacheEntry.Second.Task.ContinueWith((_, e) =>
                    {
                        bool ignore;
                        Pair<ConnectorKeyRange, TaskCompletionSource<API.ConnectorInfo>> key =
                            e as Pair<ConnectorKeyRange, TaskCompletionSource<API.ConnectorInfo>>;
                        if (null != key)
                            _rangePromiseCacheList.TryRemove(key, out ignore);
                    }, cacheEntry);
                    _rangePromiseCacheList.TryAdd(cacheEntry, true);

                    foreach (KeyValuePair<API.ConnectorKey, ConnectorEntry<TV>> entry in _managedConnectorInfos)
                    {
                        API.ConnectorInfo connectorInfo = entry.Value.ConnectorInfo;
                        if (null != connectorInfo && keyRange.IsInRange(connectorInfo.ConnectorKey))
                        {
                            cacheEntry.Second.SetResult(connectorInfo);
                            return cacheEntry.Second.Task;
                        }
                    }

                    if (IsRunning != 1)
                    {
                        bool ignore;
                        _rangePromiseCacheList.TryRemove(cacheEntry, out ignore);
                        var result = new TaskCompletionSource<API.ConnectorInfo>();
                        result.SetException(new InvalidOperationException(ClosedExceptionMsg));
                        return result.Task;
                    }
                    return cacheEntry.Second.Task;
                }
            }
        }

        public override async Task<API.ConnectorInfo> FindConnectorInfoAsync(API.ConnectorKey key)
        {
            if (IsRunning != 1)
            {
                throw new InvalidOperationException("AsyncConnectorInfoManager is shut down!");
            }

            TaskCompletionSource<API.ConnectorInfo> promise = new TaskCompletionSource<API.ConnectorInfo>();
            ConnectorEntry<TV> entry = _managedConnectorInfos.GetOrAdd(key, new ConnectorEntry<TV>());
            entry.AddOrFirePromise(promise);
            if (IsRunning != 1 && !promise.Task.IsCompleted)
            {
                promise.SetException(new InvalidOperationException(ClosedExceptionMsg));
            }
            return await promise.Task;
        }
    }


    internal class ConnectorEntry<TV> where TV : API.ConnectorInfo
    {
        private TV _connectorInfo;

        private readonly ConcurrentQueue<TaskCompletionSource<API.ConnectorInfo>> _listeners =
            new ConcurrentQueue<TaskCompletionSource<API.ConnectorInfo>>();

        internal virtual TV ConnectorInfo
        {
            set
            {
                _connectorInfo = value;
                TaskCompletionSource<API.ConnectorInfo> listener;
                while (_listeners.TryDequeue(out listener))
                {
                    Trace.TraceInformation("Complete TaskSource:{0}", listener.Task.Id);
                    listener.SetResult(_connectorInfo);
                }
            }
            get { return _connectorInfo; }
        }

        internal virtual void Shutdown()
        {
            TaskCompletionSource<API.ConnectorInfo> listener;
            while (_listeners.TryDequeue(out listener))
            {
                //listener.SetException(CLOSED_EXCEPTION);
            }
        }

        internal virtual void AddOrFirePromise(TaskCompletionSource<API.ConnectorInfo> listener)
        {
            API.ConnectorInfo registered = _connectorInfo;
            if (null != registered && !listener.Task.IsCompleted)
            {
                listener.SetResult(registered);
            }
            else
            {
                _listeners.Enqueue(listener);
                API.ConnectorInfo registeredAfter = _connectorInfo;
                if (null != registeredAfter && !listener.Task.IsCompleted)
                {
                    listener.SetResult(registeredAfter);
                }
            }
        }
    }

    #endregion

    #region MessagesUtil

    public class MessagesUtil
    {
        public static PRB.RemoteMessage CreateErrorResponse(long messageId, Exception error)
        {
            PRB.RPCResponse builder = new PRB.RPCResponse
            {
                Error = FromException(error, 4)
            };
            var message = CreateRemoteMessage(messageId);
            message.Response = builder;
            return message;
        }

        public static PRB.ExceptionMessage FromException(Exception error, int depth)
        {
            PRB.ExceptionMessage builder = new PRB.ExceptionMessage
            {
                ExceptionClass = error.GetType().FullName,
                StackTrace = error.StackTrace,
                Message = error.Message
            };

            PRB.ExceptionMessage.Types.InnerCause cause = FromCause(error.InnerException, depth);
            if (null != cause)
            {
                builder.InnerCause = cause;
            }

            return builder;
        }

        private static PRB.ExceptionMessage.Types.InnerCause FromCause(Exception error, int depth)
        {
            if (null != error && depth > 0)
            {
                PRB.ExceptionMessage.Types.InnerCause builder = new
                    PRB.ExceptionMessage.Types.InnerCause
                {
                    ExceptionClass = error.GetType().FullName,
                    Message = error.Message
                };

                PRB.ExceptionMessage.Types.InnerCause cause = FromCause(error.InnerException, --depth);
                if (null != cause)
                {
                    builder.Cause = cause;
                }
                return builder;
            }
            return null;
        }

        public static Exception FromExceptionMessage(PRB.ExceptionMessage exceptionMessage)
        {
            string message = null;
            try
            {
                string throwableClass = exceptionMessage.ExceptionClass ?? typeof (ConnectorException).FullName;
                message = exceptionMessage.Message ?? "";
                string stackTrace = !String.IsNullOrEmpty(exceptionMessage.StackTrace)
                    ? exceptionMessage.StackTrace
                    : null;

                return new RemoteWrappedException(throwableClass, message, GetCause(exceptionMessage.InnerCause),
                    stackTrace);
            }
            catch (Exception t)
            {
                return
                    new ConnectorException(
                        !String.IsNullOrEmpty(message) ? message : "Failed to process ExceptionMessage response", t);
            }
        }

        private static RemoteWrappedException GetCause(PRB.ExceptionMessage.Types.InnerCause cause)
        {
            if (null != cause)
            {
                string throwableClass = cause.ExceptionClass ?? typeof (ConnectorException).FullName;
                string message = cause.Message ?? "";
                RemoteWrappedException originalCause = cause.Cause != null ? GetCause(cause.Cause) : null;
                return new RemoteWrappedException(throwableClass, message, originalCause, null);
            }
            return null;
        }

        public static PRB.Uid FromUid(PRB.Uid uid)
        {
            PRB.Uid builder = new PRB.Uid
            {
                Value = uid.Value
            };
            if (null != uid.Revision)
            {
                builder.Revision = uid.Value;
            }
            return builder;
        }

        public static PRB.RemoteMessage CreateResponse(long messageId, PRB.RPCResponse builderForValue)
        {
            var message = CreateRemoteMessage(messageId);
            message.Response = builderForValue;
            return message;
        }

        public static PRB.RemoteMessage CreateRequest(int messageId, PRB.RPCRequest builderForValue)
        {
            var message = CreateRemoteMessage(messageId);
            message.Request = builderForValue;
            return message;
        }

        public static PRB.RemoteMessage CreateRemoteMessage(long messageId)
        {
            PRB.RemoteMessage builder = new PRB.RemoteMessage
            {
                MessageId = 0
            };
            if (0 != messageId)
            {
                builder.MessageId = messageId;
            }
            return builder;
        }

        [Obsolete]
        public static T DeserializeLegacy<T>(ByteString byteString)
        {
            if (null == byteString || byteString.IsEmpty)
            {
                return default(T);
            }
            return (T) SerializerUtil.DeserializeBinaryObject(byteString.ToByteArray());
        }

        [Obsolete]
        public static ByteString SerializeLegacy(object source)
        {
            if (null != source)
            {
                return ByteString.CopyFrom(SerializerUtil.SerializeBinaryObject(source));
            }
            return ByteString.Empty;
        }

        public static T DeserializeMessage<T>(Object source)
        {
            if (source == null)
            {
                return default(T);
            }
            var type = typeof (T);
            // UID
            if (typeof (IdentityConnectors.Framework.Common.Objects.Uid) == type)
            {
                var from = source as PRB.Uid;
                if (null != from)
                {
                    if (String.IsNullOrEmpty(from.Revision))
                    {
                        return (T) (object) new IdentityConnectors.Framework.Common.Objects.Uid(from.Value);
                    }
                    return (T) (object) new IdentityConnectors.Framework.Common.Objects.Uid(from.Value, from.Revision);
                }
            }
            //ConnectorKey
            if (typeof (API.ConnectorKey) == type)
            {
                var from = source as Common.ProtoBuf.ConnectorKey;
                if (null != from)
                {
                    return (T) (object) new API.ConnectorKey(from.BundleName, from.BundleVersion, from.ConnectorName);
                }
            }
            //ScriptContext
            if (typeof (OBJ.ScriptContext) == type)
            {
                var from = source as Common.ProtoBuf.ScriptContext;
                if (null != from)
                {
                    var options = DeserializeLegacy<Dictionary<Object, Object>>(from.ScriptArguments);
                    IDictionary<string, object> arguments =
                        CollectionUtil.NewDictionary<object, object, string, object>(options);
                    return
                        (T)
                            (object)
                                new OBJ.ScriptContext(from.Script.ScriptLanguage, from.Script.ScriptText,
                                    arguments ?? new Dictionary<String, Object>());
                }
            }
            //SearchResult
            if (typeof (OBJ.SearchResult) == type)
            {
                var from = source as Common.ProtoBuf.SearchResult;
                if (null != from)
                {
                    return (T) (object) new OBJ.SearchResult(from.PagedResultsCookie, from.RemainingPagedResults);
                }
            }
            //ConnectorObject
            if (typeof (OBJ.ConnectorObject) == type)
            {
                var from = source as Common.ProtoBuf.ConnectorObject;
                if (null != from)
                {
                    ICollection<Object> attsObj = DeserializeLegacy<ICollection<Object>>(from.Attriutes);
                    ICollection<OBJ.ConnectorAttribute> atts =
                        CollectionUtil.NewSet<Object, OBJ.ConnectorAttribute>(attsObj);
                    return (T) (object) new OBJ.ConnectorObject(new OBJ.ObjectClass(from.ObjectClass), atts);
                }
            }
            //Locale/CultureInfo
            if (typeof (CultureInfo) == type)
            {
                var from = source as Common.ProtoBuf.Locale;
                if (null != from)
                {
                    return
                        (T)
                            (object)
                                new Org.IdentityConnectors.Common.Locale(from.Language, from.Country, from.Variant)
                                    .ToCultureInfo();
                }
            }
            //SyncToken
            if (typeof (OBJ.SyncToken) == type)
            {
                var from = source as Common.ProtoBuf.SyncToken;
                if (null != from)
                {
                    return (T) (object) new OBJ.SyncToken(DeserializeLegacy<object>(from.Value));
                }
            }
            //SyncDelta
            if (typeof (IdentityConnectors.Framework.Common.Objects.SyncDelta) == type)
            {
                var from = source as Org.ForgeRock.OpenICF.Common.ProtoBuf.SyncDelta;
                if (null != from)
                {
                    OBJ.SyncDeltaBuilder builder = new OBJ.SyncDeltaBuilder();
                    builder.Token = DeserializeMessage<OBJ.SyncToken>(from.Token);
                    switch (from.DeltaType)
                    {
                        case Common.ProtoBuf.SyncDelta.Types.SyncDeltaType.CREATE:
                            builder.DeltaType = OBJ.SyncDeltaType.CREATE;
                            break;
                        case Common.ProtoBuf.SyncDelta.Types.SyncDeltaType.CREATE_OR_UPDATE:
                            builder.DeltaType = OBJ.SyncDeltaType.CREATE_OR_UPDATE;
                            break;
                        case Common.ProtoBuf.SyncDelta.Types.SyncDeltaType.UPDATE:
                            builder.DeltaType = OBJ.SyncDeltaType.UPDATE;
                            break;
                        case Common.ProtoBuf.SyncDelta.Types.SyncDeltaType.DELETE:
                            builder.DeltaType = OBJ.SyncDeltaType.DELETE;
                            break;
                    }
                    if (from.PreviousUid != null)
                    {
                        builder.PreviousUid =
                            DeserializeMessage<IdentityConnectors.Framework.Common.Objects.Uid>(from.PreviousUid);
                    }
                    if (!String.IsNullOrEmpty(from.ObjectClass))
                    {
                        builder.ObjectClass = new OBJ.ObjectClass(from.ObjectClass);
                    }
                    if (from.Uid != null)
                    {
                        builder.Uid = DeserializeMessage<IdentityConnectors.Framework.Common.Objects.Uid>(from.Uid);
                    }
                    if (!from.ConnectorObject.IsEmpty)
                    {
                        ICollection<Object> attsObj = DeserializeLegacy<ICollection<Object>>(from.ConnectorObject);
                        ICollection<OBJ.ConnectorAttribute> atts =
                            CollectionUtil.NewSet<Object, OBJ.ConnectorAttribute>(attsObj);
                        builder.Object = new OBJ.ConnectorObject(builder.ObjectClass, atts);
                    }
                    return (T) (object) builder.Build();
                }
            }

            throw new NotImplementedException("From not supported");
        }

        public static T SerializeMessage<T>(Object source)
        {
            if (source == null)
            {
                return default(T);
            }
            var type = typeof (T);
            // UID
            if (typeof (PRB.Uid) == type)
            {
                var from = source as IdentityConnectors.Framework.Common.Objects.Uid;
                if (null != from)
                {
                    var to = new PRB.Uid
                    {
                        Value = from.GetUidValue()
                    };
                    if (null != from.Revision) to.Revision = from.Revision;
                    return (T) (object) to;
                }
            }
            //ConnectorKey
            if (typeof (Common.ProtoBuf.ConnectorKey) == type)
            {
                var from = source as API.ConnectorKey;
                if (null != from)
                {
                    return (T) (object) new Common.ProtoBuf.ConnectorKey
                    {
                        BundleName = from.BundleName,
                        BundleVersion = from.BundleVersion,
                        ConnectorName = from.ConnectorName
                    };
                }
            }
            //ScriptContext
            if (typeof (Common.ProtoBuf.ScriptContext) == type)
            {
                var from = source as OBJ.ScriptContext;
                if (null != from)
                {
                    return (T) (object) new Common.ProtoBuf.ScriptContext
                    {
                        Script = new Common.ProtoBuf.Script
                        {
                            ScriptLanguage = from.ScriptLanguage,
                            ScriptText = from.ScriptText
                        },
                        ScriptArguments = SerializeLegacy(from.ScriptArguments)
                    };
                }
            }
            //SearchResult
            if (typeof (Common.ProtoBuf.SearchResult) == type)
            {
                var from = source as OBJ.SearchResult;
                if (null != from)
                {
                    return
                        (T)
                            (object)
                                new Common.ProtoBuf.SearchResult
                                {
                                    PagedResultsCookie = from.PagedResultsCookie,
                                    RemainingPagedResults = from.RemainingPagedResults
                                };
                }
            }
            //ConnectorObject
            if (typeof (Common.ProtoBuf.ConnectorObject) == type)
            {
                var from = source as OBJ.ConnectorObject;
                if (null != from)
                {
                    return (T) (object) new Common.ProtoBuf.ConnectorObject
                    {
                        ObjectClass = from.ObjectClass.GetObjectClassValue(),
                        Attriutes = SerializeLegacy(from.GetAttributes())
                    };
                }
            }
            //Locale/CultureInfo
            if (typeof (Common.ProtoBuf.Locale) == type)
            {
                var from = source as CultureInfo;
                if (null != from)
                {
                    Org.IdentityConnectors.Common.Locale locale = Org.IdentityConnectors.Common.Locale.FindLocale(from);
                    return (T) (object) new PRB.Locale
                    {
                        Language = locale.Language,
                        Country = locale.Country,
                        Variant = locale.Variant
                    };
                }
            }
            //SyncToken
            if (typeof (Common.ProtoBuf.SyncToken) == type)
            {
                var from = source as OBJ.SyncToken;
                if (null != from)
                {
                    return (T) (object) new Common.ProtoBuf.SyncToken
                    {
                        Value = SerializeLegacy(from.Value)
                    };
                }
            }
            //SyncDelta
            if (typeof (Org.ForgeRock.OpenICF.Common.ProtoBuf.SyncDelta) == type)
            {
                var from = source as IdentityConnectors.Framework.Common.Objects.SyncDelta;
                if (null != from)
                {
                    Org.ForgeRock.OpenICF.Common.ProtoBuf.SyncDelta builder =
                        new Org.ForgeRock.OpenICF.Common.ProtoBuf.SyncDelta();
                    builder.Token = SerializeMessage<Common.ProtoBuf.SyncToken>(from.Token);


                    switch (from.DeltaType)
                    {
                        case OBJ.SyncDeltaType.CREATE:
                            builder.DeltaType =
                                Org.ForgeRock.OpenICF.Common.ProtoBuf.SyncDelta.Types.SyncDeltaType.CREATE;
                            break;
                        case OBJ.SyncDeltaType.CREATE_OR_UPDATE:
                            builder.DeltaType =
                                Org.ForgeRock.OpenICF.Common.ProtoBuf.SyncDelta.Types.SyncDeltaType.CREATE_OR_UPDATE;
                            break;
                        case OBJ.SyncDeltaType.UPDATE:
                            builder.DeltaType =
                                Org.ForgeRock.OpenICF.Common.ProtoBuf.SyncDelta.Types.SyncDeltaType.UPDATE;
                            break;
                        case OBJ.SyncDeltaType.DELETE:
                            builder.DeltaType =
                                Org.ForgeRock.OpenICF.Common.ProtoBuf.SyncDelta.Types.SyncDeltaType.DELETE;
                            break;
                    }
                    if (null != from.Uid)
                    {
                        builder.Uid = SerializeMessage<PRB.Uid>(from.Uid);
                    }
                    if (null != from.ObjectClass)
                    {
                        builder.ObjectClass = from.ObjectClass.GetObjectClassValue();
                    }
                    if (null != from.Object)
                    {
                        builder.ConnectorObject = SerializeLegacy(from.Object.GetAttributes());
                    }
                    if (null != from.PreviousUid)
                    {
                        builder.PreviousUid = SerializeMessage<PRB.Uid>(from.PreviousUid);
                    }

                    return (T) (object) builder;
                }
            }
            throw new NotImplementedException("To not supported");
        }
    }

    #endregion

    #region OpenICFServerAdapter

    public class OpenICFServerAdapter :
        IMessageListener<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
    {
        public static TraceSource traceSource = new TraceSource("OpenICFServerAdapter");
        //private readonly KeyPair keyPair = SecurityUtil.generateKeyPair();

        private readonly ConnectorFramework _connectorFramework;
        private readonly PRB.HandshakeMessage _handshakeMessage;
        private readonly IAsyncConnectorInfoManager _connectorInfoManager;

        public OpenICFServerAdapter(ConnectorFramework framework, IAsyncConnectorInfoManager defaultConnectorInfoManager,
            bool isClient)
        {
            Client = isClient;
            _connectorFramework = Assertions.NullChecked(framework, "connectorFramework");
            _connectorInfoManager = Assertions.NullChecked(defaultConnectorInfoManager, "connectorInfoManager");
            _handshakeMessage = new PRB.HandshakeMessage
            {
                SessionId = Guid.NewGuid().ToString(),
                ServerType = PRB.HandshakeMessage.Types.ServerType.DOTNET
            };
        }

        public virtual void OnClose(WebSocketConnectionHolder socket, int code, string reason)
        {
            Trace.TraceInformation("{0} onClose({1},{2}) ", LoggerName(), Convert.ToString(code),
                Convert.ToString(reason));
        }

        public virtual void OnConnect(WebSocketConnectionHolder socket)
        {
            if (Client)
            {
                Trace.TraceInformation("Client onConnect() - Send 'HandshakeMessage({0})'",
                    _handshakeMessage.SessionId);
                PRB.RemoteMessage requestBuilder = MessagesUtil.CreateRequest(0,
                    new PRB.RPCRequest
                    {
                        HandshakeMessage = _handshakeMessage
                    });
                socket.SendBytesAsync(requestBuilder.ToByteArray(), CancellationToken.None);
            }
            else
            {
                Trace.TraceInformation("Server onConnect()");
            }
        }

        public virtual void OnError(Exception t)
        {
            //traceSource.TraceVerbose("Socket error {0}", t);
            TraceUtil.TraceException("Socket error", t);
        }

        public virtual void OnMessage(WebSocketConnectionHolder socket, string data)
        {
            Trace.TraceWarning("String message is ignored: {0}", data);
        }

        public virtual void OnMessage(WebSocketConnectionHolder socket, byte[] bytes)
        {
#if DEBUG
            Debug.WriteLine("{0} onMessage(socket[{1}], {2}:bytes)", LoggerName(), socket.Id, bytes.Length);
#else
            Trace.TraceInformation("{0} onMessage({1}:bytes)", LoggerName(), bytes.Length);
#endif
            try
            {
                PRB.RemoteMessage message = PRB.RemoteMessage.Parser.ParseFrom(bytes);

                if (null != message.Request)
                {
                    if (null != message.Request.HandshakeMessage)
                    {
                        if (Client)
                        {
                            Trace.TraceInformation("Error = The client must send the Handshake first");
                        }
                        else
                        {
                            ProcessHandshakeRequest(socket, message.MessageId, message.Request.HandshakeMessage);
                        }
                    }
                    else if (socket.HandHooked)
                    {
                        if (null != message.Request.OperationRequest)
                        {
                            ProcessOperationRequest(socket, message.MessageId, message.Request.OperationRequest);
                        }
                        else if (null != message.Request.CancelOpRequest)
                        {
                            ProcessCancelOpRequest(socket, message.MessageId, message.Request.CancelOpRequest);
                        }
                        else if (null != message.Request.ControlRequest)
                        {
                            ProcessControlRequest(socket, message.MessageId, message.Request.ControlRequest);
                        }
                        else
                        {
                            HandleRequestMessage(socket, message.MessageId, message.Request);
                        }
                    }
                    else
                    {
                        HandleRequestMessage(socket, message.MessageId, message.Request);
                    }
                }
                else if (null != message.Response)
                {
                    if (null != message.Response.HandshakeMessage)
                    {
                        if (Client)
                        {
                            ProcessHandshakeResponse(socket, message.MessageId, message.Response.HandshakeMessage);
                        }
                        else
                        {
                            Trace.TraceInformation("Error = The server must send the Handshake response");
                        }
                    }
                    else if (null != message.Response.Error)
                    {
                        ProcessExceptionMessage(socket, message.MessageId, message.Response.Error);
                    }
                    else if (socket.HandHooked)
                    {
                        if (null != message.Response.OperationResponse)
                        {
                            ProcessOperationResponse(socket, message.MessageId, message.Response.OperationResponse);
                        }
                        else if (null != message.Response.ControlResponse)
                        {
                            ProcessControlResponse(socket, message.MessageId, message.Response.ControlResponse);
                        }
                        else
                        {
                            HandleResponseMessage(socket, message.MessageId, message.Response);
                        }
                    }
                    else
                    {
                        HandleResponseMessage(socket, message.MessageId, message.Response);
                    }
                }
                else
                {
                    HandleRemoteMessage(socket, message);
                }
            }
            catch (InvalidProtocolBufferException e)
            {
                Trace.TraceWarning("{0} failed parse message {1}", LoggerName(), e);
            }
            catch (Exception t)
            {
                Trace.TraceInformation("{0} Unhandled exception {1}", LoggerName(), t);
            }
        }

        protected virtual void HandleRemoteMessage(WebSocketConnectionHolder socket, PRB.RemoteMessage message)
        {
            if (socket.HandHooked)
            {
                socket.RemoteConnectionContext.RemoteConnectionGroup.TrySendMessage(
                    MessagesUtil.CreateErrorResponse(message.MessageId, new ConnectorException("Unknown RemoteMessage")));
            }
        }

        protected virtual void HandleRequestMessage(WebSocketConnectionHolder socket, long messageId,
            PRB.RPCRequest message)
        {
            if (socket.HandHooked)
            {
                socket.RemoteConnectionContext.RemoteConnectionGroup.TrySendMessage(
                    MessagesUtil.CreateErrorResponse(messageId, new ConnectorException("Unknown Request message")));
            }
            else
            {
                socket.SendBytesAsync(
                    MessagesUtil.CreateErrorResponse(messageId,
                        new ConnectorException("Connection received request before handshake")).ToByteArray(),
                    CancellationToken.None);
            }
        }

        protected internal virtual void HandleResponseMessage(WebSocketConnectionHolder socket, long messageId,
            PRB.RPCResponse message)
        {
            if (socket.HandHooked)
            {
                socket.RemoteConnectionContext.RemoteConnectionGroup.TrySendMessage(
                    MessagesUtil.CreateErrorResponse(messageId, new ConnectorException("Unknown Request message")));
            }
            else
            {
                socket.SendBytesAsync(
                    MessagesUtil.CreateErrorResponse(messageId,
                        new ConnectorException("Connection received response before handshake")).ToByteArray(),
                    CancellationToken.None);
            }
        }

        public virtual void OnPing(WebSocketConnectionHolder socket, byte[] bytes)
        {
            // Nothing to do, pong response has been sent
            Trace.TraceInformation("{0} onPing()", LoggerName());
            try
            {
#if DEBUG
                PRB.PingMessage message = PRB.PingMessage.Parser.ParseFrom(bytes);
#endif
            }
            catch (InvalidProtocolBufferException e)
            {
                Trace.TraceWarning("{0} failed parse message {1}", LoggerName(), e);
            }
        }

        public virtual void OnPong(WebSocketConnectionHolder socket, byte[] bytes)
        {
            // Confirm ping response!
            Trace.TraceInformation("{0} onPong()", LoggerName());
            try
            {
#if DEBUG
                PRB.PingMessage message = PRB.PingMessage.Parser.ParseFrom(bytes);
#endif
            }
            catch (InvalidProtocolBufferException e)
            {
                Trace.TraceWarning("{0} failed parse message {1}", LoggerName(), e);
            }
        }

        protected internal virtual bool Client { get; private set; }

        // Handshake Operations

        public virtual void ProcessHandshakeRequest(WebSocketConnectionHolder socket, long messageId,
            PRB.HandshakeMessage message)
        {
            PRB.RemoteMessage responseBuilder = MessagesUtil.CreateResponse(messageId, new
                PRB.RPCResponse {HandshakeMessage = _handshakeMessage});
            socket.SendBytesAsync(responseBuilder.ToByteArray(), CancellationToken.None);
            // Set Operational
            socket.ReceiveHandshake(message);
            Trace.TraceInformation("{0} accept Handshake ({1})", LoggerName(), message.SessionId);
        }

        public virtual void ProcessHandshakeResponse(WebSocketConnectionHolder socket, long messageId,
            PRB.HandshakeMessage message)
        {
            // Set Operational
            socket.ReceiveHandshake(message);

            Trace.TraceInformation("{0} accept Handshake ({1})", LoggerName(), message.SessionId);
        }

        // Control Operations

        public virtual void ProcessControlRequest(WebSocketConnectionHolder socket, long messageId,
            PRB.ControlRequest message)
        {
            PRB.ControlResponse builder = new PRB.ControlResponse();
            if (message.InfoLevel.Contains(PRB.ControlRequest.Types.InfoLevel.CONNECTOR_INFO))
            {
                if (_connectorInfoManager.ConnectorInfos.Any())
                {
                    IList<Org.IdentityConnectors.Framework.Impl.Api.Remote.RemoteConnectorInfoImpl> connectorInfos =
                        _connectorInfoManager.ConnectorInfos.Select(
                            ci => toRemote((AbstractConnectorInfo) ci)
                            ).ToList();
                    ByteString response = MessagesUtil.SerializeLegacy(connectorInfos);
                    builder.ConnectorInfos = response;
                }
            }

            PRB.RemoteMessage responseBuilder = MessagesUtil.CreateResponse(messageId,
                new PRB.RPCResponse
                {
                    ControlResponse = builder
                });

            socket.SendBytesAsync(responseBuilder.ToByteArray(), CancellationToken.None);
            Trace.TraceInformation("{0} accept ControlRequest ({1})", LoggerName(), message);
        }

        private Org.IdentityConnectors.Framework.Impl.Api.Remote.RemoteConnectorInfoImpl toRemote(
            AbstractConnectorInfo source)
        {
            Org.IdentityConnectors.Framework.Impl.Api.Remote.RemoteConnectorInfoImpl rv = new Org.IdentityConnectors.
                Framework.Impl.Api.Remote.RemoteConnectorInfoImpl
            {
                ConnectorDisplayNameKey = source.ConnectorDisplayNameKey,
                ConnectorKey = source.ConnectorKey,
                DefaultAPIConfiguration = source.DefaultAPIConfiguration,
                Messages = source.Messages
            };
            return rv;
        }

        public virtual void ProcessControlResponse(WebSocketConnectionHolder socket, long messageId,
            PRB.ControlResponse message)
        {
            socket.RemoteConnectionContext.RemoteConnectionGroup.ReceiveRequestResponse(socket, messageId, message);

            Trace.TraceInformation("{0} accept ControlResponse", LoggerName());
        }

        public virtual void ProcessOperationRequest(WebSocketConnectionHolder socket, long messageId,
            PRB.OperationRequest message)
        {
            Debug.WriteLine("IN Request({0}:{1})", messageId,
                socket.RemoteConnectionContext.RemotePrincipal.Identity.Name);
            Org.ForgeRock.OpenICF.Common.ProtoBuf.ConnectorKey connectorKey = message.ConnectorKey;

            API.ConnectorInfo info = FindConnectorInfo(connectorKey);
            if (info == null)
            {
                PRB.RemoteMessage response = MessagesUtil.CreateErrorResponse(messageId,
                    new ConnectorException("Connector not found: " + connectorKey + " "));
                socket.RemoteConnectionContext.RemoteConnectionGroup.TrySendMessage(response);
                return;
            }

            try
            {
                try
                {
                    if (null != message.Locale)
                    {
                        CultureInfo local = MessagesUtil.DeserializeMessage<CultureInfo>(message.Locale);
                        Thread.CurrentThread.CurrentUICulture = local;
                    }
                }
                catch (Exception e)
                {
                    TraceUtil.TraceException("Failed to set request CultureInfo", e);
                }

                string connectorFacadeKey = message.ConnectorFacadeKey.ToStringUtf8();

                API.ConnectorFacade connectorFacade = NewInstance(info, connectorFacadeKey);

                if (null != message.AuthenticateOpRequest)
                {
                    AuthenticationAsyncApiOpImpl.CreateProcessor(messageId, socket, message.AuthenticateOpRequest)
                        .Execute(connectorFacade);
                }
                else if (null != message.CreateOpRequest)
                {
                    CreateAsyncApiOpImpl.CreateProcessor(messageId, socket, message.CreateOpRequest)
                        .Execute(connectorFacade);
                }
                else if (null != message.ConnectorEventSubscriptionOpRequest)
                {
                    ConnectorEventSubscriptionApiOpImpl.CreateProcessor(messageId, socket,
                        message.ConnectorEventSubscriptionOpRequest).Execute(connectorFacade);
                }
                else if (null != message.DeleteOpRequest)
                {
                    DeleteAsyncApiOpImpl.CreateProcessor(messageId, socket, message.DeleteOpRequest)
                        .Execute(connectorFacade);
                }
                else if (null != message.GetOpRequest)
                {
                    GetAsyncApiOpImpl.CreateProcessor(messageId, socket, message.GetOpRequest).Execute(connectorFacade);
                }
                else if (null != message.ResolveUsernameOpRequest)
                {
                    ResolveUsernameAsyncApiOpImpl.CreateProcessor(messageId, socket, message.ResolveUsernameOpRequest)
                        .Execute(connectorFacade);
                }
                else if (null != message.SchemaOpRequest)
                {
                    SchemaAsyncApiOpImpl.CreateProcessor(messageId, socket, message.SchemaOpRequest)
                        .Execute(connectorFacade);
                }
                else if (null != message.ScriptOnConnectorOpRequest)
                {
                    ScriptOnConnectorAsyncApiOpImpl.CreateProcessor(messageId, socket,
                        message.ScriptOnConnectorOpRequest).Execute(connectorFacade);
                }
                else if (null != message.ScriptOnResourceOpRequest)
                {
                    ScriptOnResourceAsyncApiOpImpl.CreateProcessor(messageId, socket, message.ScriptOnResourceOpRequest)
                        .Execute(connectorFacade);
                }
                else if (null != message.SearchOpRequest)
                {
                    SearchAsyncApiOpImpl.CreateProcessor(messageId, socket, message.SearchOpRequest)
                        .Execute(connectorFacade);
                }
                else if (null != message.SyncOpRequest)
                {
                    SyncAsyncApiOpImpl.CreateProcessor(messageId, socket, message.SyncOpRequest)
                        .Execute(connectorFacade);
                }
                else if (null != message.SyncEventSubscriptionOpRequest)
                {
                    SyncEventSubscriptionApiOpImpl.CreateProcessor(messageId, socket,
                        message.SyncEventSubscriptionOpRequest).Execute(connectorFacade);
                }
                else if (null != message.TestOpRequest)
                {
                    TestAsyncApiOpImpl.CreateProcessor(messageId, socket, message.TestOpRequest)
                        .Execute(connectorFacade);
                }
                else if (null != message.UpdateOpRequest)
                {
                    UpdateAsyncApiOpImpl.CreateProcessor(messageId, socket, message.UpdateOpRequest)
                        .Execute(connectorFacade);
                }
                else if (null != message.ValidateOpRequest)
                {
                    ValidateAsyncApiOpImpl.CreateProcessor(messageId, socket, message.ValidateOpRequest)
                        .Execute(connectorFacade);
                }
                else
                {
                    socket.RemoteConnectionContext.RemoteConnectionGroup.TrySendMessage(
                        MessagesUtil.CreateErrorResponse(messageId, new ConnectorException("Unknown OperationRequest")));
                }
            }
            catch (Exception t)
            {
                TraceUtil.TraceException("Failed handle OperationRequest " + messageId, t);
                socket.RemoteConnectionContext.RemoteConnectionGroup.TrySendMessage(
                    MessagesUtil.CreateErrorResponse(messageId, t));
            }
        }

        public virtual void ProcessOperationResponse(WebSocketConnectionHolder socket, long messageId,
            PRB.OperationResponse message)
        {
            Debug.WriteLine("IN Response({0}:{1})", messageId,
                socket.RemoteConnectionContext.RemotePrincipal.Identity.Name);
            socket.RemoteConnectionContext.RemoteConnectionGroup.ReceiveRequestResponse(socket, messageId, message);
        }

        public virtual void ProcessExceptionMessage(WebSocketConnectionHolder socket, long messageId,
            PRB.ExceptionMessage message)
        {
            socket.RemoteConnectionContext.RemoteConnectionGroup.ReceiveRequestResponse(socket, messageId, message);
        }

        public virtual void ProcessCancelOpRequest(WebSocketConnectionHolder socket, long messageId,
            PRB.CancelOpRequest message)
        {
            socket.RemoteConnectionContext.RemoteConnectionGroup.ReceiveRequestCancel(messageId);
        }

        /*
        protected internal virtual Encryptor initialiseEncryptor()
        {
            HandshakeMessage message = null;
            // Create Encryptor
            if (message.hasPublicKey())
            {
                PublicKey publicKey = SecurityUtil.createPublicKey(message.PublicKey.toByteArray());
                try
                {
                    Encryptor encryptor = new ECIESEncryptor(keyPair.Private, publicKey);
                }
                catch (InvalidKeyException)
                {
                    sbyte[] error = null;
                    // socket.sendBytes(error);
                }
            }
            return null;
        }*/

        protected internal virtual string LoggerName()
        {
            return Client ? "Client" : "Server";
        }

        public virtual API.ConnectorFacade NewInstance(API.ConnectorInfo connectorInfo, string config)
        {
            return _connectorFramework.NewManagedInstance(connectorInfo, config);
        }

        public virtual API.ConnectorInfo FindConnectorInfo(Org.ForgeRock.OpenICF.Common.ProtoBuf.ConnectorKey key)
        {
            return
                _connectorInfoManager.FindConnectorInfo(new API.ConnectorKey(key.BundleName, key.BundleVersion,
                    key.ConnectorName));
        }
    }

    #endregion

    #region ReferenceCountedObject

    /// <summary>
    ///     An object which is lazily created when first referenced, and destroyed when
    ///     the last reference is released.
    /// </summary>
    /// <typeparamref name="T">The type of referenced object. </typeparamref>
    public abstract class ReferenceCountedObject<T>
    {
        /// <summary>
        ///     A reference to the reference counted object which will automatically be
        ///     released during garbage collection.
        /// </summary>
        public sealed class Reference<TR>
        {
            private readonly ReferenceCountedObject<TR> _outerInstance;

            /// <summary>
            ///     The value will be accessed by the finalizer thread so it needs to be
            ///     volatile in order to ensure that updates are published.
            /// </summary>
            internal TR Value;

            internal Reference(ReferenceCountedObject<TR> outerInstance, TR value)
            {
                _outerInstance = outerInstance;
                Value = value;
            }

            /// <summary>
            ///     Returns the referenced object.
            /// </summary>
            /// <returns> The referenced object. </returns>
            /// <exception cref="NullReferenceException">
            ///     If the referenced object has already been released.
            /// </exception>
            public TR Get()
            {
                if (Value == null)
                {
                    throw new NullReferenceException(); // Fail-fast.
                }
                return Value;
            }

            /// <summary>
            ///     Decrements the reference count for the reference counted object if
            ///     this reference refers to the reference counted instance. If the
            ///     reference count drops to zero then the referenced object will be
            ///     destroyed.
            /// </summary>
            public void Release()
            {
                TR instanceToRelease = default(TR);
                lock (_outerInstance._lock)
                {
                    if (Value != null)
                    {
                        if (Value.Equals(_outerInstance._instance) && --_outerInstance._refCount == 0)
                        {
                            // This was the last reference.
                            instanceToRelease = Value;
                            _outerInstance._instance = default(TR);
                        }

                        /*
					 * Force NPE for subsequent get() attempts and prevent
					 * multiple releases.
					 */
                        Value = default(TR);
                    }
                }
                if (instanceToRelease != null)
                {
                    _outerInstance.DestroyInstance(instanceToRelease);
                }
            }

            /// <summary>
            ///     Provide a finalizer because reference counting is intended for
            ///     expensive rarely created resources which should not be accidentally
            ///     left around.
            /// </summary>
            ~Reference()
            {
                Release();
            }
        }

        private T _instance;
        private readonly object _lock = new object();
        private int _refCount;

        /// <summary>
        ///     Creates a new referenced object whose reference count is initially zero.
        /// </summary>
        protected internal ReferenceCountedObject()
        {
            // Nothing to do.
        }

        /// <summary>
        ///     Returns a reference to the reference counted object.
        /// </summary>
        /// <returns> A reference to the reference counted object. </returns>
        public Reference<T> Acquire()
        {
            lock (_lock)
            {
                if (_refCount++ == 0)
                {
                    Debug.Assert(_instance == null);
                    _instance = NewInstance();
                }
                Debug.Assert(_instance != null);
                return new Reference<T>(this, _instance);
            }
        }

        protected internal virtual bool Null
        {
            get { return _instance == null; }
        }

        /// <summary>
        ///     Returns a reference to the provided object or, if it is {@code null}, a
        ///     reference to the reference counted object.
        /// </summary>
        /// <param name="value">
        ///     The object to be referenced, or {@code null} if the reference
        ///     counted object should be used.
        /// </param>
        /// <returns>
        ///     A reference to the provided object or, if it is {@code null}, a
        ///     reference to the reference counted object.
        /// </returns>
        public Reference<T> AcquireIfNull(T value)
        {
            return value != null ? new Reference<T>(this, value) : Acquire();
        }

        /// <summary>
        ///     Invoked when a reference is released and the reference count will become
        ///     zero. Implementations should release any resources associated with the
        ///     resource and should not return until the resources have been released.
        /// </summary>
        /// <param name="instance">
        ///     The instance to be destroyed.
        /// </param>
        protected internal abstract void DestroyInstance(T instance);

        /// <summary>
        ///     Invoked when a reference is acquired and the current reference count is
        ///     zero. Implementations should create a new instance as fast as possible.
        /// </summary>
        /// <returns> The new instance. </returns>
        protected internal abstract T NewInstance();
    }

    #endregion

    #region RemoteAsyncConnectorFacade

    public class RemoteAsyncConnectorFacade : AbstractConnectorFacade, IAsyncConnectorFacade
    {
        private readonly ConcurrentDictionary<String, ByteString> _facadeKeys;

        private readonly IAuthenticationAsyncApiOp _authenticationApiOp;
        private readonly ICreateAsyncApiOp _createApiOp;
        private readonly IConnectorEventSubscriptionApiOp _connectorEventSubscriptionApiOp;
        private readonly IDeleteAsyncApiOp _deleteApiOp;
        private readonly IGetAsyncApiOp _getApiOp;
        private readonly IResolveUsernameAsyncApiOp _resolveUsernameApiOp;
        private readonly ISchemaAsyncApiOp _schemaApiOp;
        private readonly IScriptOnConnectorAsyncApiOp _scriptOnConnectorApiOp;
        private readonly IScriptOnResourceAsyncApiOp _scriptOnResourceApiOp;
        private readonly ISearchAsyncApiOp _searchApiOp;
        private readonly ISyncAsyncApiOp _syncApiOp;
        private readonly ISyncEventSubscriptionApiOp _syncEventSubscriptionApiOp;
        private readonly ITestAsyncApiOp _testApiOp;
        private readonly IUpdateAsyncApiOp _updateApiOp;
        private readonly IValidateAsyncApiOp _validateApiOp;


        private class InnerFacadeContext : ILoadBalancingConnectorFacadeContext
        {
            private readonly API.ConnectorInfo _connectorInfo;
            private readonly RemoteOperationContext _context;

            public InnerFacadeContext(API.ConnectorInfo connectorInfo, RemoteOperationContext context)
            {
                _connectorInfo = connectorInfo;
                _context = context;
            }

            public API.APIConfiguration ApiConfiguration
            {
                get { return _connectorInfo.CreateDefaultAPIConfiguration(); }
            }

            public string PrincipalName
            {
                get { return _context.RemotePrincipal.Identity.Name; }
            }

            public RemoteOperationContext RemoteOperationContext
            {
                get { return _context; }
            }
        }

        protected internal RemoteAsyncConnectorFacade(APIConfigurationImpl configuration,
            Func<ILoadBalancingConnectorFacadeContext, API.APIConfiguration> transformer)
            : base(configuration)
        {
            if (configuration.ConnectorInfo is RemoteConnectorInfoImpl)
            {
                IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                    remoteConnection =
                        Assertions.NullChecked(
                            ((RemoteConnectorInfoImpl) configuration.ConnectorInfo).messageDistributor,
                            "messageDistributor");

                API.ConnectorKey connectorKey = GetAPIConfiguration().ConnectorInfo.ConnectorKey;
                Func<RemoteOperationContext, ByteString> facadeKeyFunction;
                if (null != transformer)
                {
                    _facadeKeys = new ConcurrentDictionary<string, ByteString>();
                    facadeKeyFunction = (value) =>
                    {
                        ByteString facadeKey;
                        _facadeKeys.TryGetValue(value.RemotePrincipal.Identity.Name, out facadeKey);
                        if (null == facadeKey)
                        {
                            API.ConnectorInfo connectorInfo = value.RemoteConnectionGroup.FindConnectorInfo(connectorKey);
                            if (null != connectorInfo)
                            {
                                // Remote server has the ConnectorInfo
                                try
                                {
                                    API.APIConfiguration fullConfiguration =
                                        transformer(
                                            new InnerFacadeContext(connectorInfo, value));
                                    if (null != fullConfiguration)
                                    {
                                        string connectorFacadeKey =
                                            (SerializerUtil.SerializeBase64Object(fullConfiguration));
                                        facadeKey = ByteString.CopyFromUtf8(connectorFacadeKey);
                                        if (null != GetAPIConfiguration().ChangeListener)
                                        {
                                            value.RemoteConnectionGroup.AddConfigurationChangeListener(
                                                connectorFacadeKey,
                                                GetAPIConfiguration().ChangeListener);
                                        }
                                        _facadeKeys.TryAdd(value.RemotePrincipal.Identity.Name, facadeKey);
                                    }
                                }
                                catch (Exception t)
                                {
                                    TraceUtil.TraceException(TraceLevel.Warning,
                                        "Failed to build APIConfiguration for {0}", t,
                                        value.RemotePrincipal.Identity.Name);
                                }
                            }
                            else
                            {
                                Trace.TraceInformation(
                                    "Can not execute Operation on {0} because ConnectorInfo [{1}] is not installed",
                                    value.RemotePrincipal.Identity.Name, connectorKey);
                            }
                        }
                        return facadeKey;
                    };
                }
                else
                {
                    _facadeKeys = null;
                    ByteString facadeKey = ByteString.CopyFromUtf8(ConnectorFacadeKey);
                    facadeKeyFunction = context =>
                    {
                        context.RemoteConnectionGroup.FindConnectorInfo(GetAPIConfiguration().ConnectorInfo.ConnectorKey);
                        if (null != GetAPIConfiguration().ChangeListener)
                        {
                            //context.RemoteConnectionGroup.AddConfigurationChangeListener(ConnectorFacadeKey, GetAPIConfiguration().ChangeListener);
                        }
                        return facadeKey;
                    };
                }

                // initialise operations
                if (configuration.IsSupportedOperation(SafeType<APIOperation>.Get<AuthenticationApiOp>()))
                {
                    _authenticationApiOp = new AuthenticationAsyncApiOpImpl(remoteConnection, connectorKey,
                        facadeKeyFunction);
                }
                else
                {
                    _authenticationApiOp = null;
                }
                if (configuration.IsSupportedOperation(SafeType<APIOperation>.Get<CreateApiOp>()))
                {
                    _createApiOp = new CreateAsyncApiOpImpl(remoteConnection, connectorKey, facadeKeyFunction);
                }
                else
                {
                    _createApiOp = null;
                }
                if (configuration.IsSupportedOperation(SafeType<APIOperation>.Get<IConnectorEventSubscriptionApiOp>()))
                {
                    _connectorEventSubscriptionApiOp = new ConnectorEventSubscriptionApiOpImpl(remoteConnection,
                        connectorKey,
                        facadeKeyFunction);
                }
                else
                {
                    _connectorEventSubscriptionApiOp = null;
                }
                if (configuration.IsSupportedOperation(SafeType<APIOperation>.Get<DeleteApiOp>()))
                {
                    _deleteApiOp = new DeleteAsyncApiOpImpl(remoteConnection, connectorKey, facadeKeyFunction);
                }
                else
                {
                    _deleteApiOp = null;
                }
                if (configuration.IsSupportedOperation(SafeType<APIOperation>.Get<ResolveUsernameApiOp>()))
                {
                    _resolveUsernameApiOp = new ResolveUsernameAsyncApiOpImpl(remoteConnection, connectorKey,
                        facadeKeyFunction);
                }
                else
                {
                    _resolveUsernameApiOp = null;
                }
                if (configuration.IsSupportedOperation(SafeType<APIOperation>.Get<SchemaApiOp>()))
                {
                    _schemaApiOp = new SchemaAsyncApiOpImpl(remoteConnection, connectorKey, facadeKeyFunction);
                }
                else
                {
                    _schemaApiOp = null;
                }
                if (configuration.IsSupportedOperation(SafeType<APIOperation>.Get<ScriptOnConnectorApiOp>()))
                {
                    _scriptOnConnectorApiOp = new ScriptOnConnectorAsyncApiOpImpl(remoteConnection, connectorKey,
                        facadeKeyFunction);
                }
                else
                {
                    _scriptOnConnectorApiOp = null;
                }
                if (configuration.IsSupportedOperation(SafeType<APIOperation>.Get<ScriptOnResourceApiOp>()))
                {
                    _scriptOnResourceApiOp = new ScriptOnResourceAsyncApiOpImpl(remoteConnection, connectorKey,
                        facadeKeyFunction);
                }
                else
                {
                    _scriptOnResourceApiOp = null;
                }
                if (configuration.IsSupportedOperation(SafeType<APIOperation>.Get<SearchApiOp>()))
                {
                    _searchApiOp = new SearchAsyncApiOpImpl(remoteConnection, connectorKey, facadeKeyFunction);
                    _getApiOp = new GetAsyncApiOpImpl(remoteConnection, connectorKey, facadeKeyFunction);
                }
                else
                {
                    _searchApiOp = null;
                    _getApiOp = null;
                }
                if (configuration.IsSupportedOperation(SafeType<APIOperation>.Get<SyncApiOp>()))
                {
                    _syncApiOp = new SyncAsyncApiOpImpl(remoteConnection, connectorKey, facadeKeyFunction);
                }
                else
                {
                    _syncApiOp = null;
                }
                if (configuration.IsSupportedOperation(SafeType<APIOperation>.Get<ISyncEventSubscriptionApiOp>()))
                {
                    _syncEventSubscriptionApiOp = new SyncEventSubscriptionApiOpImpl(remoteConnection, connectorKey,
                        facadeKeyFunction);
                }
                else
                {
                    _syncEventSubscriptionApiOp = null;
                }
                if (configuration.IsSupportedOperation(SafeType<APIOperation>.Get<TestApiOp>()))
                {
                    _testApiOp = new TestAsyncApiOpImpl(remoteConnection, connectorKey, facadeKeyFunction);
                }
                else
                {
                    _testApiOp = null;
                }
                if (configuration.IsSupportedOperation(SafeType<APIOperation>.Get<UpdateApiOp>()))
                {
                    _updateApiOp = new UpdateAsyncApiOpImpl(remoteConnection, connectorKey, facadeKeyFunction);
                }
                else
                {
                    _updateApiOp = null;
                }
                _validateApiOp = new ValidateAsyncApiOpImpl(remoteConnection, connectorKey, facadeKeyFunction);
            }
            else
            {
                throw new InvalidParameterException("Unsupported ConnectorInfo type");
            }
        }


        public RemoteAsyncConnectorFacade(RemoteConnectorInfoImpl firstConnectorInfo,
            Func<ILoadBalancingConnectorFacadeContext, API.APIConfiguration> transformer)
            : this((APIConfigurationImpl) firstConnectorInfo.CreateDefaultAPIConfiguration(), transformer)
        {
        }

        public RemoteAsyncConnectorFacade(APIConfigurationImpl configuration) : this(configuration, null)
        {
        }

        protected internal virtual T GetAsyncOperationCheckSupported<T>() where T : APIOperation
        {
            T op = (T) GetOperationImplementation(SafeType<APIOperation>.ForRawType(typeof (T)));

            // check if this operation is supported.
            if (null == op)
            {
                throw new NotSupportedException(String.Format(Msg, typeof (T)));
            }
            return op;
        }

        protected override APIOperation GetOperationImplementation(SafeType<APIOperation> api)
        {
            if (typeof (AuthenticationApiOp).IsAssignableFrom(api.RawType))
            {
                return _authenticationApiOp;
            }
            if (typeof (CreateApiOp).IsAssignableFrom(api.RawType))
            {
                return _createApiOp;
            }
            if (typeof (IConnectorEventSubscriptionApiOp).IsAssignableFrom(api.RawType))
            {
                return _connectorEventSubscriptionApiOp;
            }
            if (typeof (DeleteApiOp).IsAssignableFrom(api.RawType))
            {
                return _deleteApiOp;
            }
            if (typeof (GetApiOp).IsAssignableFrom(api.RawType))
            {
                return _getApiOp;
            }
            if (typeof (ResolveUsernameApiOp).IsAssignableFrom(api.RawType))
            {
                return _resolveUsernameApiOp;
            }
            if (typeof (SchemaApiOp).IsAssignableFrom(api.RawType))
            {
                return _schemaApiOp;
            }
            if (typeof (ScriptOnConnectorApiOp).IsAssignableFrom(api.RawType))
            {
                return _scriptOnConnectorApiOp;
            }
            if (typeof (ScriptOnResourceApiOp).IsAssignableFrom(api.RawType))
            {
                return _scriptOnResourceApiOp;
            }
            if (typeof (SearchApiOp).IsAssignableFrom(api.RawType))
            {
                return _searchApiOp;
            }
            if (typeof (SyncApiOp).IsAssignableFrom(api.RawType))
            {
                return _syncApiOp;
            }
            if (typeof (ISyncEventSubscriptionApiOp).IsAssignableFrom(api.RawType))
            {
                return _syncEventSubscriptionApiOp;
            }
            if (typeof (TestApiOp).IsAssignableFrom(api.RawType))
            {
                return _testApiOp;
            }
            if (typeof (UpdateApiOp).IsAssignableFrom(api.RawType))
            {
                return _updateApiOp;
            }
            if (typeof (ValidateApiOp).IsAssignableFrom(api.RawType))
            {
                return _validateApiOp;
            }
            return null;
        }

        public async Task<IdentityConnectors.Framework.Common.Objects.Uid> AuthenticateAsync(
            OBJ.ObjectClass objectClass,
            string username, GuardedString password, OBJ.OperationOptions options, CancellationToken cancellationToken)
        {
            return await GetAsyncOperationCheckSupported<IAuthenticationAsyncApiOp>()
                .AuthenticateAsync(objectClass, username, password, options, cancellationToken);
        }

        public async Task<IdentityConnectors.Framework.Common.Objects.Uid> CreateAsync(OBJ.ObjectClass objectClass,
            ICollection<OBJ.ConnectorAttribute> createAttributes, OBJ.OperationOptions options,
            CancellationToken cancellationToken)
        {
            return await GetAsyncOperationCheckSupported<ICreateAsyncApiOp>()
                .CreateAsync(objectClass, createAttributes, options, cancellationToken);
        }

        public async Task DeleteAsync(OBJ.ObjectClass objectClass, IdentityConnectors.Framework.Common.Objects.Uid uid,
            OBJ.OperationOptions options, CancellationToken cancellationToken)
        {
            await
                GetAsyncOperationCheckSupported<IDeleteAsyncApiOp>()
                    .DeleteAsync(objectClass, uid, options, cancellationToken);
        }

        public async Task<OBJ.ConnectorObject> GetObjectAsync(OBJ.ObjectClass objectClass,
            IdentityConnectors.Framework.Common.Objects.Uid uid, OBJ.OperationOptions options,
            CancellationToken cancellationToken)
        {
            return
                await
                    GetAsyncOperationCheckSupported<IGetAsyncApiOp>()
                        .GetObjectAsync(objectClass, uid, options, cancellationToken);
        }

        public async Task<IdentityConnectors.Framework.Common.Objects.Uid> ResolveUsernameAsync(
            OBJ.ObjectClass objectClass,
            string username, OBJ.OperationOptions options, CancellationToken cancellationToken)
        {
            return await GetAsyncOperationCheckSupported<IResolveUsernameAsyncApiOp>()
                .ResolveUsernameAsync(objectClass, username, options, cancellationToken);
        }

        public async Task<OBJ.Schema> SchemaAsync(CancellationToken cancellationToken)
        {
            return await GetAsyncOperationCheckSupported<ISchemaAsyncApiOp>().SchemaAsync(cancellationToken);
        }

        public async Task<object> RunScriptOnConnectorAsync(OBJ.ScriptContext request, OBJ.OperationOptions options,
            CancellationToken cancellationToken)
        {
            return await GetAsyncOperationCheckSupported<IScriptOnConnectorAsyncApiOp>()
                .RunScriptOnConnectorAsync(request, options, cancellationToken);
        }

        public async Task<object> RunScriptOnResourceAsync(OBJ.ScriptContext request, OBJ.OperationOptions options,
            CancellationToken cancellationToken)
        {
            return await GetAsyncOperationCheckSupported<IScriptOnResourceAsyncApiOp>()
                .RunScriptOnResourceAsync(request, options, cancellationToken);
        }

        public async Task<OBJ.SearchResult> SearchAsync(OBJ.ObjectClass objectClass, Filter filter,
            OBJ.ResultsHandler handler,
            OBJ.OperationOptions options, CancellationToken cancellationToken)
        {
            return await GetAsyncOperationCheckSupported<ISearchAsyncApiOp>()
                .SearchAsync(objectClass, filter, handler, options, cancellationToken);
        }

        public Task<OBJ.SyncToken> SyncAsync(OBJ.ObjectClass objectClass, OBJ.SyncToken token,
            OBJ.SyncResultsHandler handler,
            OBJ.OperationOptions options, CancellationToken cancellationToken)
        {
            return GetAsyncOperationCheckSupported<ISyncAsyncApiOp>()
                .SyncAsync(objectClass, token, handler, options, cancellationToken);
        }

        public Task<OBJ.SyncToken> GetLatestSyncTokenAsync(OBJ.ObjectClass objectClass,
            CancellationToken cancellationToken)
        {
            return GetAsyncOperationCheckSupported<ISyncAsyncApiOp>()
                .GetLatestSyncTokenAsync(objectClass, cancellationToken);
        }

        public Task TestAsync(CancellationToken cancellationToken)
        {
            return GetAsyncOperationCheckSupported<ITestAsyncApiOp>()
                .TestAsync(cancellationToken);
        }

        public Task<IdentityConnectors.Framework.Common.Objects.Uid> UpdateAsync(OBJ.ObjectClass objectClass,
            IdentityConnectors.Framework.Common.Objects.Uid uid, ICollection<OBJ.ConnectorAttribute> replaceAttributes,
            OBJ.OperationOptions options, CancellationToken cancellationToken)
        {
            return GetAsyncOperationCheckSupported<IUpdateAsyncApiOp>()
                .UpdateAsync(objectClass, uid, replaceAttributes, options, cancellationToken);
        }

        public Task<IdentityConnectors.Framework.Common.Objects.Uid> AddAttributeValuesAsync(
            OBJ.ObjectClass objectClass,
            IdentityConnectors.Framework.Common.Objects.Uid uid, ICollection<OBJ.ConnectorAttribute> valuesToAdd,
            OBJ.OperationOptions options, CancellationToken cancellationToken)
        {
            return GetAsyncOperationCheckSupported<IUpdateAsyncApiOp>()
                .AddAttributeValuesAsync(objectClass, uid, valuesToAdd, options, cancellationToken);
        }

        public Task<IdentityConnectors.Framework.Common.Objects.Uid> RemoveAttributeValuesAsync(
            OBJ.ObjectClass objectClass,
            IdentityConnectors.Framework.Common.Objects.Uid uid, ICollection<OBJ.ConnectorAttribute> valuesToRemove,
            OBJ.OperationOptions options, CancellationToken cancellationToken)
        {
            return GetAsyncOperationCheckSupported<IUpdateAsyncApiOp>()
                .RemoveAttributeValuesAsync(objectClass, uid, valuesToRemove, options, cancellationToken);
        }

        public Task ValidateAsync(CancellationToken cancellationToken)
        {
            return GetAsyncOperationCheckSupported<IValidateAsyncApiOp>()
                .ValidateAsync(cancellationToken);
        }
    }

    #endregion

    #region RemoteConnectorInfoImpl

    public class RemoteConnectorInfoImpl : AbstractConnectorInfo
    {
        internal readonly
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
            messageDistributor;

        public RemoteConnectorInfoImpl(
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                remoteConnection, AbstractConnectorInfo copyFrom)
        {
            messageDistributor = Assertions.NullChecked(remoteConnection, "remoteConnection");
            Assertions.NullCheck(copyFrom, "copyFrom");
            ConnectorDisplayNameKey = copyFrom.ConnectorDisplayNameKey;
            ConnectorKey = copyFrom.ConnectorKey;
            Messages = copyFrom.Messages;
            ConnectorCategoryKey = copyFrom.ConnectorCategoryKey;
            DefaultAPIConfiguration = copyFrom.DefaultAPIConfiguration;
        }
    }

    #endregion

    #region RoundRobinLoadBalancingAlgorithmFactory

    public class RoundRobinLoadBalancingAlgorithmFactory : LoadBalancingAlgorithmFactory
    {
        protected override
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
            CreateLoadBalancer(
            IList<IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>>
                delegates)
        {
            return
                new RoundRobinLoadBalancingAlgorithm
                    <WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>(delegates);
        }
    }

    #endregion

    #region SecurityUtil

    /// <summary>
    /// </summary>
    /// <remarks>since 1.5</remarks>
    public class SecurityUtil
    {
        public static AsymmetricCipherKeyPair GenerateKeyPair()
        {
            try
            {
                var ecP = CustomNamedCurves.GetByName(NamedCurve.secp256r1.ToString());
                var ecParams = new ECDomainParameters(ecP.Curve, ecP.G, ecP.N, ecP.H, ecP.GetSeed());
                ECKeyPairGenerator keyPairGenerator = new ECKeyPairGenerator();
                keyPairGenerator.Init(new ECKeyGenerationParameters(ecParams, new SecureRandom()));
                return keyPairGenerator.GenerateKeyPair();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                Console.Write(e.StackTrace);
            }
            return null;
        }
    }

    #endregion
}