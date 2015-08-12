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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Org.ForgeRock.OpenICF.Common.RPC;
using Org.ForgeRock.OpenICF.Framework.Remote;
using Org.IdentityConnectors.Common;
using Org.IdentityConnectors.Framework.Api;
using Org.IdentityConnectors.Framework.Common;
using Org.IdentityConnectors.Framework.Common.Serializer;
using Org.IdentityConnectors.Framework.Impl.Api;
using Org.IdentityConnectors.Framework.Impl.Api.Local;
using Org.IdentityConnectors.Framework.Impl.Api.Remote;
using RemoteConnectorInfoImpl = Org.ForgeRock.OpenICF.Framework.Remote.RemoteConnectorInfoImpl;

namespace Org.ForgeRock.OpenICF.Framework
{

    #region ConnectorFramework

    public class ConnectorFramework : IDisposable
    {
        public const string RemoteLibraryMissingException = "Remote Connection Library is not initialised";

        private RemoteConnectionInfoManagerFactory _remoteConnectionInfoManagerFactory = null;

        private Int32 _isRunning = 1;

        public bool Running
        {
            get { return _isRunning == 1; }
        }

        public virtual void Dispose()
        {
            if (Interlocked.CompareExchange(ref _isRunning, 0, 1) == 1)
            {
                // Notify CloseListeners
                RemoteConnectionInfoManagerFactory factory = RemoteConnectionInfoManagerFactory;
                if (null != factory)
                {
                    //logger.ok("Closing RemoteConnectionInfoManagerFactory");
                    factory.Dispose();
                }

                // We need to complete all pending Promises
                /*while (remoteManagerCache.Any())
                {
                    foreach (AsyncRemoteLegacyConnectorInfoManager manager in remoteManagerCache.Values)
                    {
                        manager.Dispose();
                    }
                }*/

                // We need to complete all pending Promises
                foreach (AsyncLocalConnectorInfoManager manager in _localConnectorInfoManagerCache.Values)
                {
                    manager.Dispose();
                }
                _localConnectorInfoManagerCache.Clear();
            }
        }

        ////
        public ConnectorFacade NewInstance(APIConfiguration config)
        {
            ConnectorFacade ret = null;
            APIConfigurationImpl impl = (APIConfigurationImpl) config;
            AbstractConnectorInfo connectorInfo = impl.ConnectorInfo;
            if (connectorInfo is LocalConnectorInfoImpl)
            {
                LocalConnectorInfoImpl localInfo = (LocalConnectorInfoImpl) connectorInfo;
                try
                {
                    // create a new Provisioner.
                    ret = new LocalConnectorFacadeImpl(localInfo, impl);
                }
                catch (Exception ex)
                {
                    string connector = impl.ConnectorInfo.ConnectorKey.ToString();
                    //connector, config
                    TraceUtil.TraceException("Failed to create new connector facade: {0}, {1}", ex);
                    throw;
                }
            }
            else if (connectorInfo is Org.IdentityConnectors.Framework.Impl.Api.Remote.RemoteConnectorInfoImpl)
            {
                ret = new Org.IdentityConnectors.Framework.Impl.Api.Remote.RemoteConnectorFacadeImpl(impl);
            }
            else if (connectorInfo is Org.ForgeRock.OpenICF.Framework.Remote.RemoteConnectorInfoImpl)
            {
                ret = new RemoteAsyncConnectorFacade(impl);
            }
            else
            {
                throw new System.ArgumentException("Unknown ConnectorInfo type");
            }
            return ret;
        }

        private readonly ConcurrentDictionary<String, ConnectorFacade> MANAGED_FACADE_CACHE =
            new ConcurrentDictionary<String, ConnectorFacade>();

        private Timer _scheduledManagedFacadeCacheTimer = null;

        public virtual ConnectorFacade newManagedInstance(ConnectorInfo connectorInfo, string config)
        {
            ConnectorFacade facade = null;
            MANAGED_FACADE_CACHE.TryGetValue(config, out facade);
            if (null == facade)
            {
                // new ConnectorFacade creation must remain cheap operation
                facade = NewInstance(connectorInfo, config);
                if (facade is LocalConnectorFacadeImpl)
                {
                    ConnectorFacade ret = MANAGED_FACADE_CACHE.GetOrAdd(facade.ConnectorFacadeKey, facade);
                    if (null != ret)
                    {
                        Trace.TraceInformation("ConnectorFacade found in cache");
                        facade = ret;
                    }
                    else
                    {
                        lock (MANAGED_FACADE_CACHE)
                        {
                            if (null == _scheduledManagedFacadeCacheTimer)
                            {
                                _scheduledManagedFacadeCacheTimer = new Timer(state =>
                                {
                                    foreach (var connectorFacade in MANAGED_FACADE_CACHE)
                                    {
                                        LocalConnectorFacadeImpl value =
                                            connectorFacade.Value as LocalConnectorFacadeImpl;
                                        if (null != value && value.IsUnusedFor(TimeSpan.FromHours(2)))
                                        {
                                            ConnectorFacade ignore;
                                            MANAGED_FACADE_CACHE.TryRemove(connectorFacade.Key, out ignore);
                                            if (ignore == value)
                                            {
                                                Trace.TraceInformation(
                                                    "LocalConnectorFacade is disposed after 120min inactivity");
                                                value.Dispose();
                                            }
                                        }
                                    }
                                }, MANAGED_FACADE_CACHE, TimeSpan.FromHours(2), TimeSpan.FromHours(2));
                            }
                        }
                    }
                }
            }
            return facade;
        }

        public ConnectorFacade NewInstance(ConnectorInfo connectorInfo, string config)
        {
            ConnectorFacade ret = null;
            if (connectorInfo is LocalConnectorInfoImpl)
            {
                try
                {
                    // create a new Provisioner.
                    ret = new LocalConnectorFacadeImpl((LocalConnectorInfoImpl) connectorInfo, config);
                }
                catch (Exception ex)
                {
                    string connector = connectorInfo.ConnectorKey.ToString();
                    //Trace.TraceError(ex, "Failed to create new connector facade: {0}, {1}", connector, config);
                    throw;
                }
            }
            else if (connectorInfo is Org.IdentityConnectors.Framework.Impl.Api.Remote.RemoteConnectorInfoImpl)
            {
                ret =
                    new RemoteConnectorFacadeImpl(
                        (Org.IdentityConnectors.Framework.Impl.Api.Remote.RemoteConnectorInfoImpl) connectorInfo, config);
            }
            else if (connectorInfo is Org.ForgeRock.OpenICF.Framework.Remote.RemoteConnectorInfoImpl)
            {
                Assertions.NullCheck(connectorInfo, "connectorInfo");
                APIConfigurationImpl configuration =
                    (APIConfigurationImpl)
                        SerializerUtil.DeserializeBase64Object(Assertions.NullChecked(config, "configuration"));
                configuration.ConnectorInfo = (Remote.RemoteConnectorInfoImpl) connectorInfo;
                ret = NewInstance(configuration);
            }
            else
            {
                throw new System.ArgumentException("Unknown ConnectorInfo type");
            }
            return ret;
        }

        // ------ LocalConnectorFramework Implementation Start ------

        private readonly ConcurrentDictionary<String, AsyncLocalConnectorInfoManager> _localConnectorInfoManagerCache =
            new ConcurrentDictionary<String, AsyncLocalConnectorInfoManager>();

        public virtual AsyncLocalConnectorInfoManager LocalManager
        {
            get { return GetLocalConnectorInfoManager("default"); }
        }

        public virtual AsyncLocalConnectorInfoManager GetLocalConnectorInfoManager(
            String connectorBundleParentClassLoader)
        {
            String key = connectorBundleParentClassLoader ?? "default";
            AsyncLocalConnectorInfoManager manager = null;
            _localConnectorInfoManagerCache.TryGetValue(key, out manager);
            return manager ?? (_localConnectorInfoManagerCache.GetOrAdd(key, new AsyncLocalConnectorInfoManager()));
        }

        // ------ LocalConnectorFramework Implementation End ------

        // ------ Legacy RemoteConnectorInfoManager Support ------
        /* private readonly ConcurrentDictionary<Pair<string, Int32>, AsyncRemoteLegacyConnectorInfoManager> remoteManagerCache = new ConcurrentDictionary<Pair<string, Int32>, AsyncRemoteLegacyConnectorInfoManager>();

         public virtual AsyncRemoteLegacyConnectorInfoManager getRemoteManager(RemoteFrameworkConnectionInfo info)
     {
         if (null == info)
         {
             return null;
         }
         if (Running)
         {
             Pair<string, Int32> key = Pair.Of(info.ToLower(CultureInfo.GetCultureInfo("en"))), info.Port);
             AsyncRemoteLegacyConnectorInfoManager rv = remoteManagerCache[key];
             if (rv == null)
             {
                 lock (remoteManagerCache)
                 {
                     rv = remoteManagerCache[key];
                     if (rv == null)
                     {
                         rv = new AsyncRemoteLegacyConnectorInfoManager(info, scheduler);
                         rv.addCloseListener((x)={remoteManagerCache.Remove(key);});
                         if (!Running && remoteManagerCache.Remove(key) != null)
                         {
                             rv.close();
                             throw new IllegalStateException("ConnectorFramework is shut down");
                         }
                     }
                     remoteManagerCache[key] = rv;
                 }
             }
             return rv;
         }
         else
         {
             throw new InvalidOperationException("ConnectorFramework is shut down");
         }
     }*/

        // ------ RemoteConnectorFramework Implementation Start ------

        public virtual AsyncRemoteConnectorInfoManager GetRemoteManager(RemoteWSFrameworkConnectionInfo info)
        {
            if (null == info)
            {
                return null;
            }
            return new AsyncRemoteConnectorInfoManager(RemoteConnectionInfoManagerFactory.Connect(info));
        }

        public virtual LoadBalancingConnectorInfoManager GetRemoteManager(
            LoadBalancingAlgorithmFactory loadBalancingAlgorithmFactory)
        {
            if (null != loadBalancingAlgorithmFactory &&
                loadBalancingAlgorithmFactory.AsyncRemoteConnectorInfoManager.Any())
            {
                return new LoadBalancingConnectorInfoManager(loadBalancingAlgorithmFactory);
            }
            return null;
        }

        public const String REMOTE_LIBRARY_MISSING_EXCEPTION =
            "Remote Connection Library is not initialised";

        public virtual ConnectorFacade NewInstance(ConnectorInfo connectorInfo,
            Func<Org.ForgeRock.OpenICF.Framework.Remote.RemoteConnectorInfoImpl, APIConfiguration> transformer)
        {
            if (null != _remoteConnectionInfoManagerFactory)
            {
                return null;
            }
            throw new System.NotSupportedException(REMOTE_LIBRARY_MISSING_EXCEPTION);
        }

        public virtual Task<ConnectorFacade> NewInstanceAsync(ConnectorKey key,
            Func<Org.ForgeRock.OpenICF.Framework.Remote.RemoteConnectorInfoImpl, APIConfiguration> transformer)
        {
            if (null != _remoteConnectionInfoManagerFactory)
            {
                return null;
            }
            throw new System.NotSupportedException(REMOTE_LIBRARY_MISSING_EXCEPTION);
        }

        // ------ RemoteConnectorFramework Implementation End ------

        public virtual RemoteConnectionInfoManagerFactory RemoteConnectionInfoManagerFactory
        {
            get
            {
                lock (this)
                {
                    if (null == _remoteConnectionInfoManagerFactory && Running)
                    {
                        OpenICFServerAdapter listener = new OpenICFServerAdapter(this, ConnectionInfoManager, true);
                        try
                        {
                            _remoteConnectionInfoManagerFactory = new RemoteConnectionInfoManagerFactory(listener,
                                ConnectionManagerConfig);
                        }
                        catch (Exception e)
                        {
                            TraceUtil.TraceException("RemoteConnectionInfoManagerFactory is not available", e);
                            //remoteConnectionInfoManagerFactory = new RemoteConnectionInfoManagerFactoryAnonymousInnerClassHelper(this, listener, Client.ConnectionManagerConfig, e);
                        }
                        //_remoteConnectionInfoManagerFactory.AddCloseListener( new CloseListenerAnonymousInnerClassHelper(this));
                    }
                    return _remoteConnectionInfoManagerFactory;
                }
            }
        }


        private ConnectionManagerConfig connectionManagerConfig = new ConnectionManagerConfig();


        protected internal virtual IAsyncConnectorInfoManager ConnectionInfoManager
        {
            get { return LocalManager; }
        }

        public virtual ConnectionManagerConfig ConnectionManagerConfig
        {
            get { return connectionManagerConfig; }
            set { connectionManagerConfig = value ?? new ConnectionManagerConfig(); }
        }
    }

    #endregion

    #region DelegatingAsyncConnectorInfoManager

    public abstract class DelegatingAsyncConnectorInfoManager :
        DisposableAsyncConnectorInfoManager<DelegatingAsyncConnectorInfoManager>
    {
        protected internal readonly ConcurrentDictionary<IAsyncConnectorInfoManager, Boolean> delegates =
            new ConcurrentDictionary<IAsyncConnectorInfoManager, Boolean>();

        private readonly ConcurrentDictionary<Pair<ConnectorKeyRange, DeferredPromise>, Boolean>
            deferredRangePromiseCacheList;

        private readonly ConcurrentDictionary<Pair<ConnectorKey, DeferredPromise>, Boolean> deferredKeyPromiseCacheList;

        private readonly bool allowDeferred;

        public delegate void CloseListener(DelegatingAsyncConnectorInfoManager connectorInfoManager);

        /// <devdoc>
        ///     <para>Adds a event handler to listen to the Disposed event on the DelegatingAsyncConnectorInfoManager.</para>
        /// </devdoc>
        public event EventHandler Disposed;

        protected DelegatingAsyncConnectorInfoManager(bool allowDeferred)
        {
            this.allowDeferred = allowDeferred;
            if (allowDeferred)
            {
                deferredRangePromiseCacheList =
                    new ConcurrentDictionary<Pair<ConnectorKeyRange, DeferredPromise>, Boolean>();

                deferredKeyPromiseCacheList = new ConcurrentDictionary<Pair<ConnectorKey, DeferredPromise>, Boolean>();
                Disposed += (sender, args) =>
                {
                    DelegatingAsyncConnectorInfoManager outerInstance = sender as DelegatingAsyncConnectorInfoManager;
                    if (null != outerInstance)
                    {
                        foreach (
                            Pair<ConnectorKeyRange, DeferredPromise> promise in
                                outerInstance.deferredRangePromiseCacheList.Keys)
                        {
                            promise.Second.Shutdown();
                        }
                        outerInstance.deferredRangePromiseCacheList.Clear();

                        foreach (
                            Pair<ConnectorKey, DeferredPromise> promise in
                                outerInstance.deferredKeyPromiseCacheList.Keys)
                        {
                            promise.Second.Shutdown();
                        }
                        outerInstance.deferredKeyPromiseCacheList.Clear();
                    }
                };
            }
            else
            {
                deferredRangePromiseCacheList = null;
                deferredKeyPromiseCacheList = null;
            }
        }

        protected internal abstract
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
            MessageDistributor { get; }


        protected virtual IReadOnlyCollection<IAsyncConnectorInfoManager> Delegates
        {
            get { return delegates.Keys as IReadOnlyCollection<IAsyncConnectorInfoManager>; }
        }

        protected bool AddAsyncConnectorInfoManager(IAsyncConnectorInfoManager @delegate)
        {
            if (null != @delegate && delegates.TryAdd(@delegate, true))
            {
                Trace.TraceInformation("Add AsyncConnectorInfoManager to delegates");
                OnAddAsyncConnectorInfoManager(@delegate);
                return true;
            }
            return false;
        }

        protected internal virtual bool RemoveAsyncConnectorInfoManager(IAsyncConnectorInfoManager @delegate)
        {
            bool ignore;
            return null != @delegate && delegates.TryRemove(@delegate, out ignore);
        }

        protected internal virtual void OnAddAsyncConnectorInfoManager(IAsyncConnectorInfoManager @delegate)
        {
            if (allowDeferred)
            {
                foreach (Pair<ConnectorKeyRange, DeferredPromise> promise in deferredRangePromiseCacheList.Keys)
                {
                    promise.Second.Add(@delegate.FindConnectorInfoAsync(promise.First));
                }
                foreach (Pair<ConnectorKey, DeferredPromise> promise in deferredKeyPromiseCacheList.Keys)
                {
                    promise.Second.Add(@delegate.FindConnectorInfoAsync(promise.First));
                }
            }
        }

        private sealed class DeferredPromise : TaskCompletionSource<ConnectorInfo>
        {
            private Int32 _remaining;

            public DeferredPromise(bool neverFail)
            {
                _remaining = neverFail ? 1 : 0;
            }

            public void Shutdown()
            {
                if (!Task.IsCompleted)
                    SetException(new InvalidOperationException("AsyncConnectorInfoManager is shut down!"));
            }

            internal bool Add(Task<ConnectorInfo> promise)
            {
                Interlocked.Increment(ref _remaining);
                promise.ContinueWith(task =>
                {
                    if (task.IsCompleted)
                    {
                        TrySetResult(task.Result);
                    }
                    else
                    {
                        if (Interlocked.Decrement(ref _remaining) == 0 && !Task.IsCompleted)
                        {
                            if (task.IsFaulted)
                            {
                                TrySetException(task.Exception);
                            }
                            else
                            {
                                TrySetCanceled();
                            }
                        }
                    }
                });
                return !Task.IsCompleted;
            }
        }

        public override async Task<ConnectorInfo> FindConnectorInfoAsync(ConnectorKey key)
        {
            if (!Running)
            {
                TaskCompletionSource<ConnectorInfo> promise = new TaskCompletionSource<ConnectorInfo>();
                promise.SetException(new InvalidOperationException("AsyncConnectorInfoManager is shut down!"));
                return await promise.Task;
            }
            else
            {
                IEnumerator<IAsyncConnectorInfoManager> safeDelegates = Delegates.GetEnumerator();
                DeferredPromise promise = new DeferredPromise(allowDeferred);
                Pair<ConnectorKey, DeferredPromise> entry = Pair<ConnectorKey, DeferredPromise>.Of(key, promise);

                if (allowDeferred)
                {
                    deferredKeyPromiseCacheList.TryAdd(entry, true);
                }

                bool pending = true;
                while (pending && safeDelegates.MoveNext())
                {
                    pending = promise.Add(safeDelegates.Current.FindConnectorInfoAsync(key));
                }

                if (allowDeferred && Running)
                {
                    if (pending)
                    {
                        promise.Task.ContinueWith(task =>
                        {
                            bool ignore;
                            deferredKeyPromiseCacheList.TryRemove(entry, out ignore);
                        });
                    }
                    else
                    {
                        bool ignore;
                        deferredKeyPromiseCacheList.TryRemove(entry, out ignore);
                    }
                }
                else if (!Running)
                {
                    promise.Shutdown();
                }

                return await
                    promise.Task.ContinueWith(
                        task => new RemoteConnectorInfoImpl(MessageDistributor,
                            (RemoteConnectorInfoImpl) task.Result));
            }
        }

        public override async Task<ConnectorInfo> FindConnectorInfoAsync(ConnectorKeyRange keyRange)
        {
            if (!Running)
            {
                throw new InvalidOperationException("AsyncConnectorInfoManager is shut down!");
            }
            else
            {
                if (keyRange.BundleVersionRange.Empty)
                {
                    TaskCompletionSource<ConnectorInfo> result = new TaskCompletionSource<ConnectorInfo>();
                    result.SetException(new ArgumentException("ConnectorBundle VersionRange is Empty"));
                    return await result.Task;
                }
                else if (keyRange.BundleVersionRange.Exact)
                {
                    return await FindConnectorInfoAsync(keyRange.ExactConnectorKey);
                }
                else
                {
                    IEnumerator<IAsyncConnectorInfoManager> safeDelegates = Delegates.GetEnumerator();

                    DeferredPromise promise = new DeferredPromise(allowDeferred);
                    Pair<ConnectorKeyRange, DeferredPromise> entry =
                        Pair<ConnectorKeyRange, DeferredPromise>.Of(keyRange, promise);

                    if (allowDeferred)
                    {
                        deferredRangePromiseCacheList.TryAdd(entry, true);
                    }

                    bool pending = true;
                    while (pending && safeDelegates.MoveNext())
                    {
                        pending = promise.Add(safeDelegates.Current.FindConnectorInfoAsync(keyRange));
                    }
                    if (allowDeferred && Running)
                    {
                        if (pending)
                        {
                            promise.Task.ContinueWith((task, state) =>
                            {
                                bool ignore;
                                deferredRangePromiseCacheList.TryRemove(
                                    (Pair<ConnectorKeyRange, DeferredPromise>) state, out ignore);
                            }, entry);
                        }
                        else
                        {
                            bool ignore;
                            deferredRangePromiseCacheList.TryRemove(entry, out ignore);
                        }
                    }
                    else if (!Running)
                    {
                        promise.Shutdown();
                    }

                    return await
                        promise.Task.ContinueWith(
                            task => new RemoteConnectorInfoImpl(MessageDistributor,
                                (RemoteConnectorInfoImpl) task.Result));
                }
            }
        }

        public override IList<ConnectorInfo> ConnectorInfos
        {
            get
            {
                List<ConnectorKey> keys = new List<ConnectorKey>();
                List<ConnectorInfo> result = new List<ConnectorInfo>();
                foreach (IAsyncConnectorInfoManager group in Delegates)
                {
                    foreach (ConnectorInfo info in group.ConnectorInfos)
                    {
                        if (!keys.Contains(info.ConnectorKey))
                        {
                            keys.Add(info.ConnectorKey);
                            result.Add(info);
                        }
                    }
                }
                return result;
            }
        }

        public override ConnectorInfo FindConnectorInfo(ConnectorKey key)
        {
            return Delegates.Select(@group => @group.FindConnectorInfo(key)).FirstOrDefault(result => null != result);
        }
    }

    #endregion
}