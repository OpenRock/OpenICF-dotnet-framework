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
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Org.ForgeRock.OpenICF.Common.ProtoBuf;
using Org.ForgeRock.OpenICF.Common.RPC;
using Org.IdentityConnectors.Common;
using Org.IdentityConnectors.Common.Security;
using Org.IdentityConnectors.Framework.Common.Exceptions;

namespace Org.ForgeRock.OpenICF.Framework.Remote
{

    #region ClientRemoteConnectorInfoManager

    public class ClientRemoteConnectorInfoManager : ConnectionPrincipal, IRemoteConnectorInfoManager
    {
        private readonly RemoteDelegatingAsyncConnectorInfoManager _delegatingAsyncConnectorInfoManager;

        private readonly WebSocketWrapper[] _connections = new WebSocketWrapper[2];

        private readonly RemoteWSFrameworkConnectionInfo _info;

        private readonly Timer _timer;

        public ClientRemoteConnectorInfoManager(RemoteWSFrameworkConnectionInfo info,
            IMessageListener<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext> listener,
            ConcurrentDictionary<string, WebSocketConnectionGroup> globalConnectionGroups)
            : base(listener, globalConnectionGroups)
        {
            _delegatingAsyncConnectorInfoManager =
                new RemoteDelegatingAsyncConnectorInfoManager(this);

            _info = info;
            _timer = new Timer(state =>
            {
                WebSocketConnectionHolder[] connections = state as WebSocketConnectionHolder[];
                if (null != connections)
                {
                    for (int i = 0; i < 2; i++)
                    {
                        if (connections[i] == null || !connections[i].Operational)
                        {
                            WebSocketWrapper vr = WebSocketWrapper.Create(_info, listener, Handshake);
                            vr.ConnectAsync().ContinueWith((result, o) =>
                            {
                                if (result.IsCompleted)
                                {
                                    connections[(int) o] = result.Result;
                                }
                                else if (result.IsFaulted)
                                {
                                    TraceUtil.TraceException("Failed to establish connection", result.Exception);
                                }
                            }, i);
                        }
                    }
                }
            }, _connections, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5));
        }

        protected override void DoClose()
        {
            _timer.Dispose();
            foreach (var connection in _connections)
            {
                connection.Dispose();
            }
        }

        public IAsyncConnectorInfoManager AsyncConnectorInfoManager
        {
            get { return _delegatingAsyncConnectorInfoManager; }
        }

        public IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
            RequestDistributor
        {
            get { return this; }
        }

        protected override void OnNewWebSocketConnectionGroup(WebSocketConnectionGroup connectionGroup)
        {
            Trace.TraceInformation("Activating new ConnectionGroup {0}:{1}", Identity.Name,
                connectionGroup.RemoteSessionId);
            _delegatingAsyncConnectorInfoManager.OnAddAsyncConnectorInfoManager(connectionGroup);
        }

        private sealed class RemoteDelegatingAsyncConnectorInfoManager : DelegatingAsyncConnectorInfoManager
        {
            private readonly ClientRemoteConnectorInfoManager _parent;

            public RemoteDelegatingAsyncConnectorInfoManager(ClientRemoteConnectorInfoManager parent) : base(true)
            {
                _parent = parent;
            }

            protected override
                IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                MessageDistributor
            {
                get { return _parent.RequestDistributor; }
            }

            protected override void DoClose()
            {
                _parent.Dispose();
            }

            protected override IReadOnlyCollection<IAsyncConnectorInfoManager> Delegates
            {
                get { return _parent.ConnectionGroups.Values as IReadOnlyCollection<WebSocketConnectionGroup>; }
            }
        }
    }

    #endregion

    #region ConnectionManagerConfig

    public class ConnectionManagerConfig
    {
    }

    #endregion

    #region RemoteConnectionInfoManagerFactory

    public class RemoteConnectionInfoManagerFactory : IDisposable
    {
        private readonly IMessageListener<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
            _messageListener;

        protected readonly ConnectionManagerConfig managerConfig;

        protected readonly ConcurrentDictionary<String, WebSocketConnectionGroup> connectionGroups =
            new ConcurrentDictionary<String, WebSocketConnectionGroup>();

        private readonly ConcurrentDictionary<RemoteWSFrameworkConnectionInfo, ClientRemoteConnectorInfoManager>
            registry =
                new ConcurrentDictionary<RemoteWSFrameworkConnectionInfo, ClientRemoteConnectorInfoManager>();

        internal RemoteConnectionInfoManagerFactory(
            IMessageListener<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                messageListener,
            ConnectionManagerConfig managerConfig)
        {
            this._messageListener = messageListener;
            this.managerConfig = managerConfig;
        }

        public IRemoteConnectorInfoManager Connect(RemoteWSFrameworkConnectionInfo info)
        {
            if (Running)
            {
                ClientRemoteConnectorInfoManager manager;
                registry.TryGetValue(info, out manager);
                if (null == manager)
                {
                    lock (registry)
                    {
                        registry.TryGetValue(info, out manager);
                        if (null == manager)
                        {
                            manager = new ClientRemoteConnectorInfoManager(info, MessageListener, connectionGroups);
                            manager.Disposed += (sender, args) =>
                            {
                                ClientRemoteConnectorInfoManager ignore;
                                registry.TryRemove(info, out ignore);
                            };

                            registry.TryAdd(info, manager);
                            if (!Running && registry.TryRemove(info, out manager))
                            {
                                manager.Dispose();
                                throw new InvalidOperationException("RemoteConnectionInfoManagerFactory is shut down");
                            }
                        }
                    }
                }
                return manager;
            }
            throw new InvalidOperationException("RemoteConnectionInfoManagerFactory is shut down");
        }


        protected internal void doClose()
        {
            foreach (var clientRemoteConnectorInfoManager in registry.Values)
            {
                clientRemoteConnectorInfoManager.Dispose();
            }
        }

        protected internal virtual
            IMessageListener<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
            MessageListener
        {
            get { return _messageListener; }
        }

        protected internal virtual ConnectionManagerConfig ManagerConfig
        {
            get { return managerConfig; }
        }

        private Int32 _isRunning = 1;

        public virtual bool Running
        {
            get { return _isRunning != 0; }
        }

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref _isRunning, 0, 1) == 0)
            {
                doClose();
                // Notify CloseListeners
                /*
                CloseListener<RemoteConnectionInfoManagerFactory> closeListener;
                while ((closeListener = closeListeners.RemoveFirst()) != null)
                {
                    invokeCloseListener(closeListener);
                }*/
            }
        }
    }

    #endregion

    #region IRemoteConnectorInfoManager

    public interface IRemoteConnectorInfoManager : IDisposable
    {
        IAsyncConnectorInfoManager AsyncConnectorInfoManager { get; }

        IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
            RequestDistributor { get; }
    }

    #endregion

    #region RemoteWSFrameworkConnectionInfo

    public class RemoteWSFrameworkConnectionInfo
    {
        public const string OPENICF_PROTOCOL = "v1.openicf.forgerock.org";

        /// <summary>
        ///     The host to use as proxy.
        /// </summary>
        public const string PROXY_HOST = "http.proxyHost";

        /// <summary>
        ///     The port to use for the proxy.
        /// </summary>
        public const string PROXY_PORT = "http.proxyPort";

        private bool secure = false;
        private Uri remoteURI;
        private string encoding = "UTF-8";
        private long heartbeatInterval;

        private string proxyHost;
        private int proxyPort = -1;
        private string proxyPrincipal = null;
        private GuardedString proxyPassword = null;


        public Uri RemoteUri
        {
            set
            {
                Assertions.NullCheck(value, "remoteURI");

                if ("https".Equals(value.Scheme, StringComparison.CurrentCultureIgnoreCase) ||
                    "wss".Equals(value.Scheme, StringComparison.CurrentCultureIgnoreCase))
                {
                    int port = value.Port > 0 ? value.Port : 443;
                    secure = true;

                    remoteURI = new UriBuilder(value)
                    {
                        Scheme = "wss",
                        Port = port
                    }.Uri;
                }
                else if ("http".Equals(value.Scheme, StringComparison.CurrentCultureIgnoreCase) ||
                         "ws".Equals(value.Scheme, StringComparison.CurrentCultureIgnoreCase))
                {
                    int port = value.Port > 0 ? value.Port : 80;
                    remoteURI = new UriBuilder(value)
                    {
                        Scheme = "ws",
                        Port = port
                    }.Uri;
                    secure = false;
                }
                else
                {
                    throw new ArgumentException("Unsupported protocol:" + value);
                }
            }
            get { return remoteURI; }
        }

        public virtual string Principal { get; set; }

        public virtual GuardedString Password { get; set; }

        public virtual bool Secure
        {
            get { return secure; }
        }

        /// <summary>
        ///     Returns the heartbeat interval (in seconds) to use for the connection. A value
        ///     of zero means default 60 seconds timeout.
        /// </summary>
        /// <returns> the heartbeat interval (in seconds) to use for the connection. </returns>
        public virtual long HeartbeatInterval
        {
            get { return heartbeatInterval; }
        }
    }

    #endregion

    #region sd

    public class WebSocketWrapper : WebSocketConnectionHolder
    {
        private const int ReceiveChunkSize = 4096;
        private const int SendChunkSize = 4096;

        private readonly ClientWebSocket _ws;
        private readonly TaskCompletionSource<WebSocketConnectionHolder> _connectPromise;
        private readonly RemoteWSFrameworkConnectionInfo _info;
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private readonly CancellationToken _cancellationToken;

        private readonly IMessageListener<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
            _messageListener;

        private readonly Func<WebSocketConnectionHolder, HandshakeMessage, RemoteOperationContext> _handshaker;

        protected WebSocketWrapper(RemoteWSFrameworkConnectionInfo info,
            IMessageListener<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                messageListener,
            Func<WebSocketConnectionHolder, HandshakeMessage, RemoteOperationContext> handshaker)
        {
            _messageListener = messageListener;
            _handshaker = handshaker;
            _ws = new ClientWebSocket();
            _connectPromise = new TaskCompletionSource<WebSocketConnectionHolder>();
            _ws.Options.KeepAliveInterval = TimeSpan.FromSeconds(20);
            _ws.Options.AddSubProtocol(RemoteWSFrameworkConnectionInfo.OPENICF_PROTOCOL);
            string enc =
                Convert.ToBase64String(
                    Encoding.GetEncoding("iso-8859-1")
                        .GetBytes(info.Principal + ":" +
                                  IdentityConnectors.Common.Security.SecurityUtil.Decrypt(info.Password)));
            _ws.Options.SetRequestHeader("Authorization", "Basic " + enc);
            _info = info;
            _cancellationToken = _cancellationTokenSource.Token;
        }

        /// <summary>
        ///     Creates a new instance.
        /// </summary>
        /// <param name="uri">The URI of the WebSocket server.</param>
        /// <param name="messageListener"></param>
        /// <param name="handshaker"></param>
        /// <returns></returns>
        public static WebSocketWrapper Create(RemoteWSFrameworkConnectionInfo info,
            IMessageListener<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                messageListener,
            Func<WebSocketConnectionHolder, HandshakeMessage, RemoteOperationContext> handshaker)
        {
            return new WebSocketWrapper(info, messageListener, handshaker);
        }


        private async void SendMessageAsync(byte[] message)
        {
            if (_ws.State != WebSocketState.Open)
            {
                throw new Exception("Connection is not open.");
            }

            await WriteMessageAsync(message, WebSocketMessageType.Binary);
        }

        private async void SendMessageAsync(string message)
        {
            if (_ws.State != WebSocketState.Open)
            {
                throw new Exception("Connection is not open.");
            }

            var messageBuffer = Encoding.UTF8.GetBytes(message);
            await WriteMessageAsync(messageBuffer, WebSocketMessageType.Text);
        }

        private static Int32 count = 0;

        public async Task<WebSocketConnectionHolder> ConnectAsync()
        {
            Trace.TraceInformation("Client make attempt to connect {0}", count++);
            _ws.ConnectAsync(_info.RemoteUri, _cancellationToken).ContinueWith(task =>
            {
                if (task.IsCompleted)
                {
                    StartListen();
                    RunInTask(WriteMessageAsync);
                    RunInTask(() => _messageListener.OnConnect(this));
                }
                else
                {
                    _connectPromise.SetException(task.Exception ?? new Exception("Failed to Connect Remote Server"));
                }
            }, CancellationToken.None).ConfigureAwait(false);
            return await _connectPromise.Task;
        }

        private async void StartListen()
        {
            var buffer = new byte[ReceiveChunkSize];
            try
            {
                while (_ws.State == WebSocketState.Open)
                {
                    StringBuilder stringResult = null;
                    MemoryStream binaryResult = null;

                    WebSocketReceiveResult result;
                    do
                    {
                        result = await _ws.ReceiveAsync(new ArraySegment<byte>(buffer), _cancellationToken);

                        if (result.MessageType == WebSocketMessageType.Close)
                        {
                            await
                                _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, string.Empty, CancellationToken.None);
                            RunInTask(() => _messageListener.OnClose(this, 1000, null));
                        }
                        else if (result.MessageType == WebSocketMessageType.Binary)
                        {
                            if (null == binaryResult)
                            {
                                binaryResult = new MemoryStream();
                            }
                            binaryResult.Write(buffer, 0, result.Count);
                        }
                        else
                        {
                            var str = Encoding.UTF8.GetString(buffer, 0, result.Count);
                            if (null == stringResult)
                            {
                                stringResult = new StringBuilder();
                            }
                            stringResult.Append(str);
                        }
                    } while (!result.EndOfMessage);

                    if (result.MessageType == WebSocketMessageType.Binary)
                    {
                        if (binaryResult != null)
                            RunInTask(() => _messageListener.OnMessage(this, binaryResult.ToArray()));
                    }
                    else
                    {
                        if (stringResult != null)
                            RunInTask(() => _messageListener.OnMessage(this, stringResult.ToString()));
                    }
                }
            }
            catch (Exception e)
            {
                TraceUtil.TraceException("Client processing exception", e);
                RunInTask(() => _messageListener.OnError(e));
            }
            finally
            {
                _ws.Dispose();
            }
        }

        private static void RunInTask(Action action)
        {
            Task.Factory.StartNew(action);
        }

        protected override async Task WriteMessageAsync(byte[] entry, WebSocketMessageType messageType)
        {
            var messageBuffer = entry;
            var messagesCount = (int) Math.Ceiling((double) messageBuffer.Length/SendChunkSize);

            for (var i = 0; i < messagesCount; i++)
            {
                var offset = (SendChunkSize*i);
                var count = SendChunkSize;
                var lastMessage = ((i + 1) == messagesCount);

                if ((count*(i + 1)) > messageBuffer.Length)
                {
                    count = messageBuffer.Length - offset;
                }

                await
                    _ws.SendAsync(new ArraySegment<byte>(messageBuffer, offset, count), messageType,
                        lastMessage, _cancellationToken);
            }
        }

        private RemoteOperationContext _context;

        protected override void Handshake(HandshakeMessage message)
        {
            _context = _handshaker(this, message);

            if (null != _context)
            {
                _connectPromise.TrySetResult(this);
            }
            else
            {
                _connectPromise.TrySetException(new ConnectorException(
                    "Failed Application HandShake"));
                TryClose();
            }
        }

        protected override void TryClose()
        {
        }

        public override bool Operational
        {
            get { return _ws.State == WebSocketState.Open; }
        }

        public override RemoteOperationContext RemoteConnectionContext
        {
            get { return _context; }
        }
    }

    #endregion
}