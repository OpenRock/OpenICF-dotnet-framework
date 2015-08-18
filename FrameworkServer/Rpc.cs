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
using System.Net.WebSockets;
using System.Security.Principal;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Org.ForgeRock.OpenICF.Common.ProtoBuf;
using Org.ForgeRock.OpenICF.Common.RPC;
using Org.IdentityConnectors.Common;
using Org.IdentityConnectors.Common.Security;
using Org.IdentityConnectors.Framework.Api;
using Org.IdentityConnectors.Framework.Common;
using Org.IdentityConnectors.Framework.Common.Exceptions;
using Org.IdentityConnectors.Framework.Impl.Api;

namespace Org.ForgeRock.OpenICF.Framework.Remote
{

    #region LocalOperationProcessor

    public abstract class LocalOperationProcessor<TV> :
        LocalRequest<TV, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
    {
        protected bool StickToConnection = false;

        protected WebSocketConnectionHolder ReverseConnection;

        protected internal LocalOperationProcessor(long requestId, WebSocketConnectionHolder socket)
            : base(requestId, socket)
        {
        }

        protected abstract RPCResponse CreateOperationResponse(RemoteOperationContext remoteContext,
            TV result);

        protected override bool TryHandleResult(TV result)
        {
            try
            {
                byte[] responseMessage =
                    new RemoteMessage
                    {
                        MessageId = RequestId,
                        Response = CreateOperationResponse(RemoteConnectionContext, result)
                    }.ToByteArray();

                return null != TrySendBytes(responseMessage);
            }
            catch (ConnectorIOException e)
            {
                // May not be complete / failed to send response
                TraceUtil.TraceException("Operation complete successfully but failed to send result", e);
            }
            catch (Exception e)
            {
                TraceUtil.TraceException("Operation complete successfully but failed to build result message", e);
            }
            return false;
        }

        protected override bool TryHandleError(Exception error)
        {
            byte[] responseMessage = MessagesUtil.CreateErrorResponse(RequestId, error).ToByteArray();
            try
            {
                return null != TrySendBytes(responseMessage, true).Result;
            }
            catch (ConnectorIOException e)
            {
                TraceUtil.TraceException("Operation complete unsuccessfully and failed to send error", e);
            }
            catch (Exception e)
            {
                TraceUtil.TraceException("Operation complete unsuccessfully and failed to build result message", e);
            }
            return false;
        }

        protected WebSocketConnectionHolder TrySendBytes(byte[] responseMessage)
        {
            return TrySendBytes(responseMessage, false).Result;
        }

        protected async Task<WebSocketConnectionHolder> TrySendBytes(byte[] responseMessage, bool useAnyConnection)
        {
            if (StickToConnection && !useAnyConnection)
            {
                if (null != ReverseConnection)
                {
                    try
                    {
                        await ReverseConnection.SendBytesAsync(responseMessage, CancellationToken.None);
                    }
                    catch (AggregateException e)
                    {
                        throw new ConnectorIOException(e.Message, e.InnerException);
                    }
                }
                else
                {
                    lock (this)
                    {
                        if (null == ReverseConnection)
                        {
                            ReverseConnection = TrySendMessageNow(responseMessage);
                            if (null == ReverseConnection)
                            {
                                throw new ConnectorIOException("Transport layer is not operational");
                            }
                        }
                        else
                        {
                            TrySendBytes(responseMessage);
                        }
                    }
                }
            }
            else
            {
                return TrySendMessageNow(responseMessage);
            }
            return ReverseConnection;
        }

        private WebSocketConnectionHolder TrySendMessageNow(byte[] responseMessage)
        {
            return
                RemoteConnectionContext.RemoteConnectionGroup.TrySendMessage(
                    connection =>
                        connection(new WebSocketConnectionHolder.InternalAsyncMessageQueueRecord(responseMessage))
                            .Result);
        }

        protected override bool TryCancel()
        {
            return false;
        }
    }

    #endregion

    #region RemoteOperationContext

    public class RemoteOperationContext :
        IRemoteConnectionContext<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
    {
        private readonly WebSocketConnectionGroup _connectionGroup;

        private readonly IPrincipal _connectionPrincipal;

        protected internal RemoteOperationContext(IPrincipal connectionPrincipal,
            WebSocketConnectionGroup connectionGroup)
        {
            _connectionPrincipal = connectionPrincipal;
            _connectionGroup = connectionGroup;
        }

        public virtual WebSocketConnectionGroup RemoteConnectionGroup
        {
            get { return _connectionGroup; }
        }

        public virtual IPrincipal RemotePrincipal
        {
            get { return _connectionPrincipal; }
        }
    }

    #endregion

    #region RemoteOperationRequest

    public abstract class RemoteOperationRequest<TV> :
        RemoteRequest<TV, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
    {
        protected RemoteOperationRequest(RemoteOperationContext context, long requestId,
            Action
                <
                    RemoteRequest
                        <TV, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>>
                completionCallback, CancellationToken cancellationToken)
            : base(context, requestId, completionCallback, cancellationToken)
        {
        }

        protected internal abstract bool HandleResponseMessage(WebSocketConnectionHolder sourceConnection,
            Object message);

        protected internal abstract RPCRequest CreateOperationRequest(RemoteOperationContext remoteContext);

        public override void HandleIncomingMessage(WebSocketConnectionHolder sourceConnection, Object message)
        {
            var exceptionMessage = message as ExceptionMessage;
            if (exceptionMessage != null)
            {
                HandleExceptionMessage(exceptionMessage);
            }
            else
            {
                if (message != null)
                {
                    if (!HandleResponseMessage(sourceConnection, message))
                    {
#if DEBUG
                        Debug.WriteLine("Request {0} has unknown response message type:{1}", RequestId, this.GetType().Name);
#endif
                        HandleError(new ConnectorException("Unknown response message type:" + message.GetType()));
                    }
                }
            }
        }

        protected override
            RemoteConnectionGroup<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>.
                AsyncMessageQueueRecord CreateMessageElement(RemoteOperationContext remoteContext, long requestId)
        {
            return new WebSocketConnectionHolder.InternalAsyncMessageQueueRecord(new RemoteMessage
            {
                MessageId = requestId,
                Request = CreateOperationRequest(remoteContext)
            }.ToByteArray());
        }

        protected override void TryCancelRemote(RemoteOperationContext remoteContext, long requestId)
        {
            byte[] cancelMessage =
                new RemoteMessage
                {
                    MessageId = requestId,
                    Request = new RPCRequest
                    {
                        CancelOpRequest = new CancelOpRequest()
                    }
                }.ToByteArray();

            TrySendBytes(cancelMessage);
        }

        protected override Exception CreateCancellationException(Exception cancellationException)
        {
            var canceledException = cancellationException as OperationCanceledException;
            if (canceledException != null)
            {
                return cancellationException;
            }
            if (null != cancellationException)
            {
                OperationCanceledException exception = new OperationCanceledException(cancellationException.Message,
                    cancellationException);
                return exception;
            }

            return new OperationCanceledException("Operation is cancelled #" + RequestId);
        }

        protected internal virtual void TrySendBytes(byte[] cancelMessage)
        {
            if (null == ConnectionContext.RemoteConnectionGroup.TrySendMessage(async connection =>
            {
                await connection(new WebSocketConnectionHolder.InternalAsyncMessageQueueRecord(cancelMessage));
                return true;
            }))
            {
                // Failed to send remote message
                throw new ConnectorIOException("Transport layer is not operational");
            }
        }

        protected internal virtual void HandleExceptionMessage(ExceptionMessage exceptionMessage)
        {
            try
            {
                HandleError(MessagesUtil.FromExceptionMessage(exceptionMessage));
            }
            catch (Exception e)
            {
                Trace.TraceInformation("Exception received but failed to handle it: {0}:{1}", RequestId, e.Message);
            }
        }
    }

    #endregion

    #region WebSocketConnectionGroup

    public class WebSocketConnectionGroup :
        RemoteConnectionGroup<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>,
        IAsyncConnectorInfoManager
    {
        private readonly Encryptor _encryptor = null;

        private RemoteOperationContext _operationContext;

        private readonly HashSet<string> _principals = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentDictionary<String, IConfigurationPropertyChangeListener>
            _configurationChangeListeners =
                new ConcurrentDictionary<String, IConfigurationPropertyChangeListener>();

        public delegate void WebSocketConnectionGroupEventListenerOnDispose(WebSocketConnectionHolder disposeable);

        private readonly EventHandler _closeListener = (sender, args) =>
        {
            WebSocketConnectionHolder connection = sender as WebSocketConnectionHolder;
            if (null != connection)
            {
                foreach (var p in connection.RemoteConnectionContext.RemoteConnectionGroup.WebSockets)
                {
                    if (connection.Equals(p.Key))
                    {
                        String ignore;
                        connection.RemoteConnectionContext.RemoteConnectionGroup.WebSockets.TryRemove(p.Key, out ignore);
                    }
                }
            }
        };

        private readonly RemoteConnectorInfoManager _delegate;

        public WebSocketConnectionGroup(string remoteSessionId)
            : base(remoteSessionId)
        {
            _delegate = new RemoteConnectorInfoManager(this);
        }

        public virtual RemoteOperationContext Handshake(IPrincipal connectionPrincipal,
            WebSocketConnectionHolder webSocketConnection, HandshakeMessage message)
        {
            if (null == _operationContext)
            {
                lock (this)
                {
                    if (null == _operationContext)
                    {
                        _operationContext = new RemoteOperationContext(connectionPrincipal, this);
                    }
                }
            }
            if (RemoteSessionId.Equals(message.SessionId))
            {
                WebSockets.TryAdd(webSocketConnection, connectionPrincipal.Identity.Name);
                webSocketConnection.Disposed += _closeListener;
                if (null != connectionPrincipal.Identity.Name)
                    _principals.Add(connectionPrincipal.Identity.Name);
                //This is not thread-safe, it could yield true for cuncurrent threads 
                if (webSocketConnection.Equals(WebSockets.Keys.First()))
                {
                    ControlMessageRequestFactory requestFactory = new ControlMessageRequestFactory();
                    requestFactory.InfoLevels.Add(ControlRequest.Types.InfoLevel.CONNECTOR_INFO);
                    TrySubmitRequest(requestFactory);
                }
            }
            return _operationContext;
        }

        public virtual void PrincipalIsShuttingDown(IPrincipal connectionPrincipal)
        {
            string name = connectionPrincipal.Identity.Name;
            if (_principals.Remove(name))
            {
                if (!_principals.Any())
                {
                    // Gracefully close all request and shut down this group.
                    foreach (
                        var local in
                            LocalRequests.Select(entry => entry.Value)
                                .OfType
                                <IDisposable>())
                    {
                        local.Dispose();
                    }
                    foreach (
                        var remote in
                            RemoteRequests.Select(entry => entry.Value)
                                .OfType
                                <IDisposable>())
                    {
                        remote.Dispose();
                    }
                    _delegate.Dispose();
                }
                foreach (var entry in WebSockets)
                {
                    if (name.Equals(entry.Value, StringComparison.CurrentCultureIgnoreCase))
                    {
                        String ignore;
                        WebSockets.TryRemove(entry.Key, out ignore);
                    }
                }
            }
        }

        public bool TrySendMessage(IMessage message)
        {
            byte[] messageBytes = message.ToByteArray();
            return true.Equals(TrySendMessage(connection =>
            {
                connection(new WebSocketConnectionHolder.InternalAsyncMessageQueueRecord(messageBytes)).Wait();
                return true;
            }));
        }


        protected override RemoteOperationContext RemoteConnectionContext
        {
            get { return _operationContext; }
        }

        public override bool Operational
        {
            get { return WebSockets.Keys.Any(e => e.Operational); }
        }

        public virtual Encryptor Encryptor
        {
            get { return _encryptor; }
        }

        // --- AsyncConnectorInfoManager implementation ---

        public virtual Task<ConnectorInfo> FindConnectorInfoAsync(Org.IdentityConnectors.Framework.Api.ConnectorKey key)
        {
            return _delegate.FindConnectorInfoAsync(key);
        }

        public virtual Task<ConnectorInfo> FindConnectorInfoAsync(ConnectorKeyRange keyRange)
        {
            return _delegate.FindConnectorInfoAsync(keyRange);
        }

        public virtual IList<ConnectorInfo> ConnectorInfos
        {
            get { return _delegate.ConnectorInfos; }
        }

        public virtual ConnectorInfo FindConnectorInfo(Org.IdentityConnectors.Framework.Api.ConnectorKey key)
        {
            return _delegate.FindConnectorInfo(key);
        }

        // --- AsyncConnectorInfoManager implementation ---

        public void AddConfigurationChangeListener(String key, IConfigurationPropertyChangeListener listener)
        {
            if (!String.IsNullOrEmpty(key))
            {
                if (null != listener)
                {
                    _configurationChangeListeners.TryAdd(key, listener);
                }
                else
                {
                    IConfigurationPropertyChangeListener ignore;
                    _configurationChangeListeners.TryRemove(key, out ignore);
                }
            }
        }

        public void NotifyConfigurationChangeListener(string key, IList<ConfigurationProperty> change)
        {
            if (null != key && null != change)
            {
                IConfigurationPropertyChangeListener listener;
                _configurationChangeListeners.TryGetValue(key, out listener);
                if (null != listener)
                {
                    try
                    {
                        listener.ConfigurationPropertyChange(change);
                    }
                    catch (Exception e)
                    {
#if DEBUG
                        StringBuilder builder = new StringBuilder("Failed to notify connfiguration change - ");
                        TraceUtil.ExceptionToString(builder, e, String.Empty);
                        Debug.WriteLine(builder.ToString());
#endif
                    }
                }
            }
        }

        // -- Static Classes

        private sealed class RemoteConnectorInfoManager :
            ManagedAsyncConnectorInfoManager<RemoteConnectorInfoImpl, RemoteConnectorInfoManager>
        {
            private readonly
                IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                _outerInstance;

            public RemoteConnectorInfoManager(
                IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                    remoteConnection)
            {
                _outerInstance = remoteConnection;
            }

            internal void AddOtherConnectorInfo<T1>(T1 connectorInfo) where T1 : AbstractConnectorInfo
            {
                AddConnectorInfo(new RemoteConnectorInfoImpl(_outerInstance, connectorInfo));
            }

            internal void AddAll<T1>(ICollection<T1> connectorInfos) where T1 : AbstractConnectorInfo
            {
                foreach (var connectorInfo in connectorInfos)
                {
                    AddConnectorInfo(new RemoteConnectorInfoImpl(_outerInstance, connectorInfo));
                }
            }
        }

        private sealed class ControlMessageRequestFactory :
            IRemoteRequestFactory
                <ControlMessageRequest, Boolean, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                    RemoteOperationContext>
        {
            public readonly List<ControlRequest.Types.InfoLevel> InfoLevels = new List<ControlRequest.Types.InfoLevel>();

            public ControlMessageRequest CreateRemoteRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <bool, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback)
            {
                return new ControlMessageRequest(context, requestId, completionCallback, InfoLevels);
            }
        }

        private class ControlMessageRequest : RemoteOperationRequest<Boolean>
        {
            private readonly List<ControlRequest.Types.InfoLevel> _infoLevels;

            public ControlMessageRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <Boolean, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback,
                List<ControlRequest.Types.InfoLevel> infoLevels)
                : base(context, requestId, completionCallback, CancellationToken.None)
            {
                _infoLevels = infoLevels;
            }

            protected internal override RPCRequest CreateOperationRequest(RemoteOperationContext remoteContext)
            {
                ControlRequest builder = new ControlRequest();
                foreach (ControlRequest.Types.InfoLevel infoLevel in _infoLevels)
                {
                    builder.InfoLevel.Add(infoLevel);
                }
                return new RPCRequest
                {
                    ControlRequest = builder
                };
            }

            protected internal override bool HandleResponseMessage(WebSocketConnectionHolder sourceConnection,
                Object message)
            {
                var controlResponse = message as ControlResponse;
                if (controlResponse != null)
                {
                    var response = MessagesUtil.DeserializeLegacy<List<Object>>(controlResponse.ConnectorInfos);
                    if (null != response && response.Any())
                    {
                        foreach (var o in response)
                        {
                            var connectorInfo = o as AbstractConnectorInfo;
                            if (null != connectorInfo)
                            {
                                sourceConnection.RemoteConnectionContext.RemoteConnectionGroup._delegate
                                    .AddOtherConnectorInfo(connectorInfo);
                            }
                        }
                    }
                    HandleResult(true);
                }
                else
                {
                    return false;
                }
                return true;
            }
        }
    }

    #endregion

    #region WebSocketConnectionHolder

    public abstract class WebSocketConnectionHolder :
        IRemoteConnectionHolder<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
    {
#if DEBUG
        private static Int32 _counter;
        private Int32 _id;

        public Int32 Id
        {
            get
            {
                if (_id == 0)
                {
                    _id = Interlocked.Increment(ref _counter);
                }
                return _id;
            }
        }
#endif

        private readonly
            ConcurrentDictionary
                <
                    RemoteConnectionGroup<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>.
                        AsyncMessageQueueRecord, Boolean> _messageQueue =
                            new ConcurrentDictionary
                                <
                                    RemoteConnectionGroup
                                        <WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>.
                                        AsyncMessageQueueRecord, Boolean>();

        private readonly AutoResetEvent _onMessageToSendEvent = new AutoResetEvent(true);

        /// <devdoc>
        ///     <para>Adds a event handler to listen to the Disposed event on the WebSocketConnectionHolder.</para>
        /// </devdoc>
        private event EventHandler DisposedEvent;

        public event EventHandler Disposed
        {
            add
            {
                // check if this is still running
                if (Operational)
                {
                    // add close listener
                    DisposedEvent += value;
                    // check the its state again
                    if (DisposedEvent != null && (!Operational && DisposedEvent.GetInvocationList().Contains(value)))
                    {
                        // if this was closed during the method call - notify the
                        // listener
                        try
                        {
                            value(this, EventArgs.Empty);
                        }
                        catch (Exception)
                        {
                            // ignored
                        }
                    }
                } // if this is closed - notify the listener
                else
                {
                    try
                    {
                        value(this, EventArgs.Empty);
                    }
                    catch (Exception)
                    {
                        // ignored
                    }
                }
            }
            remove { DisposedEvent -= value; }
        }

        protected abstract Task WriteMessageAsync(byte[] entry, WebSocketMessageType messageType);

        protected async void WriteMessageAsync()
        {
            while (Operational)
            {
                do
                {
                    foreach (var entry in _messageQueue)
                    {
                        if (await entry.Key.AcquireAndTryComplete(this))
                        {
                            bool ignore;
                            _messageQueue.TryRemove(entry.Key, out ignore);
#if DEBUG
                            Debug.WriteLine("Dequeue from {2} Message:{1}, Pending:{0}", _messageQueue.Count,
                                entry.Key.Id, Id);
#endif
                        }
                    }
                } while (_messageQueue.Any());

                _onMessageToSendEvent.WaitOne(TimeSpan.FromMinutes(1));
            }
#if DEBUG
            Debug.WriteLine("Finish Writting messages over Socket:{0}", Id);
#endif
            foreach (var asyncQueueRecord in _messageQueue.Keys)
            {
                asyncQueueRecord.Detach(this);
            }
        }

        public virtual bool ReceiveHandshake(HandshakeMessage message)
        {
            if (null == RemoteConnectionContext)
            {
                Handshake(message);
#if DEBUG
                Debug.WriteLine("New Connection accepted {0}:{1}", GetType().FullName, Id);
#endif
            }
            return HandHooked;
        }

        public virtual bool HandHooked
        {
            get { return null != RemoteConnectionContext; }
        }

        public void Dispose()
        {
            TryClose();
            _onMessageToSendEvent.Set();
            OnDisposed();
        }

        protected void OnDisposed()
        {
            try
            {
                var handler = DisposedEvent;
                if (handler != null) handler(this, EventArgs.Empty);
            }
            catch (Exception e)
            {
#if DEBUG
                StringBuilder builder = new StringBuilder("DisposedEvent failed - ");
                TraceUtil.ExceptionToString(builder, e, String.Empty);
                Debug.WriteLine(builder.ToString());
#endif
            }
        }

        protected abstract void Handshake(HandshakeMessage message);

        protected abstract void TryClose();

        public abstract bool Operational { get; }

        public abstract RemoteOperationContext RemoteConnectionContext { get; }

        public void Enqueue(
            RemoteConnectionGroup<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>.
                AsyncMessageQueueRecord record)
        {
            if (Operational && record.Accept(this))
            {
                if (_messageQueue.TryAdd(record, true) && !Operational)
                {
                    bool ignore;
                    _messageQueue.TryRemove(record, out ignore);
                    record.Detach(this);
                }
                else
                {
#if DEBUG
                    Debug.WriteLine("Enqueue Socket:{0} Message:{1}", Id, record.Id);
#endif
                    _onMessageToSendEvent.Set();
                }
            }
        }


        public class InternalAsyncMessageQueueRecord :
            RemoteConnectionGroup<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>.
                AsyncMessageQueueRecord
        {
            private readonly WebSocketMessageType _messageType;
            private readonly byte[] _message;

            public override string ToString()
            {
#if DEBUG
                return String.Format("{1} Message - Id:[{2}] size:{0}", _message.Length, _messageType, Id);
#else
                return String.Format("{1} Message - size:{0}", _message.Length, _messageType);
#endif
            }

            public InternalAsyncMessageQueueRecord(byte[] message)
            {
                _messageType = WebSocketMessageType.Binary;
                _message = message;
            }

            public InternalAsyncMessageQueueRecord(string message)
            {
                _messageType = WebSocketMessageType.Text;
                _message = Encoding.UTF8.GetBytes(message);
            }

            protected override Task DoSend(WebSocketConnectionHolder connection)
            {
#if DEBUG
                Debug.WriteLine("WebSocket: {0} writes {1} bytes of message: {2}", connection.Id, _message.Length, Id);
#endif
                return connection.WriteMessageAsync(_message, _messageType);
            }
        }


        public Task SendBytesAsync(byte[] data, CancellationToken cancellationToken)
        {
            var record = new InternalAsyncMessageQueueRecord(data);
            Enqueue(record);
            return record.SendAsync(TimeSpan.FromMinutes(1));
        }

        public Task SendStringAsync(string data, CancellationToken cancellationToken)
        {
            var record = new InternalAsyncMessageQueueRecord(data);
            Enqueue(record);
            return record.SendAsync(TimeSpan.FromMinutes(1));
        }

        public void SendPing(byte[] applicationData)
        {
            throw new NotImplementedException();
        }

        public void SendPong(byte[] applicationData)
        {
            throw new NotImplementedException();
        }
    }

    #endregion
}