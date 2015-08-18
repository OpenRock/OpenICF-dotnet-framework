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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Org.ForgeRock.OpenICF.Common.ProtoBuf;
using Org.ForgeRock.OpenICF.Common.RPC;
using Org.IdentityConnectors.Common;
using Org.IdentityConnectors.Framework.Api;
using Org.IdentityConnectors.Framework.Api.Operations;
using Org.IdentityConnectors.Framework.Common.Exceptions;
using Org.IdentityConnectors.Framework.Common.Objects.Filters;
using OBJ = Org.IdentityConnectors.Framework.Common.Objects;

namespace Org.ForgeRock.OpenICF.Framework.Remote
{

    #region ConnectorEventSubscriptionApiOpImpl

    public class ConnectorEventSubscriptionApiOpImpl : AbstractAPIOperation, IConnectorEventSubscriptionApiOp
    {
        public ConnectorEventSubscriptionApiOpImpl(
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                remoteConnection, Org.IdentityConnectors.Framework.Api.ConnectorKey connectorKey,
            Func<RemoteOperationContext, ByteString> facadeKeyFunction)
            : base(remoteConnection, connectorKey, facadeKeyFunction)
        {
        }

        public virtual OBJ.ISubscription Subscribe(OBJ.ObjectClass objectClass, Filter eventFilter,
            IObserver<Org.IdentityConnectors.Framework.Common.Objects.ConnectorObject> handler,
            OBJ.OperationOptions operationOptions)
        {
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            Task<Object> promise =
                TrySubscribe(objectClass, eventFilter, handler, operationOptions, CancellationToken.None);

            promise.ContinueWith((task, state) =>
            {
                var observer = state as IObserver<Org.IdentityConnectors.Framework.Common.Objects.ConnectorObject>;
                if (task.IsFaulted)
                {
                    if (null != observer)
                    {
                        if (task.Exception != null) observer.OnError(task.Exception);
                    }
                }
                else if (task.IsCanceled)
                {
                    //Ignore
                }
                else
                {
                    if (null != observer)
                    {
                        observer.OnCompleted();
                    }
                }
            }, handler, cancellationTokenSource.Token);

            return new SubscriptionImpl(promise, cancellationTokenSource);
        }


        public virtual Task<Object> TrySubscribe(OBJ.ObjectClass objectClass, Filter eventFilter,
            IObserver<Org.IdentityConnectors.Framework.Common.Objects.ConnectorObject> handler,
            OBJ.OperationOptions options,
            CancellationToken cancellationToken)
        {
            Assertions.NullCheck(objectClass, "objectClass");
            Assertions.NullCheck(handler, "handler");
            ConnectorEventSubscriptionOpRequest requestBuilder =
                new ConnectorEventSubscriptionOpRequest {ObjectClass = objectClass.GetObjectClassValue()};
            if (eventFilter != null)
            {
                requestBuilder.EventFilter = MessagesUtil.SerializeLegacy(eventFilter);
            }

            if (options != null && options.Options.Any())
            {
                requestBuilder.Options = MessagesUtil.SerializeLegacy(options);
            }

            return
                SubmitRequest<Object, ConnectorEventSubscriptionOpResponse, InternalRequest>(
                    new InternalRequestFactory(ConnectorKey, FacadeKeyFunction,
                        new OperationRequest
                        {
                            ConnectorEventSubscriptionOpRequest = requestBuilder
                        }, handler,
                        cancellationToken));
        }

        private class InternalRequestFactory : AbstractRemoteOperationRequestFactory<Object, InternalRequest>
        {
            private readonly OperationRequest _operationRequest;
            private readonly IObserver<Org.IdentityConnectors.Framework.Common.Objects.ConnectorObject> _handler;

            public InternalRequestFactory(Org.IdentityConnectors.Framework.Api.ConnectorKey connectorKey,
                Func<RemoteOperationContext, ByteString> facadeKeyFunction, OperationRequest operationRequest,
                IObserver<Org.IdentityConnectors.Framework.Common.Objects.ConnectorObject> handler,
                CancellationToken cancellationToken)
                : base(connectorKey, facadeKeyFunction, cancellationToken)
            {
                _operationRequest = operationRequest;
                _handler = handler;
            }

            public override InternalRequest CreateRemoteRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <object, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback)
            {
                RPCRequest builder = CreateRpcRequest(context);
                if (null != builder)
                {
                    return new InternalRequest(context, requestId, completionCallback, builder, _handler,
                        CancellationToken);
                }
                    return null;
                
            }

            protected internal override OperationRequest CreateOperationRequest(
                RemoteOperationContext remoteContext)
            {
                return _operationRequest;
            }
        }

        private class InternalRequest :
            AbstractRemoteOperationRequestFactory<Object, InternalRequest>.AbstractRemoteOperationRequest
                <Object, ConnectorEventSubscriptionOpResponse>
        {
            private readonly IObserver<Org.IdentityConnectors.Framework.Common.Objects.ConnectorObject> _handler;
            private Int32 _confirmed = 0;

            public InternalRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <object, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback, RPCRequest requestBuilder,
                IObserver<Org.IdentityConnectors.Framework.Common.Objects.ConnectorObject> handler,
                CancellationToken cancellationToken)
                : base(context, requestId, completionCallback, requestBuilder, cancellationToken)
            {
                _handler = handler;
            }

            protected internal override ConnectorEventSubscriptionOpResponse GetOperationResponseMessages(
                OperationResponse message)
            {
                if (null != message.ConnectorEventSubscriptionOpResponse)
                {
                    return message.ConnectorEventSubscriptionOpResponse;
                }
                    Trace.TraceInformation(OperationExpectsMessage, RequestId, "ConnectorEventSubscriptionOpResponse");
                    return null;
                
            }

            protected internal override void HandleOperationResponseMessages(WebSocketConnectionHolder sourceConnection,
                ConnectorEventSubscriptionOpResponse message)
            {
                if (null != _handler && null != message.ConnectorObject)
                {
                    Org.IdentityConnectors.Framework.Common.Objects.ConnectorObject delta =
                        MessagesUtil.DeserializeMessage<Org.IdentityConnectors.Framework.Common.Objects.ConnectorObject>
                            (message.ConnectorObject);
                    try
                    {
                        _handler.OnNext(delta);
                    }
                    catch (Exception)
                    {
                        if (!Promise.IsCompleted)
                        {
                            HandleError(new ConnectorException("ResultsHandler stopped processing results"));
                            TryCancelRemote(ConnectionContext, RequestId);
                        }
                    }
                }
                else if (message.Completed && message.Completed)
                {
                    HandleResult(null);
                    Trace.TraceInformation("Subscription is completed");
                }
                else if (Interlocked.CompareExchange(ref _confirmed, 0, 1) == 0)
                {
                    Trace.TraceInformation("Subscription has been made successfully on remote side");
                }
            }
        }

        // ----

        public static
            AbstractLocalOperationProcessor<ConnectorEventSubscriptionOpResponse, ConnectorEventSubscriptionOpRequest>
            CreateProcessor(long requestId, WebSocketConnectionHolder socket,
                ConnectorEventSubscriptionOpRequest message)
        {
            return new InternalLocalOperationProcessor(requestId, socket, message);
        }

        private class InternalLocalOperationProcessor :
            AbstractLocalOperationProcessor<ConnectorEventSubscriptionOpResponse, ConnectorEventSubscriptionOpRequest>,
            IObserver<Org.IdentityConnectors.Framework.Common.Objects.ConnectorObject>
        {
            private OBJ.ISubscription _subscription;

            protected internal InternalLocalOperationProcessor(long requestId, WebSocketConnectionHolder socket,
                ConnectorEventSubscriptionOpRequest message)
                : base(requestId, socket, message)
            {
                StickToConnection = false;
            }

            protected override RPCResponse CreateOperationResponse(RemoteOperationContext remoteContext,
                ConnectorEventSubscriptionOpResponse result)
            {
                return
                    new RPCResponse
                    {
                        OperationResponse = new OperationResponse
                        {
                            ConnectorEventSubscriptionOpResponse = result
                        }
                    };
            }

            public override void Execute(ConnectorFacade connectorFacade)
            {
                try
                {
                    TryHandleResult(ExecuteOperation(connectorFacade, _requestMessage));
                }
                catch (Exception error)
                {
                    HandleError(error);
                }
            }

            protected internal override ConnectorEventSubscriptionOpResponse ExecuteOperation(
                ConnectorFacade connectorFacade, ConnectorEventSubscriptionOpRequest requestMessage)
            {
                OBJ.ObjectClass objectClass = new OBJ.ObjectClass(requestMessage.ObjectClass);

                Filter token = null;
                if (null != requestMessage.EventFilter)
                {
                    token = MessagesUtil.DeserializeLegacy<Filter>(requestMessage.EventFilter);
                }

                OBJ.OperationOptions operationOptions = null;
                if (null != requestMessage.Options)
                {
                    operationOptions = MessagesUtil.DeserializeLegacy<OBJ.OperationOptions>(requestMessage.Options);
                }

                _subscription = connectorFacade.Subscribe(objectClass, token, this, operationOptions);

                return new ConnectorEventSubscriptionOpResponse();
            }


            protected override bool TryCancel()
            {
                _subscription.Dispose();
                return base.TryCancel();
            }

            public void OnNext(Org.IdentityConnectors.Framework.Common.Objects.ConnectorObject value)
            {
                if (null != value)
                {
                    TryHandleResult(new
                        ConnectorEventSubscriptionOpResponse
                    {
                        ConnectorObject = MessagesUtil.SerializeMessage<Common.ProtoBuf.ConnectorObject>(value)
                    });
                }
            }

            public void OnError(Exception error)
            {
                try
                {
                    byte[] responseMessage = MessagesUtil.CreateErrorResponse(RequestId, error).ToByteArray();
                    TrySendBytes(responseMessage, true).ConfigureAwait(false);
                }
                catch (Exception t)
                {
                    Trace.TraceInformation
                        ("Operation encountered an exception and failed to send the exception response {0}", t.Message);
                }
            }

            public void OnCompleted()
            {
                TryHandleResult(new ConnectorEventSubscriptionOpResponse {Completed = true});
            }
        }
    }

    #endregion

    #region SyncEventSubscriptionApiOpImpl

    public class SyncEventSubscriptionApiOpImpl : AbstractAPIOperation, ISyncEventSubscriptionApiOp
    {
        public SyncEventSubscriptionApiOpImpl(
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                remoteConnection, Org.IdentityConnectors.Framework.Api.ConnectorKey connectorKey,
            Func<RemoteOperationContext, ByteString> facadeKeyFunction)
            : base(remoteConnection, connectorKey, facadeKeyFunction)
        {
        }

        public virtual OBJ.ISubscription Subscribe(OBJ.ObjectClass objectClass, OBJ.SyncToken eventFilter,
            IObserver<OBJ.SyncDelta> handler, OBJ.OperationOptions operationOptions)
        {
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            Task<Object> promise =
                TrySubscribe(objectClass, eventFilter, handler, operationOptions);

            promise.ContinueWith((task, state) =>
            {
                var observer = state as IObserver<Org.IdentityConnectors.Framework.Common.Objects.ConnectorObject>;
                if (task.IsFaulted)
                {
                    if (null != observer)
                    {
                        if (task.Exception != null) observer.OnError(task.Exception);
                    }
                }
                else if (task.IsCanceled)
                {
                    //Ignore
                }
                else
                {
                    if (null != observer)
                    {
                        observer.OnCompleted();
                    }
                }
            }, handler, cancellationTokenSource.Token);

            return new SubscriptionImpl(promise, cancellationTokenSource);
        }


        public virtual Task<Object> TrySubscribe(OBJ.ObjectClass objectClass, OBJ.SyncToken eventFilter,
            IObserver<OBJ.SyncDelta> handler, OBJ.OperationOptions options)
        {
            Assertions.NullCheck(objectClass, "objectClass");
            Assertions.NullCheck(handler, "handler");
            SyncEventSubscriptionOpRequest requestBuilder = new
                SyncEventSubscriptionOpRequest {ObjectClass = objectClass.GetObjectClassValue()};
            if (eventFilter != null)
            {
                requestBuilder.Token =
                    MessagesUtil.SerializeMessage<Org.ForgeRock.OpenICF.Common.ProtoBuf.SyncToken>(eventFilter);
            }

            if (options != null && options.Options.Any())
            {
                requestBuilder.Options = MessagesUtil.SerializeLegacy(options);
            }

            return
                SubmitRequest<Object, SyncEventSubscriptionOpResponse, InternalRequest>(
                    new InternalRequestFactory(ConnectorKey, FacadeKeyFunction,
                        new OperationRequest {SyncEventSubscriptionOpRequest = requestBuilder}, handler,
                        CancellationToken.None));
        }

        private class InternalRequestFactory : AbstractRemoteOperationRequestFactory<Object, InternalRequest>
        {
            private readonly OperationRequest _operationRequest;
            private readonly IObserver<OBJ.SyncDelta> _handler;

            public InternalRequestFactory(Org.IdentityConnectors.Framework.Api.ConnectorKey connectorKey,
                Func<RemoteOperationContext, ByteString> facadeKeyFunction, OperationRequest operationRequest,
                IObserver<OBJ.SyncDelta> handler, CancellationToken cancellationToken)
                : base(connectorKey, facadeKeyFunction, cancellationToken)
            {
                _operationRequest = operationRequest;
                _handler = handler;
            }

            public override InternalRequest CreateRemoteRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <object, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback)
            {
                RPCRequest builder = CreateRpcRequest(context);
                if (null != builder)
                {
                    return new InternalRequest(context, requestId, completionCallback, builder, _handler,
                        CancellationToken);
                }
                else
                {
                    return null;
                }
            }

            protected internal override OperationRequest CreateOperationRequest(
                RemoteOperationContext remoteContext)
            {
                return _operationRequest;
            }
        }

        private class InternalRequest :
            AbstractRemoteOperationRequestFactory<Object, InternalRequest>.AbstractRemoteOperationRequest
                <Object, SyncEventSubscriptionOpResponse>
        {
            private readonly IObserver<OBJ.SyncDelta> _handler;
            private Int32 _confirmed = 0;

            public InternalRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <object, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback, RPCRequest requestBuilder,
                IObserver<OBJ.SyncDelta> handler, CancellationToken cancellationToken)
                : base(context, requestId, completionCallback, requestBuilder, cancellationToken)
            {
                _handler = handler;
            }

            protected internal override SyncEventSubscriptionOpResponse GetOperationResponseMessages(
                OperationResponse message)
            {
                if (null != message.SyncEventSubscriptionOpResponse)
                {
                    return message.SyncEventSubscriptionOpResponse;
                }
                else
                {
                    Trace.TraceInformation(OperationExpectsMessage, RequestId, "HasSyncEventSubscriptionOpResponse");
                    return null;
                }
            }

            protected internal override void HandleOperationResponseMessages(WebSocketConnectionHolder sourceConnection,
                SyncEventSubscriptionOpResponse message)
            {
                if (null != _handler && null != message.SyncDelta)
                {
                    OBJ.SyncDelta delta = MessagesUtil.DeserializeMessage<OBJ.SyncDelta>(message.SyncDelta);
                    try
                    {
                        _handler.OnNext(delta);
                    }
                    catch (Exception)
                    {
                        if (!Promise.IsCompleted)
                        {
                            HandleError(new ConnectorException("ResultsHandler stopped processing results"));
                            TryCancelRemote(ConnectionContext, RequestId);
                        }
                    }
                }
                else if (message.Completed && message.Completed)
                {
                    HandleResult(null);
                    Trace.TraceInformation("Subscription is completed");
                }
                else if (Interlocked.CompareExchange(ref _confirmed, 0, 1) == 0)
                {
                    Trace.TraceInformation("Subscription has been made successfully on remote side");
                }
            }
        }

        // ----

        public static AbstractLocalOperationProcessor<SyncEventSubscriptionOpResponse, SyncEventSubscriptionOpRequest>
            CreateProcessor(long requestId, WebSocketConnectionHolder socket, SyncEventSubscriptionOpRequest message)
        {
            return new InternalLocalOperationProcessor(requestId, socket, message);
        }

        private class InternalLocalOperationProcessor :
            AbstractLocalOperationProcessor<SyncEventSubscriptionOpResponse, SyncEventSubscriptionOpRequest>,
            IObserver<OBJ.SyncDelta>
        {
            private OBJ.ISubscription _subscription;

            protected internal InternalLocalOperationProcessor(long requestId, WebSocketConnectionHolder socket,
                SyncEventSubscriptionOpRequest message)
                : base(requestId, socket, message)
            {
                StickToConnection = false;
            }

            protected override RPCResponse CreateOperationResponse(RemoteOperationContext remoteContext,
                SyncEventSubscriptionOpResponse result)
            {
                return new
                    RPCResponse
                {
                    OperationResponse = new OperationResponse
                    {
                        SyncEventSubscriptionOpResponse = result
                    }
                };
            }

            public override void Execute(ConnectorFacade connectorFacade)
            {
                try
                {
                    TryHandleResult(ExecuteOperation(connectorFacade, _requestMessage));
                }
                catch (Exception error)
                {
                    HandleError(error);
                }
            }

            protected internal override SyncEventSubscriptionOpResponse ExecuteOperation(
                ConnectorFacade connectorFacade, SyncEventSubscriptionOpRequest requestMessage)
            {
                OBJ.ObjectClass objectClass = new OBJ.ObjectClass(requestMessage.ObjectClass);

                OBJ.SyncToken token = null;
                if (null != requestMessage.Token)
                {
                    token = MessagesUtil.DeserializeMessage<OBJ.SyncToken>(requestMessage.Token);
                }

                OBJ.OperationOptions operationOptions = null;
                if (null != requestMessage.Options)
                {
                    operationOptions = MessagesUtil.DeserializeLegacy<OBJ.OperationOptions>(requestMessage.Options);
                }

                _subscription = connectorFacade.Subscribe(objectClass, token, this, operationOptions);

                return new SyncEventSubscriptionOpResponse();
            }


            protected override bool TryCancel()
            {
                _subscription.Dispose();
                return base.TryCancel();
            }

            public void OnNext(OBJ.SyncDelta value)
            {
                if (null != value)
                {
                    TryHandleResult(new
                        SyncEventSubscriptionOpResponse
                    {
                        SyncDelta = MessagesUtil.SerializeMessage<Common.ProtoBuf.SyncDelta>(value)
                    });
                }
            }

            public void OnError(Exception error)
            {
                try
                {
                    byte[] responseMessage = MessagesUtil.CreateErrorResponse(RequestId, error).ToByteArray();
                    TrySendBytes(responseMessage, true).ConfigureAwait(false);
                }
                catch (Exception t)
                {
                    Trace.TraceInformation
                        ("Operation encountered an exception and failed to send the exception response {0}", t.Message);
                }
            }

            public void OnCompleted()
            {
                TryHandleResult(new SyncEventSubscriptionOpResponse {Completed = true});
            }
        }
    }

    #endregion

    internal class SubscriptionImpl : OBJ.ISubscription
    {
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly Task _childTask;

        public SubscriptionImpl(Task childTask, CancellationTokenSource cancellationTokenSource)
        {
            _childTask = childTask;
            _cancellationTokenSource = cancellationTokenSource;
        }

        public void Dispose()
        {
            if (!_cancellationTokenSource.IsCancellationRequested)
            {
                _cancellationTokenSource.Cancel();
                _cancellationTokenSource.Dispose();
            }
        }

        public bool Unsubscribed
        {
            get { return _childTask.IsCompleted; }
        }
    }
}