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
using Google.Protobuf;
using Org.ForgeRock.OpenICF.Common.RPC;
using Org.IdentityConnectors.Common;
using Org.IdentityConnectors.Common.Security;
using Org.IdentityConnectors.Framework.Api.Operations;
using Org.IdentityConnectors.Framework.Common.Exceptions;
using Org.IdentityConnectors.Framework.Common.Objects;
using Org.IdentityConnectors.Framework.Common.Objects.Filters;
using Org.IdentityConnectors.Framework.Impl.Api.Local.Operations;
using API = Org.IdentityConnectors.Framework.Api;
using PRB = Org.ForgeRock.OpenICF.Common.ProtoBuf;

namespace Org.ForgeRock.OpenICF.Framework.Remote
{

    #region AbstractAPIOperation

    public abstract class AbstractAPIOperation
    {
        public static readonly ConnectionFailedException FailedExceptionMessage =
            new ConnectionFailedException("No remote Connector Server is available at this moment");

        protected AbstractAPIOperation(
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                remoteConnection, API.ConnectorKey connectorKey,
            Func<RemoteOperationContext, ByteString> facadeKeyFunction, long timeout)
        {
            RemoteConnection = Assertions.NullChecked(remoteConnection, "remoteConnection");
            ConnectorKey = Assertions.NullChecked(connectorKey, "connectorKey");
            FacadeKeyFunction = Assertions.NullChecked(facadeKeyFunction, "facadeKeyFunction");
            Timeout = Math.Max(timeout, API.APIConstants.NO_TIMEOUT);
        }

        public API.ConnectorKey ConnectorKey { get; internal set; }

        public long Timeout { get; internal set; }

        protected internal
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
            RemoteConnection { get; internal set; }

        protected internal Func<RemoteOperationContext, ByteString> FacadeKeyFunction { get; internal set; }


        protected internal virtual Task<TV> SubmitRequest<TV, TM, TR>(
            AbstractRemoteOperationRequestFactory<TV, TR> requestFactory)
            where TR : AbstractRemoteOperationRequestFactory<TV, TR>.AbstractRemoteOperationRequest<TV, TM>
        {
            TR request = RemoteConnection.TrySubmitRequest(requestFactory);
            if (null != request)
            {
                return request.Promise;
            }
            TaskCompletionSource<TV> result = new TaskCompletionSource<TV>();
            result.SetException(FailedExceptionMessage);
            return result.Task;
        }
    }

    internal abstract class ResultBuffer<T, TR>
    {
        internal class Pair
        {
            public T Item { get; set; }
            public TR Result { get; set; }
        }

        private readonly int _timeoutMillis;
        private volatile Boolean _stopped = false;
        private readonly BlockingCollection<Pair> _queue = new BlockingCollection<Pair>(new ConcurrentQueue<Pair>());

        private readonly ConcurrentDictionary<long, Pair> _buffer = new ConcurrentDictionary<long, Pair>();

        private Int64 _nextPermit = 1;

        private long _lastSequenceNumber = -1;

        protected ResultBuffer(long timeoutMillis)
        {

            if (timeoutMillis == API.APIConstants.NO_TIMEOUT)
            {
                _timeoutMillis = int.MaxValue;
            }
            else if (timeoutMillis == 0)
            {
                _timeoutMillis = 60 * 1000;
            }
            else
            {
                _timeoutMillis = unchecked((int)timeoutMillis);
            }
        }

        public virtual bool Stopped
        {
            get
            {
                return _stopped;
            }
        }

        public virtual bool HasLast()
        {
            return _lastSequenceNumber > 0;
        }

        public virtual bool HasAll()
        {
            return HasLast() && _nextPermit > _lastSequenceNumber;
        }

        public virtual int Remaining
        {
            get
            {
                return _queue.Count;
            }
        }

        public virtual void Clear()
        {
            _stopped = true;
            _buffer.Clear();
            Pair item;
            while (_queue.TryTake(out item))
            {
                // do nothing
            }
        }

        public virtual void ReceiveNext(long sequence, T result)
        {
            if (null != result)
            {
                if (_nextPermit == sequence)
                {
                    Enqueue(new Pair { Item = result });
                }
                else
                {
                    _buffer[sequence] = new Pair { Item = result };
                }
            }
            // Feed the queue
            Pair o = null;
            while ((_buffer.TryRemove(_nextPermit, out o)))
            {
                Enqueue(o);
            }
        }

        public virtual void ReceiveLast(long resultCount, TR result)
        {
            if (0 == resultCount && null != result)
            {
                // Empty result set
                Enqueue(new Pair { Result = result });
                _lastSequenceNumber = 0;
            }
            else
            {
                long idx = resultCount + 1;
                _buffer[idx] = new Pair { Result = result };
                _lastSequenceNumber = idx;
            }

            if (_lastSequenceNumber == _nextPermit)
            {
                // Operation finished
                Enqueue(new Pair { Result = result });
            }
            else
            {
                ReceiveNext(_nextPermit, default(T));
            }
        }

        protected internal virtual void Enqueue(Pair result)
        {
            // Block if queue is full
            if (_queue.TryAdd(result))
            {
                // Let the next go through
                _nextPermit++;
            }
            else
            {
                // What to do?
                Trace.TraceInformation("Failed to Enqueue: ");
            }
        }

        protected internal abstract bool Handle(object result);


        public void Process()
        {
            while (!_stopped)
            {

                Pair obj;

                if (!_queue.TryTake(out obj, _timeoutMillis))
                {
                    // we timed out
                    ReceiveNext(-1L, default(T));
                    if (_queue.Any())
                    {
                        Clear();
                        throw new OperationTimeoutException();
                    }
                }
                else
                {
                    try
                    {
                        Boolean keepGoing = Handle(obj);
                        if (!keepGoing)
                        {
                            // stop and wait
                            Clear();
                        }
                    }
                    catch (Exception t)
                    {
                        Clear();
                        throw new ConnectorException(t.Message, t);
                    }
                }
            }
        }
    }

    #endregion

    #region AbstractLocalOperationProcessor

    public abstract class AbstractLocalOperationProcessor<TV, TM> : LocalOperationProcessor<TV>
    {
        protected readonly TM _requestMessage;

        protected internal AbstractLocalOperationProcessor(long requestId, WebSocketConnectionHolder socket, TM message)
            : base(requestId, socket)
        {
            _requestMessage = message;
        }

        protected internal abstract TV ExecuteOperation(API.ConnectorFacade connectorFacade, TM requestMessage);

        public virtual void Execute(API.ConnectorFacade connectorFacade)
        {
            try
            {
                HandleResult(ExecuteOperation(connectorFacade, _requestMessage));
            }
            catch (Exception error)
            {
                HandleError(error);
            }
        }
    }

    #endregion

    #region AbstractRemoteOperationRequestFactory

    public abstract class AbstractRemoteOperationRequestFactory<TV, TR> :
        IRemoteRequestFactory
            <TR, TV, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
        where TR : RemoteOperationRequest<TV>
    {
        private readonly IdentityConnectors.Framework.Api.ConnectorKey _connectorKey;

        private readonly Func<RemoteOperationContext, ByteString> _facadeKeyFunction;

        protected AbstractRemoteOperationRequestFactory(API.ConnectorKey connectorKey,
            Func<RemoteOperationContext, ByteString> facadeKeyFunction, CancellationToken cancellationToken)
        {
            _connectorKey = connectorKey;
            _facadeKeyFunction = facadeKeyFunction;
            CancellationToken = cancellationToken;
        }

        protected CancellationToken CancellationToken { get; private set; }

        protected Common.ProtoBuf.ConnectorKey CreateConnectorKey()
        {
            return new
                Common.ProtoBuf.ConnectorKey
            {
                BundleName = _connectorKey.BundleName,
                BundleVersion = _connectorKey.BundleVersion,
                ConnectorName = _connectorKey.ConnectorName
            };
        }

        protected internal ByteString CreateConnectorFacadeKey(RemoteOperationContext context)
        {
            return _facadeKeyFunction(context);
        }

        protected internal abstract PRB.OperationRequest CreateOperationRequest(RemoteOperationContext remoteContext);

        protected internal virtual PRB.RPCRequest CreateRpcRequest(RemoteOperationContext context)
        {
            ByteString facadeKey = CreateConnectorFacadeKey(context);
            if (null != facadeKey)
            {
                PRB.OperationRequest operationBuilder = CreateOperationRequest(context);
                operationBuilder.ConnectorKey = CreateConnectorKey();
                operationBuilder.ConnectorFacadeKey = facadeKey;
                operationBuilder.Locale =
                    MessagesUtil.SerializeMessage<PRB.Locale>(Thread.CurrentThread.CurrentUICulture);
                return new PRB.RPCRequest { OperationRequest = operationBuilder };
            }
            return null;
        }

        public abstract class AbstractRemoteOperationRequest<TT, TM> : RemoteOperationRequest<TT>
        {
            public const string OperationExpectsMessage = "RemoteOperation[{0}] expects {1}";

            internal readonly PRB.RPCRequest Request;

            protected AbstractRemoteOperationRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <TT, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                        > completionCallback, PRB.RPCRequest requestBuilder, CancellationToken cancellationToken)
                : base(context, requestId, completionCallback, cancellationToken)
            {
                Request = requestBuilder;
            }

            protected internal abstract TM GetOperationResponseMessages(PRB.OperationResponse message);

            protected internal abstract void HandleOperationResponseMessages(WebSocketConnectionHolder sourceConnection,
                TM message);

            protected internal override PRB.RPCRequest CreateOperationRequest(RemoteOperationContext remoteContext)
            {
                return Request;
            }

            protected internal override bool HandleResponseMessage(WebSocketConnectionHolder sourceConnection,
                Object message)
            {
                var response = message as PRB.OperationResponse;
                if (response != null)
                {
                    TM responseMessage = GetOperationResponseMessages(response);
                    if (null != responseMessage)
                    {
                        try
                        {
                            HandleOperationResponseMessages(sourceConnection, responseMessage);
                        }
                        catch (Exception e)
                        {
                            Debug.WriteLine("Failed to handle the result of operation {0}", e);
                            HandleError(e);
                        }
                        return true;
                    }
                }
                return false;
            }
        }

        public abstract TR CreateRemoteRequest(RemoteOperationContext context, long requestId,
            Action
                <
                    RemoteRequest
                        <TV, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>>
                completionCallback);
    }

    #endregion

    #region AuthenticationAsyncApiOpImpl

    public class AuthenticationAsyncApiOpImpl : AbstractAPIOperation, IAuthenticationAsyncApiOp
    {
        public AuthenticationAsyncApiOpImpl(
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                remoteConnection, IdentityConnectors.Framework.Api.ConnectorKey connectorKey,
            Func<RemoteOperationContext, ByteString> facadeKeyFunction, long timeout)
            : base(remoteConnection, connectorKey, facadeKeyFunction, timeout)
        {
        }

        public virtual Uid Authenticate(ObjectClass objectClass, string username, GuardedString password,
            OperationOptions options)
        {
            try
            {
                return AuthenticateAsync(objectClass, username, password, options, CancellationToken.None).Result;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        public async Task<Uid> AuthenticateAsync(ObjectClass objectClass, string username, GuardedString password,
            OperationOptions options, CancellationToken cancellationToken)
        {
            Assertions.NullCheck(objectClass, "objectClass");
            if (ObjectClass.ALL.Equals(objectClass))
            {
                throw new NotSupportedException("Operation is not allowed on __ALL__ object class");
            }
            Assertions.NullCheck(username, "username");
            Assertions.NullCheck(password, "password");

            PRB.AuthenticateOpRequest requestBuilder =
                new PRB.AuthenticateOpRequest
                {
                    ObjectClass = objectClass.GetObjectClassValue(),
                    Username = username,
                    Password = MessagesUtil.SerializeLegacy(password)
                };

            if (options != null)
            {
                requestBuilder.Options = MessagesUtil.SerializeLegacy(options);
            }
            return
                await
                    SubmitRequest<Uid, PRB.AuthenticateOpResponse, InternalRequest>(
                        new InternalRequestFactory(ConnectorKey,
                            FacadeKeyFunction, new PRB.OperationRequest { AuthenticateOpRequest = requestBuilder },
                            cancellationToken));
        }

        private class InternalRequestFactory : AbstractRemoteOperationRequestFactory<Uid, InternalRequest>
        {
            private readonly PRB.OperationRequest _operationRequest;

            public InternalRequestFactory(Org.IdentityConnectors.Framework.Api.ConnectorKey connectorKey,
                Func<RemoteOperationContext, ByteString> facadeKeyFunction,
                PRB.OperationRequest operationRequest, CancellationToken cancellationToken)
                : base(connectorKey, facadeKeyFunction, cancellationToken)
            {
                _operationRequest = operationRequest;
            }

            public override InternalRequest CreateRemoteRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <Uid, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext
                                >> completionCallback)
            {
                PRB.RPCRequest builder = CreateRpcRequest(context);
                if (null != builder)
                {
                    return new InternalRequest(context, requestId, completionCallback, builder, CancellationToken);
                }
                return null;
            }

            protected internal override PRB.OperationRequest CreateOperationRequest(
                RemoteOperationContext remoteContext)
            {
                return _operationRequest;
            }
        }

        private class InternalRequest :
            AbstractRemoteOperationRequestFactory<Uid, InternalRequest>.AbstractRemoteOperationRequest
                <Uid, PRB.AuthenticateOpResponse>
        {
            public InternalRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <Uid, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext
                                >> completionCallback, PRB.RPCRequest requestBuilder,
                CancellationToken cancellationToken)
                : base(context, requestId, completionCallback, requestBuilder, cancellationToken)
            {
            }

            protected internal override PRB.AuthenticateOpResponse GetOperationResponseMessages(
                PRB.OperationResponse message)
            {
                if (null != message.AuthenticateOpResponse)
                {
                    return message.AuthenticateOpResponse;
                }
                else
                {
                    Trace.TraceInformation(OperationExpectsMessage, RequestId, "AuthenticateOpResponse");
                    return null;
                }
            }

            protected internal override void HandleOperationResponseMessages(WebSocketConnectionHolder sourceConnection,
                PRB.AuthenticateOpResponse message)
            {
                HandleResult(null != message.Uid
                    ? MessagesUtil.DeserializeMessage<Uid>(message.Uid)
                    : null);
            }
        }

        // ----

        public static AbstractLocalOperationProcessor<Uid, PRB.AuthenticateOpRequest> CreateProcessor(long requestId,
            WebSocketConnectionHolder socket, PRB.AuthenticateOpRequest message)
        {
            return new InternalLocalOperationProcessor(requestId, socket, message);
        }

        private class InternalLocalOperationProcessor : AbstractLocalOperationProcessor<Uid, PRB.AuthenticateOpRequest>
        {
            protected internal InternalLocalOperationProcessor(long requestId, WebSocketConnectionHolder socket,
                PRB.AuthenticateOpRequest message)
                : base(requestId, socket, message)
            {
            }

            protected override PRB.RPCResponse CreateOperationResponse(
                RemoteOperationContext remoteContext, Uid result)
            {
                PRB.AuthenticateOpResponse response = new PRB.AuthenticateOpResponse();
                if (null != result)
                {
                    response.Uid = MessagesUtil.SerializeMessage<Common.ProtoBuf.Uid>(result);
                }
                return new
                    PRB.RPCResponse
                {
                    OperationResponse = new PRB.OperationResponse
                    {
                        AuthenticateOpResponse = response
                    }
                };
            }

            protected internal override Uid ExecuteOperation(API.ConnectorFacade connectorFacade,
                PRB.AuthenticateOpRequest requestMessage)
            {
                ObjectClass objectClass = new ObjectClass(requestMessage.ObjectClass);
                OperationOptions operationOptions = null;
                if (null != requestMessage.Options)
                {
                    operationOptions = MessagesUtil.DeserializeLegacy<OperationOptions>(requestMessage.Options);
                }
                return connectorFacade.Authenticate(objectClass, requestMessage.Username,
                    MessagesUtil.DeserializeLegacy<GuardedString>(requestMessage.Password), operationOptions);
            }
        }
    }

    #endregion

    #region CreateAsyncApiOpImpl

    public class CreateAsyncApiOpImpl : AbstractAPIOperation, ICreateAsyncApiOp
    {
        public CreateAsyncApiOpImpl(
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                remoteConnection, API.ConnectorKey connectorKey,
            Func<RemoteOperationContext, ByteString> facadeKeyFunction, long timeout)
            : base(remoteConnection, connectorKey, facadeKeyFunction, timeout)
        {
        }

        public virtual Uid Create(ObjectClass objectClass, ICollection<ConnectorAttribute> createAttributes,
            OperationOptions options)
        {
            try
            {
                return CreateAsync(objectClass, createAttributes, options, CancellationToken.None).Result;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        public Task<Uid> CreateAsync(ObjectClass objectClass, ICollection<ConnectorAttribute> createAttributes,
            OperationOptions options, CancellationToken cancellationToken)
        {
            Assertions.NullCheck(objectClass, "objectClass");
            if (ObjectClass.ALL.Equals(objectClass))
            {
                throw new NotSupportedException("Operation is not allowed on __ALL__ object class");
            }
            Assertions.NullCheck(createAttributes, "createAttributes");
            // check to make sure there's not a uid..
            if (ConnectorAttributeUtil.GetUidAttribute(createAttributes) != null)
            {
                throw new InvalidAttributeValueException("Parameter 'createAttributes' contains a uid.");
            }

            PRB.CreateOpRequest requestBuilder = new
                PRB.CreateOpRequest
            {
                ObjectClass = objectClass.GetObjectClassValue(),
                CreateAttributes = MessagesUtil.SerializeLegacy(createAttributes)
            };


            if (options != null)
            {
                requestBuilder.Options = MessagesUtil.SerializeLegacy(options);
            }

            return
                SubmitRequest<Uid, PRB.CreateOpResponse, InternalCreateRequest>(new InternalRequestFactory(ConnectorKey,
                    FacadeKeyFunction, new PRB.OperationRequest { CreateOpRequest = requestBuilder },
                    cancellationToken));
        }

        private class InternalRequestFactory : AbstractRemoteOperationRequestFactory<Uid, InternalCreateRequest>
        {
            private readonly PRB.OperationRequest _operationRequest;

            public InternalRequestFactory(IdentityConnectors.Framework.Api.ConnectorKey connectorKey,
                Func<RemoteOperationContext, ByteString> facadeKeyFunction, PRB.OperationRequest operationRequest,
                CancellationToken cancellationToken)
                : base(connectorKey, facadeKeyFunction, cancellationToken)
            {
                _operationRequest = operationRequest;
            }

            public override InternalCreateRequest CreateRemoteRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <Uid, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext
                                >> completionCallback)
            {
                PRB.RPCRequest builder = CreateRpcRequest(context);
                if (null != builder)
                {
                    return new InternalCreateRequest(context, requestId, completionCallback, builder, CancellationToken);
                }
                return null;
            }

            protected internal override PRB.OperationRequest CreateOperationRequest(
                RemoteOperationContext remoteContext)
            {
                return _operationRequest;
            }
        }

        internal class InternalCreateRequest :
            AbstractRemoteOperationRequestFactory<Uid, InternalCreateRequest>.AbstractRemoteOperationRequest
                <Uid, PRB.CreateOpResponse>
        {
            public InternalCreateRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <Uid, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext
                                >> completionCallback, PRB.RPCRequest requestBuilder,
                CancellationToken cancellationToken)
                : base(context, requestId, completionCallback, requestBuilder, cancellationToken)
            {
            }

            protected internal override PRB.CreateOpResponse GetOperationResponseMessages(PRB.OperationResponse message)
            {
                if (null != message.CreateOpResponse)
                {
                    return message.CreateOpResponse;
                }
                Trace.TraceInformation(OperationExpectsMessage, RequestId, "CreateOpResponse");
                return null;
            }

            protected internal override void HandleOperationResponseMessages(WebSocketConnectionHolder sourceConnection,
                PRB.CreateOpResponse message)
            {
                HandleResult(null != message.Uid ? MessagesUtil.DeserializeMessage<Uid>(message.Uid) : null);
            }
        }

        // ----

        public static AbstractLocalOperationProcessor<Uid, PRB.CreateOpRequest> CreateProcessor(long requestId,
            WebSocketConnectionHolder socket, PRB.CreateOpRequest message)
        {
            return new InternalCreateLocalOperationProcessor(requestId, socket, message);
        }

        private class InternalCreateLocalOperationProcessor : AbstractLocalOperationProcessor<Uid, PRB.CreateOpRequest>
        {
            protected internal InternalCreateLocalOperationProcessor(long requestId, WebSocketConnectionHolder socket,
                PRB.CreateOpRequest message)
                : base(requestId, socket, message)
            {
            }

            protected override PRB.RPCResponse CreateOperationResponse(
                RemoteOperationContext remoteContext, Uid result)
            {
                PRB.CreateOpResponse response = new PRB.CreateOpResponse();
                if (null != result)
                {
                    response.Uid = MessagesUtil.SerializeMessage<PRB.Uid>(result);
                }

                return new
                    PRB.RPCResponse
                {
                    OperationResponse = new PRB.OperationResponse
                    {
                        CreateOpResponse = response
                    }
                };
            }

            protected internal override Uid ExecuteOperation(API.ConnectorFacade connectorFacade,
                PRB.CreateOpRequest requestMessage)
            {
                ObjectClass objectClass = new ObjectClass(requestMessage.ObjectClass);
                var attributes = MessagesUtil.DeserializeLegacy<HashSet<Object>>(requestMessage.CreateAttributes);

                OperationOptions operationOptions = null;
                if (null != requestMessage.Options)
                {
                    operationOptions = MessagesUtil.DeserializeLegacy<OperationOptions>(requestMessage.Options);
                }
                return connectorFacade.Create(objectClass, CollectionUtil.NewSet<Object, ConnectorAttribute>(attributes),
                    operationOptions);
            }
        }
    }

    #endregion

    #region  DeleteAsyncApiOpImpl

    public class DeleteAsyncApiOpImpl : AbstractAPIOperation, IDeleteAsyncApiOp
    {
        public DeleteAsyncApiOpImpl(
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                remoteConnection, API.ConnectorKey connectorKey,
            Func<RemoteOperationContext, ByteString> facadeKeyFunction, long timeout)
            : base(remoteConnection, connectorKey, facadeKeyFunction, timeout)
        {
        }

        public virtual void Delete(ObjectClass objectClass, Uid uid, OperationOptions options)
        {
            try
            {
                DeleteAsync(objectClass, uid, options, CancellationToken.None).Wait();
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        public Task DeleteAsync(ObjectClass objectClass, Uid uid, OperationOptions options,
            CancellationToken cancellationToken)
        {
            Assertions.NullCheck(objectClass, "objectClass");
            if (ObjectClass.ALL.Equals(objectClass))
            {
                throw new NotSupportedException("Operation is not allowed on __ALL__ object class");
            }
            Assertions.NullCheck(uid, "uid");

            PRB.DeleteOpRequest requestBuilder =
                new PRB.DeleteOpRequest
                {
                    ObjectClass = objectClass.GetObjectClassValue(),
                    Uid = MessagesUtil.SerializeMessage<PRB.Uid>(uid)
                };


            if (options != null)
            {
                requestBuilder.Options = MessagesUtil.SerializeLegacy(options);
            }
            return SubmitRequest<Object, PRB.DeleteOpResponse, InternalDeleteRequest>(
                new InternalRequestFactory(ConnectorKey, FacadeKeyFunction, new
                    PRB.OperationRequest { DeleteOpRequest = requestBuilder }, cancellationToken));
        }

        private class InternalRequestFactory : AbstractRemoteOperationRequestFactory<Object, InternalDeleteRequest>
        {
            private readonly PRB.OperationRequest _operationRequest;

            public InternalRequestFactory(API.ConnectorKey connectorKey,
                Func<RemoteOperationContext, ByteString> facadeKeyFunction, PRB.OperationRequest operationRequest,
                CancellationToken cancellationToken)
                : base(connectorKey, facadeKeyFunction, cancellationToken)
            {
                _operationRequest = operationRequest;
            }

            public override InternalDeleteRequest CreateRemoteRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <object, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback)
            {
                PRB.RPCRequest builder = CreateRpcRequest(context);
                if (null != builder)
                {
                    return new InternalDeleteRequest(context, requestId, completionCallback, builder, CancellationToken);
                }
                return null;
            }

            protected internal override PRB.OperationRequest CreateOperationRequest(
                RemoteOperationContext remoteContext)
            {
                return _operationRequest;
            }
        }

        private class InternalDeleteRequest :
            AbstractRemoteOperationRequestFactory<Object, InternalDeleteRequest>.AbstractRemoteOperationRequest
                <Object, PRB.DeleteOpResponse>
        {
            public InternalDeleteRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <object, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback, PRB.RPCRequest requestBuilder,
                CancellationToken cancellationToken)
                : base(context, requestId, completionCallback, requestBuilder, cancellationToken)
            {
            }

            protected internal override PRB.DeleteOpResponse GetOperationResponseMessages(PRB.OperationResponse message)
            {
                if (null != message.DeleteOpResponse)
                {
                    return message.DeleteOpResponse;
                }
                Trace.TraceInformation(OperationExpectsMessage, RequestId, "DeleteOpResponse");
                return null;
            }

            protected internal override void HandleOperationResponseMessages(WebSocketConnectionHolder sourceConnection,
                PRB.DeleteOpResponse message)
            {
                HandleResult(null);
            }
        }

        // ----

        public static AbstractLocalOperationProcessor<Object, PRB.DeleteOpRequest> CreateProcessor(long requestId,
            WebSocketConnectionHolder socket, PRB.DeleteOpRequest message)
        {
            return new InternalLocalOperationProcessor(requestId, socket, message);
        }

        private class InternalLocalOperationProcessor : AbstractLocalOperationProcessor<Object, PRB.DeleteOpRequest>
        {
            protected internal InternalLocalOperationProcessor(long requestId, WebSocketConnectionHolder socket,
                PRB.DeleteOpRequest message)
                : base(requestId, socket, message)
            {
            }

            protected override PRB.RPCResponse CreateOperationResponse(
                RemoteOperationContext remoteContext, Object result)
            {
                return new
                    PRB.RPCResponse
                {
                    OperationResponse = new PRB.OperationResponse
                    {
                        DeleteOpResponse = new PRB.DeleteOpResponse()
                    }
                };
            }

            protected internal override Object ExecuteOperation(API.ConnectorFacade connectorFacade,
                PRB.DeleteOpRequest requestMessage)
            {
                ObjectClass objectClass = new ObjectClass(requestMessage.ObjectClass);
                Uid uid = MessagesUtil.DeserializeMessage<Uid>(requestMessage.Uid);

                OperationOptions operationOptions = null;
                if (null != requestMessage.Options)
                {
                    operationOptions = MessagesUtil.DeserializeLegacy<OperationOptions>(requestMessage.Options);
                }
                connectorFacade.Delete(objectClass, uid, operationOptions);
                return null;
            }
        }
    }

    #endregion

    #region  GetAsyncApiOpImpl

    public class GetAsyncApiOpImpl : AbstractAPIOperation, IGetAsyncApiOp
    {
        public GetAsyncApiOpImpl(
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                remoteConnection, API.ConnectorKey connectorKey,
            Func<RemoteOperationContext, ByteString> facadeKeyFunction, long timeout)
            : base(remoteConnection, connectorKey, facadeKeyFunction, timeout)
        {
        }

        public virtual ConnectorObject GetObject(ObjectClass objectClass, Uid uid, OperationOptions options)
        {
            try
            {
                return GetObjectAsync(objectClass, uid, options, CancellationToken.None).Result;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        public Task<ConnectorObject> GetObjectAsync(ObjectClass objectClass, Uid uid, OperationOptions options,
            CancellationToken cancellationToken)
        {
            Assertions.NullCheck(objectClass, "objectClass");
            if (ObjectClass.ALL.Equals(objectClass))
            {
                throw new NotSupportedException("Operation is not allowed on __ALL__ object class");
            }
            Assertions.NullCheck(uid, "uid");

            PRB.GetOpRequest requestBuilder =
                new PRB.GetOpRequest
                {
                    ObjectClass = objectClass.GetObjectClassValue(),
                    Uid = MessagesUtil.SerializeMessage<PRB.Uid>(uid)
                };


            if (options != null)
            {
                requestBuilder.Options = MessagesUtil.SerializeLegacy(options);
            }

            return
                SubmitRequest<ConnectorObject, PRB.GetOpResponse, InternalRequest>(
                    new InternalRequestFactory(ConnectorKey,
                        FacadeKeyFunction, new PRB.OperationRequest { GetOpRequest = requestBuilder },
                        cancellationToken));
        }

        private class InternalRequestFactory : AbstractRemoteOperationRequestFactory<ConnectorObject, InternalRequest>
        {
            private readonly PRB.OperationRequest _operationRequest;

            public InternalRequestFactory(API.ConnectorKey connectorKey,
                Func<RemoteOperationContext, ByteString> facadeKeyFunction, PRB.OperationRequest operationRequest,
                CancellationToken cancellationToken)
                : base(connectorKey, facadeKeyFunction, cancellationToken)
            {
                _operationRequest = operationRequest;
            }

            public override InternalRequest CreateRemoteRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <ConnectorObject, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback)
            {
                PRB.RPCRequest builder = CreateRpcRequest(context);
                if (null != builder)
                {
                    return new InternalRequest(context, requestId, completionCallback, builder, CancellationToken);
                }
                else
                {
                    return null;
                }
            }

            protected internal override PRB.OperationRequest CreateOperationRequest(
                RemoteOperationContext remoteContext)
            {
                return _operationRequest;
            }
        }

        private class InternalRequest :
            AbstractRemoteOperationRequestFactory<ConnectorObject, InternalRequest>.AbstractRemoteOperationRequest
                <ConnectorObject, PRB.GetOpResponse>
        {
            public InternalRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <ConnectorObject, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback, PRB.RPCRequest requestBuilder,
                CancellationToken cancellationToken)
                : base(context, requestId, completionCallback, requestBuilder, cancellationToken)
            {
            }

            protected internal override PRB.GetOpResponse GetOperationResponseMessages(PRB.OperationResponse message)
            {
                if (null != message.GetOpResponse)
                {
                    return message.GetOpResponse;
                }
                else
                {
                    Trace.TraceInformation(OperationExpectsMessage, RequestId, "GetOpResponse");
                    return null;
                }
            }

            protected internal override void HandleOperationResponseMessages(WebSocketConnectionHolder sourceConnection,
                PRB.GetOpResponse message)
            {
                HandleResult(null != message.ConnectorObject
                    ? MessagesUtil.DeserializeLegacy<ConnectorObject>(message.ConnectorObject)
                    : null);
            }
        }

        // -------

        public static AbstractLocalOperationProcessor<ConnectorObject, PRB.GetOpRequest> CreateProcessor(long requestId,
            WebSocketConnectionHolder socket, PRB.GetOpRequest message)
        {
            return new InternalLocalOperationProcessor(requestId, socket, message);
        }

        private class InternalLocalOperationProcessor :
            AbstractLocalOperationProcessor<ConnectorObject, PRB.GetOpRequest>
        {
            protected internal InternalLocalOperationProcessor(long requestId, WebSocketConnectionHolder socket,
                PRB.GetOpRequest message)
                : base(requestId, socket, message)
            {
            }

            protected override PRB.RPCResponse CreateOperationResponse(
                RemoteOperationContext remoteContext, ConnectorObject result)
            {
                PRB.GetOpResponse response = new PRB.GetOpResponse();
                if (null != result)
                {
                    response.ConnectorObject = MessagesUtil.SerializeLegacy(result);
                }

                return new
                    PRB.RPCResponse
                {
                    OperationResponse = new PRB.OperationResponse
                    {
                        GetOpResponse = response
                    }
                };
            }

            protected internal override ConnectorObject ExecuteOperation(API.ConnectorFacade connectorFacade,
                PRB.GetOpRequest requestMessage)
            {
                ObjectClass objectClass = new ObjectClass(requestMessage.ObjectClass);
                Uid uid = MessagesUtil.DeserializeMessage<Uid>(requestMessage.Uid);

                OperationOptions operationOptions = null;
                if (null != requestMessage.Options)
                {
                    operationOptions = MessagesUtil.DeserializeLegacy<OperationOptions>(requestMessage.Options);
                }
                return connectorFacade.GetObject(objectClass, uid, operationOptions);
            }
        }
    }

    #endregion

    #region  ResolveUsernameAsyncApiOpImpl

    public class ResolveUsernameAsyncApiOpImpl : AbstractAPIOperation, IResolveUsernameAsyncApiOp
    {
        public ResolveUsernameAsyncApiOpImpl(
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                remoteConnection, API.ConnectorKey connectorKey,
            Func<RemoteOperationContext, ByteString> facadeKeyFunction, long timeout)
            : base(remoteConnection, connectorKey, facadeKeyFunction, timeout)
        {
        }

        public virtual Uid ResolveUsername(ObjectClass objectClass, string username, OperationOptions options)
        {
            try
            {
                return ResolveUsernameAsync(objectClass, username, options, CancellationToken.None).Result;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        public async Task<Uid> ResolveUsernameAsync(ObjectClass objectClass, string username, OperationOptions options,
            CancellationToken cancellationToken)
        {
            Assertions.NullCheck(objectClass, "objectClass");
            if (ObjectClass.ALL.Equals(objectClass))
            {
                throw new NotSupportedException("Operation is not allowed on __ALL__ object class");
            }
            Assertions.NullCheck(username, "username");

            PRB.ResolveUsernameOpRequest requestBuilder =
                new PRB.ResolveUsernameOpRequest
                {
                    ObjectClass = objectClass.GetObjectClassValue(),
                    Username = username
                };


            if (options != null)
            {
                requestBuilder.Options = MessagesUtil.SerializeLegacy(options);
            }

            return await
                SubmitRequest<Uid, PRB.ResolveUsernameOpResponse, InternalRequest>(
                    new InternalRequestFactory(ConnectorKey,
                        FacadeKeyFunction, new PRB.OperationRequest { ResolveUsernameOpRequest = requestBuilder },
                        cancellationToken));
        }

        private class InternalRequestFactory : AbstractRemoteOperationRequestFactory<Uid, InternalRequest>
        {
            private readonly PRB.OperationRequest _operationRequest;

            public InternalRequestFactory(API.ConnectorKey connectorKey,
                Func<RemoteOperationContext, ByteString> facadeKeyFunction, PRB.OperationRequest operationRequest,
                CancellationToken cancellationToken)
                : base(connectorKey, facadeKeyFunction, cancellationToken)
            {
                _operationRequest = operationRequest;
            }

            public override InternalRequest CreateRemoteRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <Uid, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext
                                >> completionCallback)
            {
                PRB.RPCRequest builder = CreateRpcRequest(context);
                if (null != builder)
                {
                    return new InternalRequest(context, requestId, completionCallback, builder, CancellationToken);
                }
                else
                {
                    return null;
                }
            }

            protected internal override PRB.OperationRequest CreateOperationRequest(
                RemoteOperationContext remoteContext)
            {
                return _operationRequest;
            }
        }

        private class InternalRequest :
            AbstractRemoteOperationRequestFactory<Uid, InternalRequest>.AbstractRemoteOperationRequest
                <Uid, PRB.ResolveUsernameOpResponse>
        {
            public InternalRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <Uid, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext
                                >> completionCallback, PRB.RPCRequest requestBuilder,
                CancellationToken cancellationToken)
                : base(context, requestId, completionCallback, requestBuilder, cancellationToken)
            {
            }

            protected internal override PRB.ResolveUsernameOpResponse GetOperationResponseMessages(
                PRB.OperationResponse message)
            {
                if (null != message.ResolveUsernameOpResponse)
                {
                    return message.ResolveUsernameOpResponse;
                }
                else
                {
                    Trace.TraceInformation(OperationExpectsMessage, RequestId, "ResolveUsernameOpResponse");
                    return null;
                }
            }

            protected internal override void HandleOperationResponseMessages(WebSocketConnectionHolder sourceConnection,
                PRB.ResolveUsernameOpResponse message)
            {
                if (null != message.Uid)
                {
                    HandleResult(MessagesUtil.DeserializeMessage<Uid>(message.Uid));
                }
                else
                {
                    HandleResult(null);
                }
            }
        }

        // ----

        public static AbstractLocalOperationProcessor<Uid, PRB.ResolveUsernameOpRequest> CreateProcessor(long requestId,
            WebSocketConnectionHolder socket, PRB.ResolveUsernameOpRequest message)
        {
            return new InternalLocalOperationProcessor(requestId, socket, message);
        }

        private class InternalLocalOperationProcessor :
            AbstractLocalOperationProcessor<Uid, PRB.ResolveUsernameOpRequest>
        {
            protected internal InternalLocalOperationProcessor(long requestId, WebSocketConnectionHolder socket,
                PRB.ResolveUsernameOpRequest message)
                : base(requestId, socket, message)
            {
            }

            protected override PRB.RPCResponse CreateOperationResponse(
                RemoteOperationContext remoteContext, Uid result)
            {
                PRB.ResolveUsernameOpResponse response = new PRB.ResolveUsernameOpResponse();
                if (null != result)
                {
                    response.Uid = MessagesUtil.SerializeMessage<PRB.Uid>(result);
                }

                return new
                    PRB.RPCResponse
                {
                    OperationResponse = new PRB.OperationResponse
                    {
                        ResolveUsernameOpResponse = response
                    }
                };
            }

            protected internal override Uid ExecuteOperation(API.ConnectorFacade connectorFacade,
                PRB.ResolveUsernameOpRequest requestMessage)
            {
                ObjectClass objectClass = new ObjectClass(requestMessage.ObjectClass);
                OperationOptions operationOptions = null;
                if (null != requestMessage.Options)
                {
                    operationOptions = MessagesUtil.DeserializeLegacy<OperationOptions>(requestMessage.Options);
                }
                return connectorFacade.ResolveUsername(objectClass, requestMessage.Username, operationOptions);
            }
        }
    }

    #endregion

    #region  SchemaAsyncApiOpImpl

    public class SchemaAsyncApiOpImpl : AbstractAPIOperation, ISchemaAsyncApiOp
    {
        public SchemaAsyncApiOpImpl(
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                remoteConnection, API.ConnectorKey connectorKey,
            Func<RemoteOperationContext, ByteString> facadeKeyFunction, long timeout)
            : base(remoteConnection, connectorKey, facadeKeyFunction, timeout)
        {
        }

        public virtual Schema Schema()
        {
            try
            {
                return SchemaAsync(CancellationToken.None).Result;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        public async Task<Schema> SchemaAsync(CancellationToken cancellationToken)
        {
            return await
                SubmitRequest<Schema, PRB.SchemaOpResponse, InternalRequest>(new InternalRequestFactory(ConnectorKey,
                    FacadeKeyFunction,
                    new PRB.OperationRequest { SchemaOpRequest = new PRB.SchemaOpRequest() },
                    cancellationToken));
        }

        private class InternalRequestFactory : AbstractRemoteOperationRequestFactory<Schema, InternalRequest>
        {
            private readonly PRB.OperationRequest _operationRequest;

            public InternalRequestFactory(API.ConnectorKey connectorKey,
                Func<RemoteOperationContext, ByteString> facadeKeyFunction, PRB.OperationRequest operationRequest,
                CancellationToken cancellationToken)
                : base(connectorKey, facadeKeyFunction, cancellationToken)
            {
                _operationRequest = operationRequest;
            }

            public override InternalRequest CreateRemoteRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <Schema, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback)
            {
                PRB.RPCRequest builder = CreateRpcRequest(context);
                if (null != builder)
                {
                    return new InternalRequest(context, requestId, completionCallback, builder, CancellationToken);
                }
                else
                {
                    return null;
                }
            }

            protected internal override PRB.OperationRequest CreateOperationRequest(
                RemoteOperationContext remoteContext)
            {
                return _operationRequest;
            }
        }

        private class InternalRequest :
            AbstractRemoteOperationRequestFactory<Schema, InternalRequest>.AbstractRemoteOperationRequest
                <Schema, PRB.SchemaOpResponse>
        {
            public InternalRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <Schema, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback, PRB.RPCRequest requestBuilder,
                CancellationToken cancellationToken)
                : base(context, requestId, completionCallback, requestBuilder, cancellationToken)
            {
            }

            protected internal override PRB.SchemaOpResponse GetOperationResponseMessages(PRB.OperationResponse message)
            {
                if (null != message.SchemaOpResponse)
                {
                    return message.SchemaOpResponse;
                }
                else
                {
                    Trace.TraceInformation(OperationExpectsMessage, RequestId, "SchemaOpResponse");
                    return null;
                }
            }

            protected internal override void HandleOperationResponseMessages(WebSocketConnectionHolder sourceConnection,
                PRB.SchemaOpResponse message)
            {
                if (null != message.Schema)
                {
                    HandleResult(MessagesUtil.DeserializeLegacy<Schema>(message.Schema));
                }
                else
                {
                    HandleResult(null);
                }
            }
        }

        // -------

        public static AbstractLocalOperationProcessor<ByteString, PRB.SchemaOpRequest> CreateProcessor(long requestId,
            WebSocketConnectionHolder socket, PRB.SchemaOpRequest message)
        {
            return new InternalLocalOperationProcessor(requestId, socket, message);
        }

        private class InternalLocalOperationProcessor : AbstractLocalOperationProcessor<ByteString, PRB.SchemaOpRequest>
        {
            protected internal InternalLocalOperationProcessor(long requestId, WebSocketConnectionHolder socket,
                PRB.SchemaOpRequest message)
                : base(requestId, socket, message)
            {
            }

            protected override PRB.RPCResponse CreateOperationResponse(
                RemoteOperationContext remoteContext, ByteString result)
            {
                PRB.SchemaOpResponse response = new PRB.SchemaOpResponse();
                if (null != result)
                {
                    response.Schema = result;
                }
                return new
                    PRB.RPCResponse
                {
                    OperationResponse = new PRB.OperationResponse
                    {
                        SchemaOpResponse = response
                    }
                };
            }

            protected internal override ByteString ExecuteOperation(API.ConnectorFacade connectorFacade,
                PRB.SchemaOpRequest requestMessage)
            {
                Schema schema = connectorFacade.Schema();
                if (null != schema)
                {
                    return MessagesUtil.SerializeLegacy(schema);
                }
                return null;
            }
        }
    }

    #endregion

    #region  ScriptOnConnectorAsyncApiOpImpl

    public class ScriptOnConnectorAsyncApiOpImpl : AbstractAPIOperation, IScriptOnConnectorAsyncApiOp
    {
        public ScriptOnConnectorAsyncApiOpImpl(
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                remoteConnection, API.ConnectorKey connectorKey,
            Func<RemoteOperationContext, ByteString> facadeKeyFunction, long timeout)
            : base(remoteConnection, connectorKey, facadeKeyFunction, timeout)
        {
        }

        public virtual Object RunScriptOnConnector(ScriptContext request, OperationOptions options)
        {
            try
            {
                return RunScriptOnConnectorAsync(request, options, CancellationToken.None).Result;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        public Task<object> RunScriptOnConnectorAsync(ScriptContext request, OperationOptions options,
            CancellationToken cancellationToken)
        {
            Assertions.NullCheck(request, "request");

            PRB.ScriptOnConnectorOpRequest requestBuilder = new PRB.ScriptOnConnectorOpRequest();

            requestBuilder.ScriptContext = MessagesUtil.SerializeMessage<PRB.ScriptContext>(request);

            if (options != null)
            {
                requestBuilder.Options = MessagesUtil.SerializeLegacy(options);
            }

            return
                SubmitRequest<Object, PRB.ScriptOnConnectorOpResponse, InternalRequest>(
                    new InternalRequestFactory(ConnectorKey, FacadeKeyFunction,
                        new PRB.OperationRequest { ScriptOnConnectorOpRequest = requestBuilder },
                        cancellationToken));
        }

        private class InternalRequestFactory : AbstractRemoteOperationRequestFactory<Object, InternalRequest>
        {
            private readonly PRB.OperationRequest _operationRequest;

            public InternalRequestFactory(API.ConnectorKey connectorKey,
                Func<RemoteOperationContext, ByteString> facadeKeyFunction, PRB.OperationRequest operationRequest,
                CancellationToken cancellationToken)
                : base(connectorKey, facadeKeyFunction, cancellationToken)
            {
                _operationRequest = operationRequest;
            }

            public override InternalRequest CreateRemoteRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <Object, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback)
            {
                PRB.RPCRequest builder = CreateRpcRequest(context);
                if (null != builder)
                {
                    return new InternalRequest(context, requestId, completionCallback, builder, CancellationToken);
                }
                else
                {
                    return null;
                }
            }

            protected internal override PRB.OperationRequest CreateOperationRequest(
                RemoteOperationContext remoteContext)
            {
                return _operationRequest;
            }
        }

        private class InternalRequest :
            AbstractRemoteOperationRequestFactory<Object, InternalRequest>.AbstractRemoteOperationRequest
                <Object, PRB.ScriptOnConnectorOpResponse>
        {
            public InternalRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <Object, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback, PRB.RPCRequest requestBuilder,
                CancellationToken cancellationToken)
                : base(context, requestId, completionCallback, requestBuilder, cancellationToken)
            {
            }

            protected internal override PRB.ScriptOnConnectorOpResponse GetOperationResponseMessages(
                PRB.OperationResponse message)
            {
                if (null != message.ScriptOnConnectorOpResponse)
                {
                    return message.ScriptOnConnectorOpResponse;
                }
                else
                {
                    Trace.TraceInformation(OperationExpectsMessage, RequestId, "ScriptOnConnectorOpResponse");
                    return null;
                }
            }

            protected internal override void HandleOperationResponseMessages(WebSocketConnectionHolder sourceConnection,
                PRB.ScriptOnConnectorOpResponse message)
            {
                if (null != message.Object)
                {
                    HandleResult(MessagesUtil.DeserializeLegacy<Object>(message.Object));
                }
                else
                {
                    HandleResult(null);
                }
            }
        }

        // ----

        public static AbstractLocalOperationProcessor<ByteString, PRB.ScriptOnConnectorOpRequest> CreateProcessor(
            long requestId, WebSocketConnectionHolder socket, PRB.ScriptOnConnectorOpRequest message)
        {
            return new InternalLocalOperationProcessor(requestId, socket, message);
        }

        private class InternalLocalOperationProcessor :
            AbstractLocalOperationProcessor<ByteString, PRB.ScriptOnConnectorOpRequest>
        {
            protected internal InternalLocalOperationProcessor(long requestId, WebSocketConnectionHolder socket,
                PRB.ScriptOnConnectorOpRequest message)
                : base(requestId, socket, message)
            {
            }

            protected override PRB.RPCResponse CreateOperationResponse(
                RemoteOperationContext remoteContext, ByteString result)
            {
                PRB.ScriptOnConnectorOpResponse response = new PRB.ScriptOnConnectorOpResponse();
                if (null != result)
                {
                    response.Object = result;
                }

                return new
                    PRB.RPCResponse
                {
                    OperationResponse = new PRB.OperationResponse
                    {
                        ScriptOnConnectorOpResponse = response
                    }
                };
            }

            protected internal override ByteString ExecuteOperation(API.ConnectorFacade connectorFacade,
                PRB.ScriptOnConnectorOpRequest requestMessage)
            {
                ScriptContext request = MessagesUtil.DeserializeMessage<ScriptContext>(requestMessage.ScriptContext);

                OperationOptions operationOptions = null;
                if (null != requestMessage.Options)
                {
                    operationOptions = MessagesUtil.DeserializeLegacy<OperationOptions>(requestMessage.Options);
                }
                object result = connectorFacade.RunScriptOnConnector(request, operationOptions);
                if (null != result)
                {
                    return MessagesUtil.SerializeLegacy(result);
                }
                return null;
            }
        }
    }

    #endregion

    #region  ScriptOnResourceAsyncApiOpImpl

    public class ScriptOnResourceAsyncApiOpImpl : AbstractAPIOperation, IScriptOnResourceAsyncApiOp
    {
        public ScriptOnResourceAsyncApiOpImpl(
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                remoteConnection, API.ConnectorKey connectorKey,
            Func<RemoteOperationContext, ByteString> facadeKeyFunction, long timeout)
            : base(remoteConnection, connectorKey, facadeKeyFunction, timeout)
        {
        }

        public virtual object RunScriptOnResource(ScriptContext request, OperationOptions options)
        {
            try
            {
                return RunScriptOnResourceAsync(request, options, CancellationToken.None).Result;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        public async Task<object> RunScriptOnResourceAsync(ScriptContext request, OperationOptions options,
            CancellationToken cancellationToken)
        {
            Assertions.NullCheck(request, "request");

            PRB.ScriptOnResourceOpRequest requestBuilder = new PRB.ScriptOnResourceOpRequest
            {
                ScriptContext = MessagesUtil.SerializeMessage<PRB.ScriptContext>(request)
            };


            if (options != null)
            {
                requestBuilder.Options = MessagesUtil.SerializeLegacy(options);
            }

            return await
                SubmitRequest<Object, PRB.ScriptOnResourceOpResponse, InternalRequest>(
                    new InternalRequestFactory(ConnectorKey, FacadeKeyFunction,
                        new PRB.OperationRequest { ScriptOnResourceOpRequest = requestBuilder }, cancellationToken));
        }

        private class InternalRequestFactory : AbstractRemoteOperationRequestFactory<Object, InternalRequest>
        {
            private readonly PRB.OperationRequest _operationRequest;

            public InternalRequestFactory(API.ConnectorKey connectorKey,
                Func<RemoteOperationContext, ByteString> facadeKeyFunction, PRB.OperationRequest operationRequest,
                CancellationToken cancellationToken)
                : base(connectorKey, facadeKeyFunction, cancellationToken)
            {
                _operationRequest = operationRequest;
            }

            public override InternalRequest CreateRemoteRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <object, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback)
            {
                PRB.RPCRequest builder = CreateRpcRequest(context);
                if (null != builder)
                {
                    return new InternalRequest(context, requestId, completionCallback, builder, CancellationToken);
                }
                else
                {
                    return null;
                }
            }

            protected internal override PRB.OperationRequest CreateOperationRequest(
                RemoteOperationContext remoteContext)
            {
                return _operationRequest;
            }
        }

        private class InternalRequest :
            AbstractRemoteOperationRequestFactory<Object, InternalRequest>.AbstractRemoteOperationRequest
                <Object, PRB.ScriptOnResourceOpResponse>
        {
            public InternalRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <object, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback, PRB.RPCRequest requestBuilder,
                CancellationToken cancellationToken)
                : base(context, requestId, completionCallback, requestBuilder, cancellationToken)
            {
            }

            protected internal override PRB.ScriptOnResourceOpResponse GetOperationResponseMessages(
                PRB.OperationResponse message)
            {
                if (null != message.ScriptOnResourceOpResponse)
                {
                    return message.ScriptOnResourceOpResponse;
                }
                else
                {
                    Trace.TraceInformation(OperationExpectsMessage, RequestId, "ScriptOnResourceOpResponse");
                    return null;
                }
            }

            protected internal override void HandleOperationResponseMessages(WebSocketConnectionHolder sourceConnection,
                PRB.ScriptOnResourceOpResponse message)
            {
                if (null != message.Object)
                {
                    HandleResult(MessagesUtil.DeserializeLegacy<Object>(message.Object));
                }
                else
                {
                    HandleResult(null);
                }
            }
        }

        // ----

        public static AbstractLocalOperationProcessor<ByteString, PRB.ScriptOnResourceOpRequest> CreateProcessor(
            long requestId, WebSocketConnectionHolder socket, PRB.ScriptOnResourceOpRequest message)
        {
            return new InternalLocalOperationProcessor(requestId, socket, message);
        }

        private class InternalLocalOperationProcessor :
            AbstractLocalOperationProcessor<ByteString, PRB.ScriptOnResourceOpRequest>
        {
            protected internal InternalLocalOperationProcessor(long requestId, WebSocketConnectionHolder socket,
                PRB.ScriptOnResourceOpRequest message)
                : base(requestId, socket, message)
            {
            }

            protected override PRB.RPCResponse CreateOperationResponse(
                RemoteOperationContext remoteContext, ByteString result)
            {
                PRB.ScriptOnResourceOpResponse response = new PRB.ScriptOnResourceOpResponse();
                if (null != result)
                {
                    response.Object = result;
                }

                return new
                    PRB.RPCResponse
                {
                    OperationResponse = new PRB.OperationResponse
                    {
                        ScriptOnResourceOpResponse = response
                    }
                };
            }

            protected internal override ByteString ExecuteOperation(API.ConnectorFacade connectorFacade,
                PRB.ScriptOnResourceOpRequest requestMessage)
            {
                ScriptContext request = MessagesUtil.DeserializeMessage<ScriptContext>(requestMessage.ScriptContext);

                OperationOptions operationOptions = null;
                if (null != requestMessage.Options)
                {
                    operationOptions = MessagesUtil.DeserializeLegacy<OperationOptions>(requestMessage.Options);
                }
                object result = connectorFacade.RunScriptOnResource(request, operationOptions);
                if (null != result)
                {
                    return MessagesUtil.SerializeLegacy(result);
                }
                return null;
            }
        }
    }

    #endregion

    #region SearchAsyncApiOpImpl

    public class SearchAsyncApiOpImpl : AbstractAPIOperation, SearchApiOp
    {
        public SearchAsyncApiOpImpl(
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                remoteConnection, API.ConnectorKey connectorKey,
            Func<RemoteOperationContext, ByteString> facadeKeyFunction, long timeout)
            : base(remoteConnection, connectorKey, facadeKeyFunction, timeout)
        {
        }

        public virtual SearchResult Search(ObjectClass objectClass, Filter filter, ResultsHandler handler,
            OperationOptions options)
        {
            try
            {
                return SearchAsync(objectClass, filter, handler, options, CancellationToken.None).Result;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        public async Task<SearchResult> SearchAsync(ObjectClass objectClass, Filter filter, ResultsHandler handler,
            OperationOptions options, CancellationToken cancellationToken)
        {
            Assertions.NullCheck(objectClass, "objectClass");
            if (ObjectClass.ALL.Equals(objectClass))
            {
                throw new NotSupportedException("Operation is not allowed on __ALL__ object class");
            }
            Assertions.NullCheck(handler, "handler");

            PRB.SearchOpRequest requestBuilder = new
                PRB.SearchOpRequest { ObjectClass = objectClass.GetObjectClassValue() };
            if (filter != null)
            {
                requestBuilder.Filter = MessagesUtil.SerializeLegacy(filter);
            }
            if (options != null)
            {
                requestBuilder.Options = MessagesUtil.SerializeLegacy(options);
            }

            return await
                SubmitRequest<SearchResult, PRB.SearchOpResponse, InternalRequest>(
                    new InternalRequestFactory(ConnectorKey,
                        FacadeKeyFunction, new PRB.OperationRequest { SearchOpRequest = requestBuilder }, handler,
                        cancellationToken));
        }

        private class InternalRequestFactory : AbstractRemoteOperationRequestFactory<SearchResult, InternalRequest>
        {
            private readonly PRB.OperationRequest _operationRequest;
            private readonly ResultsHandler _handler;

            public InternalRequestFactory(API.ConnectorKey connectorKey,
                Func<RemoteOperationContext, ByteString> facadeKeyFunction, PRB.OperationRequest operationRequest,
                ResultsHandler handler, CancellationToken cancellationToken)
                : base(connectorKey, facadeKeyFunction, cancellationToken)
            {
                _operationRequest = operationRequest;
                _handler = handler;
            }

            public override InternalRequest CreateRemoteRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <SearchResult, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback)
            {
                // This is the context aware request
                PRB.RPCRequest builder = CreateRpcRequest(context);
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

            protected internal override PRB.OperationRequest CreateOperationRequest(
                RemoteOperationContext remoteContext)
            {
                return _operationRequest;
            }
        }

        private class InternalRequest :
            AbstractRemoteOperationRequestFactory<SearchResult, InternalRequest>.AbstractRemoteOperationRequest
                <SearchResult, PRB.SearchOpResponse>
        {
            private readonly ResultsHandler _handler;
            private Int64 _sequence;
            private Int64 _expectedResult = -1;
            private SearchResult _result;

            public InternalRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <SearchResult, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback, PRB.RPCRequest requestBuilder,
                ResultsHandler handler, CancellationToken cancellationToken)
                : base(context, requestId, completionCallback, requestBuilder, cancellationToken)
            {
                _handler = handler;
            }

            protected internal override PRB.SearchOpResponse GetOperationResponseMessages(PRB.OperationResponse message)
            {
                if (null != message.SearchOpResponse)
                {
                    return message.SearchOpResponse;
                }
                else
                {
                    Trace.TraceInformation(OperationExpectsMessage, RequestId, "SearchOpResponse");
                    return null;
                }
            }

            protected internal override void HandleOperationResponseMessages(WebSocketConnectionHolder sourceConnection,
                PRB.SearchOpResponse message)
            {
                if (null != message.ConnectorObject)
                {
                    try
                    {
                        ConnectorObject co = MessagesUtil.DeserializeMessage<ConnectorObject>(message.ConnectorObject);

                        if (!_handler.Handle(co) && !Promise.IsCompleted)
                        {
                            HandleError(new ConnectorException("ResultsHandler stopped processing results"));
                            TryCancelRemote(ConnectionContext, RequestId);
                        }
                    }
                    finally
                    {
                        Interlocked.Increment(ref _sequence);
                    }
                }
                else
                {
                    if (null != message.Result)
                    {
                        _result = MessagesUtil.DeserializeMessage<SearchResult>(message.Result);
                    }
                    _expectedResult = message.Sequence;
                    if (_expectedResult == 0 || _sequence == _expectedResult)
                    {
                        HandleResult(_result);
                    }
                    else
                    {
                        Trace.TraceInformation("Response processed before all result has arrived");
                    }
                }
                if (_expectedResult > 0 && _sequence == _expectedResult)
                {
                    HandleResult(_result);
                }
            }
        }

        // ----

        public static AbstractLocalOperationProcessor<PRB.SearchOpResponse, PRB.SearchOpRequest> CreateProcessor(
            long requestId, WebSocketConnectionHolder socket, PRB.SearchOpRequest message)
        {
            return new InternalLocalOperationProcessor(requestId, socket, message);
        }

        private class InternalLocalOperationProcessor :
            AbstractLocalOperationProcessor<PRB.SearchOpResponse, PRB.SearchOpRequest>
        {
            private Int32 _doContinue = 1;
            private Int64 _sequence;

            protected internal InternalLocalOperationProcessor(long requestId, WebSocketConnectionHolder socket,
                PRB.SearchOpRequest message)
                : base(requestId, socket, message)
            {
            }

            protected override PRB.RPCResponse CreateOperationResponse(
                RemoteOperationContext remoteContext, PRB.SearchOpResponse response)
            {
                return new
                    PRB.RPCResponse
                {
                    OperationResponse = new PRB.OperationResponse
                    {
                        SearchOpResponse = response
                    }
                };
            }

            protected internal override PRB.SearchOpResponse ExecuteOperation(API.ConnectorFacade connectorFacade,
                PRB.SearchOpRequest requestMessage)
            {
                ObjectClass objectClass = new ObjectClass(requestMessage.ObjectClass);
                Filter filter = null;
                if (null != requestMessage.Filter)
                {
                    filter = MessagesUtil.DeserializeLegacy<Filter>(requestMessage.Filter);
                }

                OperationOptions operationOptions = null;
                if (null != requestMessage.Options)
                {
                    operationOptions = MessagesUtil.DeserializeLegacy<OperationOptions>(requestMessage.Options);
                }
                SearchResult searchResult = connectorFacade.Search(objectClass, filter, new ResultsHandler()
                {
                    Handle = connectorObject =>
                    {
                        if (null != connectorObject)
                        {
                            PRB.SearchOpResponse result =
                                new PRB.SearchOpResponse
                                {
                                    ConnectorObject =
                                        MessagesUtil.SerializeMessage<PRB.ConnectorObject>(connectorObject),
                                    Sequence = Interlocked.Increment(ref _sequence)
                                };

                            if (TryHandleResult(result))
                            {
                                Trace.TraceInformation("SearchResult sent in sequence:{0}", _sequence);
                            }
                            else
                            {
                                Trace.TraceInformation("Failed to send response {0}", _sequence);
                            }
                        }
                        return _doContinue == 1;
                    }
                }, operationOptions);

                PRB.SearchOpResponse response = new PRB.SearchOpResponse { Sequence = _sequence };
                if (null != searchResult)
                {
                    response.Result = MessagesUtil.SerializeMessage<PRB.SearchResult>(searchResult);
                }
                return response;
            }

            protected override bool TryCancel()
            {
                _doContinue = 0;
                return base.TryCancel();
            }
        }
    }

    #endregion

    #region SyncAsyncApiOpImpl

    public class SyncAsyncApiOpImpl : AbstractAPIOperation, SyncApiOp
    {
        public SyncAsyncApiOpImpl(
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                remoteConnection, API.ConnectorKey connectorKey,
            Func<RemoteOperationContext, ByteString> facadeKeyFunction, long timeout)
            : base(remoteConnection, connectorKey, facadeKeyFunction, timeout)
        {
        }

        public virtual SyncToken GetLatestSyncToken(ObjectClass objectClass)
        {
            try
            {
                return GetLatestSyncTokenAsync(objectClass, CancellationToken.None).Result;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        public virtual SyncToken Sync(ObjectClass objectClass, SyncToken token, SyncResultsHandler handler,
            OperationOptions options)
        {
            try
            {
                return SyncAsync(objectClass, token, handler, options, CancellationToken.None).Result;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        public async Task<SyncToken> GetLatestSyncTokenAsync(ObjectClass objectClass,
            CancellationToken cancellationToken)
        {
            Assertions.NullCheck(objectClass, "objectClass");
            return await
                SubmitRequest<SyncToken, PRB.SyncOpResponse, InternalRequest>(new InternalRequestFactory(ConnectorKey,
                    FacadeKeyFunction,
                    new PRB.OperationRequest
                    {
                        SyncOpRequest =
                            new PRB.SyncOpRequest
                            {
                                LatestSyncToken = new PRB.SyncOpRequest.Types.LatestSyncToken
                                {
                                    ObjectClass = objectClass.GetObjectClassValue()
                                }
                            }
                    }, null, cancellationToken));
        }

        public Task<SyncToken> SyncAsync(ObjectClass objectClass, SyncToken token, SyncResultsHandler handler,
            OperationOptions options, CancellationToken cancellationToken)
        {
            Assertions.NullCheck(objectClass, "objectClass");
            Assertions.NullCheck(handler, "handler");
            PRB.SyncOpRequest.Types.Sync requestBuilder =
                new PRB.SyncOpRequest.Types.Sync { ObjectClass = objectClass.GetObjectClassValue() };
            if (token != null)
            {
                requestBuilder.Token = MessagesUtil.SerializeMessage<PRB.SyncToken>(token);
            }

            if (options != null)
            {
                requestBuilder.Options = MessagesUtil.SerializeLegacy(options);
            }

            return
                SubmitRequest<SyncToken, PRB.SyncOpResponse, InternalRequest>(new InternalRequestFactory(ConnectorKey,
                    FacadeKeyFunction,
                    new PRB.OperationRequest
                    {
                        SyncOpRequest = new PRB.SyncOpRequest { Sync = requestBuilder }
                    }, handler,
                    cancellationToken));
        }

        private class InternalRequestFactory : AbstractRemoteOperationRequestFactory<SyncToken, InternalRequest>
        {
            private readonly PRB.OperationRequest _operationRequest;
            private readonly SyncResultsHandler _handler;

            public InternalRequestFactory(API.ConnectorKey connectorKey,
                Func<RemoteOperationContext, ByteString> facadeKeyFunction, PRB.OperationRequest operationRequest,
                SyncResultsHandler handler, CancellationToken cancellationToken)
                : base(connectorKey, facadeKeyFunction, cancellationToken)
            {
                _operationRequest = operationRequest;
                _handler = handler;
            }

            public override InternalRequest CreateRemoteRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <SyncToken, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback)
            {
                PRB.RPCRequest builder = CreateRpcRequest(context);
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

            protected internal override PRB.OperationRequest CreateOperationRequest(
                RemoteOperationContext remoteContext)
            {
                return _operationRequest;
            }
        }

        private class InternalRequest :
            AbstractRemoteOperationRequestFactory<SyncToken, InternalRequest>.AbstractRemoteOperationRequest
                <SyncToken, PRB.SyncOpResponse>
        {
            private readonly SyncResultsHandler _handler;
            private Int64 _sequence;
            private Int64 _expectedResult = -1;
            private SyncToken _result;

            public InternalRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <SyncToken, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback, PRB.RPCRequest requestBuilder,
                SyncResultsHandler handler, CancellationToken cancellationToken)
                : base(context, requestId, completionCallback, requestBuilder, cancellationToken)
            {
                _handler = handler;
            }

            protected internal override PRB.SyncOpResponse GetOperationResponseMessages(PRB.OperationResponse message)
            {
                if (null != message.SyncOpResponse)
                {
                    return message.SyncOpResponse;
                }
                else
                {
                    Trace.TraceInformation(OperationExpectsMessage, RequestId, "SyncOpResponse");
                    return null;
                }
            }

            protected internal override void HandleOperationResponseMessages(WebSocketConnectionHolder sourceConnection,
                PRB.SyncOpResponse message)
            {
                if (null != message.LatestSyncToken)
                {
                    if (null != message.LatestSyncToken.SyncToken)
                    {
                        HandleResult(MessagesUtil.DeserializeMessage<SyncToken>(message.LatestSyncToken.SyncToken));
                    }
                    else
                    {
                        HandleResult(null);
                    }
                }
                else if (null != message.Sync)
                {
                    Trace.TraceInformation("SyncOp Response received in sequence:{0} of {1}", message.Sync.Sequence,
                        _sequence);
                    if (null != message.Sync.SyncDelta)
                    {
                        try
                        {
                            SyncDelta delta =
                                MessagesUtil.DeserializeMessage<SyncDelta>(
                                    message.Sync.SyncDelta);

                            if (!_handler.Handle(delta) && !Promise.IsCompleted)
                            {
                                HandleError(new ConnectorException("SyncResultsHandler stopped processing results"));
                                TryCancelRemote(ConnectionContext, RequestId);
                            }
                        }
                        finally
                        {
                            Interlocked.Increment(ref _sequence);
                        }
                    }
                    else
                    {
                        if (null != message.Sync.SyncToken)
                        {
                            _result = MessagesUtil.DeserializeMessage<SyncToken>(message.Sync.SyncToken);
                        }
                        _expectedResult = message.Sync.Sequence;
                        if (_expectedResult == 0 || _sequence == _expectedResult)
                        {
                            HandleResult(_result);
                        }
                        else
                        {
                            Trace.TraceInformation("Response processed before all result has arrived");
                        }
                    }
                    if (_expectedResult > 0 && _sequence == _expectedResult)
                    {
                        HandleResult(_result);
                    }
                }
                else
                {
                    Trace.TraceInformation("Invalid SyncOpResponse Response:{0}", RequestId);
                }
            }
        }

        // ----

        public static AbstractLocalOperationProcessor<PRB.SyncOpResponse, PRB.SyncOpRequest> CreateProcessor(
            long requestId, WebSocketConnectionHolder socket, PRB.SyncOpRequest message)
        {
            return new InternalLocalOperationProcessor(requestId, socket, message);
        }

        private class InternalLocalOperationProcessor :
            AbstractLocalOperationProcessor<PRB.SyncOpResponse, PRB.SyncOpRequest>
        {
            private Int32 _doContinue = 1;
            private Int64 _sequence;

            protected internal InternalLocalOperationProcessor(long requestId, WebSocketConnectionHolder socket,
                PRB.SyncOpRequest message)
                : base(requestId, socket, message)
            {
            }

            protected override PRB.RPCResponse CreateOperationResponse(
                RemoteOperationContext remoteContext, PRB.SyncOpResponse response)
            {
                return new
                    PRB.RPCResponse
                {
                    OperationResponse = new PRB.OperationResponse
                    {
                        SyncOpResponse = response
                    }
                };
            }

            protected internal override PRB.SyncOpResponse ExecuteOperation(API.ConnectorFacade connectorFacade,
                PRB.SyncOpRequest requestMessage)
            {
                if (null != requestMessage.LatestSyncToken)
                {
                    ObjectClass objectClass = new ObjectClass(requestMessage.LatestSyncToken.ObjectClass);
                    SyncToken token = connectorFacade.GetLatestSyncToken(objectClass);

                    // Enable returnNullTest
                    PRB.SyncOpResponse.Types.LatestSyncToken builder = new
                        PRB.SyncOpResponse.Types.LatestSyncToken();
                    if (null != token)
                    {
                        builder.SyncToken = MessagesUtil.SerializeMessage<PRB.SyncToken>(token);
                    }
                    return new PRB.SyncOpResponse { LatestSyncToken = builder };
                }
                else if (null != requestMessage.Sync)
                {
                    ObjectClass objectClass = new ObjectClass(requestMessage.Sync.ObjectClass);
                    SyncToken token = MessagesUtil.DeserializeMessage<SyncToken>(requestMessage.Sync.Token);

                    OperationOptions operationOptions = null;
                    if (null != requestMessage.Sync.Options)
                    {
                        operationOptions = MessagesUtil.DeserializeLegacy<OperationOptions>(requestMessage.Sync.Options);
                    }

                    SyncToken syncResult = connectorFacade.Sync(objectClass, token, new SyncResultsHandler()
                    {
                        Handle = delta =>
                        {
                            if (null != delta)
                            {
                                PRB.SyncOpResponse result =
                                    new PRB.SyncOpResponse
                                    {
                                        Sync =
                                            new PRB.SyncOpResponse.Types.Sync
                                            {
                                                SyncDelta = MessagesUtil.SerializeMessage<PRB.SyncDelta>(delta),
                                                Sequence = Interlocked.Increment(ref _sequence)
                                            }
                                    };

                                if (TryHandleResult(result))
                                {
                                    Trace.TraceInformation("SyncResult sent in sequence:{0}", _sequence);
                                }
                                else
                                {
                                    Trace.TraceInformation("Failed to send response {0}", _sequence);
                                }
                            }
                            return _doContinue == 1;
                        }
                    }, operationOptions);

                    PRB.SyncOpResponse.Types.Sync builder =
                        new PRB.SyncOpResponse.Types.Sync { Sequence = _sequence };
                    if (null != syncResult)
                    {
                        builder.SyncToken = MessagesUtil.SerializeMessage<PRB.SyncToken>(syncResult);
                    }

                    return new PRB.SyncOpResponse { Sync = builder };
                }
                else
                {
                    Trace.TraceInformation("Invalid SyncOpRequest Request:{0}", RequestId);
                }
                return null;
            }


            protected override bool TryCancel()
            {
                _doContinue = 0;
                return base.TryCancel();
            }
        }
    }

    #endregion

    #region ConnectorEventSubscriptionApiOpImpl

    #endregion

    #region TestAsyncApiOpImpl

    public class TestAsyncApiOpImpl : AbstractAPIOperation, ITestAsyncApiOp
    {
        public TestAsyncApiOpImpl(
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                remoteConnection, API.ConnectorKey connectorKey,
            Func<RemoteOperationContext, ByteString> facadeKeyFunction, long timeout)
            : base(remoteConnection, connectorKey, facadeKeyFunction, timeout)
        {
        }

        public virtual void Test()
        {
            try
            {
                TestAsync(CancellationToken.None).Wait();
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        public async Task TestAsync(CancellationToken cancellationToken)
        {
            await SubmitRequest<Object, PRB.TestOpResponse, InternalRequest>(new InternalRequestFactory(ConnectorKey,
                FacadeKeyFunction, new PRB.OperationRequest { TestOpRequest = new PRB.TestOpRequest() },
                cancellationToken));
        }

        private class InternalRequestFactory : AbstractRemoteOperationRequestFactory<Object, InternalRequest>
        {
            private readonly PRB.OperationRequest _operationRequest;

            public InternalRequestFactory(API.ConnectorKey connectorKey,
                Func<RemoteOperationContext, ByteString> facadeKeyFunction, PRB.OperationRequest operationRequest,
                CancellationToken cancellationToken)
                : base(connectorKey, facadeKeyFunction, cancellationToken)
            {
                _operationRequest = operationRequest;
            }

            public override InternalRequest CreateRemoteRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <object, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback)
            {
                PRB.RPCRequest builder = CreateRpcRequest(context);
                if (null != builder)
                {
                    return new InternalRequest(context, requestId, completionCallback, builder, CancellationToken);
                }
                else
                {
                    return null;
                }
            }

            protected internal override PRB.OperationRequest CreateOperationRequest(
                RemoteOperationContext remoteContext)
            {
                return _operationRequest;
            }
        }

        private class InternalRequest :
            AbstractRemoteOperationRequestFactory<Object, InternalRequest>.AbstractRemoteOperationRequest
                <Object, PRB.TestOpResponse>
        {
            public InternalRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <object, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback, PRB.RPCRequest requestBuilder,
                CancellationToken cancellationToken)
                : base(context, requestId, completionCallback, requestBuilder, cancellationToken)
            {
            }

            protected internal override PRB.TestOpResponse GetOperationResponseMessages(PRB.OperationResponse message)
            {
                if (null != message.TestOpResponse)
                {
                    return message.TestOpResponse;
                }
                else
                {
                    Trace.TraceInformation(OperationExpectsMessage, RequestId, "TestOpResponse");
                    return null;
                }
            }

            protected internal override void HandleOperationResponseMessages(WebSocketConnectionHolder sourceConnection,
                PRB.TestOpResponse message)
            {
                HandleResult(null);
            }

            public override string ToString()
            {
                return "Test Request {}" + RequestId;
            }
        }

        // ----
        public static AbstractLocalOperationProcessor<Object, PRB.TestOpRequest> CreateProcessor(long requestId,
            WebSocketConnectionHolder socket, PRB.TestOpRequest message)
        {
            return new TestLocalOperationProcessor(requestId, socket, message);
        }

        private class TestLocalOperationProcessor : AbstractLocalOperationProcessor<Object, PRB.TestOpRequest>
        {
            protected internal TestLocalOperationProcessor(long requestId, WebSocketConnectionHolder socket,
                PRB.TestOpRequest message)
                : base(requestId, socket, message)
            {
            }

            protected override PRB.RPCResponse CreateOperationResponse(
                RemoteOperationContext remoteContext, Object response)
            {
                return new
                    PRB.RPCResponse
                {
                    OperationResponse = new PRB.OperationResponse
                    {
                        TestOpResponse = new PRB.TestOpResponse()
                    }
                };
            }

            protected internal override Object ExecuteOperation(API.ConnectorFacade connectorFacade,
                PRB.TestOpRequest requestMessage)
            {
                connectorFacade.Test();
                return null;
            }
        }
    }

    #endregion

    #region UpdateAsyncApiOpImpl

    public class UpdateAsyncApiOpImpl : AbstractAPIOperation, IUpdateAsyncApiOp
    {
        public UpdateAsyncApiOpImpl(
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                remoteConnection, API.ConnectorKey connectorKey,
            Func<RemoteOperationContext, ByteString> facadeKeyFunction, long timeout)
            : base(remoteConnection, connectorKey, facadeKeyFunction, timeout)
        {
        }

        public virtual Uid Update(ObjectClass objectClass, Uid uid, ICollection<ConnectorAttribute> replaceAttributes,
            OperationOptions options)
        {
            try
            {
                return UpdateAsync(objectClass, uid, replaceAttributes, options, CancellationToken.None).Result;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        public virtual Uid AddAttributeValues(ObjectClass objectClass, Uid uid,
            ICollection<ConnectorAttribute> valuesToAdd, OperationOptions options)
        {
            try
            {
                return AddAttributeValuesAsync(objectClass, uid, valuesToAdd, options, CancellationToken.None).Result;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        public virtual Uid RemoveAttributeValues(ObjectClass objectClass, Uid uid,
            ICollection<ConnectorAttribute> valuesToRemove, OperationOptions options)
        {
            try
            {
                return
                    RemoveAttributeValuesAsync(objectClass, uid, valuesToRemove, options, CancellationToken.None).Result;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        public async Task<Uid> UpdateAsync(ObjectClass objectClass, Uid uid,
            ICollection<ConnectorAttribute> replaceAttributes, OperationOptions options,
            CancellationToken cancellationToken)
        {
            return
                await
                    DoUpdate(objectClass, uid, PRB.UpdateOpRequest.Types.UpdateType.REPLACE, replaceAttributes, options,
                        cancellationToken);
        }

        public async Task<Uid> AddAttributeValuesAsync(ObjectClass objectClass, Uid uid,
            ICollection<ConnectorAttribute> valuesToAdd, OperationOptions options, CancellationToken cancellationToken)
        {
            return await DoUpdate(objectClass, uid, PRB.UpdateOpRequest.Types.UpdateType.ADD, valuesToAdd, options,
                cancellationToken);
        }

        public async Task<Uid> RemoveAttributeValuesAsync(ObjectClass objectClass, Uid uid,
            ICollection<ConnectorAttribute> valuesToRemove, OperationOptions options,
            CancellationToken cancellationToken)
        {
            return
                await DoUpdate(objectClass, uid, PRB.UpdateOpRequest.Types.UpdateType.REMOVE, valuesToRemove, options,
                    cancellationToken);
        }

        public async Task<Uid> DoUpdate(ObjectClass objectClass, Uid uid,
            PRB.UpdateOpRequest.Types.UpdateType updateType,
            ICollection<ConnectorAttribute> replaceAttributes, OperationOptions options,
            CancellationToken cancellationToken)
        {
            UpdateImpl.ValidateInput(objectClass, uid, replaceAttributes,
                !PRB.UpdateOpRequest.Types.UpdateType.REPLACE.Equals(updateType));
            PRB.UpdateOpRequest requestBuilder =
                new PRB.UpdateOpRequest
                {
                    ObjectClass = objectClass.GetObjectClassValue(),
                    Uid = MessagesUtil.SerializeMessage<PRB.Uid>(uid),
                    UpdateType = updateType,
                    ReplaceAttributes = MessagesUtil.SerializeLegacy(replaceAttributes)
                };


            if (options != null)
            {
                requestBuilder.Options = MessagesUtil.SerializeLegacy(options);
            }

            return await
                SubmitRequest<Uid, PRB.UpdateOpResponse, InternalRequest>(new InternalRequestFactory(ConnectorKey,
                    FacadeKeyFunction, new PRB.OperationRequest { UpdateOpRequest = requestBuilder },
                    cancellationToken));
        }

        private class InternalRequestFactory : AbstractRemoteOperationRequestFactory<Uid, InternalRequest>
        {
            private readonly PRB.OperationRequest _operationRequest;

            public InternalRequestFactory(API.ConnectorKey connectorKey,
                Func<RemoteOperationContext, ByteString> facadeKeyFunction, PRB.OperationRequest operationRequest,
                CancellationToken cancellationToken)
                : base(connectorKey, facadeKeyFunction, cancellationToken)
            {
                _operationRequest = operationRequest;
            }

            public override InternalRequest CreateRemoteRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <Uid, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext
                                >> completionCallback)
            {
                PRB.RPCRequest builder = CreateRpcRequest(context);
                if (null != builder)
                {
                    return new InternalRequest(context, requestId, completionCallback, builder, CancellationToken);
                }
                else
                {
                    return null;
                }
            }

            protected internal override PRB.OperationRequest CreateOperationRequest(
                RemoteOperationContext remoteContext)
            {
                return _operationRequest;
            }
        }

        private class InternalRequest :
            AbstractRemoteOperationRequestFactory<Uid, InternalRequest>.AbstractRemoteOperationRequest
                <Uid, PRB.UpdateOpResponse>
        {
            public InternalRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <Uid, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext
                                >> completionCallback, PRB.RPCRequest requestBuilder,
                CancellationToken cancellationToken)
                : base(context, requestId, completionCallback, requestBuilder, cancellationToken)
            {
            }

            protected internal override PRB.UpdateOpResponse GetOperationResponseMessages(PRB.OperationResponse message)
            {
                if (null != message.UpdateOpResponse)
                {
                    return message.UpdateOpResponse;
                }
                else
                {
                    Trace.TraceInformation(OperationExpectsMessage, RequestId, "UpdateOpResponse");
                    return null;
                }
            }

            protected internal override void HandleOperationResponseMessages(WebSocketConnectionHolder sourceConnection,
                PRB.UpdateOpResponse message)
            {
                if (null != message.Uid)
                {
                    HandleResult(MessagesUtil.DeserializeMessage<Uid>(message.Uid));
                }
                else
                {
                    HandleResult(null);
                }
            }
        }

        // ----

        public static AbstractLocalOperationProcessor<Uid, PRB.UpdateOpRequest> CreateProcessor(long requestId,
            WebSocketConnectionHolder socket, PRB.UpdateOpRequest message)
        {
            return new InternalLocalOperationProcessor(requestId, socket, message);
        }

        private class InternalLocalOperationProcessor : AbstractLocalOperationProcessor<Uid, PRB.UpdateOpRequest>
        {
            protected internal InternalLocalOperationProcessor(long requestId, WebSocketConnectionHolder socket,
                PRB.UpdateOpRequest message)
                : base(requestId, socket, message)
            {
            }

            protected override PRB.RPCResponse CreateOperationResponse(
                RemoteOperationContext remoteContext, Uid result)
            {
                PRB.UpdateOpResponse response = new PRB.UpdateOpResponse();
                if (null != result)
                {
                    response.Uid = MessagesUtil.SerializeMessage<PRB.Uid>(result);
                }

                return new
                    PRB.RPCResponse
                {
                    OperationResponse = new PRB.OperationResponse
                    {
                        UpdateOpResponse = response
                    }
                };
            }

            protected internal override Uid ExecuteOperation(API.ConnectorFacade connectorFacade,
                PRB.UpdateOpRequest requestMessage)
            {
                ObjectClass objectClass = new ObjectClass(requestMessage.ObjectClass);
                Uid uid = MessagesUtil.DeserializeMessage<Uid>(requestMessage.Uid);

                var attributes = MessagesUtil.DeserializeLegacy<HashSet<Object>>(requestMessage.ReplaceAttributes);

                OperationOptions operationOptions = null;
                if (null != requestMessage.Options)
                {
                    operationOptions = MessagesUtil.DeserializeLegacy<OperationOptions>(requestMessage.Options);
                }

                switch (requestMessage.UpdateType)
                {
                    case PRB.UpdateOpRequest.Types.UpdateType.REPLACE:
                        return connectorFacade.Update(objectClass, uid,
                            CollectionUtil.NewSet<Object, ConnectorAttribute>(attributes), operationOptions);
                    case PRB.UpdateOpRequest.Types.UpdateType.ADD:
                        return connectorFacade.AddAttributeValues(objectClass, uid,
                            CollectionUtil.NewSet<Object, ConnectorAttribute>(attributes), operationOptions);
                    case PRB.UpdateOpRequest.Types.UpdateType.REMOVE:
                        return connectorFacade.RemoveAttributeValues(objectClass, uid,
                            CollectionUtil.NewSet<Object, ConnectorAttribute>(attributes), operationOptions);
                    default:
                        Trace.TraceInformation("Invalid UpdateOpRequest#UpdateType Request:{0}", RequestId);
                        break;
                }

                return null;
            }
        }
    }

    #endregion

    #region ValidateAsyncApiOpImpl

    public class ValidateAsyncApiOpImpl : AbstractAPIOperation, IValidateAsyncApiOp
    {
        public ValidateAsyncApiOpImpl(
            IRequestDistributor<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext>
                remoteConnection, API.ConnectorKey connectorKey,
            Func<RemoteOperationContext, ByteString> facadeKeyFunction, long timeout)
            : base(remoteConnection, connectorKey, facadeKeyFunction, timeout)
        {
        }

        public virtual void Validate()
        {
            try
            {
                ValidateAsync(CancellationToken.None).Wait();
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        public async Task ValidateAsync(CancellationToken cancellationToken)
        {
            await SubmitRequest<Object, PRB.ValidateOpResponse, InternalRequest>(
                new InternalRequestFactory(ConnectorKey, FacadeKeyFunction,
                    new PRB.OperationRequest { ValidateOpRequest = new PRB.ValidateOpRequest() },
                    cancellationToken));
        }

        private class InternalRequestFactory : AbstractRemoteOperationRequestFactory<Object, InternalRequest>
        {
            private readonly PRB.OperationRequest _operationRequest;

            public InternalRequestFactory(API.ConnectorKey connectorKey,
                Func<RemoteOperationContext, ByteString> facadeKeyFunction, PRB.OperationRequest operationRequest,
                CancellationToken cancellationToken)
                : base(connectorKey, facadeKeyFunction, cancellationToken)
            {
                _operationRequest = operationRequest;
            }

            public override InternalRequest CreateRemoteRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <object, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback)
            {
                PRB.RPCRequest builder = CreateRpcRequest(context);
                if (null != builder)
                {
                    return new InternalRequest(context, requestId, completionCallback, builder, CancellationToken);
                }
                else
                {
                    return null;
                }
            }

            protected internal override PRB.OperationRequest CreateOperationRequest(
                RemoteOperationContext remoteContext)
            {
                return _operationRequest;
            }
        }

        private class InternalRequest :
            AbstractRemoteOperationRequestFactory<Object, InternalRequest>.AbstractRemoteOperationRequest
                <Object, PRB.ValidateOpResponse>
        {
            public InternalRequest(RemoteOperationContext context, long requestId,
                Action
                    <
                        RemoteRequest
                            <Object, Exception, WebSocketConnectionGroup, WebSocketConnectionHolder,
                                RemoteOperationContext>> completionCallback, PRB.RPCRequest requestBuilder,
                CancellationToken cancellationToken)
                : base(context, requestId, completionCallback, requestBuilder, cancellationToken)
            {
            }

            protected internal override PRB.ValidateOpResponse GetOperationResponseMessages(
                PRB.OperationResponse message)
            {
                if (null != message.ValidateOpResponse)
                {
                    return message.ValidateOpResponse;
                }
                else
                {
                    Trace.TraceInformation(OperationExpectsMessage, RequestId, "ValidateOpResponse");
                    return null;
                }
            }

            protected internal override void HandleOperationResponseMessages(WebSocketConnectionHolder sourceConnection,
                PRB.ValidateOpResponse message)
            {
                HandleResult(null);
            }
        }

        // ----

        public static AbstractLocalOperationProcessor<Object, PRB.ValidateOpRequest> CreateProcessor(long requestId,
            WebSocketConnectionHolder socket, PRB.ValidateOpRequest message)
        {
            return new ValidateLocalOperationProcessor(requestId, socket, message);
        }

        private class ValidateLocalOperationProcessor : AbstractLocalOperationProcessor<Object, PRB.ValidateOpRequest>
        {
            protected internal ValidateLocalOperationProcessor(long requestId, WebSocketConnectionHolder socket,
                PRB.ValidateOpRequest message)
                : base(requestId, socket, message)
            {
            }

            protected override PRB.RPCResponse CreateOperationResponse(
                RemoteOperationContext remoteContext, Object result)
            {
                return new
                    PRB.RPCResponse
                {
                    OperationResponse = new PRB.OperationResponse
                    {
                        ValidateOpResponse = new PRB.ValidateOpResponse()
                    }
                };
            }

            protected internal override Object ExecuteOperation(API.ConnectorFacade connectorFacade,
                PRB.ValidateOpRequest requestMessage)
            {
                connectorFacade.Validate();
                return null;
            }
        }
    }

    #endregion
}