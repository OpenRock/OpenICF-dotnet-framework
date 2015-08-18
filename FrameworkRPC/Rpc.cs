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

namespace Org.ForgeRock.OpenICF.Common.RPC
{

    #region AbstractLoadBalancingAlgorithm

    /// <summary>
    ///     A AbstractLoadBalancingAlgorithm is a base class to implementing different
    ///     LoadBalancingAlgorithm for multiple <seealso cref="IRequestDistributor{TG,TH,TP}" />.
    /// </summary>
    public abstract class AbstractLoadBalancingAlgorithm<TG, TH, TP> : IRequestDistributor<TG, TH, TP>
        where TG : RemoteConnectionGroup<TG, TH, TP>
        where TH : IRemoteConnectionHolder<TG, TH, TP>
        where TP : IRemoteConnectionContext<TG, TH, TP>
    {
        protected internal readonly IList<IRequestDistributor<TG, TH, TP>> RequestDistributors;

        protected internal AbstractLoadBalancingAlgorithm(IList<IRequestDistributor<TG, TH, TP>> requestDistributors)
        {
            RequestDistributors = requestDistributors;
        }

        /// <summary>
        ///     {@inheritDoc}
        /// </summary>
        public virtual bool Operational
        {
            get { return RequestDistributors.Any(e => e.Operational); }
        }

        /// <summary>
        ///     {@inheritDoc}
        /// </summary>
        public virtual TR TrySubmitRequest<TR, TV, TE>(IRemoteRequestFactory<TR, TV, TE, TG, TH, TP> requestFactory)
            where TR : RemoteRequest<TV, TE, TG, TH, TP>
            where TE : Exception
        {
            return TrySubmitRequest(InitialConnectionFactoryIndex, requestFactory);
        }

        protected internal virtual TR TrySubmitRequest<TR, TV, TE>(int initialIndex,
            IRemoteRequestFactory<TR, TV, TE, TG, TH, TP> requestFactory)
            where TR : RemoteRequest<TV, TE, TG, TH, TP>
            where TE : Exception
        {
            int index = initialIndex;
            int maxIndex = RequestDistributors.Count;
            do
            {
                IRequestDistributor<TG, TH, TP> factory = RequestDistributors[index];
                TR result = factory.TrySubmitRequest(requestFactory);
                if (null != result)
                {
                    return result;
                }
                index = (index + 1)%maxIndex;
            } while (index != initialIndex);

            /*
             * All factories are offline so give up.
             */
            return null;
        }

        protected internal abstract int InitialConnectionFactoryIndex { get; }
    }

    #endregion

    #region FailoverLoadBalancingAlgorithm

    /// <summary>
    ///     A fail-over load balancing algorithm provides fault tolerance across multiple
    ///     underlying <seealso cref="IRequestDistributor{TG,TH,TP}" />s.
    ///     <para>
    ///         This algorithm is typically used for load-balancing <i>between</i> data
    ///         centers, where there is preference to always forward connection requests to
    ///         the <i>closest available</i> data center. This algorithm contrasts with the
    ///         <seealso cref="RoundRobinLoadBalancingAlgorithm{TG,TH,TP}" /> which is used for load-balancing
    ///         <i>within</i> a data center.
    ///     </para>
    ///     <para>
    ///         This algorithm selects <seealso cref="IRequestDistributor{TG,TH,TP}" />s based on the order in
    ///         which they were provided during construction. More specifically, an attempt
    ///         to obtain a <seealso cref="IRequestDistributor{TG,TH,TP}" /> will always return the
    ///         <i>
    ///             first
    ///             operational
    ///         </i>
    ///         <seealso cref="IRequestDistributor{TG,TH,TP}" /> in the list. Applications should,
    ///         therefore, organize the connection factories such that the <i>preferred</i>
    ///         (usually the closest) <seealso cref="IRequestDistributor{TG,TH,TP}" /> appear before those which
    ///         are less preferred.
    ///     </para>
    ///     <para>
    ///         If a problem occurs that temporarily prevents connections from being obtained
    ///         for one of the <seealso cref="IRequestDistributor{TG,TH,TP}" />, then this algorithm automatically
    ///         "fails over" to the next operational <seealso cref="IRequestDistributor{TG,TH,TP}" /> in the list.
    ///         If none of the <seealso cref="IRequestDistributor{TG,TH,TP}" /> are operational then a {@code null}
    ///         is returned to the client.
    ///     </para>
    ///     <para>
    ///     </para>
    /// </summary>
    /// <seealso cref="RoundRobinLoadBalancingAlgorithm{TG,TH,TP}" />
    public class FailoverLoadBalancingAlgorithm<TG, TH, TP> : AbstractLoadBalancingAlgorithm<TG, TH, TP>
        where TG : RemoteConnectionGroup<TG, TH, TP>
        where TH : IRemoteConnectionHolder<TG, TH, TP>
        where TP : IRemoteConnectionContext<TG, TH, TP>
    {
        public FailoverLoadBalancingAlgorithm(IList<IRequestDistributor<TG, TH, TP>> requestDistributors)
            : base(requestDistributors)
        {
        }

        protected internal override int InitialConnectionFactoryIndex
        {
            get { return 0; }
        }
    }

    #endregion

    #region LocalRequest

    /// <summary>
    ///     A LocalRequest represents a remotely requested procedure call locally.
    ///     <p />
    ///     The <seealso cref="RemoteRequest{TV,TE,TG,TH,TP}" /> and LocalRequest are the representation of the same
    ///     call on caller and receiver side.
    /// </summary>
    public abstract class LocalRequest<TV, TE, TG, TH, TP> : IDisposable
        where TE : Exception
        where TG : RemoteConnectionGroup<TG, TH, TP>
        where TH : IRemoteConnectionHolder<TG, TH, TP>
        where TP : IRemoteConnectionContext<TG, TH, TP>
    {
        private readonly long _requestId;

        private readonly TP _remoteConnectionContext;

        protected internal LocalRequest(long requestId, TH socket)
        {
            _requestId = requestId;
            _remoteConnectionContext = socket.RemoteConnectionContext;
            _remoteConnectionContext.RemoteConnectionGroup.ReceiveRequest<TV, TE, LocalRequest<TV, TE, TG, TH, TP>>(this);
        }

        protected internal abstract bool TryHandleResult(TV result);

        protected internal abstract bool TryHandleError(TE error);

        protected internal abstract bool TryCancel();

        public long RequestId
        {
            get { return _requestId; }
        }

        public TP RemoteConnectionContext
        {
            get { return _remoteConnectionContext; }
        }

        public void Dispose()
        {
            _remoteConnectionContext.RemoteConnectionGroup.RemoveRequest(RequestId);
            TryCancel();
        }

        public void HandleResult(TV result)
        {
            _remoteConnectionContext.RemoteConnectionGroup.RemoveRequest(RequestId);
            TryHandleResult(result);
        }

        public void HandleError(TE error)
        {
            _remoteConnectionContext.RemoteConnectionGroup.RemoveRequest(RequestId);
            TryHandleError(error);
        }

        public virtual void HandleIncomingMessage(TH sourceConnection, Object message)
        {
            throw new NotSupportedException("This request does not supports");
        }
    }

    #endregion

    #region MessageListener

    public interface IMessageListener<TG, in TH, TP>
        where TG : RemoteConnectionGroup<TG, TH, TP>
        where TH : IRemoteConnectionHolder<TG, TH, TP>
        where TP : IRemoteConnectionContext<TG, TH, TP>
    {
        /// <summary>
        ///     <para>
        ///         Invoked when the opening handshake has been completed for a specific
        ///         <seealso cref="IRemoteConnectionHolder{TG,TH,TP}" /> instance.
        ///     </para>
        /// </summary>
        /// <param name="socket">
        ///     the newly connected <seealso cref="IRemoteConnectionHolder{TG,TH,TP}" />
        /// </param>
        void OnConnect(TH socket);

        /// <summary>
        ///     <para>
        ///         Invoked when <seealso cref="IRemoteConnectionHolder{TG,TH,TP}#Dispose()" /> has been called on a
        ///         particular <seealso cref="IRemoteConnectionHolder{TG,TH,TP}" /> instance.
        ///     </para>
        ///     <para>
        ///     </para>
        /// </summary>
        /// <param name="socket">
        ///     the <seealso cref="IRemoteConnectionHolder{TG,TH,TP}" /> being closed.
        /// </param>
        /// <param name="code">
        ///     the closing code sent by the remote end-point.
        /// </param>
        /// <param name="reason">
        ///     the closing reason sent by the remote end-point.
        /// </param>
        void OnClose(TH socket, int code, string reason);

        /// <summary>
        ///     <para>
        ///         Invoked when the <seealso cref="IRemoteConnectionHolder{TG,TH,TP}" /> is open and an error
        ///         occurs processing the request.
        ///     </para>
        /// </summary>
        /// <param name="t"> </param>
        void OnError(Exception t);

        /// <summary>
        ///     <para>
        ///         Invoked when <seealso cref="IRemoteConnectionHolder{TG,TH,TP}.SendBytesAsync" /> has been
        ///         called on a particular <seealso cref="IRemoteConnectionHolder{TG,TH,TP}" /> instance.
        ///     </para>
        /// </summary>
        /// <param name="socket">
        ///     the <seealso cref="IRemoteConnectionHolder{TG,TH,TP}" /> that received a message.
        /// </param>
        /// <param name="data">
        ///     the message received.
        /// </param>
        void OnMessage(TH socket, byte[] data);

        /// <summary>
        ///     <para>
        ///         Invoked when <seealso cref="IRemoteConnectionHolder{TG,TH,TP}.SendStringAsync" /> has been
        ///         called on a particular <seealso cref="IRemoteConnectionHolder{TG,TH,TP}" /> instance.
        ///     </para>
        /// </summary>
        /// <param name="socket">
        ///     the <seealso cref="IRemoteConnectionHolder{TG,TH,TP}" /> that received a message.
        /// </param>
        /// <param name="data">
        ///     the message received.
        /// </param>
        void OnMessage(TH socket, string data);

        /// <summary>
        ///     <para>
        ///         Invoked when <seealso cref="IRemoteConnectionHolder{TG,TH,TP}.SendPing" /> has been
        ///         called on a particular <seealso cref="IRemoteConnectionHolder{TG,TH,TP}" /> instance.
        ///     </para>
        /// </summary>
        /// <param name="socket">
        ///     the <seealso cref="IRemoteConnectionHolder{TG,TH,TP}" /> that received the ping.
        /// </param>
        /// <param name="bytes">
        ///     the payload of the ping frame, if any.
        /// </param>
        void OnPing(TH socket, byte[] bytes);

        /// <summary>
        ///     <para>
        ///         Invoked when <seealso cref="IRemoteConnectionHolder{TG,TH,TP}.SendPong" /> has been
        ///         called on a particular <seealso cref="IRemoteConnectionHolder{TG,TH,TP}" /> instance.
        ///     </para>
        /// </summary>
        /// <param name="socket">
        ///     the <seealso cref="IRemoteConnectionHolder{TG,TH,TP}" /> that received the pong.
        /// </param>
        /// <param name="bytes">
        ///     the payload of the pong frame, if any.
        /// </param>
        void OnPong(TH socket, byte[] bytes);
    }

    #endregion

    #region RemoteConnectionContext

    /// <summary>
    ///     A RemoteConnectionContext is a custom context to provide application specific
    ///     information to create the
    ///     <seealso cref="RemoteRequest{TV,TE,TG,TH,TP}" />.
    ///     <p />
    ///     The <seealso cref="RemoteRequest{TV,TE,TG,TH,TP}" /> may depends on
    ///     which <seealso cref="RemoteConnectionGroup" />
    ///     distributes the request. Instance of this class is provided to
    ///     <seealso cref="IRemoteRequestFactory{TR,TV,TE,TG,TH,TP}" /> to produce the
    ///     <seealso cref="RemoteRequest{TV,TE,TG,TH,TP}" /> before
    ///     <seealso cref="RemoteConnectionGroup" />
    ///     sending the message.
    /// </summary>
    public interface IRemoteConnectionContext<out TG, TH, TP>
        where TG : RemoteConnectionGroup<TG, TH, TP>
        where TH : IRemoteConnectionHolder<TG, TH, TP>
        where TP : IRemoteConnectionContext<TG, TH, TP>
    {
        /// <summary>
        ///     Return the <seealso cref="RemoteConnectionGroup" />
        ///     <p />
        ///     Return the <seealso cref="RemoteConnectionGroup" />
        ///     in which this instance belongs to.
        /// </summary>
        /// <returns>
        ///     the
        ///     <seealso cref="RemoteConnectionGroup" /> in
        ///     which this instance belongs to.
        /// </returns>
        TG RemoteConnectionGroup { get; }
    }

    #endregion

    #region RemoteConnectionGroup

    public abstract class RemoteConnectionGroup<TG, TH, TP> : IRequestDistributor<TG, TH, TP>
        where TG : RemoteConnectionGroup<TG, TH, TP>
        where TH : IRemoteConnectionHolder<TG, TH, TP>
        where TP : IRemoteConnectionContext<TG, TH, TP>
    {
        public abstract class AsyncMessageQueueRecord : IDisposable
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
            private Int32 _busy;
            private bool _canFail;
            private readonly HashSet<TH> _references = new HashSet<TH>();
            private readonly TaskCompletionSource<TH> _completionHandler = new TaskCompletionSource<TH>();

            protected abstract Task DoSend(TH connection);

            public bool Accept(TH connection)
            {
                if (!_canFail && !_completionHandler.Task.IsCompleted)
                {
                    return _references.Add(connection);
                }
                return false;
            }

            /// <summary>
            ///     Called when ConnnectionHolder will not try to write this message any more
            /// </summary>
            public void Detach(TH connection)
            {
                if (_references.Remove(connection) && !_references.Any())
                {
                    NotifyFailureAndRecycle(new Exception("No Connection is available to send the message"));
                }
            }

            public async Task<TH> SendAsync(TimeSpan timeout)
            {
#if DEBUG
                Trace.TraceInformation("Sending Async Message {0}", Id);
#endif
                _canFail = true;
                if (!_completionHandler.Task.IsCompleted)
                {
                    if (_busy == 1 || _references.Any())
                    {
                        await Task.WhenAny(_completionHandler.Task, Task.Run(async () =>
                        {
                            do
                            {
                                await Task.Delay(timeout);
                                _references.Clear();
                            } while (_busy == 1);
                            NotifyFailureAndRecycle(new TimeoutException("WriteTimeout reached"));
                        }));
                    }
                    else
                    {
                        _completionHandler.SetException(new Exception("No Connection was available to send the message"));
                    }
                }
                return await _completionHandler.Task;
            }

            /// <summary>
            ///     Called when ConnnectionHolder try to gain exclusive access to write this message.
            /// </summary>
            /// <returns></returns>
            public async Task<bool> AcquireAndTryComplete(TH connection)
            {
                if (null != connection && !_completionHandler.Task.IsCompleted &&
                    Interlocked.Increment(ref _busy) == 1)
                {
                    try
                    {
                        if (_references.Remove(connection))
                        {
#if DEBUG
                            Debug.WriteLine("Sending message:{0} over connection:{1}:{2} bussy:{3}", Id,
                                connection.Id, connection.GetType().FullName, _busy);
#endif
                            await DoSend(connection);
                            //Sent successfully
                            NotifyCompleteAndRecycle(connection);
#if DEBUG
                            Debug.WriteLine("Sent message:{0} over connection:{1} completed:{2}", Id,
                                connection.Id, _completionHandler.Task.IsCompleted);
#endif
                        }
                    }
                    catch (Exception e)
                    {
                        if (!_references.Any())
                        {
                            NotifyFailureAndRecycle(e);
                        }
                    }
                    finally
                    {
                        _busy = 0;
                    }
                    return true;
                }
                return _completionHandler.Task.IsCompleted;
            }

            public void Dispose()
            {
                _completionHandler.SetCanceled();
                _references.Clear();
                Recycle();
            }

            protected virtual void NotifyFailureAndRecycle(Exception e)
            {
                if (_canFail && !_completionHandler.Task.IsCompleted)
                {
                    _completionHandler.SetException(e);
                    _references.Clear();
                    Recycle();
                }
            }

            protected virtual void NotifyCompleteAndRecycle(TH connection)
            {
                _completionHandler.SetResult(connection);
                _references.Clear();
                Recycle();
            }

            protected void Recycle()
            {
            }
        }

        protected readonly ConcurrentDictionary<Int64, dynamic> RemoteRequests =
            new ConcurrentDictionary<Int64, dynamic>();

        protected readonly ConcurrentDictionary<Int64, dynamic> LocalRequests =
            new ConcurrentDictionary<Int64, dynamic>();

        protected readonly ConcurrentDictionary<TH, string> WebSockets = new ConcurrentDictionary<TH, string>();

        private Int64 _messageId;

        protected RemoteConnectionGroup(string remoteSessionId)
        {
            RemoteSessionId = remoteSessionId;
        }

        public abstract bool Operational { get; }

        public string RemoteSessionId { get; internal set; }

        protected internal abstract TP RemoteConnectionContext { get; }

        protected internal virtual Int64 NextRequestId
        {
            get
            {
                Int64 next = Interlocked.Increment(ref _messageId);
                while (next == 0)
                {
                    next = Interlocked.Increment(ref _messageId);
                }
                return next;
            }
        }

        protected internal virtual TR AllocateRequest<TR, TV, TE>(
            IRemoteRequestFactory<TR, TV, TE, TG, TH, TP> requestFactory)
            where TR : RemoteRequest<TV, TE, TG, TH, TP>
            where TE : Exception
        {
            Int64 newMessageId;
            TR request;
            Action<RemoteRequest<TV, TE, TG, TH, TP>> completionCallback = x =>
            {
                dynamic ignored;
                RemoteRequests.TryRemove(x.RequestId, out ignored);
            };

            do
            {
                newMessageId = NextRequestId;
                request = requestFactory.CreateRemoteRequest(RemoteConnectionContext, newMessageId, completionCallback);
                if (null == request)
                {
                    break;
                }
            } while (!RemoteRequests.TryAdd(newMessageId, request));
            return request;
        }

        public TV TrySendMessage<TV>(Func<Func<AsyncMessageQueueRecord, Task<TH>>, TV> function)
        {
            return function(messsage =>
            {
                foreach (var connection in WebSockets.Keys)
                {
                    connection.Enqueue(messsage);
                }
                return messsage.SendAsync(TimeSpan.FromMinutes(2));
            });
        }

        // -- Pair of methods to Submit and Receive the new Request Start --
        public virtual TR TrySubmitRequest<TR, TV, TE>(IRemoteRequestFactory<TR, TV, TE, TG, TH, TP> requestFactory)
            where TR : RemoteRequest<TV, TE, TG, TH, TP>
            where TE : Exception
        {
            TR remoteRequest = AllocateRequest(requestFactory);
            if (null != remoteRequest)
            {
                Task result = TrySendMessage(remoteRequest.SendFunction);
                if (null == result)
                {
                    dynamic ignored;
                    RemoteRequests.TryRemove(remoteRequest.RequestId, out ignored);
                    remoteRequest = null;
                }
                else if (remoteRequest.Promise == null)
                {
                    Trace.TraceError("FATAL: Concurrency Problem! Prmoise can not be null if result is not null.");
                }
            }
            return remoteRequest;
        }

        public virtual TR ReceiveRequest<TV, TE, TR>(TR localRequest)
            where TE : Exception
            where TR : LocalRequest<TV, TE, TG, TH, TP>
        {
            TR tmp = LocalRequests.GetOrAdd(localRequest.RequestId, localRequest);
            if (null != tmp && !tmp.Equals(localRequest))
            {
                throw new InvalidOperationException("Request has been registered with id: " + localRequest.RequestId);
            }
            return localRequest;
        }

        // -- Pair of methods to Submit and Receive the new Request End --
        // -- Pair of methods to Cancel pending Request Start --

        public dynamic /*RemoteRequest<dynamic, Exception, TG, TH, TP>*/ SubmitRequestCancel(Int64 messageId)
        {
            dynamic tmp;
            RemoteRequests.TryRemove(messageId, out tmp);
            if (null != tmp)
            {
                tmp.Cancel();
            }
            return tmp;
        }

        public dynamic /*LocalRequest<dynamic, Exception, TG, TH, TP>*/ ReceiveRequestCancel(Int64 messageId)
        {
            dynamic tmp;
            LocalRequests.TryRemove(messageId, out tmp);
            if (null != tmp)
            {
                tmp.Cancel();
            }
            return tmp;
        }

        // -- Pair of methods to Cancel pending Request End --
        // -- Pair of methods to Communicate pending Request Start --
        public dynamic /*RemoteRequest<dynamic, Exception, TG, TH, TP>*/ ReceiveRequestResponse(TH sourceConnection,
            Int64 messageId, Object message)
        {
            dynamic tmp;
            RemoteRequests.TryGetValue(messageId, out tmp);
            if (null != tmp)
            {
                tmp.HandleIncomingMessage(sourceConnection, message);
            }
            return tmp;
        }

        public dynamic /*LocalRequest<dynamic, Exception, TG, TH, TP>*/ ReceiveRequestUpdate(TH sourceConnection,
            Int64 messageId, Object message)
        {
            dynamic tmp;
            LocalRequests.TryGetValue(messageId, out tmp);
            if (null != tmp)
            {
                tmp.HandleIncomingMessage(sourceConnection, message);
            }
            return tmp;
        }

        // -- Pair of methods to Communicate pending Request End --
        public dynamic /*LocalRequest<dynamic, Exception, TG, TH, TP>*/ RemoveRequest(Int64 messageId)
        {
            dynamic value;
            LocalRequests.TryRemove(messageId, out value);
            return value;
        }
    }

    #endregion

    #region RemoteConnectionHolder

    /// <summary>
    ///     A RemoteConnectionHolder is a wrapper class for the underlying communication
    ///     chanel.
    ///     <p />
    ///     The API is detached from the real underlying protocol and abstracted through
    ///     this interface. The message transmitted via this implementation should
    ///     trigger the appropriate method on
    ///     <seealso cref="IMessageListener{TG,TH,TP}" />.
    /// </summary>
    public interface IRemoteConnectionHolder<TG, TH, TP> : IDisposable
        where TG : RemoteConnectionGroup<TG, TH, TP>
        where TH : IRemoteConnectionHolder<TG, TH, TP>
        where TP : IRemoteConnectionContext<TG, TH, TP>
    {
#if DEBUG
        Int32 Id { get; }
#endif

        TP RemoteConnectionContext { get; }

        void Enqueue(RemoteConnectionGroup<TG, TH, TP>.AsyncMessageQueueRecord record);


        /// <summary>
        ///     Initiates the asynchronous transmission of a binary message. This method
        ///     returns before the message is transmitted. Developers may use the
        ///     returned Future object to track progress of the transmission.
        /// </summary>
        /// <param name="data">
        ///     The data to be sent over the connection.
        /// </param>
        /// <param name="cancellationToken">The token that propagates the notification that operations should be canceled.</param>
        /// <returns> the Future object representing the send operation. </returns>
        Task SendBytesAsync(byte[] data, CancellationToken cancellationToken);

        /// <summary>
        ///     Initiates the asynchronous transmission of a string message. This method
        ///     returns before the message is transmitted. Developers may use the
        ///     returned Future object to track progress of the transmission.
        /// </summary>
        /// <param name="data">
        ///     the data being sent
        /// </param>
        /// <param name="cancellationToken">The token that propagates the notification that operations should be canceled.</param>
        /// <returns> the Future object representing the send operation. </returns>
        Task SendStringAsync(string data, CancellationToken cancellationToken);

        /// <summary>
        ///     Send a Ping message containing the given application data to the remote
        ///     endpoint. The corresponding Pong message may be picked up using the
        ///     MessageHandler.Pong handler.
        /// </summary>
        /// <param name="applicationData">
        ///     the data to be carried in the ping request
        /// </param>
        void SendPing(byte[] applicationData);

        /// <summary>
        ///     Allows the developer to send an unsolicited Pong message containing the
        ///     given application data in order to serve as a unidirectional heartbeat
        ///     for the session.
        /// </summary>
        /// <param name="applicationData">
        ///     the application data to be carried in the pong response.
        /// </param>
        void SendPong(byte[] applicationData);
    }

    #endregion

    #region RemoteRequest

    /// <summary>
    ///     A RemoteRequest represents a locally requested procedure call executed
    ///     remotely.
    ///     <p />
    ///     The RemoteRequest and <seealso cref="LocalRequest{TV,TE,TG,TH,TP}" /> are the representation of the same
    ///     call on caller and receiver side.
    /// </summary>
    public abstract class RemoteRequest<TV, TE, TG, TH, TP> : IDisposable
        where TE : Exception
        where TG : RemoteConnectionGroup<TG, TH, TP>
        where TH : IRemoteConnectionHolder<TG, TH, TP>
        where TP : IRemoteConnectionContext<TG, TH, TP>
    {
        private readonly TP _context;
        private readonly Int64 _requestId;
        private readonly Action<RemoteRequest<TV, TE, TG, TH, TP>> _completionCallback;

        private TaskCompletionSource<TV> _promise;
        private readonly CancellationToken _cancellationToken;
        private readonly ReaderWriterLock _lock = new ReaderWriterLock();

        protected RemoteRequest(TP context, Int64 requestId,
            Action<RemoteRequest<TV, TE, TG, TH, TP>> completionCallback, CancellationToken cancellationToken)
        {
            _context = context;
            _requestId = requestId;
            _completionCallback = completionCallback;
            _cancellationToken = cancellationToken;
        }

        public abstract void HandleIncomingMessage(TH sourceConnection, Object message);

        protected internal abstract RemoteConnectionGroup<TG, TH, TP>.AsyncMessageQueueRecord CreateMessageElement(
            TP remoteContext, long requestId);

        protected internal abstract void TryCancelRemote(TP remoteContext, Int64 requestId);
        protected internal abstract TE CreateCancellationException(Exception cancellationException);

        public virtual Int64 RequestId
        {
            get { return _requestId; }
        }

        public virtual Int64 RequestTime { get; internal set; }

        public virtual Task<TV> Promise
        {
            get { return _promise.Task; }
        }

        /// <summary>
        ///     Invoked when the asynchronous task has completed successfully.
        /// </summary>
        /// <param name="result">
        ///     The result of the asynchronous task.
        /// </param>
        protected internal virtual void HandleResult(TV result)
        {
            if (!_promise.Task.IsCompleted)
            {
                try
                {
                    _promise.SetResult(result);
                }
                catch (InvalidOperationException)
                {
                    //An attempt was made to transition a task to a final state when it had already completed.
                }
            }
        }

        /// <summary>
        ///     Invoked when the asynchronous task has failed.
        /// </summary>
        /// <param name="error">
        ///     The error indicating why the asynchronous task has failed.
        /// </param>
        protected internal virtual void HandleError(Exception error)
        {
            if (!_promise.Task.IsCompleted)
            {
                try
                {
                    _promise.SetException(error);
                }
                catch (InvalidOperationException)
                {
                    //An attempt was made to transition a task to a final state when it had already completed.
                }
            }
        }

        protected internal virtual TP ConnectionContext
        {
            get { return _context; }
        }


        public CancellationToken CancellationToken
        {
            get { return _cancellationToken; }
        }

        public void Dispose()
        {
            try
            {
                TryCancelRemote(_context, _requestId);
            }
            catch (Exception)
            {
                //Ignore
            }
        }

        public virtual Func<Func<RemoteConnectionGroup<TG, TH, TP>.AsyncMessageQueueRecord, Task<TH>>, Task<TV>>
            SendFunction
        {
            get
            {
                return async send =>
                {
                    TaskCompletionSource<TV> resultPromise = _promise;
                    if (null == resultPromise)
                    {
                        RemoteConnectionGroup<TG, TH, TP>.AsyncMessageQueueRecord message =
                            CreateMessageElement(_context, _requestId);
                        if (message == null)
                        {
                            throw new InvalidOperationException("RemoteRequest has empty message");
                        }

                        try
                        {
                            // Single thread should process it so it should not fail
                            _lock.AcquireWriterLock(new TimeSpan(0, 1, 0));
                            IDisposable cancellationTokenRegistration = null;
                            try
                            {
                                if (null == _promise)
                                {
                                    _promise = new TaskCompletionSource<TV>();
                                    // ReSharper disable once ImpureMethodCallOnReadonlyValueField
                                    cancellationTokenRegistration = _cancellationToken.Register(promise =>
                                    {
                                        var p = promise as TaskCompletionSource<TV>;
                                        if (null != p)
                                        {
                                            p.TrySetCanceled();
                                        }
                                        Dispose();
                                    }, _promise);
                                    _promise.Task.ContinueWith(x => { _completionCallback(this); })
                                        .ConfigureAwait(false);
                                    if (_cancellationToken.IsCancellationRequested)
                                    {
                                        _promise.SetCanceled();
                                    }
                                    else
                                    {
                                        await send(message);
                                    }
                                    RequestTime = DateTime.Now.Ticks;
                                }
                            }
                            catch (Exception)
                            {
                                _promise = null;
                                if (null != cancellationTokenRegistration)
                                {
                                    cancellationTokenRegistration.Dispose();
                                }
                                throw;
                            }
                            finally
                            {
                                _lock.ReleaseWriterLock();
                            }
                        }
                        catch (ApplicationException)
                        {
                            // The writer lock request timed out.
                        }

                        return null != _promise ? await _promise.Task : default(TV);
                    }
                    return await resultPromise.Task;
                };
            }
        }
    }

    #endregion

    #region RemoteRequestFactory

    /// <summary>
    ///     A RemoteRequestFactory creates a new
    ///     <seealso cref="IRemoteConnectionContext{TG,TH,TP}" /> aware
    ///     <seealso cref="RemoteRequest{TV,TE,TG,TH,TP}" /> before sending in
    ///     <seealso cref="RemoteConnectionGroup{TG,TH,TP}" />.
    /// </summary>
    public interface IRemoteRequestFactory<out TR, TV, TE, TG, TH, TP>
        where TR : RemoteRequest<TV, TE, TG, TH, TP>
        where TE : Exception
        where TG : RemoteConnectionGroup<TG, TH, TP>
        where TH : IRemoteConnectionHolder<TG, TH, TP>
        where TP : IRemoteConnectionContext<TG, TH, TP>
    {
        TR CreateRemoteRequest(TP context, long requestId, Action<RemoteRequest<TV, TE, TG, TH, TP>> completionCallback);
    }

    #endregion

    #region RequestDistributor

    /// <summary>
    ///     A RequestDistributor delivers the
    ///     <seealso cref="RemoteRequest{TV,TE,TG,TH,TP}" /> to the connected
    ///     endpoint.
    ///     <p />
    ///     The <seealso cref="IRemoteRequestFactory{TR,TV,TE,TG,TH,TP}" /> is used to
    ///     create a <seealso cref="IRemoteConnectionContext{TG,TH,TP}" />
    ///     aware <seealso cref="RemoteRequest{TV,TE,TG,TH,TP}" /> which will be
    ///     delivered.
    ///     <p />
    ///     The implementation may hold multiple transmission channels and try all to
    ///     deliver the message before if fails.
    ///     <p />
    ///     The failed delivery signaled with null empty to avoid the expensive Throw and
    ///     Catch especially when many implementation are chained together.
    /// </summary>
    /// <seealso cref="FailoverLoadBalancingAlgorithm{TG,TH,TP}" />
    /// <seealso cref="RoundRobinLoadBalancingAlgorithm{TG,TH,TP}" />
    /// <seealso cref="RemoteConnectionGroup{TG,TH,TP}" />
    public interface IRequestDistributor<TG, TH, TP>
        where TG : RemoteConnectionGroup<TG, TH, TP>
        where TH : IRemoteConnectionHolder<TG, TH, TP>
        where TP : IRemoteConnectionContext<TG, TH, TP>
    {
        /// <param name="requestFactory">
        ///     the factory to create the
        ///     <seealso cref="RemoteRequest{TV,TE,TG,TH,TP}" />
        /// </param>
        /// <typeparam name="TR">
        ///     type of <seealso cref="RemoteRequest{TV,TE,TG,TH,TP}" />
        /// </typeparam>
        /// <typeparam name="TV">
        ///     The type of the task's result, or <seealso cref="Void" /> if the task
        ///     does not return anything (i.e. it only has side-effects).
        /// </typeparam>
        /// <typeparam name="TE">
        ///     The type of the exception thrown by the task if it fails
        /// </typeparam>
        /// <returns> new promise if succeeded otherwise {@code null}. </returns>
        TR TrySubmitRequest<TR, TV, TE>(IRemoteRequestFactory<TR, TV, TE, TG, TH, TP> requestFactory)
            where TR : RemoteRequest<TV, TE, TG, TH, TP>
            where TE : Exception;

        /// <summary>
        ///     Check if this implementation is operational.
        /// </summary>
        /// <returns>
        ///     {@code true} is operational and ready the submit
        ///     <seealso cref="IRemoteRequestFactory{TR,TV,TE,TG,TH,TP}" />
        ///     otherwise {@code false}.
        /// </returns>
        bool Operational { get; }
    }

    #endregion

    #region RoundRobinLoadBalancingAlgorithm

    /// <summary>
    ///     A round robin load balancing algorithm distributes
    ///     <seealso cref="RemoteRequest{TV,TE,TG,TH,TP}" />s across a list of
    ///     <seealso cref="IRequestDistributor{TG,TH,TP}" />s one at a time. When the end of the list is
    ///     reached, the algorithm starts again from the beginning.
    ///     <para>
    ///         This algorithm is typically used for load-balancing <i>within</i> data
    ///         centers, where load must be distributed equally across multiple servers. This
    ///         algorithm contrasts with the <seealso cref="FailoverLoadBalancingAlgorithm{TG,TH,TP}" /> which is
    ///         used for load-balancing <i>between</i> data centers.
    ///     </para>
    ///     <para>
    ///         If a problem occurs that temporarily prevents connections from being obtained
    ///         for one of the <seealso cref="IRequestDistributor{TG,TH,TP}" />s, then this algorithm automatically
    ///         "fails over" to the next operational <seealso cref="IRequestDistributor{TG,TH,TP}" /> in the list.
    ///         If none of the <seealso cref="IRequestDistributor{TG,TH,TP}" /> are operational then a {@code null}
    ///         is returned to the client.
    ///     </para>
    ///     <para>
    ///     </para>
    /// </summary>
    /// <seealso cref="FailoverLoadBalancingAlgorithm{TG,TH,TP}" />
    public class RoundRobinLoadBalancingAlgorithm<TG, TH, TP> : AbstractLoadBalancingAlgorithm<TG, TH, TP>
        where TG : RemoteConnectionGroup<TG, TH, TP>
        where TH : IRemoteConnectionHolder<TG, TH, TP>
        where TP : IRemoteConnectionContext<TG, TH, TP>
    {
        private Int32 _counter;

        public RoundRobinLoadBalancingAlgorithm(IList<IRequestDistributor<TG, TH, TP>> requestDistributors)
            : base(requestDistributors)
        {
        }

        protected internal override int InitialConnectionFactoryIndex
        {
            get
            {
                // A round robin pool of one connection factories is unlikely in
                // practice and requires special treatment.
                int maxSize = RequestDistributors.Count();
                if (maxSize == 1)
                {
                    return 0;
                }
                return (Interlocked.Increment(ref _counter) & 0x7fffffff)%maxSize;
            }
        }
    }

    #endregion
}