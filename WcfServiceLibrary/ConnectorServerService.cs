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
using System.Collections.Specialized;
using System.Configuration;
using System.Diagnostics;
using System.IdentityModel.Claims;
using System.IdentityModel.Policy;
using System.IdentityModel.Selectors;
using System.IdentityModel.Tokens;
using System.Linq;
using System.Net.WebSockets;
using System.Security.Principal;
using System.ServiceModel;
using System.ServiceModel.Activation;
using System.ServiceModel.Channels;
using System.ServiceModel.Description;
using System.ServiceModel.Dispatcher;
using System.ServiceModel.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Org.ForgeRock.OpenICF.Common.ProtoBuf;
using Org.ForgeRock.OpenICF.Common.RPC;
using Org.ForgeRock.OpenICF.Framework.Remote;
using Org.IdentityConnectors.Common;
using Org.IdentityConnectors.Common.Security;

namespace Org.ForgeRock.OpenICF.Framework.Service.WcfServiceLibrary
{
    [ServiceBehavior(InstanceContextMode = InstanceContextMode.PerSession)]
    public class WcfWebsocket : WebSocketConnectionHolder, IWebSocketService
    {
        private readonly IWebSocketCallback _callback;
        private ConnectionPrincipal _principal;

        //WCF uses this method when it's public
        private static void Configure(ServiceConfiguration configuration)
        {
            Trace.TraceInformation("Blank Configuration");
        }

        public WcfWebsocket()
        {
            _callback = OperationContext.Current.GetCallbackChannel<IWebSocketCallback>();
            OperationContext.Current.InstanceContext.Closed += InstanceContext_Closed;
            Task.Factory.StartNew(WriteMessageAsync);
        }

        private void InstanceContext_Closed(object sender, EventArgs e)
        {
            Trace.TraceInformation("Session closed here");
            _principal.OperationMessageListener.OnClose(this, 1000, "");
        }

        public async Task OnMessage(Message message)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            WebSocketMessageProperty property =
                (WebSocketMessageProperty) message.Properties[WebSocketMessageProperty.Name];


            if (!message.IsEmpty)
            {
                byte[] body = message.GetBody<byte[]>();
                if (body == null || body.Length == 0) // Connection open message
                {
                    // _principal.OperationMessageListener.OnConnect(this);
                }
                else if (property.MessageType == WebSocketMessageType.Text)
                {
                    string content = Encoding.UTF8.GetString(body);
                    _principal.OperationMessageListener.OnMessage(this, content);
                }
                else if (property.MessageType == WebSocketMessageType.Binary)
                {
                    _principal.OperationMessageListener.OnMessage(this, body);
                }
                else
                {
                    //Close Message  
                    //TODO Debug the close reason
                    _principal.OperationMessageListener.OnClose(this, 1000, "");
                }
            }
            await Task.Yield();
        }

        public void OnOpen()
        {
            Trace.TraceInformation("WCFWebsocket is Opened");
            _principal = Thread.CurrentPrincipal as ConnectionPrincipal;
            if (_principal == null)
            {
                throw new Exception("Unauthenticated");
            }
            _principal.OperationMessageListener.OnConnect(this);
        }


        protected override async Task WriteMessageAsync(byte[] entry, WebSocketMessageType messageType)
        {
            Message message = ByteStreamMessage.CreateMessage(
                new ArraySegment<byte>(entry));
            message.Properties[WebSocketMessageProperty.Name] =
                new WebSocketMessageProperty {MessageType = messageType};
            await _callback.OnMessage(message);
        }

        private RemoteOperationContext _context;

        protected override void Handshake(HandshakeMessage message)
        {
            _context = _principal.Handshake(this, message);

            if (null != _context)
            {
                Trace.TraceInformation("Client Connection Handshake succeeded");
            }
            else
            {
                Trace.TraceError("Client Connection Handshake failed - Close Connection");
                TryClose();
            }
        }

        protected override void TryClose()
        {
            OperationContext.Current.Channel.Close();
        }

        public override bool Operational
        {
            get
            {
                var communicationObject = _callback as ICommunicationObject; //IContextChannel
                return communicationObject != null && communicationObject.State == CommunicationState.Opened;
            }
        }

        public override RemoteOperationContext RemoteConnectionContext
        {
            get { return _context; }
        }
    }

    #region ConnectorServiceHostFactory

    public class ConnectorServiceHostFactory : ServiceHostFactory
    {
        private const string PropKey = "connectorserver.key";

        private readonly ClientAuthenticationValidator _authenticator;

        public ConnectorServiceHostFactory(ClientAuthenticationValidator authenticator)
        {
            if (authenticator == null)
            {
                throw new ArgumentNullException("authenticator");
            }
            _authenticator = authenticator;
        }

        public ConnectorServiceHostFactory()
        {
            NameValueCollection settings = ConfigurationManager.AppSettings;
            String authenticatorType = settings.Get(typeof (ClientAuthenticationValidator).FullName);
            if (String.IsNullOrEmpty(authenticatorType))
            {
                throw new Exception("Missing required app configuration: " +
                                    typeof (ClientAuthenticationValidator).FullName);
            }

            var type = AppDomain.CurrentDomain.GetAssemblies()
                .Where(a => !a.IsDynamic)
                .SelectMany(a => a.GetTypes())
                .FirstOrDefault(t => t.FullName.Equals(authenticatorType));
            if (type != null)
            {
                _authenticator = (ClientAuthenticationValidator) Activator.CreateInstance(type);
                //Add Principal and SharedKey
                String keyHash = settings.Get(PropKey);
                if (null != keyHash)
                {
                    _authenticator.Add(new SingleTenantPrincipal(new ConnectorFramework()), keyHash);
                }
            }
            else
            {
                Trace.TraceWarning("Type not found: " + authenticatorType);
            }

            if (null == _authenticator)
            {
                throw new ArgumentNullException(authenticatorType);
            }
        }

        public ClientAuthenticationValidator ClientAuthenticationValidator
        {
            get { return _authenticator; }
        }

        protected override ServiceHost CreateServiceHost(Type serviceType,
            Uri[] baseAddresses)
        {
            return new ConnectorServiceHost(_authenticator, baseAddresses);
        }
    }

    #endregion

    public class ConnectorServiceHost : ServiceHost
    {
        public ConnectorServiceHost(ClientAuthenticationValidator validator, params Uri[] baseAddresses)
            : base(typeof (WcfWebsocket), baseAddresses)
        {
            if (validator == null)
            {
                throw new ArgumentNullException("validator");
            }

            // Add a custom authentication validator
            Credentials.UserNameAuthentication.UserNamePasswordValidationMode = UserNamePasswordValidationMode.Custom;
            Credentials.UserNameAuthentication.CustomUserNamePasswordValidator = validator;

            // Add a custom authorization policy
            var policies = new List<IAuthorizationPolicy> {validator};
            Authorization.PrincipalPermissionMode = PrincipalPermissionMode.Custom;
            Authorization.ExternalAuthorizationPolicies = policies.AsReadOnly();

            /*
            foreach (var cd in ImplementedContracts.Values)
            {
                cd.Behaviors.Add(new WebSocketServiceInstanceProvider());
            }*/
        }

        protected override void OnClosed()
        {
            base.OnClosed();
            IDisposable disposable = Credentials.UserNameAuthentication.CustomUserNamePasswordValidator as IDisposable;
            if (null != disposable)
            {
                disposable.Dispose();
            }
        }
    }

    #region ClientAuthenticationValidator

    public class ClientAuthenticationValidator : UserNamePasswordValidator, IAuthorizationPolicy, IDisposable
    {
        private readonly ConcurrentDictionary<String, Pair<ConnectionPrincipal, String>> _tenantsDictionary =
            new ConcurrentDictionary<string, Pair<ConnectionPrincipal, String>>();

        public bool Add(ConnectionPrincipal tenant, String secret)
        {
            var pair = new Pair<ConnectionPrincipal, String>(tenant, secret);
            if (_tenantsDictionary.TryAdd(tenant.Identity.Name, pair))
            {
                tenant.Disposed += (sender, args) =>
                {
                    IPrincipal principal = sender as IPrincipal;
                    Pair<ConnectionPrincipal, string> ignore;
                    if (principal != null) _tenantsDictionary.TryRemove(principal.Identity.Name, out ignore);
                };
                return true;
            }
            return false;
        }

        public override void Validate(string userName, string password)
        {
            // validate arguments
            if (String.IsNullOrEmpty(userName))
                throw new FaultException("Unknown null or empty userName");
            if (String.IsNullOrEmpty(password))
                throw new FaultException("Unknown null or empty password");

            Pair<ConnectionPrincipal, String> principal = FindPrincipal(userName);

            if (principal == null)
            {
                throw new SecurityTokenException("Unknown username");
            }

            if (!Verify(principal.Second, password))
            {
                throw new SecurityTokenException("Unknown password");
            }
        }

        public Pair<ConnectionPrincipal, String> FindPrincipal(String userName)
        {
            Pair<ConnectionPrincipal, String> principal;
            _tenantsDictionary.TryGetValue(userName, out principal);
            return principal;
        }

        public static bool Verify(String principalKey, String password)
        {
            GuardedString key = new GuardedString();
            password.ToCharArray().ToList().ForEach(p => key.AppendChar(p));
            try
            {
                return key.VerifyBase64SHA1Hash(principalKey);
            }
            finally
            {
                key.Dispose();
            }
        }

        #region IAuthorizationPolicy Members

        private string _id;

        public string Id
        {
            get { return _id ?? (_id = Guid.NewGuid().ToString()); }
        }

        public bool Evaluate(EvaluationContext evaluationContext, ref object state)
        {
            // get the authenticated client identity
            IIdentity client = GetClientIdentity(evaluationContext);

            String evaluated = state as String;
            if (null == evaluated || !client.Name.Equals(evaluated))
            {
                // set the custom principal
                Pair<ConnectionPrincipal, String> principal;

                _tenantsDictionary.TryGetValue(client.Name, out principal);
                if (principal != null)
                {
                    evaluationContext.Properties["Principal"] = principal.First;
                    state = principal.First.Identity.Name;
                }
            }
            return true;
        }

        private IIdentity GetClientIdentity(EvaluationContext evaluationContext)
        {
            object obj;
            if (!evaluationContext.Properties.TryGetValue("Identities", out obj))
                throw new Exception("No Identity found");
            IList<IIdentity> identities = obj as IList<IIdentity>;
            if (identities == null || identities.Count <= 0)
                throw new Exception("No Identity found");
            return identities[0];
        }

        public ClaimSet Issuer
        {
            get { return ClaimSet.System; }
        }

        #endregion

        public void Dispose()
        {
            foreach (var key in _tenantsDictionary.Keys)
            {
                Pair<ConnectionPrincipal, string> pair;
                _tenantsDictionary.TryRemove(key, out pair);
                if (null == pair) continue;
                try
                {
                    pair.First.Dispose();
                }
                catch (Exception e)
                {
                    TraceUtil.TraceException("Failed to dispose ConnectionPrincipal:" + key, e);
                }
            }
        }
    }

    #endregion

    #region SingleTenant 

    public class SingleTenantPrincipal : ConnectionPrincipal
    {
        private readonly ConnectorFramework _connectorFramework;

        public SingleTenantPrincipal(ConnectorFramework connectorFramework)
            : this(new OpenICFServerAdapter(connectorFramework, connectorFramework.LocalManager, false))
        {
            _connectorFramework = connectorFramework;
        }

        public SingleTenantPrincipal(
            IMessageListener<WebSocketConnectionGroup, WebSocketConnectionHolder, RemoteOperationContext> listener)
            : base(listener, new ConcurrentDictionary<string, WebSocketConnectionGroup>())
        {
        }

        protected override void DoClose()
        {
            _connectorFramework.Dispose();
        }
    }

    #endregion

    public class WebSocketServiceInstanceProvider : IInstanceProvider, IContractBehavior
    {
        private readonly Func<SecurityToken, ConnectionPrincipal> _authenticate;

        public WebSocketServiceInstanceProvider(Func<SecurityToken, ConnectionPrincipal> authenticate)
        {
            if (authenticate == null)
            {
                throw new ArgumentNullException("authenticate");
            }

            _authenticate = authenticate;
        }

        #region IInstanceProvider Members

        public object GetInstance(InstanceContext instanceContext, Message message)
        {
            return GetInstance(instanceContext);
        }

        public object GetInstance(InstanceContext instanceContext)
        {
            return new WcfWebsocket();
        }

        public void ReleaseInstance(InstanceContext instanceContext, object instance)
        {
            var disposable = instance as IDisposable;
            if (disposable != null)
            {
                disposable.Dispose();
            }
        }

        #endregion

        #region IContractBehavior Members

        public void AddBindingParameters(ContractDescription contractDescription, ServiceEndpoint endpoint,
            BindingParameterCollection bindingParameters)
        {
        }

        public void ApplyClientBehavior(ContractDescription contractDescription, ServiceEndpoint endpoint,
            ClientRuntime clientRuntime)
        {
        }

        public void ApplyDispatchBehavior(ContractDescription contractDescription, ServiceEndpoint endpoint,
            DispatchRuntime dispatchRuntime)
        {
            dispatchRuntime.InstanceProvider = this;
        }

        public void Validate(ContractDescription contractDescription, ServiceEndpoint endpoint)
        {
        }

        #endregion
    }
}