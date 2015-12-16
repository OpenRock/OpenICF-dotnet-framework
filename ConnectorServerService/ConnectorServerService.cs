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
using System.Collections.Specialized;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http.Headers;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.ServiceModel;
using System.ServiceModel.Configuration;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Org.ForgeRock.OpenICF.Common.ProtoBuf;
using Org.ForgeRock.OpenICF.Framework.Remote;
using Org.ForgeRock.OpenICF.Framework.Service.WcfServiceLibrary;
using Org.IdentityConnectors.Common;
using Org.IdentityConnectors.Framework.Common.Exceptions;
using vtortola.WebSockets;

namespace Org.ForgeRock.OpenICF.Framework.ConnectorServerService
{

    #region OpenICFWebsocketService

    public partial class ConnectorServerService : ServiceBase
    {
        public const string PropKey = "connectorserver.key";
        public const string PropCertificateThumbprint = "connectorserver.certificateThumbprint";
        public const string PropFacadeLifetime = "connectorserver.maxFacadeLifeTime";

        private Action _closeAction;

        public ConnectorServerService()
        {
            InitializeComponent();
        }

        public static void InitializeConnectors(AsyncLocalConnectorInfoManager manager)
        {
            Assembly assembly = Assembly.GetExecutingAssembly();
            FileInfo thisAssemblyFile = new FileInfo(assembly.Location);
            DirectoryInfo directory = thisAssemblyFile.Directory;
            if (directory == null) throw new ArgumentNullException("directory");
            Environment.CurrentDirectory = directory.FullName;
            Trace.TraceInformation("Starting connector server from: " + Environment.CurrentDirectory);

            FileInfo[] files = directory.GetFiles("*.Connector.dll");
            foreach (FileInfo file in files)
            {
                Assembly lib = Assembly.LoadFrom(file.ToString());
                manager.AddConnectorAssembly(lib);
            }

            // also handle connector DLL file names with a version 
            FileInfo[] versionedFiles = directory.GetFiles("*.Connector-*.dll");
            foreach (FileInfo versionedFile in versionedFiles)
            {
                Assembly lib = Assembly.LoadFrom(versionedFile.ToString());
                manager.AddConnectorAssembly(lib);
            }
        }

        protected virtual Uri[] ExtractEndPoint()
        {
            ServicesSection servicesSection =
                ConfigurationManager.GetSection("system.serviceModel/services") as ServicesSection;

            if (servicesSection != null)
            {
                foreach (ServiceElement service in servicesSection.Services)
                {
                    foreach (ServiceEndpointElement endpoint in service.Endpoints)
                    {
                        if (String.Equals(typeof (IWebSocketService).FullName, endpoint.Contract))
                        {
                            return
                                (from BaseAddressElement baseAddress in service.Host.BaseAddresses
                                    select new Uri(baseAddress.BaseAddress)).ToArray();
                        }
                    }
                }
            }
            throw new Exception("No BaseAddress //TODO fix message");
        }

        public void StartService(string[] args)
        {
            OnStart(args);
        }

        protected override void OnStart(string[] args)
        {
            try
            {
                ConnectorFramework connectorFramework = new ConnectorFramework();
                InitializeConnectors(connectorFramework.LocalManager);

                NameValueCollection settings = ConfigurationManager.AppSettings;

                String keyHash = settings.Get(PropKey);
                if (keyHash == null)
                {
                    throw new Org.IdentityConnectors.Framework.Common.Exceptions.ConfigurationException(
                        "Missing required configuration property: " + PropKey);
                }

                ClientAuthenticationValidator validator = new ClientAuthenticationValidator();
                validator.Add(new SingleTenantPrincipal(connectorFramework), keyHash);

                String facadeLifeTimeStr = settings.Get(PropFacadeLifetime);
                if (facadeLifeTimeStr != null)
                {
                    // _server.MaxFacadeLifeTime = Int32.Parse(facedeLifeTimeStr);
                }

                String disableWcf = settings.Get("disableWcf");
                OperatingSystem os = Environment.OSVersion;
                if (disableWcf == null && (os.Platform == PlatformID.Win32NT) &&
                    ((os.Version.Major > 6) || ((os.Version.Major == 6) && (os.Version.Minor >= 2))))
                {
                    ServiceHost host = new ConnectorServiceHost(validator);
                    host.Open();

                    _closeAction = () => host.Close();
                    Trace.TraceInformation("Started WCF connector server");
                }
                else
                {
                    Uri[] endpointUri = ExtractEndPoint();
                    if (endpointUri == null)
                    {
                        throw new Org.IdentityConnectors.Framework.Common.Exceptions.ConfigurationException(
                            "Missing required baseAddress");
                    }
                    VtortConnectorServiceHost host = new VtortConnectorServiceHost(validator, endpointUri);
                    host.Open();

                    _closeAction = () => host.Close();
                    Trace.TraceInformation("Started WebSocketListener connector server");
                }
            }
            catch (Exception e)
            {
                TraceUtil.TraceException("Exception occured starting connector server", e);
                throw;
            }
        }

        public void StopService()
        {
            OnStop();
        }

        protected override void OnStop()
        {
            try
            {
                Trace.TraceInformation("Stopping connector server");
                if (_closeAction != null)
                {
                    _closeAction();
                    _closeAction = null;
                }
                Trace.TraceInformation("Stopped connector server");
            }
            catch (Exception e)
            {
                TraceUtil.TraceException("Exception occured stopping connector server", e);
            }
        }
    }

    #endregion

    #region VtortConnectorServiceHost

    public class VtortConnectorServiceHost
    {
        private WebSocketListener _listener;
        private readonly ClientAuthenticationValidator _validator;
        private readonly Uri _endPointUri;

        public VtortConnectorServiceHost(ClientAuthenticationValidator validator, params Uri[] baseAddresses)
        {
            _validator = validator;
            _endPointUri = baseAddresses[0];
        }

        public void Open()
        {
            IPAddress ipAddress = IPAddress.Any;
            if (_endPointUri.IsLoopback)
            {
                ipAddress = IPAddress.Loopback;
            }
            else if (!"0.0.0.0".Equals(_endPointUri.DnsSafeHost))
            {
                ipAddress = IOUtil.GetIPAddress(_endPointUri.DnsSafeHost);
            }

            var options = new WebSocketListenerOptions()
            {
                NegotiationQueueCapacity = 128,
                ParallelNegotiations = 16,
                PingTimeout = Timeout.InfiniteTimeSpan,
                SubProtocols = new[] {"v1.openicf.forgerock.org"},
                OnHttpNegotiation = (request, response) =>
                {
                    var authHeader = request.Headers["Authorization"];
                    if (authHeader != null)
                    {
                        var authHeaderVal = AuthenticationHeaderValue.Parse(authHeader);

                        // RFC 2617 sec 1.2, "scheme" name is case-insensitive
                        if (authHeaderVal.Scheme.Equals("basic",
                            StringComparison.OrdinalIgnoreCase) &&
                            authHeaderVal.Parameter != null)
                        {
                            var encoding = Encoding.GetEncoding("iso-8859-1");
                            var credentials = encoding.GetString(Convert.FromBase64String(authHeaderVal.Parameter));

                            int separator = credentials.IndexOf(':');
                            if (separator != -1)
                            {
                                string name = credentials.Substring(0, separator);
                                string password = credentials.Substring(separator + 1);

                                var pair = _validator.FindPrincipal(name);
                                if (null != pair)
                                {
                                    if (ClientAuthenticationValidator.Verify(pair.Second, password))
                                    {
                                        request.Items["ConnectionPrincipal"] = pair.First;
                                    }
                                    else
                                    {
                                        Trace.TraceWarning("Incorrect password - username: {0}", name);
                                        response.Status = HttpStatusCode.Forbidden;
                                    }
                                }
                                else
                                {
                                    Trace.TraceWarning("Unknown username: {0}", name);
                                    response.Status = HttpStatusCode.Forbidden;
                                }
                            }
                            else
                            {
                                Trace.TraceWarning("Invalid Basic Authorization : {0}", credentials);
                                response.Status = HttpStatusCode.BadRequest;
                            }
                        }
                        else
                        {
                            Trace.TraceWarning("Basic Authorization header expected but found{0}", authHeader);
                            response.Status = HttpStatusCode.BadRequest;
                        }
                    }
                    else
                    {
                        //401 + Realm
                        response.Status = HttpStatusCode.Unauthorized;
                    }
                }
            };
            _listener = new WebSocketListener(new IPEndPoint(ipAddress, _endPointUri.Port), options);

            bool useSsl = String.Equals("https", _endPointUri.Scheme, StringComparison.OrdinalIgnoreCase) ||
                          String.Equals("wss", _endPointUri.Scheme, StringComparison.OrdinalIgnoreCase);
            if (useSsl)
            {
                _listener.ConnectionExtensions.RegisterExtension(
                    new WebSocketSecureConnectionExtension(GetCertificate()));
            }

            var rfc6455 = new vtortola.WebSockets.Rfc6455.WebSocketFactoryRfc6455(_listener);
            _listener.Standards.RegisterStandard(rfc6455);
            _listener.Start();
            Task.Run((Func<Task>) ListenAsync);
        }

        public void Close()
        {
            if (null != _listener)
            {
                _listener.Stop();
                _validator.Dispose();
            }
        }

        protected X509Certificate2 GetCertificate()
        {
            NameValueCollection settings = ConfigurationManager.AppSettings;
            String certificateThumbprint = settings.Get(ConnectorServerService.PropCertificateThumbprint);
            if (String.IsNullOrWhiteSpace(certificateThumbprint))
            {
                throw new Org.IdentityConnectors.Framework.Common.Exceptions.ConfigurationException(
                    "Missing required configuration setting: " + ConnectorServerService.PropCertificateThumbprint);
            }

            X509Store store = new X509Store(StoreName.My, StoreLocation.LocalMachine);
            try
            {
                store.Open(OpenFlags.ReadOnly | OpenFlags.OpenExistingOnly);

                X509Certificate2 certificate =
                    store.Certificates.Cast<X509Certificate2>()
                        .FirstOrDefault(
                            certificate1 =>
                                String.Equals(certificate1.Thumbprint, certificateThumbprint,
                                    StringComparison.CurrentCultureIgnoreCase));
                if (certificate == null)
                {
                    throw new Org.IdentityConnectors.Framework.Common.Exceptions.ConfigurationException(
                        "The Certificate can not be found with thumbprint: " + certificateThumbprint);
                }
                return certificate;
            }
            finally
            {
                store.Close();
            }
        }

        private async Task ListenAsync()
        {
            while (_listener.IsStarted)
            {
                try
                {
                    var websocket = await _listener.AcceptWebSocketAsync(CancellationToken.None)
                        .ConfigureAwait(false);
                    if (websocket != null)
                    {
                        object value;
                        websocket.HttpRequest.Items.TryGetValue("ConnectionPrincipal", out value);
                        ConnectionPrincipal connectionPrincipal = value as ConnectionPrincipal;
                        if (null != connectionPrincipal)
                        {
                            Task.Run(() => HandleWebSocketAsync(websocket, connectionPrincipal)).ConfigureAwait(false);
                        }
                        else
                        {
                            Trace.TraceInformation("Unidentified WebSocket from {0}", websocket.RemoteEndpoint);
                            websocket.Close();
                        }
                    }
                    else
                    {
                        Trace.TraceInformation("Stopping Connector Server!?");
                    }
                }
                catch (Exception e)
                {
                    TraceUtil.TraceException("Failed to Accept WebSocket", e);
                }
            }
        }

        private async Task HandleWebSocketAsync(WebSocket websocket, ConnectionPrincipal connectionPrincipal)
        {
            VtortWebsocket soWebsocket = new VtortWebsocket(websocket, connectionPrincipal);
            try
            {
                Trace.TraceInformation("Server onConnect()");
                connectionPrincipal.OperationMessageListener.OnConnect(soWebsocket);

                while (websocket.IsConnected)
                {
                    var message = await websocket.ReadMessageAsync(CancellationToken.None)
                        .ConfigureAwait(false);
                    if (message != null)
                    {
                        switch (message.MessageType)
                        {
                            case WebSocketMessageType.Text:
                                using (var sr = new StreamReader(message, Encoding.UTF8))
                                {
                                    String msgContent = await sr.ReadToEndAsync();
                                    connectionPrincipal.OperationMessageListener.OnMessage(soWebsocket, msgContent);
                                }
                                break;
                            case WebSocketMessageType.Binary:
                                using (var ms = new MemoryStream())
                                {
                                    await message.CopyToAsync(ms);
                                    if (ms.Length > 0)
                                    {
                                        connectionPrincipal.OperationMessageListener.OnMessage(soWebsocket, ms.ToArray());
                                    }
                                }
                                break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                connectionPrincipal.OperationMessageListener.OnError(ex);
            }
            finally
            {
                connectionPrincipal.OperationMessageListener.OnClose(soWebsocket, 1000, "");
                //Dispose before onClose is complete ???
                websocket.Dispose();
            }
        }
    }

    #endregion

    #region VtortWebsocket

    public class VtortWebsocket : WebSocketConnectionHolder
    {
        private readonly WebSocket _webSocket;
        private RemoteOperationContext _context;
        private readonly ConnectionPrincipal _principal;

        public VtortWebsocket(WebSocket webSocket, ConnectionPrincipal principal)
        {
            _webSocket = webSocket;
            _principal = principal;
            Task.Factory.StartNew(WriteMessageAsync);
        }

        protected override async Task WriteMessageAsync(byte[] entry,
            System.Net.WebSockets.WebSocketMessageType messageType)
        {
            try
            {
                switch (messageType)
                {
                    case System.Net.WebSockets.WebSocketMessageType.Binary:
                        using (var writer = _webSocket.CreateMessageWriter(WebSocketMessageType.Binary))
                        {
                            await writer.WriteAsync(entry, 0, entry.Length);
                        }
                        break;
                    case System.Net.WebSockets.WebSocketMessageType.Text:
                        using (var writer = _webSocket.CreateMessageWriter(WebSocketMessageType.Text))
                        {
                            await writer.WriteAsync(entry, 0, entry.Length);
                        }
                        break;
                    default:
                        throw new InvalidAttributeValueException("Unsupported WebSocketMessageType: " + messageType);
                }
            }
            catch (Exception e)
            {
                TraceUtil.TraceException("Failed to write", e);
            }
        }

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
            _webSocket.Close();
        }

        public override bool Operational
        {
            get { return _webSocket.IsConnected; }
        }

        public override RemoteOperationContext RemoteConnectionContext
        {
            get { return _context; }
        }
    }

    #endregion

    /*
    #region BasicAuthHttpModule

    public abstract class BasicAuthHttpModule : IHttpModule
    {
        private const string Realm = "OpenICF";

        public void Init(HttpApplication context)
        {
            // Register event handlers
            context.AuthenticateRequest += OnApplicationAuthenticateRequest;
            context.EndRequest += OnApplicationEndRequest;
        }

        private static void SetPrincipal(IPrincipal principal)
        {
            Thread.CurrentPrincipal = principal;
            if (HttpContext.Current != null)
            {
                HttpContext.Current.User = principal;
            }
        }

        protected abstract ConnectionPrincipal CheckPassword(SecurityToken token);

        private void AuthenticateUser(string credentials)
        {
            try
            {
                var encoding = Encoding.GetEncoding("iso-8859-1");
                credentials = encoding.GetString(Convert.FromBase64String(credentials));

                int separator = credentials.IndexOf(':');
                if (separator != -1)
                {
                    string name = credentials.Substring(0, separator);
                    string password = credentials.Substring(separator + 1);

                    UserNameSecurityToken token = new UserNameSecurityToken(name, password);
                    ConnectionPrincipal principal = CheckPassword(token);

                    if (null != principal)
                    {
                        SetPrincipal(principal);
                    }
                    else
                    {
                        // Invalid username or password.
                        HttpContext.Current.Response.StatusCode = 403;
                    }
                }
                else
                {
                    //Bad Request
                    HttpContext.Current.Response.StatusCode = 400;
                }
            }
            catch (FormatException)
            {
                // Credentials were not formatted correctly.
                HttpContext.Current.Response.StatusCode = 401;
            }
        }

        private void OnApplicationAuthenticateRequest(object sender, EventArgs e)
        {
            var request = HttpContext.Current.Request;
            var authHeader = request.Headers["Authorization"];
            if (authHeader != null)
            {
                var authHeaderVal = AuthenticationHeaderValue.Parse(authHeader);

                // RFC 2617 sec 1.2, "scheme" name is case-insensitive
                if (authHeaderVal.Scheme.Equals("basic",
                    StringComparison.OrdinalIgnoreCase) &&
                    authHeaderVal.Parameter != null)
                {
                    AuthenticateUser(authHeaderVal.Parameter);
                }
            }
            else
            {
                HttpContext.Current.Response.StatusCode = 401;
            }
        }

        // If the request was unauthorized, add the WWW-Authenticate header 
        // to the response.
        private static void OnApplicationEndRequest(object sender, EventArgs e)
        {
            var response = HttpContext.Current.Response;
            if (response.StatusCode == 401)
            {
                response.Headers.Add("WWW-Authenticate",
                    string.Format("Basic realm=\"{0}\"", Realm));
            }
        }

        public void Dispose()
        {
        }
    }

    #endregion
    */
}
