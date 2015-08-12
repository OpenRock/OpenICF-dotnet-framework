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
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.Threading;
using NUnit.Framework;
using Org.ForgeRock.OpenICF.Framework.ConnectorServerService;
using Org.ForgeRock.OpenICF.Framework.Service.WcfServiceLibrary;
using Org.IdentityConnectors.Common;
using Org.IdentityConnectors.Common.Security;
using Org.IdentityConnectors.Framework.Api;
using Org.IdentityConnectors.Framework.Common.Objects;
using Org.IdentityConnectors.Framework.Common.Objects.Filters;
using Org.IdentityConnectors.Framework.Impl.Api.Remote;

namespace Org.ForgeRock.OpenICF.Framework.Remote
{

    [TestFixture]
    [Explicit]
    public class ExplicitServerTest : ServerTestBase
    {
        protected override Action Start(ClientAuthenticationValidator validator, EndpointAddress serviceAddress)
        {
            return null;
        }

        protected override int FreePort
        {
            get { return 8759; }
        }
    }

    [TestFixture]
    public class NonWcfServerTest : ServerTestBase
    {
        protected override Action Start(ClientAuthenticationValidator validator, EndpointAddress serviceAddress)
        {
            VtortConnectorServiceHost host = new VtortConnectorServiceHost(validator, serviceAddress.Uri);
            host.Open();

            return () => host.Close();
        }
    }


    [TestFixture]
    public class WcfServerTest : ServerTestBase
    {
        protected override Action Start(ClientAuthenticationValidator validator, EndpointAddress serviceAddress)
        {
            ServiceHost host = new ConnectorServiceHost(validator, serviceAddress.Uri);

            CustomBinding binding = new CustomBinding();
            binding.Elements.Add(new ByteStreamMessageEncodingBindingElement());

            HttpTransportBindingElement transport = new HttpTransportBindingElement
            {
                WebSocketSettings =
                {
                    TransportUsage = WebSocketTransportUsage.Always,
                    CreateNotificationOnConnection = true,
                    SubProtocol = "v1.openicf.forgerock.org"
                },
                Realm = "openicf",
                AuthenticationScheme = AuthenticationSchemes.Basic,
            };

            binding.Elements.Add(transport);
            host.AddServiceEndpoint(typeof (IWebSocketService), binding, "");

            host.Open();

            return () => { host.Close(); };
        }
    }


    [TestFixture]
    public abstract class ServerTestBase
    {
        protected static readonly ConnectorKey TestConnectorKey = new ConnectorKey("TestBundleV1.Connector", "1.0.0.0",
            "org.identityconnectors.testconnector.TstConnector");

        protected static readonly ConnectorKey TestStatefulConnectorKey = new ConnectorKey("TestBundleV1.Connector",
            "1.0.0.0",
            "org.identityconnectors.testconnector.TstStatefulConnector");

        protected static readonly ConnectorKey TestPoolableStatefulConnectorKey =
            new ConnectorKey("TestBundleV1.Connector", "1.0.0.0",
                "org.identityconnectors.testconnector.TstStatefulPoolableConnector");

        private Action _close;
        private ConnectorFramework _clientConnectorFramework;
        private RemoteWSFrameworkConnectionInfo _connectionInfo;


        protected virtual int FreePort
        {
            get { return 8000; }
        }

        private static int FreeTcpPort()
        {
            //netsh http add urlacl url=http://+:8000/openicf user=DOMAIN\user
            TcpListener l = new TcpListener(IPAddress.Loopback, 0);
            l.Start();
            int port = ((IPEndPoint) l.LocalEndpoint).Port;
            l.Stop();
            return port;
        }

        protected abstract Action Start(ClientAuthenticationValidator validator, EndpointAddress serviceAddress);

        [TestFixtureSetUp]
        public void Init()
        {
            try
            {
                ConnectorFramework serverConnectorFramework = new ConnectorFramework();
                ConnectorServerService.ConnectorServerService.InitializeConnectors(serverConnectorFramework.LocalManager);

                foreach (var connectorInfo in serverConnectorFramework.LocalManager.ConnectorInfos)
                {
                    Trace.TraceInformation("Found Connector {0}", connectorInfo.ConnectorKey);
                }

                 int freePort = FreePort;
                EndpointAddress serviceAddress =
                    new EndpointAddress(String.Format("http://localhost:{0}/openicf", freePort));

                var secureString = new GuardedString();
                "changeit".ToCharArray().ToList().ForEach(p => secureString.AppendChar(p));

                ClientAuthenticationValidator validator = new ClientAuthenticationValidator();
                validator.Add(new SingleTenantPrincipal(serverConnectorFramework), secureString.GetBase64SHA1Hash());

                _close = Start(validator, serviceAddress);
                _close += () => serverConnectorFramework.Dispose();

                // ----------

                _clientConnectorFramework = new ConnectorFramework();

                _connectionInfo = new RemoteWSFrameworkConnectionInfo
                {
                    RemoteUri = new Uri(String.Format("http://localhost.fiddler:{0}/openicf", freePort)),
                    Principal = ConnectionPrincipal.DefaultName,
                    Password = secureString
                };
            }
            catch (Exception e)
            {
                TraceUtil.TraceException("Failed", e);
                throw;
            }
        }

        [TestFixtureTearDown]
        public void Dispose()
        {
            if (null != _close)
            {
                _close();
            }
        }

        [SetUp]
        public void InitTest()
        {
            /* ... */
        }

        [TearDown]
        public void DisposeTest()
        {
            /* ... */
        }

        public ConnectorFramework ConnectorFramework
        {
            get { return _clientConnectorFramework; }
        }

        protected AsyncRemoteConnectorInfoManager ConnectorInfoManager
        {
            get { return ConnectorFramework.GetRemoteManager(_connectionInfo); }
        }

        protected ConnectorFacade ConnectorFacade
        {
            get { return GetConnectorFacade(false, false); }
        }

        protected ConnectorFacade GetConnectorFacade(Boolean caseIgnore,
            Boolean returnNullTest)
        {
            return ConnectorInfoManager.FindConnectorInfoAsync(TestStatefulConnectorKey)
                .ContinueWith(task =>
                {
                    if (task.IsCompleted)
                    {
                        var info = task.Result;
                        APIConfiguration api = info.CreateDefaultAPIConfiguration();
                        ConfigurationProperties props = api.ConfigurationProperties;

                        props.GetProperty("randomString").Value = StringUtil.RandomString();
                        props.GetProperty("caseIgnore").Value = caseIgnore;
                        props.GetProperty("returnNullTest").Value = returnNullTest;
                        props.GetProperty("failValidation").Value = false;
                        props.GetProperty("testObjectClass").Value =
                            new[] {ObjectClass.ACCOUNT_NAME, ObjectClass.GROUP_NAME};
                        api.ProducerBufferSize = 0;
                        return ConnectorFramework.NewInstance(api);
                    }
                    task.Wait();
                    return null;
                }).Result;
        }


        [Test]
        public void TestRequiredConnectorInfo()
        {
            IAsyncConnectorInfoManager manager = ConnectorInfoManager;
            Assert.IsNotNull(manager);

            var result = manager.FindConnectorInfoAsync(TestConnectorKey);
            Assert.IsTrue(result.Wait(TimeSpan.FromMinutes(5)));
            Assert.IsNotNull(result.Result);

            result = manager.FindConnectorInfoAsync(TestStatefulConnectorKey);
            Assert.IsTrue(result.Wait(TimeSpan.FromMinutes(5)));
            Assert.IsNotNull(result.Result);

            result = manager.FindConnectorInfoAsync(TestPoolableStatefulConnectorKey);
            Assert.IsTrue(result.Wait(TimeSpan.FromMinutes(5)));
            Assert.IsNotNull(result.Result);
        }

        [Test]
        public void TestValidate()
        {
            IAsyncConnectorInfoManager manager = ConnectorInfoManager;
            var task = manager.FindConnectorInfoAsync(TestConnectorKey);
            Assert.IsTrue(task.Wait(TimeSpan.FromMinutes(5)));

            ConnectorInfo info = task.Result;
            Assert.IsNotNull(info);
            APIConfiguration api = info.CreateDefaultAPIConfiguration();

            ConfigurationProperties props = api.ConfigurationProperties;
            ConfigurationProperty property = props.GetProperty("failValidation");
            property.Value = false;

            ConnectorFacade facade = ConnectorFramework.NewInstance(api);
            facade.Validate();
            property.Value = true;
            facade = ConnectorFramework.NewInstance(api);
            try
            {
                Thread.CurrentThread.CurrentUICulture = new CultureInfo("en");
                facade.Validate();
                Assert.Fail("exception expected");
            }
            catch (Exception e)
            {
                TraceUtil.TraceException("Test Exception", e);
                Assert.AreEqual("validation failed en", e.Message);
            }
            try
            {
                Thread.CurrentThread.CurrentUICulture = new CultureInfo("es");
                facade.Validate();
                Assert.Fail("exception expected");
            }
            catch (RemoteWrappedException e)
            {
                Assert.AreEqual("validation failed es", e.Message);
            }

            // call test and also test that locale is propagated
            // properly
            try
            {
                Thread.CurrentThread.CurrentUICulture = new CultureInfo("en");
                facade.Test();
                Assert.Fail("exception expected");
            }
            catch (Exception e)
            {
                TraceUtil.TraceException("Test Exception", e);
                Assert.AreEqual("test failed en", e.Message);
            }
        }

        [Test]
        public void TestNullOperations()
        {
            IAsyncConnectorInfoManager manager = ConnectorInfoManager;
            var task = manager.FindConnectorInfoAsync(TestStatefulConnectorKey);
            Assert.IsTrue(task.Wait(TimeSpan.FromMinutes(5)));

            ConnectorFacade facade = GetConnectorFacade(true, true);
            OperationOptionsBuilder optionsBuilder = new OperationOptionsBuilder();
            facade.Test();
            Assert.IsNull(facade.Schema());

            var guardedString = new GuardedString();
            "Passw0rd".ToCharArray().ToList().ForEach(p => guardedString.AppendChar(p));

            Uid uid = facade.Create(ObjectClass.ACCOUNT,
                CollectionUtil.NewSet(new Name("CREATE_01"), ConnectorAttributeBuilder.BuildPassword(guardedString)),
                optionsBuilder.Build());
            Assert.IsNull(uid);

            Uid resolvedUid = facade.ResolveUsername(ObjectClass.ACCOUNT, "CREATE_01", optionsBuilder.Build());
            Assert.IsNull(resolvedUid);


            Uid authenticatedUid = facade.Authenticate(ObjectClass.ACCOUNT, "CREATE_01", guardedString,
                optionsBuilder.Build());
            Assert.IsNull(authenticatedUid);

            SyncToken token = facade.GetLatestSyncToken(ObjectClass.ACCOUNT);
            Assert.IsNull(token);

            SyncToken lastToken = facade.Sync(ObjectClass.ACCOUNT, new SyncToken(-1),
                new SyncResultsHandler {Handle = delta => true}, optionsBuilder.Build());

            Assert.IsNull(lastToken);

            SearchResult searchResult = facade.Search(ObjectClass.ACCOUNT, null,
                new ResultsHandler {Handle = connectorObject => true}, optionsBuilder.Build());

            Assert.IsNull(searchResult);

            Uid updatedUid = facade.Update(ObjectClass.ACCOUNT, new Uid("1"),
                CollectionUtil.NewSet(ConnectorAttributeBuilder.BuildLockOut(true)), optionsBuilder.Build());
            Assert.IsNull(updatedUid);

            ConnectorObject co = facade.GetObject(ObjectClass.ACCOUNT, new Uid("1"), optionsBuilder.Build());
            Assert.IsNull(co);


            ScriptContextBuilder contextBuilder = new ScriptContextBuilder
            {
                ScriptLanguage = "Boo",
                ScriptText = "arg"
            };
            contextBuilder.AddScriptArgument("arg", "test");

            object o = facade.RunScriptOnConnector(contextBuilder.Build(), optionsBuilder.Build());
            Assert.AreEqual(o, "test");
            o = facade.RunScriptOnResource(contextBuilder.Build(), optionsBuilder.Build());
            Assert.IsNull(o);
        }

        [Test]
        public void TestOperations()
        {
            IAsyncConnectorInfoManager manager = ConnectorInfoManager;
            var task = manager.FindConnectorInfoAsync(TestStatefulConnectorKey);
            Assert.IsTrue(task.Wait(TimeSpan.FromMinutes(5)));

            ConnectorFacade facade = ConnectorFacade;
            facade.Test();
            Assert.IsNotNull(facade.Schema());

            var guardedString = new GuardedString();
            "Passw0rd".ToCharArray().ToList().ForEach(p => guardedString.AppendChar(p));

            Uid uid1 = facade.Create(ObjectClass.ACCOUNT,
                CollectionUtil.NewSet(new Name("CREATE_01"), ConnectorAttributeBuilder.BuildPassword(guardedString)),
                null);
            Assert.IsNotNull(uid1);

            Uid uid2 = facade.Create(ObjectClass.ACCOUNT,
                CollectionUtil.NewSet(new Name("CREATE_02"), ConnectorAttributeBuilder.BuildPassword(guardedString)),
                null);

            Assert.AreNotEqual(uid1, uid2);

            Uid resolvedUid = facade.ResolveUsername(ObjectClass.ACCOUNT, "CREATE_01", null);
            Assert.AreEqual(uid1, resolvedUid);

            Uid authenticatedUid = facade.Authenticate(ObjectClass.ACCOUNT, "CREATE_01", guardedString, null);
            Assert.AreEqual(uid1, authenticatedUid);

            try
            {
                guardedString = new GuardedString();
                "wrongPassw0rd".ToCharArray().ToList().ForEach(p => guardedString.AppendChar(p));
                facade.Authenticate(ObjectClass.ACCOUNT, "CREATE_01", guardedString, null);
                Assert.Fail("This should fail");
            }
            catch (Exception e)
            {
                Assert.AreEqual("Invalid Password", e.Message);
            }

            SyncToken token = facade.GetLatestSyncToken(ObjectClass.ACCOUNT);
            Assert.AreEqual(token.Value, 2);

            IList<SyncDelta> changes = new List<SyncDelta>();
            Int32? index = null;

            SyncToken lastToken = facade.Sync(ObjectClass.ACCOUNT, new SyncToken(-1), new SyncResultsHandler
            {
                Handle = delta =>
                {
                    Int32? previous = index;
                    index = (Int32?) delta.Token.Value;
                    if (null != previous)
                    {
                        Assert.IsTrue(previous < index);
                    }
                    changes.Add(delta);
                    return true;
                }
            }, null);

            Assert.AreEqual(changes.Count, 2);
            Assert.AreEqual(facade.GetObject(ObjectClass.ACCOUNT, uid1, null).Uid, uid1);
            Assert.AreEqual(token, lastToken);

            IList<ConnectorObject> connectorObjects = new List<ConnectorObject>();
            facade.Search(ObjectClass.ACCOUNT,
                FilterBuilder.Or(FilterBuilder.EqualTo(new Name("CREATE_02")),
                    FilterBuilder.StartsWith(new Name("CREATE"))), new ResultsHandler
                    {
                        Handle =
                            connectorObject =>
                            {
                                connectorObjects.Add(connectorObject);
                                return true;
                            }
                    }, null);
            Assert.AreEqual(connectorObjects.Count, 2);

            connectorObjects = new List<ConnectorObject>();
            facade.Search(ObjectClass.ACCOUNT, null, new ResultsHandler
            {
                Handle =
                    connectorObject =>
                    {
                        connectorObjects.Add(connectorObject);
                        return true;
                    }
            }, null);
            Assert.AreEqual(connectorObjects.Count, 2);

            Uid updatedUid = facade.Update(ObjectClass.ACCOUNT, uid1,
                CollectionUtil.NewSet(ConnectorAttributeBuilder.BuildLockOut(true)), null);
            ConnectorObject co = facade.GetObject(ObjectClass.ACCOUNT, updatedUid, null);
            var isLockedOut = ConnectorAttributeUtil.IsLockedOut(co);
            Assert.IsTrue(isLockedOut != null && (bool) isLockedOut);

            facade.Delete(ObjectClass.ACCOUNT, updatedUid, null);
            Assert.IsNull(facade.GetObject(ObjectClass.ACCOUNT, updatedUid, null));
        }

        [Test]
        public void TestScriptOperations()
        {
            IAsyncConnectorInfoManager manager = ConnectorInfoManager;
            var task = manager.FindConnectorInfoAsync(TestStatefulConnectorKey);
            Assert.IsTrue(task.Wait(TimeSpan.FromMinutes(5)));

            ConnectorFacade facade = ConnectorFacade;

            ScriptContextBuilder contextBuilder = new ScriptContextBuilder
            {
                ScriptLanguage = "Boo",
                ScriptText = "arg",
            };
            contextBuilder.AddScriptArgument("arg", "test");

            object o = facade.RunScriptOnConnector(contextBuilder.Build(), null);
            Assert.AreEqual(o, "test");
            o = facade.RunScriptOnResource(contextBuilder.Build(), null);
            Assert.AreEqual(o, "test");
        }
    }
}