/*
 * ====================
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2008-2009 Sun Microsystems, Inc. All rights reserved.     
 * 
 * The contents of this file are subject to the terms of the Common Development 
 * and Distribution License("CDDL") (the "License").  You may not use this file 
 * except in compliance with the License.
 * 
 * You can obtain a copy of the License at 
 * http://opensource.org/licenses/cddl1.php
 * See the License for the specific language governing permissions and limitations 
 * under the License. 
 * 
 * When distributing the Covered Code, include this CDDL Header Notice in each file
 * and include the License file at http://opensource.org/licenses/cddl1.php.
 * If applicable, add the following below this CDDL Header, with the fields 
 * enclosed by brackets [] replaced by your own identifying information: 
 * "Portions Copyrighted [year] [name of copyright owner]"
 * ====================
 * Portions Copyrighted 2012-2015 ForgeRock AS.
 */
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Org.IdentityConnectors.Common;
using Org.IdentityConnectors.Common.Script;
using Org.IdentityConnectors.Common.Security;
using Org.IdentityConnectors.Framework.Common.Objects;
using ICF = Org.IdentityConnectors.Framework.Common.Objects;
using Org.IdentityConnectors.Framework.Common.Objects.Filters;
using Org.IdentityConnectors.Framework.Common.Exceptions;
using Org.IdentityConnectors.Framework.Spi;
using Org.IdentityConnectors.Framework.Spi.Operations;
namespace org.identityconnectors.testconnector
{
    #region MyTstConnection
    public class MyTstConnection
    {
        private readonly int _connectionNumber;
        private bool _isGood = true;

        public MyTstConnection(int connectionNumber)
        {
            _connectionNumber = connectionNumber;
        }

        public void Test()
        {
            if (!_isGood)
            {
                throw new ConnectorException("Connection is bad");
            }
        }

        public void Dispose()
        {
            _isGood = false;
        }

        public bool IsGood()
        {
            return _isGood;
        }

        public int GetConnectionNumber()
        {
            return _connectionNumber;
        }
    }
    #endregion

    #region TstAbstractConnector
    public abstract class TstAbstractConnector : AuthenticateOp, IConnectorEventSubscriptionOp,
        CreateOp, DeleteOp, ResolveUsernameOp, SchemaOp, ScriptOnResourceOp, SearchOp<Filter>,
        ISyncEventSubscriptionOp, SyncOp, TestOp, UpdateOp
    {


        internal sealed class ResourceComparator : IComparer<ConnectorObject>
        {
            private readonly IList<ICF.SortKey> sortKeys;

            public ResourceComparator(ICF.SortKey[] sortKeys)
            {
                this.sortKeys = sortKeys;
            }


            public int Compare(ConnectorObject r1, ConnectorObject r2)
            {
                foreach (ICF.SortKey sortKey in sortKeys)
                {
                    int result = Compare(r1, r2, sortKey);
                    if (result != 0)
                    {
                        return result;
                    }
                }
                return 0;
            }

            private int Compare(ConnectorObject r1, ConnectorObject r2, ICF.SortKey sortKey)
            {
                IList<object> vs1 = ValuesSorted(r1, sortKey.Field);
                IList<object> vs2 = ValuesSorted(r2, sortKey.Field);
                if (vs1.Count == 0 && vs2.Count == 0)
                {
                    return 0;
                }
                else if (vs1.Count == 0)
                {
                    // Sort resources with missing attributes last.
                    return 1;
                }
                else if (vs2.Count == 0)
                {
                    // Sort resources with missing attributes last.
                    return -1;
                }
                else
                {
                    object v1 = vs1[0];
                    object v2 = vs2[0];
                    return sortKey.IsAscendingOrder() ? CompareValues(v1, v2) : -CompareValues(v1, v2);
                }
            }

            private IList<object> ValuesSorted(ConnectorObject resource, string field)
            {
                ConnectorAttribute value = resource.GetAttributeByName(field);
                if (value == null || value.Value == null || value.Value.Count == 0)
                {
                    return new List<object>();
                }
                else if (value.Value.Count > 1)
                {
                    List<object> results = new List<object>(value.Value);
                    results.Sort(VALUE_COMPARATOR);
                    return results;
                }
                else
                {
                    return value.Value;
                }
            }
        }

        private static readonly IComparer<object> VALUE_COMPARATOR = new ComparatorAnonymousInnerClassHelper();

        private class ComparatorAnonymousInnerClassHelper : IComparer<object>
        {
            public ComparatorAnonymousInnerClassHelper()
            {
            }

            public virtual int Compare(object o1, object o2)
            {
                return CompareValues(o1, o2);
            }
        }

        private static int CompareValues(object v1, object v2)
        {
            if (v1 is string && v2 is string)
            {
                string s1 = (string)v1;
                string s2 = (string)v2;
                return StringComparer.OrdinalIgnoreCase.Compare(s1, s2);
            }
            else if (v1 is double && v2 is double)
            {
                double n1 = (double)v1;
                double n2 = (double)v2;
                return n1.CompareTo(n2);
            }
            else if (v1 is int && v2 is int)
            {
                int n1 = (int)v1;
                int n2 = (int)v2;
                return n1.CompareTo(n2);
            }
            else if (v1 is bool && v2 is bool)
            {
                bool b1 = (bool)v1;
                bool b2 = (bool)v2;
                return b1.CompareTo(b2);
            }
            else
            {
                return v1.GetType().FullName.CompareTo(v2.GetType().FullName);
            }
        }

        protected TstStatefulConnectorConfig _config;

        public void Init(Configuration cfg)
        {
            _config = (TstStatefulConnectorConfig)cfg;
            Guid g = _config.Guid;
        }

        public void Update()
        {
            _config.UpdateTest();
        }

        public virtual Uid Authenticate(ObjectClass objectClass, string username, GuardedString password, OperationOptions options)
        {
            if (_config.ReturnNullTest)
            {
                return null;
            }
            else
            {
                return _config.Authenticate(objectClass, username, password);
            }
        }

        public ISubscription Subscribe(ObjectClass objectClass, Filter eventFilter, IObserver<ConnectorObject> handler, OperationOptions operationOptions)
        {

            ConnectorObjectBuilder builder = new ConnectorObjectBuilder { ObjectClass = objectClass };

            CancellationSubscription subscription = new CancellationSubscription();

            DoPeriodicWorkAsync(runCount =>
            {
                builder.SetUid(Convert.ToString(runCount));
                builder.SetName(Convert.ToString(runCount));
                handler.OnNext(builder.Build());

                if (runCount >= 10)
                {
                    // Locally stop serving subscription
                    handler.OnError(new ConnectorException("Subscription channel is closed"));
                    // The loop should be stopped from here.
                    return false;
                }
                return true;
            }, new TimeSpan(0, 0, 0, 0, 500), new TimeSpan(0, 0, 1), subscription.Token);


            return subscription;
        }

        private async Task DoPeriodicWorkAsync(Func<Int32, Boolean> action,
            TimeSpan interval, TimeSpan dueTime, CancellationToken token)
        {
            // Initial wait time before we begin the periodic loop.
            await Task.Delay(dueTime, token);

            Int32 i = 0;
            // Repeat this loop until cancelled.
            while (!token.IsCancellationRequested)
            {
                if (action(++i))
                {
                    // Wait to repeat again.
                    await Task.Delay(interval, token);
                }
                else
                {
                    break;
                }
            }
        }

        public ISubscription Subscribe(ObjectClass objectClass, SyncToken token, IObserver<SyncDelta> handler, OperationOptions operationOptions)
        {
            var coBuilder = new ConnectorObjectBuilder() { ObjectClass = objectClass };
            coBuilder.SetUid("0");
            coBuilder.SetName("SYNC_EVENT");

            SyncDeltaBuilder builder = new SyncDeltaBuilder()
            {
                DeltaType = SyncDeltaType.CREATE_OR_UPDATE,
                Object = coBuilder.Build()
            };

            CancellationSubscription subscription = new CancellationSubscription();

            DoPeriodicWorkAsync(runCount =>
            {
                builder.Token = new SyncToken(runCount);
                handler.OnNext(builder.Build());

                if (runCount >= 10)
                {
                    // Locally stop serving subscription
                    handler.OnError(new ConnectorException("Subscription channel is closed"));
                    // ScheduledFuture should be stopped from here.
                    return false;
                }
                return true;
            }, new TimeSpan(0, 0, 0, 0, 500), new TimeSpan(0, 0, 1), subscription.Token);


            return subscription;
        }


        public Uid Create(ObjectClass objectClass, ICollection<ConnectorAttribute> createAttributes, OperationOptions options)
        {
            ConnectorAttributesAccessor accessor = new ConnectorAttributesAccessor(createAttributes);
            if (_config.ReturnNullTest)
            {
                return null;
            }
            else if (_config.IsTestObjectClass(objectClass))
            {
                return _config.GeObjectCache(objectClass).Create(createAttributes);
            }
            else
            {
                if (accessor.HasAttribute("fail"))
                {
                    throw new ConnectorException("Test Exception");
                }
                else if (accessor.HasAttribute("exist") && accessor.FindBoolean("exist") == true)
                {
                    throw new AlreadyExistsException(accessor.GetName().GetNameValue());
                }
                else if (accessor.HasAttribute("emails"))
                {
                    object value = ConnectorAttributeUtil.GetSingleValue(accessor.Find("emails"));
                    if (value is IDictionary)
                    {
                        return new Uid((string)((IDictionary)value)["email"]);
                    }
                    else
                    {
                        throw new InvalidAttributeValueException("Expecting Map");
                    }
                }
                return new Uid(_config.Guid.ToString());
            }
        }

        public void Delete(ObjectClass objectClass, Uid uid, OperationOptions options)
        {
            if (_config.ReturnNullTest)
            {
                return;
            }
            if (_config.IsTestObjectClass(objectClass))
            {
                _config.GeObjectCache(objectClass).Delete(uid);
            }
            else
            {
                if (null == uid.Revision)
                {
                    throw new PreconditionRequiredException("Version is required for MVCC");
                }
                else if (_config.Guid.ToString().Equals(uid.Revision))
                {
                    // Delete
                    String a = _config.Guid.ToString();
                    String b = _config.Guid.ToString();
                    String c = _config.Guid.ToString();

                }
                else
                {
                    throw new PreconditionFailedException("Current version of resource is 0 and not match with: " +
                                                          uid.Revision);
                }
            }
        }

        public virtual Uid ResolveUsername(ObjectClass objectClass, string username, OperationOptions options)
        {
            if (_config.ReturnNullTest)
            {
                return null;
            }
            else
            {
                return _config.ResolveByUsername(objectClass, username);
            }
        }


        public virtual Schema Schema()
        {
            if (_config.ReturnNullTest)
            {
                return null;
            }
            else
            {
                SchemaBuilder builder = new SchemaBuilder(SafeType<Connector>.ForRawType(GetType()));
                foreach (string type in _config.TestObjectClass)
                {
                    ObjectClassInfoBuilder classInfoBuilder = new ObjectClassInfoBuilder();
                    classInfoBuilder.ObjectType = type;
                    classInfoBuilder.AddAttributeInfo(OperationalAttributeInfos.PASSWORD);
                    builder.DefineObjectClass(classInfoBuilder.Build());
                }
                return builder.Build();
            }
        }

        public virtual object RunScriptOnResource(ScriptContext request, OperationOptions options)
        {
            if (_config.ReturnNullTest)
            {
                return null;
            }
            else
            {
                try
                {
                    return ScriptExecutorFactory.NewInstance(request.ScriptLanguage).NewScriptExecutor(null, request.ScriptText, true).Execute(request.ScriptArguments);
                }
                catch (Exception e)
                {
                    throw new ConnectorException(e.Message, e);
                }
            }
        }
        public FilterTranslator<Filter> CreateFilterTranslator(ObjectClass objectClass, OperationOptions options)
        {
            return new FilterTranslatorAnonymousInnerClassHelper();
        }

        private class FilterTranslatorAnonymousInnerClassHelper : FilterTranslator<Filter>
        {

            public FilterTranslatorAnonymousInnerClassHelper()
            {
            }

            public IList<Filter> Translate(Filter filter)
            {
                List<Filter> filters = new List<Filter>(1);
                filters.Add(filter);
                return filters;
            }
        }
        public void ExecuteQuery(ObjectClass objectClass, Filter query, ResultsHandler handler, OperationOptions options)
        {

            ICF.SortKey[] sortKeys = options.SortKeys;
            if (null == sortKeys)
            {
                sortKeys = new ICF.SortKey[] { new ICF.SortKey(Name.NAME, true) };
            }

            // Rebuild the full result set.
            SortedSet<ConnectorObject> resultSet = new SortedSet<ConnectorObject>(new ResourceComparator(sortKeys));
            if (_config.ReturnNullTest)
            {
                return;
            }
            else if (_config.IsTestObjectClass(objectClass))
            {
                Filter filter = FilteredResultsHandlerVisitor.WrapFilter(query, _config.CaseIgnore);
                foreach (var connectorObject in _config.GeObjectCache(objectClass).GetIterable(filter))
                {
                    resultSet.Add(connectorObject);
                }
            }
            else
            {
                if (null != query)
                {
                    foreach (ConnectorObject co in collection.Values)
                    {
                        if (query.Accept(co))
                        {
                            resultSet.Add(co);
                        }
                    }
                }
                else
                {
                    resultSet.UnionWith(collection.Values);
                }
            }
            // Handle the results
            if (null != options.PageSize)
            {
                // Paged Search
                string pagedResultsCookie = options.PagedResultsCookie;
                string currentPagedResultsCookie = options.PagedResultsCookie;
                int? pagedResultsOffset = null != options.PagedResultsOffset ? Math.Max(0, (int)options.PagedResultsOffset) : 0;
                int? pageSize = options.PageSize;
                int index = 0;
                int pageStartIndex = null == pagedResultsCookie ? 0 : -1;
                int handled = 0;
                foreach (ConnectorObject entry in resultSet)
                {
                    if (pageStartIndex < 0 && pagedResultsCookie.Equals(entry.Name.GetNameValue()))
                    {
                        pageStartIndex = index + 1;
                    }
                    if (pageStartIndex < 0 || index < pageStartIndex)
                    {
                        index++;
                        continue;
                    }
                    if (handled >= pageSize)
                    {
                        break;
                    }
                    if (index >= pagedResultsOffset + pageStartIndex)
                    {
                        if (handler.Handle(entry))
                        {
                            handled++;
                            currentPagedResultsCookie = entry.Name.GetNameValue();
                        }
                        else
                        {
                            break;
                        }
                    }
                    index++;
                }

                if (index == resultSet.Count)
                {
                    currentPagedResultsCookie = null;
                }

                if (handler is SearchResultsHandler)
                {
                    ((SearchResultsHandler)handler).HandleResult(new SearchResult(currentPagedResultsCookie, resultSet.Count - index));
                }
            }
            else
            {
                // Normal Search
                foreach (ConnectorObject entry in resultSet)
                {
                    if (!handler.Handle(entry))
                    {
                        break;
                    }
                }
                if (handler is SearchResultsHandler)
                {
                    ((SearchResultsHandler)handler).HandleResult(new SearchResult());
                }
            }
        }

        public void Sync(ObjectClass objectClass, SyncToken token, SyncResultsHandler handler, OperationOptions options)
        {
            if (_config.ReturnNullTest)
            {
                return;
            }
            if (_config.IsTestObjectClass(objectClass))
            {
                foreach (SyncDelta delta in _config.Sync(objectClass, (int?)token.Value))
                {
                    if (!handler.Handle(delta))
                    {
                        break;
                    }
                }
                if (handler is SyncTokenResultsHandler)
                {
                    ((SyncTokenResultsHandler)handler).HandleResult(new SyncToken(_config.LatestSyncToken));
                }
            }
            else
            {
                if (handler is SyncTokenResultsHandler)
                {
                    ((SyncTokenResultsHandler)handler).HandleResult(GetLatestSyncToken(objectClass));
                }
            }
        }

        public SyncToken GetLatestSyncToken(ObjectClass objectClass)
        {
            if (_config.ReturnNullTest)
            {
                return null;
            }
            else if (_config.IsTestObjectClass(objectClass))
            {
                return new SyncToken(_config.LatestSyncToken);
            }
            else
            {
                return new SyncToken(_config.Guid.ToString());
            }
        }

        public void Test()
        {
            if (_config.failValidation)
            {
                throw new ConnectorException("validation failed " + CultureInfo.CurrentUICulture.TwoLetterISOLanguageName);
            }
        }

        public Uid Update(ObjectClass objectClass, Uid uid, ICollection<ConnectorAttribute> replaceAttributes, OperationOptions options)
        {
            if (_config.ReturnNullTest)
            {
                return null;
            }
            else if (_config.IsTestObjectClass(objectClass))
            {
                return _config.GeObjectCache(objectClass).Update(uid, replaceAttributes);
            }
            else
            {
                throw new System.NotSupportedException("Object Update is not supported: " + objectClass.GetObjectClassValue());
            }
        }

        private static readonly SortedDictionary<string, ConnectorObject> collection = new SortedDictionary<string, ConnectorObject>(StringComparer.InvariantCultureIgnoreCase);
        static TstAbstractConnector()
        {
            bool enabled = true;
            for (int i = 0; i < 100; i++)
            {
                ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
                builder.SetUid(Convert.ToString(i));
                builder.SetName(string.Format("user{0:D3}", i));
                builder.AddAttribute(ConnectorAttributeBuilder.BuildEnabled(enabled));
                IDictionary<string, object> mapAttribute = new Dictionary<string, object>();
                mapAttribute["email"] = "foo@example.com";
                mapAttribute["primary"] = true;
                mapAttribute["usage"] = new List<String>() { "home", "work" };
                builder.AddAttribute("emails", mapAttribute);
                ConnectorObject co = builder.Build();
                collection[co.Name.GetNameValue()] = co;
                enabled = !enabled;
            }
        }
    }
    #endregion

    #region TstConnector
    [ConnectorClass("TestConnector",
                    "TestConnector.category",
                    typeof(TstConnectorConfig),
                    MessageCatalogPaths = new String[] { "TestBundleV1.Messages" }
                        )]
    public class TstConnector : CreateOp, PoolableConnector, SchemaOp, SearchOp<String>, SyncOp
    {
        private static int _connectionCount = 0;
        private MyTstConnection _myConnection;
        private TstConnectorConfig _config;

        public Uid Create(ObjectClass oclass, ICollection<ConnectorAttribute> attrs, OperationOptions options)
        {
            int? delay = (int?)CollectionUtil.GetValue(options.Options, "delay", null);
            if (delay != null)
            {
                Thread.Sleep((int)delay);
            }

            if (options.Options.ContainsKey("testPooling"))
            {
                return new Uid(_myConnection.GetConnectionNumber().ToString());
            }
            else
            {
                String version = GetVersion();
                return new Uid(version);
            }
        }
        public void Init(Configuration cfg)
        {
            _config = (TstConnectorConfig)cfg;
            if (_config.resetConnectionCount)
            {
                _connectionCount = 0;
            }
            _myConnection = new MyTstConnection(_connectionCount++);
        }

        public static String GetVersion()
        {
            return "1.0";
        }

        public void Dispose()
        {
            if (_myConnection != null)
            {
                _myConnection.Dispose();
                _myConnection = null;
            }
        }

        /// <summary>
        /// Used by the script tests
        /// </summary>
        public String concat(String s1, String s2)
        {
            return s1 + s2;
        }

        /// <summary>
        /// Used by the script tests
        /// </summary>
        public void Update()
        {
            _config.UpdateTest();
        }

        public void CheckAlive()
        {
            _myConnection.Test();
        }

        private class MyTranslator : AbstractFilterTranslator<String>
        {

        }
        public FilterTranslator<String> CreateFilterTranslator(ObjectClass oclass, OperationOptions options)
        {
            return new MyTranslator();
        }
        public void ExecuteQuery(ObjectClass oclass, String query, ResultsHandler handler, OperationOptions options)
        {
            int remaining = _config.numResults;
            for (int i = 0; i < _config.numResults; i++)
            {
                int? delay = (int?)CollectionUtil.GetValue(options.Options, "delay", null);
                if (delay != null)
                {
                    Thread.Sleep((int)delay);
                }
                ConnectorObjectBuilder builder =
                    new ConnectorObjectBuilder();
                builder.SetUid("" + i);
                builder.SetName(i.ToString());
                builder.ObjectClass = oclass;
                for (int j = 0; j < 50; j++)
                {
                    builder.AddAttribute("myattribute" + j, "myvaluevaluevalue" + j);
                }
                ConnectorObject rv = builder.Build();
                if (handler.Handle(rv))
                {
                    remaining--;
                }
                else
                {
                    break;
                }
            }

            if (handler is SearchResultsHandler)
            {
                ((SearchResultsHandler)handler).HandleResult(new SearchResult("", remaining));
            }
        }
        public void Sync(ObjectClass objClass, SyncToken token,
                         SyncResultsHandler handler,
                         OperationOptions options)
        {
            int remaining = _config.numResults;
            for (int i = 0; i < _config.numResults; i++)
            {
                ConnectorObjectBuilder obuilder =
                    new ConnectorObjectBuilder();
                obuilder.SetUid(i.ToString());
                obuilder.SetName(i.ToString());
                obuilder.ObjectClass = (objClass);

                SyncDeltaBuilder builder =
                    new SyncDeltaBuilder();
                builder.Object = (obuilder.Build());
                builder.DeltaType = (SyncDeltaType.CREATE_OR_UPDATE);
                builder.Token = (new SyncToken("mytoken"));
                SyncDelta rv = builder.Build();
                if (handler.Handle(rv))
                {
                    remaining--;
                }
                else
                {
                    break;
                }
            }
            if (handler is SyncTokenResultsHandler)
            {
                ((SyncTokenResultsHandler)handler).HandleResult(new SyncToken(remaining));
            }
        }

        public SyncToken GetLatestSyncToken(ObjectClass objectClass)
        {
            return new SyncToken("mylatest");
        }

        public Schema Schema()
        {
            SchemaBuilder builder = new SchemaBuilder(SafeType<Connector>.Get<TstConnector>());
            for (int i = 0; i < 2; i++)
            {
                ObjectClassInfoBuilder classBuilder = new ObjectClassInfoBuilder();
                classBuilder.ObjectType = ("class" + i);
                for (int j = 0; j < 200; j++)
                {
                    classBuilder.AddAttributeInfo(ConnectorAttributeInfoBuilder.Build("attributename" + j, typeof(String)));
                }
                builder.DefineObjectClass(classBuilder.Build());
            }
            return builder.Build();
        }
    }
    #endregion

    #region TstConnectorConfig
    public class TstConnectorConfig : AbstractConfiguration
    {
        /// <summary>
        /// keep lower case for consistent unit tests
        /// </summary>
        [ConfigurationProperty(OperationTypes = new Type[] { typeof(SyncOp) })]
        public string tstField { get; set; }

        /// <summary>
        /// keep lower case for consistent unit tests
        /// </summary>
        public int numResults { get; set; }

        /// <summary>
        /// keep lower case for consistent unit tests
        /// </summary>
        public bool failValidation { get; set; }

        /// <summary>
        /// keep lower case for consistent unit tests
        /// </summary>
        public bool resetConnectionCount { get; set; }

        public override void Validate()
        {
            if (failValidation)
            {
                throw new ConnectorException("validation failed " + CultureInfo.CurrentUICulture.TwoLetterISOLanguageName);
            }
        }

        public void UpdateTest()
        {
            tstField = "change";
            NotifyConfigurationUpdate();
        }
    }
    #endregion

    #region TstStatefulConnector
    [ConnectorClass("TestStatefulConnector",
                "TestStatefulConnector.category",
                typeof(TstStatefulConnectorConfig),
                MessageCatalogPaths = new String[] { "TestBundleV1.Messages" }
                    )]
    public class TstStatefulConnector : TstAbstractConnector, Connector
    {

        //public void Init(Configuration cfg)
        //{
        //    base.Init(cfg);
        //}

        public Configuration Configuration
        {
            get
            {
                return _config;
            }
        }

        public void Dispose()
        {
            _config = null;
        }
    }
    #endregion

    #region TstStatefulConnectorConfig
    public class TstStatefulConnectorConfig : TstConnectorConfig, StatefulConfiguration
    {
        public Boolean CaseIgnore { get; set; }

        public String[] TestObjectClass { get; set; }

        public Boolean ReturnNullTest { get; set; }

        public String RandomString { get; set; }

        private Guid? guid;

        public Guid Guid
        {
            get
            {
                lock (this)
                {
                    if (null == guid)
                    {
                        guid = Guid.NewGuid();
                    }
                    return (Guid)guid;
                }
            }
        }

        public void Release()
        {
            guid = null;
        }

        public bool IsTestObjectClass(ObjectClass objectClass)
        {
            return null != objectClass && null != TestObjectClass && TestObjectClass.Contains(objectClass.GetObjectClassValue(), StringComparer.OrdinalIgnoreCase);
        }

        public virtual Uid ResolveByUsername(ObjectClass objectClass, string username)
        {
            ObjectClassCacheEntry cache;
            objectCache.TryGetValue(objectClass, out cache);
            if (null != cache)
            {
                ConnectorObjectCacheEntry entry = cache.GetByName(username);
                if (null != entry)
                {
                    return entry.ConnectorObject.Uid;
                }
            }
            return null;
        }

        public virtual Uid Authenticate(ObjectClass objectClass, string username, GuardedString password)
        {
            ObjectClassCacheEntry cache;
            objectCache.TryGetValue(objectClass, out cache);
            if (null != cache)
            {
                ConnectorObjectCacheEntry entry = cache.GetByName(username);
                if (null != entry)
                {
                    if (entry.Authenticate(password))
                    {
                        return entry.ConnectorObject.Uid;
                    }
                    throw new InvalidPasswordException("Invalid Password");
                }
                throw new InvalidCredentialException("Unknown username: " + username);
            }
            throw new InvalidCredentialException("Empty ObjectClassCache: " + objectClass.GetObjectClassValue());
        }

        internal virtual IEnumerable<SyncDelta> Sync(ObjectClass objectClass, int? token)
        {
            ObjectClassCacheEntry cache;
            objectCache.TryGetValue(objectClass, out cache);
            if (null != cache)
            {
                return cache.ObjectCache.Values.Where(x =>
                {
                    int rev = Convert.ToInt32(x.ConnectorObject.Uid.Revision);
                    return null == token || rev > token;
                }).OrderBy(x => x.ConnectorObject.Uid.Revision).Select(x =>
                {
                    var builder = new SyncDeltaBuilder();
                    builder.DeltaType = x.DeltaType;
                    builder.Token = new SyncToken(Convert.ToInt32(x.ConnectorObject.Uid.Revision));
                    builder.Object = x.ConnectorObject;
                    return builder.Build();
                });
            }
            return Enumerable.Empty<SyncDelta>();
        }

        private Int32 _revision = 0;

        internal virtual Int32 LatestSyncToken
        {
            get
            {
                return _revision;
            }
        }

        internal virtual Uid GetNextUid(string uid)
        {
            return new Uid(uid, Convert.ToString(Interlocked.Increment(ref _id)));
        }

        private Int32 _id = 0;

        private Uid NewUid()
        {
            return GetNextUid(Convert.ToString(Interlocked.Increment(ref _id)));
        }

        private readonly ConcurrentDictionary<ObjectClass, ObjectClassCacheEntry> objectCache = new ConcurrentDictionary<ObjectClass, ObjectClassCacheEntry>();

        internal virtual ObjectClassCacheEntry GeObjectCache(ObjectClass objectClass)
        {
            ObjectClassCacheEntry cache;
            objectCache.TryGetValue(objectClass, out cache);
            if (null == cache)
            {
                cache = new ObjectClassCacheEntry(objectClass, NewUid, GetNextUid);
                ObjectClassCacheEntry rv = objectCache.GetOrAdd(objectClass, cache);
                if (null != rv)
                {
                    cache = rv;
                }
            }
            return cache;
        }

        internal class ObjectClassCacheEntry
        {
            private readonly ObjectClass _objectClass;
            private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();
            private readonly ConcurrentDictionary<string, string> _uniqueNameIndex = new ConcurrentDictionary<string, string>();
            internal readonly ConcurrentDictionary<string, ConnectorObjectCacheEntry> ObjectCache = new ConcurrentDictionary<string, ConnectorObjectCacheEntry>();
            private readonly Func<Uid> _newUid;
            private readonly Func<String, Uid> _getNextUid;
            public ObjectClassCacheEntry(ObjectClass objectClass, Func<Uid> newUid, Func<string, Uid> getNextUid)
            {
                _objectClass = objectClass;
                _newUid = newUid;
                _getNextUid = getNextUid;
            }

            internal virtual ConnectorObjectCacheEntry GetByName(string username)
            {
                ConnectorObjectCacheEntry entry = null;
                string uid;
                _uniqueNameIndex.TryGetValue(username, out uid);
                if (null != uid)
                {
                    ObjectCache.TryGetValue(uid, out entry);
                }
                return entry;
            }

            public virtual Uid Create(ICollection<ConnectorAttribute> createAttributes)
            {
                Name name = ConnectorAttributeUtil.GetNameFromAttributes(createAttributes);
                if (name == null)
                {
                    throw new InvalidAttributeValueException("__NAME__ Required");
                }
                if (String.IsNullOrWhiteSpace(name.GetNameValue()))
                {
                    throw new InvalidAttributeValueException("__NAME__ can not be blank");
                }
                Uid uid = _newUid();
                if (_uniqueNameIndex.GetOrAdd(name.GetNameValue(), uid.GetUidValue()) == null)
                {
                    var builder = new ConnectorObjectBuilder { ObjectClass = _objectClass };
                    builder.AddAttributes(createAttributes).SetUid(uid);

                    ObjectCache.TryAdd(uid.GetUidValue(), new ConnectorObjectCacheEntry(builder.Build(), _getNextUid));
                    return uid;
                }
                else
                {
                    throw (new AlreadyExistsException()).InitUid(new Uid(name.GetNameValue()));
                }
            }

            public virtual Uid Update(Uid uid, ICollection<ConnectorAttribute> updateAttributes)
            {
                ConnectorObjectCacheEntry entry = null;
                ObjectCache.TryGetValue(uid.GetUidValue(), out entry);
                if (null == entry)
                {
                    throw new UnknownUidException(uid, _objectClass);
                }
                if (_lock.TryEnterWriteLock(new TimeSpan(0, 1, 0)))
                {
                    try
                    {
                        IDictionary<string, ConnectorAttribute> attributeMap = CollectionUtil.NewCaseInsensitiveDictionary<ConnectorAttribute>();
                        foreach (ConnectorAttribute attr in entry.ConnectorObject.GetAttributes())
                        {
                            attributeMap[attr.Name] = attr;
                        }
                        foreach (ConnectorAttribute attribute in updateAttributes)
                        {
                            if (attribute.Value == null)
                            {
                                attributeMap.Remove(attribute.Name);
                            }
                            else
                            {
                                attributeMap[attribute.Name] = attribute;
                            }
                        }
                        return entry.Update(attributeMap.Values);

                    }
                    finally
                    {
                        _lock.ExitWriteLock();
                    }
                }
                else
                {
                    throw new ConnectorException("Failed to acquire lock", new TimeoutException("Failed to acquire lock"));
                }

            }

            public virtual void Delete(Uid uid)
            {
                ConnectorObjectCacheEntry entry = null;
                ObjectCache.TryGetValue(uid.GetUidValue(), out entry);
                if (null == entry)
                {
                    throw new UnknownUidException(uid, _objectClass);
                }


                if (_lock.TryEnterWriteLock(new TimeSpan(0, 1, 0)))
                {
                    try
                    {
                        entry.Update(entry.ConnectorObject.GetAttributes());
                        entry.DeltaType = SyncDeltaType.DELETE;
                    }
                    finally
                    {
                        _lock.ExitWriteLock();
                    }
                }
                else
                {
                    throw new ConnectorException("Failed to acquire lock", new TimeoutException("Failed to acquire lock"));
                }

            }
            public virtual IEnumerable<ConnectorObject> GetIterable(Filter filter)
            {
                return ObjectCache.Values.Where(x => !SyncDeltaType.DELETE.Equals(x.DeltaType) && (null == filter || filter.Accept(x.ConnectorObject))).Select(x => x.ConnectorObject);
            }
        }


        internal class ConnectorObjectCacheEntry
        {

            internal SyncDeltaType DeltaType = SyncDeltaType.CREATE;

            internal ConnectorObject ConnectorObject { get; set; }
            private readonly Func<String, Uid> _getNextUid;

            public ConnectorObjectCacheEntry(ConnectorObject connectorConnectorObject, Func<String, Uid> getNextUid)
            {
                ConnectorObject = connectorConnectorObject;
                _getNextUid = getNextUid;
            }

            public virtual bool Authenticate(GuardedString password)
            {
                ConnectorAttribute pw = ConnectorObject.GetAttributeByName(OperationalAttributes.PASSWORD_NAME);
                return null != pw && null != password && ConnectorAttributeUtil.GetSingleValue(pw).Equals(password);
            }

            public virtual Uid Update(ICollection<ConnectorAttribute> updateAttributes)
            {
                var builder = new ConnectorObjectBuilder { ObjectClass = ConnectorObject.ObjectClass };
                builder.AddAttributes(updateAttributes).SetUid(_getNextUid(ConnectorObject.Uid.GetUidValue()));
                ConnectorObject = builder.Build();
                DeltaType = SyncDeltaType.UPDATE;
                return ConnectorObject.Uid;
            }
        }
    }
    #endregion

    #region TstStatefulPoolableConnector
    [ConnectorClass("TestStatefulPoolableConnector",
                "TestStatefulPoolableConnector.category",
                typeof(TstStatefulConnectorConfig),
                MessageCatalogPaths = new String[] { "TestBundleV1.Messages" }
                    )]
    public class TstStatefulPoolableConnector : TstAbstractConnector, PoolableConnector
    {

        //public void Init(Configuration cfg)
        //{
        //    base.Init(cfg);
        //}

        public Configuration Configuration
        {
            get
            {
                return _config;
            }
        }

        public void Dispose()
        {
            _config = null;
        }

        public void CheckAlive()
        {
        }

    }
    #endregion
}
