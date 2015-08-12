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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Org.IdentityConnectors.Common.Security;
using Org.IdentityConnectors.Framework.Api;
using Org.IdentityConnectors.Framework.Api.Operations;
using Org.IdentityConnectors.Framework.Common;
using Org.IdentityConnectors.Framework.Common.Exceptions;
using Org.IdentityConnectors.Framework.Common.Objects;
using Org.IdentityConnectors.Framework.Common.Objects.Filters;
using Org.IdentityConnectors.Framework.Common.Serializer;
using Org.IdentityConnectors.Framework.Spi;

namespace Org.ForgeRock.OpenICF.Framework.Remote
{

    #region IAsyncConnectorFacade

    /// <summary>
    ///     A AsyncConnectorFacade.
    /// </summary>
    /// <remarks>since 1.5</remarks>
    public interface IAsyncConnectorFacade : ConnectorFacade, IAuthenticationAsyncApiOp, ICreateAsyncApiOp,
        IDeleteAsyncApiOp, IGetAsyncApiOp, IResolveUsernameAsyncApiOp, ISchemaAsyncApiOp, IScriptOnConnectorAsyncApiOp,
        IScriptOnResourceAsyncApiOp, ISearchAsyncApiOp, ISyncAsyncApiOp, ITestAsyncApiOp, IUpdateAsyncApiOp,
        IValidateAsyncApiOp
    {
    }

    #endregion

    #region IAsyncConnectorInfoManager

    /// <summary>
    ///     An IAsyncConnectorInfoManager maintains a list of <code>ConnectorInfo</code>
    ///     instances, each of which describes a connector that is available.
    /// </summary>
    /// <remarks>since 1.5</remarks>
    public interface IAsyncConnectorInfoManager : ConnectorInfoManager
    {
        /// <summary>
        ///     Add a promise which will be fulfilled with the
        ///     <seealso cref="ConnectorInfo" /> for the given
        ///     {@ConnectorKey}.
        ///     Add a Promise which will be fulfilled immediately if the
        ///     <seealso cref="ConnectorInfo" /> is maintained
        ///     currently by this instance or later when it became available.
        /// </summary>
        /// <param name="key"> </param>
        /// <returns> new promise </returns>
        Task<ConnectorInfo> FindConnectorInfoAsync(ConnectorKey key);

        /// <summary>
        ///     Add a promise which will be fulfilled with the
        ///     <seealso cref="ConnectorInfo" /> for the given
        ///     {@ConnectorKeyRange}.
        ///     Add a Promise which will be fulfilled immediately if the
        ///     <seealso cref="ConnectorInfo" /> is maintained
        ///     currently by this instance or later when it became available.
        ///     There may be multiple ConnectorInfo matching the range. The
        ///     implementation can only randomly fulfill the promise. It can not grantee
        ///     the highest version to return because it may became available after the
        ///     promised added and after a lower version of ConnectorInfo became
        ///     available in this manager.
        /// </summary>
        /// <param name="keyRange"> </param>
        /// <returns> new promise </returns>
        Task<ConnectorInfo> FindConnectorInfoAsync(ConnectorKeyRange keyRange);
    }

    #endregion

    #region IAuthenticationAsyncApiOp

    public interface IAuthenticationAsyncApiOp : AuthenticationApiOp
    {
        /// <summary>
        ///     Most basic authentication available.
        /// </summary>
        /// <param name="objectClass">
        ///     The object class to use for authenticate. Will typically be an
        ///     account. Must not be null.
        /// </param>
        /// <param name="username">
        ///     string that represents the account or user id.
        /// </param>
        /// <param name="password">
        ///     string that represents the password for the account or user.
        /// </param>
        /// <param name="options">
        ///     additional options that impact the way this operation is run.
        ///     May be null.
        /// </param>
        /// <param name="cancellationToken"></param>
        /// <returns> Uid The uid of the account that was used to authenticate </returns>
        /// <exception cref="Exception">
        ///     if the credentials do not pass authentication otherwise
        ///     nothing.
        /// </exception>
        Task<Uid> AuthenticateAsync(ObjectClass objectClass, String username, GuardedString password,
            OperationOptions options, CancellationToken cancellationToken);
    }

    #endregion

    #region DisposableAsyncConnectorInfoManager

    public abstract class DisposableAsyncConnectorInfoManager<T> : IAsyncConnectorInfoManager, IDisposable
        where T : DisposableAsyncConnectorInfoManager<T>
    {
        internal Int32 IsRunning = 1;

        protected abstract void DoClose();

        public virtual bool Running
        {
            get { return IsRunning != 0; }
        }

        public void Dispose()
        {
            if (CanCloseNow())
            {
                try
                {
                    DoClose();
                }
                catch (Exception)
                {
                    //logger.ok(t, "Failed to close {0}", this);
                }
                // Notify CloseListeners
                OnDisposed();
            }
        }

        protected internal virtual bool CanCloseNow()
        {
            return (Interlocked.CompareExchange(ref IsRunning, 0, 1) == 1);
        }

        /// <devdoc>
        ///     <para>Adds a event handler to listen to the Disposed event on the DisposableAsyncConnectorInfoManager.</para>
        /// </devdoc>
        private event EventHandler DisposedEvent;

        public event EventHandler Disposed
        {
            add
            {
                // check if this is still running
                if (IsRunning == 1)
                {
                    // add close listener
                    DisposedEvent += value;
                    // check the its state again
                    if (DisposedEvent != null && (IsRunning != 1 && DisposedEvent.GetInvocationList().Contains(value)))
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

        protected virtual void OnDisposed()
        {
            try
            {
                var handler = DisposedEvent;
                if (handler != null) handler(this, EventArgs.Empty);
            }
            catch (Exception)
            {
                //logger.ok(ignored, "CloseListener failed");
            }
        }


        public abstract IList<ConnectorInfo> ConnectorInfos { get; }
        public abstract ConnectorInfo FindConnectorInfo(ConnectorKey key);
        public abstract Task<ConnectorInfo> FindConnectorInfoAsync(ConnectorKey key);
        public abstract Task<ConnectorInfo> FindConnectorInfoAsync(ConnectorKeyRange keyRange);
    }

    #endregion

    #region ICreateAsyncApiOp

    public interface ICreateAsyncApiOp : CreateApiOp
    {
        /// <summary>
        ///     Create a target object based on the specified attributes.
        ///     The Connector framework always requires attribute
        ///     <code>ObjectClass</code>. The <code>Connector</code> itself may require
        ///     additional attributes. The API will confirm that the set contains the
        ///     <code>ObjectClass</code> attribute and that no two attributes in the set
        ///     have the same <seealso cref="Attribute#getName() name" />.
        /// </summary>
        /// <param name="objectClass">
        ///     the type of object to create. Must not be null.
        /// </param>
        /// <param name="createAttributes">
        ///     includes all the attributes necessary to create the target
        ///     object (including the <code>ObjectClass</code> attribute).
        /// </param>
        /// <param name="options">
        ///     additional options that impact the way this operation is run.
        ///     May be null.
        /// </param>
        /// <param name="cancellationToken"></param>
        /// <returns>
        ///     the unique id for the object that is created. For instance in
        ///     LDAP this would be the 'dn', for a database this would be the
        ///     primary key, and for 'ActiveDirectory' this would be the GUID.
        /// </returns>
        /// <exception cref="ArgumentException">
        ///     if <code>ObjectClass</code> is missing or elements of the set
        ///     produce duplicate values of <seealso cref="Attribute#getName()" />.
        /// </exception>
        /// <exception cref="NullReferenceException">
        ///     if the parameter <code>createAttributes</code> is
        ///     <code>null</code>.
        /// </exception>
        /// <exception cref="Exception">
        ///     if the <seealso cref="Connector" />
        ///     SPI throws a native <seealso cref="Exception" />.
        /// </exception>
        Task<Uid> CreateAsync(ObjectClass objectClass, ICollection<ConnectorAttribute> createAttributes,
            OperationOptions options, CancellationToken cancellationToken);
    }

    #endregion

    #region IDeleteAsyncApiOp

    public interface IDeleteAsyncApiOp : DeleteApiOp
    {
        /// <summary>
        ///     Delete the object that the specified Uid identifies (if any).
        /// </summary>
        /// <param name="objectClass">
        ///     type of object to delete.
        /// </param>
        /// <param name="uid">
        ///     The unique id that specifies the object to delete.
        /// </param>
        /// <param name="options">
        ///     additional options that impact the way this operation is run.
        ///     May be null.
        /// </param>
        /// <param name="cancellationToken"></param>
        /// <exception cref="UnknownUidException">
        ///     if the
        ///     <seealso cref="Org.IdentityConnectors.Framework.Common.Objects.Uid" />
        ///     does not exist on the resource.
        /// </exception>
        /// <exception cref="Exception">
        ///     if a problem occurs during the operation (for instance, an
        ///     operational timeout).
        /// </exception>
        Task DeleteAsync(ObjectClass objectClass, Uid uid, OperationOptions options, CancellationToken cancellationToken);
    }

    #endregion

    #region IGetAsyncApiOp

    public interface IGetAsyncApiOp : GetApiOp
    {
        /// <summary>
        ///     Get a particular
        ///     <seealso cref="Org.IdentityConnectors.Framework.Common.Objects.ConnectorObject" />
        ///     based on the <seealso cref="Org.IdentityConnectors.Framework.Common.Objects.Uid" />.
        /// </summary>
        /// <param name="objectClass">
        ///     type of object to get.
        /// </param>
        /// <param name="uid">
        ///     the unique id of the object that to get.
        /// </param>
        /// <param name="options">
        ///     additional options that impact the way this operation is run.
        ///     May be null.
        /// </param>
        /// <param name="cancellationToken"></param>
        /// <returns>
        ///     <seealso cref="Org.IdentityConnectors.Framework.Common.Objects.ConnectorObject" />
        ///     based on the
        ///     <seealso cref="Org.IdentityConnectors.Framework.Common.Objects.Uid" />
        ///     provided or <code>null</code> if no such object could be found.
        /// </returns>
        Task<ConnectorObject> GetObjectAsync(ObjectClass objectClass, Uid uid, OperationOptions options,
            CancellationToken cancellationToken);
    }

    #endregion

    #region IResolveUsernameAsyncApiOp

    public interface IResolveUsernameAsyncApiOp : ResolveUsernameApiOp
    {
        /// <summary>
        ///     Resolve the given
        ///     {@link org.identityconnectors.framework.api.operations.AuthenticationApiOp
        ///     authentication} username to the corresponding
        ///     <seealso cref="Org.IdentityConnectors.Framework.Common.Objects.Uid" />.
        ///     The <code>Uid</code> is the one that
        ///     <seealso cref="AuthenticationApiOp#authenticate" />
        ///     would return in case of a successful authentication.
        /// </summary>
        /// <param name="objectClass">
        ///     The object class to use for authenticate. Will typically be an
        ///     account. Must not be null.
        /// </param>
        /// <param name="username">
        ///     string that represents the account or user id.
        /// </param>
        /// <param name="options">
        ///     additional options that impact the way this operation is run.
        ///     May be null.
        /// </param>
        /// <param name="cancellationToken"></param>
        /// <returns> Uid The uid of the account that would be used to authenticate. </returns>
        /// <exception cref="Exception">
        ///     if the username could not be resolved.
        ///     @since 1.5
        /// </exception>
        Task<Uid> ResolveUsernameAsync(ObjectClass objectClass, String username, OperationOptions options,
            CancellationToken cancellationToken);
    }

    #endregion

    #region ISchemaAsyncApiOp

    public interface ISchemaAsyncApiOp : SchemaApiOp
    {
        /// <summary>
        ///     Retrieve the basic schema of this
        ///     <seealso cref="Connector" />.
        /// </summary>
        /// <param name="cancellationToken"></param>
        Task<Schema> SchemaAsync(CancellationToken cancellationToken);
    }

    #endregion

    #region IScriptOnConnectorAsyncApiOp

    public interface IScriptOnConnectorAsyncApiOp : ScriptOnConnectorApiOp
    {
        /// <summary>
        ///     Runs the script.
        /// </summary>
        /// <param name="request">
        ///     The script and arguments to run.
        /// </param>
        /// <param name="options">
        ///     Additional options that control how the script is run. The
        ///     framework does not currently recognize any options but
        ///     specific connectors might. Consult the documentation for each
        ///     connector to identify supported options.
        /// </param>
        /// <param name="cancellationToken"></param>
        /// <returns>
        ///     The result of the script. The return type must be a type that the
        ///     framework supports for serialization.
        /// </returns>
        Task<object> RunScriptOnConnectorAsync(ScriptContext request, OperationOptions options,
            CancellationToken cancellationToken);
    }

    #endregion

    #region IScriptOnResourceAsyncApiOp

    public interface IScriptOnResourceAsyncApiOp : ScriptOnResourceApiOp
    {
        /// <summary>
        ///     Runs a script on a specific target resource.
        /// </summary>
        /// <param name="request">
        ///     The script and arguments to run.
        /// </param>
        /// <param name="options">
        ///     Additional options which control how the script is run. Please
        ///     refer to the connector documentation for supported options.
        /// </param>
        /// <param name="cancellationToken"></param>
        /// <returns>
        ///     The result of the script. The return type must be a type that the
        ///     connector framework supports for serialization. See
        ///     <seealso cref="ObjectSerializerFactory" />
        ///     for a list of supported return types.
        /// </returns>
        Task<object> RunScriptOnResourceAsync(ScriptContext request, OperationOptions options,
            CancellationToken cancellationToken);
    }

    #endregion

    #region ISearchAsyncApiOp

    public interface ISearchAsyncApiOp : SearchApiOp
    {
        /// <summary>
        ///     Search the resource for all objects that match the object class and
        ///     filter.
        /// </summary>
        /// <param name="objectClass">
        ///     reduces the number of entries to only those that match the
        ///     <seealso cref="ObjectClass" /> provided.
        /// </param>
        /// <param name="filter">
        ///     Reduces the number of entries to only those that match the
        ///     <seealso cref="Filter" /> provided, if any. May be null.
        /// </param>
        /// <param name="handler">
        ///     class responsible for working with the objects returned from
        ///     the search.
        /// </param>
        /// <param name="options">
        ///     additional options that impact the way this operation is run.
        ///     May be null.
        /// </param>
        /// <param name="cancellationToken"></param>
        /// <returns> The query result or {@code null}. </returns>
        /// <exception cref="Exception">
        ///     if there is problem during the processing of the results.
        /// </exception>
        Task<SearchResult> SearchAsync(ObjectClass objectClass, Filter filter, ResultsHandler handler,
            OperationOptions options, CancellationToken cancellationToken);
    }

    #endregion

    #region ISyncAsyncApiOp

    public interface ISyncAsyncApiOp : SyncApiOp
    {
        /// <summary>
        ///     Request synchronization events--i.e., native changes to target objects.
        ///     <para>
        ///         This method will call the specified
        ///         {@link Org.IdentityConnectors.Framework.Common.Objects.SyncResultsHandler#handle
        ///         handler} once to pass back each matching
        ///         {@link Org.IdentityConnectors.Framework.Common.Objects.SyncDelta
        ///         synchronization event}. Once this method returns, this method will no
        ///         longer invoke the specified handler.
        ///     </para>
        ///     <para>
        ///         Each
        ///         {@link Org.IdentityConnectors.Framework.Common.Objects.SyncDelta#getToken()
        ///         synchronization event contains a token} that can be used to resume
        ///         reading events <i>starting from that point in the event stream</i>. In
        ///         typical usage, a client will save the token from the final
        ///         synchronization event that was received from one invocation of this
        ///         {@code sync()} method and then pass that token into that client's next
        ///         call to this {@code sync()} method. This allows a client to
        ///         "pick up where he left off" in receiving synchronization events. However,
        ///         a client can pass the token from <i>any</i> synchronization event into a
        ///         subsequent invocation of this {@code sync()} method. This will return
        ///         synchronization events (that represent native changes that occurred)
        ///         immediately subsequent to the event from which the client obtained the
        ///         token.
        ///     </para>
        ///     <para>
        ///         A client that wants to read synchronization events "starting now" can
        ///         call <seealso cref="GetLatestSyncTokenAsync" /> and then pass that token into this
        ///         {@code sync()} method.
        ///     </para>
        /// </summary>
        /// <param name="objectClass">
        ///     The class of object for which to return synchronization
        ///     events. Must not be null.
        /// </param>
        /// <param name="token">
        ///     The token representing the last token from the previous sync.
        ///     The {@code SyncResultsHandler} will return any number of
        ///     <seealso cref="Org.IdentityConnectors.Framework.Common.Objects.SyncDelta" />
        ///     objects, each of which contains a token. Should be
        ///     {@code null} if this is the client's first call to the
        ///     {@code sync()} method for this connector.
        /// </param>
        /// <param name="handler">
        ///     The result handler. Must not be null.
        /// </param>
        /// <param name="options">
        ///     Options that affect the way this operation is run. May be
        ///     null.
        /// </param>
        /// <param name="cancellationToken"></param>
        /// <returns> The sync token or {@code null}. </returns>
        /// <exception cref="ArgumentException">
        ///     if {@code objectClass} or {@code handler} is null or if any
        ///     argument is invalid.
        /// </exception>
        Task<SyncToken> SyncAsync(ObjectClass objectClass, SyncToken token, SyncResultsHandler handler,
            OperationOptions options, CancellationToken cancellationToken);

        /// <summary>
        ///     Returns the token corresponding to the most recent synchronization event
        ///     for any instance of the specified object class.
        ///     <para>
        ///         An application that wants to receive synchronization events
        ///         "starting now" --i.e., wants to receive only native changes that occur
        ///         after this method is called-- should call this method and then pass the
        ///         resulting token into <seealso cref="SyncAsync"> the sync() method</seealso>.
        ///     </para>
        /// </summary>
        /// <param name="objectClass">
        ///     the class of object for which to find the most recent
        ///     synchronization event (if any).
        /// </param>
        /// <param name="cancellationToken"></param>
        /// <returns> A token if synchronization events exist; otherwise {@code null}. </returns>
        Task<SyncToken> GetLatestSyncTokenAsync(ObjectClass objectClass, CancellationToken cancellationToken);
    }

    #endregion

    #region ITestAsyncApiOp

    public interface ITestAsyncApiOp : TestApiOp
    {
        /// <summary>
        ///     Tests the {@link org.identityconnectors.framework.api.APIConfiguration
        ///     Configuration} with the connector.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <exception cref="Exception">
        ///     if the configuration is not valid or the test failed.
        /// </exception>
        Task TestAsync(CancellationToken cancellationToken);
    }

    #endregion

    #region IUpdateAsyncApiOp

    public interface IUpdateAsyncApiOp : UpdateApiOp
    {
        /// <summary>
        ///     Update the object specified by the
        ///     <seealso cref="Org.IdentityConnectors.Framework.Common.Objects.ObjectClass" /> and
        ///     <seealso cref="Org.IdentityConnectors.Framework.Common.Objects.Uid" />, replacing
        ///     the current values of each attribute with the values provided.
        ///     <para>
        ///         For each input attribute, replace all of the current values of that
        ///         attribute in the target object with the values of that attribute.
        ///     </para>
        ///     <para>
        ///         If the target object does not currently contain an attribute that the
        ///         input set contains, then add this attribute (along with the provided
        ///         values) to the target object.
        ///     </para>
        ///     <para>
        ///         If the value of an attribute in the input set is {@code null}, then do
        ///         one of the following, depending on which is most appropriate for the
        ///         target:
        ///         <ul>
        ///             <li>
        ///                 If possible, <em>remove</em> that attribute from the target object
        ///                 entirely.
        ///             </li>
        ///             <li>
        ///                 Otherwise, <em>replace all of the current values</em> of that
        ///                 attribute in the target object with a single value of {@code null}.
        ///             </li>
        ///         </ul>
        ///     </para>
        /// </summary>
        /// <param name="objectClass">
        ///     the type of object to modify. Must not be null.
        /// </param>
        /// <param name="uid">
        ///     the uid of the object to modify. Must not be null.
        /// </param>
        /// <param name="replaceAttributes">
        ///     set of new
        ///     <seealso cref="Org.IdentityConnectors.Framework.Common.Objects.ConnectorAttribute" />
        ///     . the values in this set represent the new, merged values to
        ///     be applied to the object. This set may also include
        ///     {@link Org.IdentityConnectors.Framework.Common.Objects.OperationalAttributes
        ///     operational attributes}. Must not be null.
        /// </param>
        /// <param name="options">
        ///     additional options that impact the way this operation is run.
        ///     May be null.
        /// </param>
        /// <param name="cancellationToken"></param>
        /// <returns>
        ///     the <seealso cref="Org.IdentityConnectors.Framework.Common.Objects.Uid" />
        ///     of the updated object in case the update changes the formation of
        ///     the unique identifier.
        /// </returns>
        /// <exception cref="Org.IdentityConnectors.Framework.Common.Exceptions.UnknownUidException">
        ///     if the
        ///     <seealso cref="Org.IdentityConnectors.Framework.Common.Objects.Uid" />
        ///     does not exist on the resource.
        /// </exception>
        Task<Uid> UpdateAsync(ObjectClass objectClass, Uid uid, ICollection<ConnectorAttribute> replaceAttributes,
            OperationOptions options, CancellationToken cancellationToken);

        /// <summary>
        ///     Update the object specified by the <seealso cref="ObjectClass" /> and <seealso cref="Uid" />,
        ///     adding to the current values of each attribute the values provided.
        ///     <para>
        ///         For each attribute that the input set contains, add to the current values
        ///         of that attribute in the target object all of the values of that
        ///         attribute in the input set.
        ///     </para>
        ///     <para>
        ///         NOTE that this does not specify how to handle duplicate values. The
        ///         general assumption for an attribute of a {@code ConnectorObject} is that
        ///         the values for an attribute may contain duplicates. Therefore, in general
        ///         simply <em>append</em> the provided values to the current value for each
        ///         attribute.
        ///     </para>
        ///     <para>
        ///         IMPLEMENTATION NOTE: for connectors that merely implement
        ///         <seealso cref="Org.IdentityConnectors.Framework.Spi.Operations.UpdateOp" /> and not
        ///         <seealso cref="Org.IdentityConnectors.Framework.Spi.Operations.UpdateAttributeValuesOp" />
        ///         this method will be simulated by fetching, merging, and calling
        ///         <seealso
        ///             cref="Org.IdentityConnectors.Framework.Spi.Operations.UpdateOp#update(ObjectClass, Uid, Set, OperationOptions)" />
        ///         . Therefore, connector implementations are encourage to implement
        ///         <seealso cref="Org.IdentityConnectors.Framework.Spi.Operations.UpdateAttributeValuesOp" />
        ///         from a performance and atomicity standpoint.
        ///     </para>
        /// </summary>
        /// <param name="objclass">
        ///     the type of object to modify. Must not be null.
        /// </param>
        /// <param name="uid">
        ///     the uid of the object to modify. Must not be null.
        /// </param>
        /// <param name="valuesToAdd">
        ///     set of <seealso cref="Attribute" /> deltas. The values for the attributes
        ///     in this set represent the values to add to attributes in the
        ///     object. merged. This set must not include
        ///     {@link Org.IdentityConnectors.Framework.Common.Objects.OperationalAttributes
        ///     operational attributes}. Must not be null.
        /// </param>
        /// <param name="options">
        ///     additional options that impact the way this operation is run.
        ///     May be null.
        /// </param>
        /// <param name="cancellationToken"></param>
        /// <returns>
        ///     the <seealso cref="Uid" /> of the updated object in case the update changes
        ///     the formation of the unique identifier.
        /// </returns>
        /// <exception cref="Org.IdentityConnectors.Framework.Common.Exceptions.UnknownUidException">
        ///     if the <seealso cref="Uid" /> does not exist on the resource.
        /// </exception>
        Task<Uid> AddAttributeValuesAsync(ObjectClass objclass, Uid uid, ICollection<ConnectorAttribute> valuesToAdd,
            OperationOptions options, CancellationToken cancellationToken);

        /// <summary>
        ///     Update the object specified by the <seealso cref="ObjectClass" /> and <seealso cref="Uid" />,
        ///     removing from the current values of each attribute the values provided.
        ///     <para>
        ///         For each attribute that the input set contains, remove from the current
        ///         values of that attribute in the target object any value that matches one
        ///         of the values of the attribute from the input set.
        ///     </para>
        ///     <para>
        ///         NOTE that this does not specify how to handle unmatched values. The
        ///         general assumption for an attribute of a {@code ConnectorObject} is that
        ///         the values for an attribute are merely <i>representational state</i>.
        ///         Therefore, the implementer should simply ignore any provided value that
        ///         does not match a current value of that attribute in the target object.
        ///         Deleting an unmatched value should always succeed.
        ///     </para>
        ///     <para>
        ///         IMPLEMENTATION NOTE: for connectors that merely implement
        ///         <seealso cref="Org.IdentityConnectors.Framework.Spi.Operations.UpdateOp" /> and not
        ///         <seealso cref="Org.IdentityConnectors.Framework.Spi.Operations.UpdateAttributeValuesOp" />
        ///         this method will be simulated by fetching, merging, and calling
        ///         <seealso
        ///             cref="Org.IdentityConnectors.Framework.Spi.Operations.UpdateOp#update(ObjectClass, Uid, Set, OperationOptions)" />
        ///         . Therefore, connector implementations are encourage to implement
        ///         <seealso cref="Org.IdentityConnectors.Framework.Spi.Operations.UpdateAttributeValuesOp" />
        ///         from a performance and atomicity standpoint.
        ///     </para>
        /// </summary>
        /// <param name="objclass">
        ///     the type of object to modify. Must not be null.
        /// </param>
        /// <param name="uid">
        ///     the uid of the object to modify. Must not be null.
        /// </param>
        /// <param name="valuesToRemove">
        ///     set of <seealso cref="Attribute" /> deltas. The values for the attributes
        ///     in this set represent the values to remove from attributes in
        ///     the object. merged. This set must not include
        ///     {@link Org.IdentityConnectors.Framework.Common.Objects.OperationalAttributes
        ///     operational attributes}. Must not be null.
        /// </param>
        /// <param name="options">
        ///     additional options that impact the way this operation is run.
        ///     May be null.
        /// </param>
        /// <param name="cancellationToken"></param>
        /// <returns>
        ///     the <seealso cref="Uid" /> of the updated object in case the update changes
        ///     the formation of the unique identifier.
        /// </returns>
        /// <exception cref="Org.IdentityConnectors.Framework.Common.Exceptions.UnknownUidException">
        ///     if the <seealso cref="Uid" /> does not exist on the resource.
        /// </exception>
        Task<Uid> RemoveAttributeValuesAsync(ObjectClass objclass, Uid uid,
            ICollection<ConnectorAttribute> valuesToRemove, OperationOptions options,
            CancellationToken cancellationToken);
    }

    #endregion

    #region IValidateAsyncApiOp

    public interface IValidateAsyncApiOp : ValidateApiOp
    {
        /// <summary>
        ///     Validates the
        ///     {@link org.identityconnectors.framework.api.APIConfiguration
        ///     configuration}.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <exception cref="Exception">
        ///     if the configuration is not valid.
        /// </exception>
        Task ValidateAsync(CancellationToken cancellationToken);
    }

    #endregion
}