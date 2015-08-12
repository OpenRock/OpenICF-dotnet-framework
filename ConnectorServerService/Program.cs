using System;
using System.Configuration;
using System.Configuration.Install;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.ServiceProcess;
using Org.ForgeRock.OpenICF.Framework.ConnectorServerService.Properties;
using Org.IdentityConnectors.Common.Security;

namespace Org.ForgeRock.OpenICF.Framework.ConnectorServerService
{
    internal static class Program
    {
        private const string Debug = "debug";

        private static void Usage()
        {
            Console.WriteLine("Usage: ConnectorServer.exe <command> [option], where command is one of the following: ");
            Console.WriteLine("       /install [/serviceName <serviceName>] - Installs the service.");
            Console.WriteLine("       /uninstall [/serviceName <serviceName>] - Uninstalls the service.");
            Console.WriteLine("       /run - Runs the service from the console.");
            Console.WriteLine("       /setKey [<key>] - Sets the connector server key.");
            Console.WriteLine("       /setDefaults - Sets default app.config");
        }

        /// <summary>
        ///     The main entry point for the application.
        /// </summary>
        private static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Usage();
            }
            else
            {
                String cmd = args[0].ToLower();
                if (cmd.Equals("/setkey", StringComparison.InvariantCultureIgnoreCase))
                {
                    if (args.Length > 2)
                    {
                        Usage();
                        return;
                    }
                    DoSetKey(args.Length > 1 ? args[1] : null);
                    return;
                }
                if (cmd.Equals("/setDefaults", StringComparison.InvariantCultureIgnoreCase))
                {
                    if (args.Length > 1)
                    {
                        Usage();
                        return;
                    }
                    using (
                        var file = new StreamWriter(AppDomain.CurrentDomain.SetupInformation.ConfigurationFile, false))
                    {
                        file.WriteLine(Resources.ResourceManager.GetString("DefaultConfig"));
                        Console.WriteLine(@"Default configuration successfully restored.");
                    }
                    return;
                }
                if ("/install".Equals(cmd, StringComparison.InvariantCultureIgnoreCase))
                {
                    DoInstall();
                }
                else if ("/uninstall".Equals(cmd, StringComparison.InvariantCultureIgnoreCase))
                {
                    DoUninstall();
                }
                else if ("/run".Equals(cmd, StringComparison.InvariantCultureIgnoreCase))
                {
                    if (args.Length > 1 && Debug.Equals(args[1], StringComparison.InvariantCultureIgnoreCase))
                    {
                        Process currentProcess = Process.GetCurrentProcess();
                        Console.WriteLine(
                            @"It's time to attach with debugger to process:{0} and press any key to continue.",
                            currentProcess.Id);
                        Console.ReadKey();
                    }
                    DoRun();
                }
                else if ("/service".Equals(cmd, StringComparison.InvariantCultureIgnoreCase))
                {
                    ServiceBase.Run(new ServiceBase[] {new ConnectorServerService()});
                }
                else
                {
                    Usage();
                }
            }
        }

        private static void DoInstall()
        {
            TransactedInstaller ti = new TransactedInstaller();
            string[] cmdline =
            {
                Assembly.GetExecutingAssembly().Location
            };
            AssemblyInstaller ai = new AssemblyInstaller(
                cmdline[0],
                new string[0]);
            ti.Installers.Add(ai);
            InstallContext ctx = new InstallContext("install.log",
                cmdline);
            ti.Context = ctx;
            ti.Install(new System.Collections.Hashtable());
        }

        private static void DoUninstall()
        {
            TransactedInstaller ti = new TransactedInstaller();
            string[] cmdline =
            {
                Assembly.GetExecutingAssembly().Location
            };
            AssemblyInstaller ai = new AssemblyInstaller(
                cmdline[0],
                new string[0]);
            ti.Installers.Add(ai);
            InstallContext ctx = new InstallContext("uninstall.log",
                cmdline);
            ti.Context = ctx;
            ti.Uninstall(null);
        }

        private static void DoRun()
        {
            ConnectorServerService svc = new ConnectorServerService();

            svc.StartService(new String[0]);

            Console.WriteLine(@"Press q to shutdown.");

            while (true)
            {
                ConsoleKeyInfo info = Console.ReadKey();
                if (info.KeyChar == 'q')
                {
                    break;
                }
            }
            svc.StopService();
        }

        private static GuardedString ReadPassword()
        {
            GuardedString rv = new GuardedString();
            while (true)
            {
                ConsoleKeyInfo info = Console.ReadKey(true);
                if (info.Key == ConsoleKey.Enter)
                {
                    Console.WriteLine();
                    rv.MakeReadOnly();
                    return rv;
                }
                else
                {
                    Console.Write("*");
                    rv.AppendChar(info.KeyChar);
                }
            }
        }

        private static void DoSetKey(string key)
        {
            GuardedString str;
            if (key == null)
            {
                Console.Write("Please enter the new key: ");
                GuardedString v1 = ReadPassword();
                Console.Write("Please confirm the new key: ");
                GuardedString v2 = ReadPassword();
                if (!v1.Equals(v2))
                {
                    Console.WriteLine("Error: Key mismatch.");
                    return;
                }
                str = v2;
            }
            else
            {
                str = new GuardedString();
                foreach (char c in key)
                {
                    str.AppendChar(c);
                }
            }
            Configuration config =
                ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            config.AppSettings.Settings.Remove(ConnectorServerService.PropKey);
            config.AppSettings.Settings.Add(ConnectorServerService.PropKey, str.GetBase64SHA1Hash());
            config.Save(ConfigurationSaveMode.Modified);
            Console.WriteLine("Key has been successfully updated.");
        }
    }
}