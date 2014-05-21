using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Azure.Utils.Test
{
    class Program
    {
        private static Dictionary<string, Type> TestTypes = new Dictionary<string, Type>()
        {
            {"CopyBlobContainerTest", typeof(CopyBlobContainerTest) }
        };

        private static Dictionary<string, MethodInfo> TestMethods = new Dictionary<string, MethodInfo>();
        private static readonly string TestMethodFormat = "{0}.{1}";

        static void PopulateTestMethods()
        {
            foreach(Type type in TestTypes.Values)
            {
                var className = type.Name;
                var methods = type.GetMethods();
                foreach(MethodInfo method in methods)
                {
                    TestMethods.Add(String.Format(TestMethodFormat, className, method.Name), method);
                }
            }
        }

        static void Main(string[] args)
        {
            try
            {
                if (String.Equals(args[0], "dbg", StringComparison.OrdinalIgnoreCase))
                {
                    args = args.Skip(1).ToArray();
                    Debugger.Launch();
                }

                Console.WriteLine("Populating Test Methods...");
                PopulateTestMethods();
                Console.WriteLine("Population of Test Methods completed");

                if (args.Length < 2)
                {
                    throw new InvalidOperationException("ClassName and MethodName required");
                }

                MethodInfo methodToCall = TestMethods[String.Format(TestMethodFormat, args[0], args[1])];
                if(methodToCall.ReturnType != typeof(Task))
                {
                    throw new InvalidOperationException("Return type of methodToCall MUST be 'Task'");
                }

                var parameters = methodToCall.GetParameters();
                if(args.Length - 2 != parameters.Length)
                {
                    throw new InvalidOperationException("Parameters length does not match");
                }                

                foreach(var param in parameters)
                {
                    Type stringType = typeof(string);
                    if(param.ParameterType != stringType)
                    {
                        throw new InvalidOperationException("Parameter must be of type string");
                    }
                }

                Console.WriteLine("Validation of arguments successful");

                Task task = (Task)methodToCall.Invoke(null, args.Skip(2).ToArray());
                task.Wait();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            Console.WriteLine("THE END");
        }
    }
}
