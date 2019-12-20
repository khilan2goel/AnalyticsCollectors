using System;
using System.Diagnostics;

namespace AnalyticsCollector
{
    public static class Logger
    {
        public static void Error(string message)
        {
            WriteEntry(message, "Error");
        }

        public static void Error(Exception ex)
        {
            WriteEntry(ex.ToString(), "Error");
        }

        public static void Warning(string message)
        {
            WriteEntry(message, "Warning");
        }

        public static void Info(string message)
        {
            WriteEntry(message, "Info");
        }

        private static void WriteEntry(string message, string type)
        {
            Console.WriteLine(message);

            Trace.WriteLine(
                string.Format("{0}:{1}:{2}:{3}",
                    "AnalyticsCollector",
                    DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                    type,
                    message));
        }
    }
}
