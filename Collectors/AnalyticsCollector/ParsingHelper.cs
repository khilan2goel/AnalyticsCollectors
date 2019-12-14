using System;

namespace AnalyticsCollector
{
    public class ParsingHelper
    {
        public static bool TryParseWaterMark(string waterMark, out int continuationToken, out DateTime minModifiedDate)
        {
            continuationToken = 0;
            minModifiedDate = default(DateTime);

            if (!string.IsNullOrWhiteSpace(waterMark))
            {
                string[] waterMarks = waterMark.Split(',');

                if (!int.TryParse(waterMarks[0], out continuationToken))
                {
                    return false;
                }

                if (!DateTime.TryParse(waterMarks[1], out minModifiedDate))
                {
                    return false;
                }
            }
            else
            {
                return true;
            }

            return true;
        }
    }
}
