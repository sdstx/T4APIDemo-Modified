using System.Runtime.InteropServices;

namespace T4Proto.Util;

/// <summary>
/// Utility class for handling timestamps with nanosecond precision.
/// </summary>
public static class TimeUtil
{
    private static readonly TimeZoneInfo CSTZone =
        TimeZoneInfo.FindSystemTimeZoneById(
            RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? "Central Standard Time"
                : "America/Chicago");

    /// <summary>
    /// Converts a Google Protobuf Timestamp to DateTime in CST
    /// </summary>
    public static DateTime ToDateTimeCST(this Google.Protobuf.WellKnownTypes.Timestamp timestamp)
    {
        var utcDateTime = DateTimeOffset.FromUnixTimeSeconds(timestamp.Seconds)
            .AddTicks(timestamp.Nanos / 100)
            .UtcDateTime;

        return TimeZoneInfo.ConvertTimeFromUtc(utcDateTime, CSTZone);
    }

    /// <summary>
    /// Converts a DateTime to Google Protobuf Timestamp in CST timezone
    /// </summary>
    public static Google.Protobuf.WellKnownTypes.Timestamp ToProtoBufTimeStampCST(this DateTime timestamp)
    {
        // Create a DateTimeOffset using the CST timezone
        var cstOffset = new DateTimeOffset(
            timestamp.Kind == DateTimeKind.Unspecified
                ? DateTime.SpecifyKind(timestamp, DateTimeKind.Unspecified)
                : timestamp,
            CSTZone.GetUtcOffset(timestamp));

        // Convert to UTC
        var utcTime = cstOffset.ToUniversalTime();

        return new Google.Protobuf.WellKnownTypes.Timestamp
        {
            Seconds = utcTime.ToUnixTimeSeconds(),
            Nanos = (int)(utcTime.Ticks % TimeSpan.TicksPerSecond) * 100
        };
    }

    /// <summary>
    /// Converts a Google Protobuf Timestamp to DateTime in UTC
    /// </summary>
    public static DateTime ToDateTime(this Google.Protobuf.WellKnownTypes.Timestamp timestamp)
    {
        return DateTimeOffset.FromUnixTimeSeconds(timestamp.Seconds)
            .AddTicks(timestamp.Nanos / 100)
            .UtcDateTime;
    }

    /// <summary>
    /// Converts a DateTime to Google Protobuf Timestamp (expecting UTC input)
    /// </summary>
    public static Google.Protobuf.WellKnownTypes.Timestamp ToProtobufTimestamp(this DateTime timestamp)
    {
        // Ensure the timestamp is in UTC
        if (timestamp.Kind != DateTimeKind.Utc)
        {
            throw new ArgumentException("DateTime must be in UTC format", nameof(timestamp));
        }

        var unixTime = new DateTimeOffset(timestamp);

        return new Google.Protobuf.WellKnownTypes.Timestamp
        {
            Seconds = unixTime.ToUnixTimeSeconds(),
            Nanos = (int)(unixTime.Ticks % TimeSpan.TicksPerSecond) * 100
        };
    }
}