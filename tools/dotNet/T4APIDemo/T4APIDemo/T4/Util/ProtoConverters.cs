using System.Globalization;

namespace T4APIDemo.T4.Util;

public static class ProtoConverters
{
    #region Decimal Conversions

    /// <summary>
    /// Convert a .Net decimal to a Protobuf Decimal.
    /// </summary>
    /// <param name="value"></param>
    public static T4Proto.V1.Common.Decimal ToProtoDecimal(this decimal value)
    {
        return new T4Proto.V1.Common.Decimal { Value = value.ToString("G", CultureInfo.InvariantCulture) };
    }

    /// <summary>
    /// Convert a nullable .Net decimal to a Protobuf Decimal.
    /// </summary>
    /// <param name="value"></param>
    public static T4Proto.V1.Common.Decimal ToProtoDecimal(this decimal? value)
    {
        return value.HasValue ? value.Value.ToProtoDecimal() : new T4Proto.V1.Common.Decimal { Value = "" };
    }

    /// <summary>
    /// Convert a Protobuf Decimal to a .Net decimal.
    /// </summary>
    /// <param name="protoDecimal"></param>
    public static decimal ToDecimal(this T4Proto.V1.Common.Decimal protoDecimal)
    {
        return decimal.Parse(protoDecimal.Value, NumberStyles.Float, CultureInfo.InvariantCulture);
    }

    #endregion

    #region Price Conversions

    /// <summary>
    /// Convert a .Net decimal to a Protobuf Price.
    /// </summary>
    public static T4Proto.V1.Common.Price ToProtoPrice(this decimal value)
    {
        return new T4Proto.V1.Common.Price { Value = value.ToString("G", CultureInfo.InvariantCulture) };
    }

    /// <summary>
    /// Convert a nullable .Net decimal to a Protobuf Price.
    /// </summary>
    public static T4Proto.V1.Common.Price ToProtoPrice(this decimal? value)
    {
        return value.HasValue ? value.ToProtoPrice() : new T4Proto.V1.Common.Price { Value = "" };
    }

    /// <summary>
    /// Convert a Protobuf Price to a nullable .Net decimal.
    /// </summary>
    public static decimal? ToDecimal(this T4Proto.V1.Common.Price protoPrice)
    {
        if (string.IsNullOrEmpty(protoPrice?.Value))
        {
            return null;
        }

        return decimal.Parse(protoPrice.Value, NumberStyles.Float, CultureInfo.InvariantCulture);
    }

    #endregion

    #region String Conversions

    public static T4Proto.V1.Common.Decimal ToProtoDecimal(this string value)
    {
        return decimal.Parse(value, NumberStyles.Float, CultureInfo.InvariantCulture).ToProtoDecimal();
    }

    public static T4Proto.V1.Common.Price ToProtoPrice(this string value)
    {
        return decimal.Parse(value, NumberStyles.Float, CultureInfo.InvariantCulture).ToProtoPrice();
    }

    public static string ToStringValue(this T4Proto.V1.Common.Decimal protoDecimal)
    {
        return protoDecimal.Value;
    }

    public static string ToStringValue(this T4Proto.V1.Common.Price protoPrice)
    {
        return protoPrice.Value;
    }

    #endregion
}
