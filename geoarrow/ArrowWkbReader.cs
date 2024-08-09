using Apache.Arrow;

namespace geoarrow;
internal class ArrowWkbReader
{
    internal static IEnumerable<byte[]> ToWkb(BinaryArray binaryArray)
    {
        return Enumerable.Range(0, binaryArray.Length)
            .Select(i => binaryArray.GetBytes(i).ToArray())
            .ToList();
    }
}
