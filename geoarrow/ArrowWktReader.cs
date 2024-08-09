using Apache.Arrow;
using NetTopologySuite.IO;

namespace geoarrow;
internal class ArrowWktReader
{
    // Read array of wkt
    internal static IEnumerable<byte[]> ToWkb(StringArray stringArray)
    {
        for (var i = 0; i < stringArray.Length; i++)
        {
            var wkt = stringArray.GetString(i);
            var reader = new WKTReader();
            var geom = reader.Read(wkt);
            var writer = new WKBWriter();
            var wkb = writer.Write(geom);
            yield return wkb;
        }
    }


}
