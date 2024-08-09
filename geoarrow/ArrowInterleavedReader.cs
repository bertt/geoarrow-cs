using Apache.Arrow;
using NetTopologySuite.Geometries;

namespace geoarrow;
internal static class ArrowInterleavedReader
{

    internal static IEnumerable<Geometry> PointsToNts(FixedSizeListArray fixedSizeListArray)
    {
        var doubleArray = (DoubleArray)fixedSizeListArray.Values;
        return Enumerable.Range(0, fixedSizeListArray.Length)
            .Select(i =>
            {
                var x = doubleArray.GetValue(i * 2);
                var y = doubleArray.GetValue(i * 2 + 1);
                return new Point((double)x!, (double)y!);
            })
            .ToList();
    }

    internal static IEnumerable<Geometry>? PolygonsToNts(ListArray listArray)
    {
        var polygons = new List<Geometry>();
        for (int i = 0; i < listArray.Length; i++)
        {
            var offset = listArray.ValueOffsets[i];
            var length = listArray.ValueOffsets[i + 1] - offset;

            var d = (ListArray)listArray.Values;
            var linedata = (ListArray)d.Slice(offset, length);

            // use StructArray from values
            var fsl = (FixedSizeListArray)linedata.Values;

            var lines = LinesToNts(linedata);

            // get the first line
            // todo what if there are many lines?
            // var line = lines.ToArray().First();


            if (lines.Count == 1)
            {
                var poly = GetPolygonFromLine(lines.First());
                polygons.Add(poly);
            }
            else
            {
                var polys = new List<Polygon>();
                foreach (var l in lines)
                {
                    var poly = GetPolygonFromLine(l);
                    polys.Add(poly);
                }
                var mp = new MultiPolygon(polys.ToArray());
                polygons.Add(mp);
            }
        }

        return polygons;
    }

    private static Polygon GetPolygonFromLine(LineString l)
    {
        // close the line
        var poly = new Polygon(new LinearRing([.. l.Coordinates, l.Coordinates.First()]));
        return poly;
    }

    internal static List<LineString> LinesToNts(ListArray listArray)
    {
        var values = (FixedSizeListArray)listArray.Values;

        return Enumerable.Range(0, listArray.Length)
            .Select(i =>
            {
                var offset = listArray.ValueOffsets[i];
                var length = listArray.ValueOffsets[i + 1] - offset;
                var data = (FixedSizeListArray)values.Slice(offset, length);
                var line = PointsToNts(data);
                return new LineString(line.Select(p => p.Coordinate).ToArray());
            })
            .ToList();
    }
}
