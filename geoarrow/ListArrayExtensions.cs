using Apache.Arrow;
using NetTopologySuite.Geometries;

namespace geoarrow;
public static class ListArrayExtensions
{
    public static IEnumerable<Point> ToNts(this ListArray geometryArray)
    {
        var values = geometryArray.Values;

        var isInterleaved = values is FixedSizeListArray;

        var points = isInterleaved? GetInterleavedPoints((FixedSizeListArray)values): GetPoints(geometryArray);

        return points;
    }

    private static IEnumerable<Point> GetInterleavedPoints(FixedSizeListArray listArray)
    {
        var points = new List<Point>();
        for (int i = 0; i < listArray.Length; i++)
        {
            var doubleArray = (DoubleArray)listArray.Values;
            var x = doubleArray.GetValue(i * 2);
            var y = doubleArray.GetValue(i * 2 + 1);
            var point = new Point((double)x, (double)y);
            points.Add (point);
        }

        return points;
    }

    private static IEnumerable<Point> GetPoints(ListArray geometryArray)
    {
        var points = new List<Point>();
        for (int i = 0; i < geometryArray.Length; i++)
        {
            var values = (StructArray)geometryArray.GetSlicedValues(i);
            var xArray = (DoubleArray)values.Fields[0];
            var x = xArray.GetValue(0);

            var yArray = (DoubleArray)values.Fields[1];
            var y = yArray.GetValue(0);

            Point point;
            if (values.Fields.Count > 2)
            {
                var zArray = (DoubleArray)values.Fields[2];
                var z = zArray.GetValue(0);
                point = new Point((double)x, (double)y, (double)z);
            }
            else
            {
                point = new Point((double)x, (double)y);
            }

            points.Add(point);
        }

        return points;
    }
}
