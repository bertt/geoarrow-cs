using Apache.Arrow;
using NetTopologySuite.Geometries;

namespace geoarrow;
public static class ListArrayExtensions
{
    public static IEnumerable<Geometry> ToNts(this ListArray geometryArray)
    {
        var values = geometryArray.Values;

        var isInterleaved = values is FixedSizeListArray;

        var points = isInterleaved? GetInterleavedPoints((FixedSizeListArray)values): GetGeometry(geometryArray);

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

    private static IEnumerable<Geometry> GetGeometry(ListArray listArray)
    {
        return geometryArray.Values switch
        {
            StructArray => GetPoints(listArray),
            ListArray => GetLines(listArray),
            _ => throw new NotImplementedException()
        };
    }

    private static List<LineString> GetLines(ListArray listArray)
    {
        var lines = new List<LineString>();
        for (int i = 0; i < listArray.Length; i++)
        {
            var values = (StructArray)listArray.GetSlicedValues(i);
            var coordinates = GetCoordinates(values);
            var line = new LineString(coordinates.ToArray());
            lines.Add(line);
        }

        return lines;
    }

    private static List<Point> GetPoints(ListArray listArray)
    {
        var points = new List<Point>();
        for (int i = 0; i < listArray.Length; i++)
        {
            var values = (StructArray)listArray.GetSlicedValues(i);
            var point = GetPoint(values);
            points.Add(point);
        }

        return points;
    }

    private static Point GetPoint(StructArray structArray)
    {
        var coordinate = GetCoordinate(structArray);
        var point = new Point(coordinate);
        return point;
    } 

    private static List<Coordinate> GetCoordinates(StructArray structArray)
    {
        var coordinates = new List<Coordinate>();
        for (int i = 0; i < structArray.Length; i++)
        {
            var coordinate = GetCoordinate(structArray, i);
            coordinates.Add(coordinate);
        }

        return coordinates;
    }

    private static Coordinate GetCoordinate(StructArray structArray, int i = 0)
    {
        // Point point;
        var xArray = (DoubleArray)structArray.Fields[0];
        var x = xArray.GetValue(i);

        var yArray = (DoubleArray)structArray.Fields[1];
        var y = yArray.GetValue(i);

        if (structArray.Fields.Count > 2)
        {
            var zArray = (DoubleArray)structArray.Fields[2];
            var z = zArray.GetValue(i);
            return new CoordinateZ((double)x, (double)y, (double)z);
        }
        return new Coordinate((double)x, (double)y);
    }
}
