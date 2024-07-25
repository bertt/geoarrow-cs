using Apache.Arrow;
using NetTopologySuite.Geometries;
using System.Diagnostics;

namespace geoarrow;
public static class ListArrayExtensions
{
    public static IEnumerable<Geometry> ToNts(this ListArray listArray)
    {
        var values = listArray.Values;

        var isInterleaved = values is FixedSizeListArray;

        var points = isInterleaved ? GetInterleavedPoints((FixedSizeListArray)values) : GetGeometries(listArray);

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
            var point = new Point((double)x!, (double)y!);
            points.Add(point);
        }

        return points;
    }

    private static IEnumerable<Geometry> GetGeometries(ListArray listArray)
    {
        if (listArray.Values is StructArray structArray)
        {
            return GetPoints(listArray);
        }
        else if (listArray.Values is ListArray la)
        {
            if (la.Values is StructArray)
            {
                return GetLines(la);
            }
            else if (la.Values is ListArray)
            {
                return GetPolygons(la);
                throw new NotImplementedException();
            }
            else
            {
                throw new NotImplementedException();
            }
        }
        else
        {
            throw new NotImplementedException();
        }
    }

    private static List<Polygon> GetPolygons(ListArray listArray)
    {
        var polygons = new List<Polygon>();
        for (int i = 0; i < listArray.Length; i++)
        {
            var values = (ListArray)listArray.GetSlicedValues(i);
            var linearRing = GetLinearRing(values);
            var polygon = new Polygon(linearRing);
            polygons.Add(polygon);
        }

        return polygons;
    }


    private static LinearRing GetLinearRing(ListArray listArray, int i = 0)
    {
        var values = listArray.GetSlicedValues(i);
        if (values is StructArray structArray)
        {
            var coordinates = GetCoordinates(structArray);
            var ring = new LinearRing(coordinates.ToArray());
            return ring;
        }
        else if (values is FixedSizeListArray fixedSizeListArray)
        {
            // todo: implement
            return null;
        }
        else
        {
            throw new NotImplementedException();
        }
    }


    private static List<LineString> GetLines(ListArray listArray)
    {
        var lines = new List<LineString>();
        for (int i = 0; i < listArray.Length; i++)
        {
            var structArray = (StructArray)listArray.GetSlicedValues(i);
            var coordinates = GetCoordinates(structArray);

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
            return new CoordinateZ((double)x!, (double)y!, (double)z!);
        }
        return new Coordinate((double)x!, (double)y!);
    }
}
