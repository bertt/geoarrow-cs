using Apache.Arrow;
using NetTopologySuite.Geometries;

namespace geoarrow;
internal class ArrowReader
{

    internal static IEnumerable<Geometry> GetGeometries(ListArray listArray)
    {
        if (listArray.Values is StructArray)
            return ToNts(listArray);

        if (listArray.Values is ListArray la)
        {
            return la.Values switch
            {
                StructArray => GetLines(la),
                ListArray => GetPolygons(la),
                _ => throw new NotImplementedException()
            };
        }

        throw new NotImplementedException();
    }

    internal static IEnumerable<Geometry> ToNts(Apache.Arrow.Array array)
    {
        switch(array)
        {
            case ListArray listArray:
                return ToNts(listArray);
            case StructArray structArray:
                return ToNts(structArray);
            default:
                throw new NotImplementedException();
        }
    }


    internal static List<Polygon> GetPolygons(ListArray listArray)
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


    private static LinearRing GetLinearRing(ListArray listArray)
    {
        var structArray = (StructArray)listArray.GetSlicedValues(0);
        var coordinates = GetCoordinates(structArray);
        var ring = new LinearRing(coordinates.ToArray());
        return ring;
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


    private static List<Coordinate> GetCoordinates(StructArray structArray)
    {
        return Enumerable.Range(0, structArray.Length)
                         .Select(i => GetCoordinate(structArray, i))
                         .ToList();
    }

    private static IEnumerable<Geometry> ToNts(StructArray structArray)
    {
        return GetCoordinates(structArray).Select(coordinate => new Point(coordinate));
    }

    private static IEnumerable<Geometry> ToNts(ListArray listArray)
    {
        return Enumerable.Range(0, listArray.Length)
                         .Select(i => GetPoint((StructArray)listArray.GetSlicedValues(i)))
                         .ToList();
    }

    private static Point GetPoint(StructArray structArray)
    {
        return new Point(GetCoordinate(structArray));
    }


    private static Coordinate GetCoordinate(StructArray structArray, int i = 0)
    {
        var x = ((DoubleArray)structArray.Fields[0]).GetValue(i);
        var y = ((DoubleArray)structArray.Fields[1]).GetValue(i);

        return structArray.Fields.Count > 2
            ? new CoordinateZ((double)x!, (double)y!, (double)((DoubleArray)structArray.Fields[2]).GetValue(i)!)
            : new Coordinate((double)x!, (double)y!);
    }
}
