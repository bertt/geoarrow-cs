using Apache.Arrow;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;

namespace geoarrow;
public static class IArrayExtensions
{
    public static IEnumerable<byte[]> ToWkb(this IArrowArray arrowArray)
    {
        return arrowArray switch
        {
            ListArray listArray => ToWkb(ToNts(listArray)),
            StringArray stringArray => ArrowWktReader.ToWkb(stringArray),
            BinaryArray binaryArray => ArrowWkbReader.ToWkb(binaryArray),
            StructArray structArray => ToWkb(ArrowReader.ToNts(structArray)),
            FixedSizeListArray fixedSizeListArray => ToWkb(ArrowInterleavedReader.PointsToNts(fixedSizeListArray)),
            _ => throw new NotImplementedException()
        };
    }

    private static IEnumerable<byte[]> ToWkb(IEnumerable<Geometry> geometries)
    {
        return geometries.Select(g => new WKBWriter().Write(g));
    }

    private static IEnumerable<Geometry> ToNts(ListArray listArray)
    {
        var values = listArray.Values;

        var isInterleavedPoints = values is FixedSizeListArray && ((FixedSizeListArray)values).Values is DoubleArray;
        var isInterleavedLines = values is ListArray && ((ListArray)values).Values is FixedSizeListArray;
        var isInterleavedPolygons = values is ListArray && ((ListArray)values).Values is ListArray && ((ListArray)(((ListArray)values).Values)).Values is FixedSizeListArray;

        IEnumerable<Geometry> result = null;
        if (isInterleavedPoints)
        {
            result = ArrowInterleavedReader.PointsToNts((FixedSizeListArray)values);
        }
        else if (isInterleavedLines)
        {
            result = ArrowInterleavedReader.LinesToNts((ListArray)values);
        }
        else if (isInterleavedPolygons)
        {
            result = ArrowInterleavedReader.PolygonsToNts((ListArray)values);
        }
        else
        {
            result = ArrowReader.GetGeometries(listArray);
        }
        return result;
    }
}
