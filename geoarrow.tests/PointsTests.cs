using Apache.Arrow.Ipc;
using NetTopologySuite.IO;
using NetTopologySuite.Geometries;

namespace geoarrow.tests;
public class PointsTests
{
    [Test]
    public async Task ReadBasinPoint()
    {
        var file = "testfixtures/geometry_types/example-point.arrow";
        ArrowFileReader reader = Read(file);
        var recordBatch = await reader.ReadNextRecordBatchAsync();
        Assert.That(recordBatch.Length == 3);

        var geometryArray = recordBatch.Column(1);
        var points = geometryArray.ToWkb();

        DoAsserts(points);
    }

    [Test]
    public async Task ReadBasinPointInterleaved()
    {
        var file = "testfixtures/geometry_types/example-point-interleaved.arrow";
        ArrowFileReader reader = Read(file);
        var recordBatch = await reader.ReadNextRecordBatchAsync();
        Assert.That(recordBatch.Length == 3);

        var geometryArray = recordBatch.Column(1);
        var points = geometryArray.ToWkb();

        DoAsserts(points);
    }

    [Test]
    public async Task ReadBasinPointWkt()
    {
        var file = "testfixtures/geometry_types/example-point-wkt.arrow";
        ArrowFileReader reader = Read(file);
        var recordBatch = await reader.ReadNextRecordBatchAsync();
        Assert.That(recordBatch.Length == 3);

        var geometryArray = recordBatch.Column(1);
        var points = geometryArray.ToWkb();

        DoAsserts(points);
    }

    [Test]
    public async Task ReadBasinPointWkb()
    {
        var file = "testfixtures/geometry_types/example-point-wkb.arrow";
        ArrowFileReader reader = Read(file);
        var recordBatch = await reader.ReadNextRecordBatchAsync();
        Assert.That(recordBatch.Length == 3);

        var geometryArray = recordBatch.Column(1);
        var points = geometryArray.ToWkb();

        DoAsserts(points);
    }

    private static void DoAsserts(IEnumerable<byte[]> points)
    {
        Assert.That(points.Count() == 3);
        var p = (Point)new WKBReader().Read(points.First());
        Assert.That(p.X == 30);
        Assert.That(p.Y == 10);
    }


    private static ArrowFileReader Read(string file)
    {
        var fileStream = File.OpenRead(file);
        var compression = new CompressionCodecFactory();
        var reader = new ArrowFileReader(fileStream, compression);
        return reader;
    }
}
