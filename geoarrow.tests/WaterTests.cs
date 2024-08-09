using Apache.Arrow.Ipc;
using NetTopologySuite.IO;

namespace geoarrow.tests;
public class WaterTests
{
    [Test]
    public async Task ReadWaterCentPoints()
    {
        var file = "testfixtures/ns-water-water_cent.arrow";
        var fileStream = File.OpenRead(file);
        var compression = new CompressionCodecFactory();
        using (var reader = new ArrowFileReader(fileStream, compression))
        {
            var recordBatch = await reader.ReadNextRecordBatchAsync();
            Assert.That(recordBatch.Length == 58413);

            var geometryArray = recordBatch.Column(3);
            var points = geometryArray.ToWkb();
            Assert.That(points.Count() == 58413);
            var firstPoint = points.First();
            var wkbreader = new WKBReader();
            var p = wkbreader.Read(firstPoint);

            Assert.That(p.Coordinates[0].X == 290205.98857427004);
            Assert.That(p.Coordinates[0].Y == 4823949.637479655);
        }
    }

}
