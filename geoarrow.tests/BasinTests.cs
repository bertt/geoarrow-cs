using Apache.Arrow;
using Apache.Arrow.Ipc;
using NetTopologySuite.IO;

namespace geoarrow;

public class Tests
{
    [Test]
    public async Task ReadBasinPointInterleaved()
    {
        var file = "testfixtures/ns-water-basin_point-interleaved.arrow";
        var fileStream = File.OpenRead(file);
        var compression = new CompressionCodecFactory();
        using (var reader = new ArrowFileReader(fileStream, compression))
        {
            var recordBatch = await reader.ReadNextRecordBatchAsync();
            Assert.That(recordBatch.Length == 46);

            // read objectid of first record
            var objectIdArray = (Int64Array)recordBatch.Column(0);
            var firstObjectId = objectIdArray.GetValue(0);
            Assert.That(firstObjectId == 1);

            // read feat_code of first record
            var featCodeStringArray = (StringArray)recordBatch.Column(1);
            var firstFeatureCode = featCodeStringArray.GetString(0);
            Assert.That(firstFeatureCode == "WABA30");

            // read feat_code of first record
            var riverNameStringArray = (StringArray)recordBatch.Column(3);
            var firstRiverName = riverNameStringArray.GetString(0);
            Assert.That(firstRiverName == "BARRINGTON/CLYDE");

            // read geometry of first record (in a ListArray)
            var geometryArray = recordBatch.Column(5);

            var points = geometryArray.ToWkb();

            Assert.That(points.Count() == 46);
        }

        Assert.Pass();
    }

    [Test]
    public async Task ReadBasinPoint()
    {
        var file = "testfixtures/ns-water-basin_point.arrow";
        var fileStream = File.OpenRead(file);
        var compression = new CompressionCodecFactory();
        using (var reader = new ArrowFileReader(fileStream, compression))
        {
            var recordBatch = await reader.ReadNextRecordBatchAsync();
            Assert.That(recordBatch.Length == 46);

            // read objectid of first record
            var objectIdArray = (Int64Array)recordBatch.Column(0);
            var firstObjectId = objectIdArray.GetValue(0);
            Assert.That(firstObjectId == 1);

            // read feat_code of first record
            var featCodeStringArray = (StringArray)recordBatch.Column(1);
            var firstFeatureCode = featCodeStringArray.GetString(0);
            Assert.That(firstFeatureCode == "WABA30");

            // read feat_code of first record
            var riverNameStringArray = (StringArray)recordBatch.Column(3);
            var firstRiverName = riverNameStringArray.GetString(0);
            Assert.That(firstRiverName == "BARRINGTON/CLYDE");

            // read geometry of first record (in a ListArray)
            var c =recordBatch.Column(5);
            var geometryArray = recordBatch.Column(5);

            var points = geometryArray.ToWkb();

            Assert.That(points.Count() == 46);
        }

        Assert.Pass();
    }

    [Test]
    public async Task ReadBasinPointWkb()
    {
        var file = "testfixtures/ns-water-basin_point-wkb.arrow";
        var fileStream = File.OpenRead(file);
        var compression = new CompressionCodecFactory();
        using (var reader = new ArrowFileReader(fileStream, compression))
        {
            var recordBatch = await reader.ReadNextRecordBatchAsync();
            Assert.That(recordBatch.Length == 46);

            // read objectid of first record
            var objectIdArray = (Int64Array)recordBatch.Column(0);
            var firstObjectId = objectIdArray.GetValue(0);
            Assert.That(firstObjectId == 1);

            // read feat_code of first record
            var featCodeStringArray = (StringArray)recordBatch.Column(1);
            var firstFeatureCode = featCodeStringArray.GetString(0);
            Assert.That(firstFeatureCode == "WABA30");

            // read feat_code of first record
            var riverNameStringArray = (StringArray)recordBatch.Column(3);
            var firstRiverName = riverNameStringArray.GetString(0);
            Assert.That(firstRiverName == "BARRINGTON/CLYDE");

            var points = recordBatch.Column(5).ToWkb();

            Assert.That(points.Count() == 46);
        }

        Assert.Pass();
    }


    [Test]
    public async Task ReadBasinLine()
    {
        var file = "testfixtures/ns-water-basin_line.arrow";
        var fileStream = File.OpenRead(file);
        var compression = new CompressionCodecFactory();
        using (var reader = new ArrowFileReader(fileStream, compression))
        {
            var recordBatch = await reader.ReadNextRecordBatchAsync();
            Assert.That(recordBatch.Length == 255);

            // read objectid of first record
            var objectIdArray = (Int64Array)recordBatch.Column(0);
            var firstObjectId = objectIdArray.GetValue(0);
            Assert.That(firstObjectId == 1);

            var featCodeStringArray = (StringArray)recordBatch.Column(1);
            var firstFeatureCode = featCodeStringArray.GetString(0);
            Assert.That(firstFeatureCode == "WABA50");

            var geometryArray = recordBatch.Column(13);
            var lines = geometryArray.ToWkb();
            Assert.That(lines.Count() == 256);

            // read first line into a LineString
            var wkbreader = new WKBReader();
            var line = wkbreader.Read(lines.First());

            var verticesFirstLine = line.Coordinates.Length;
            Assert.That(verticesFirstLine == 405);
        }
    }

    [Test]
    public async Task ReadBasinLineInterleaved()
    {
        var file = "testfixtures/ns-water-basin_line-interleaved.arrow";
        var fileStream = File.OpenRead(file);
        var compression = new CompressionCodecFactory();
        using (var reader = new ArrowFileReader(fileStream, compression))
        {
            var recordBatch = await reader.ReadNextRecordBatchAsync();
            Assert.That(recordBatch.Length == 255);

            // read objectid of first record
            var objectIdArray = (Int64Array)recordBatch.Column(0);
            var firstObjectId = objectIdArray.GetValue(0);
            Assert.That(firstObjectId == 1);

            var featCodeStringArray = (StringArray)recordBatch.Column(1);
            var firstFeatureCode = featCodeStringArray.GetString(0);
            Assert.That(firstFeatureCode == "WABA50");

            var geometryArray = recordBatch.Column(13);
            var lines = geometryArray.ToWkb();
            Assert.That(lines.Count() == 256);
            var wkbreader = new WKBReader();
            var line = wkbreader.Read(lines.First());

            var verticesFirstLine = line.Coordinates.Length;
            Assert.That(verticesFirstLine == 405);
        }
    }

    [Test]
    public async Task ReadWaterCentInterleavedPoints()
    {
        var file = "testfixtures/ns-water-water_cent-interleaved.arrow";
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

   [Test]
    public async Task ReadBasinPolygon()
    {
        var file = "testfixtures/ns-water-basin_poly.arrow";
        var fileStream = File.OpenRead(file);
        var compression = new CompressionCodecFactory();
        using (var reader = new ArrowFileReader(fileStream, compression))
        {
            var recordBatch = await reader.ReadNextRecordBatchAsync();
            Assert.That(recordBatch.Length == 46);

            var geometryArray = recordBatch.Column(7);
            var polygons= geometryArray.ToWkb();
            Assert.That(polygons.Count() == 46);
            var firstPolygon = polygons.First();
            var wkbreader = new WKBReader();
            var p = wkbreader.Read(firstPolygon);

            Assert.That(p.Coordinates.Length == 1211);
        }
    }

    [Test]
    public async Task ReadBasinPolygonInterleaved()
    {
        var file = "testfixtures/ns-water-basin_poly-interleaved.arrow";
        var fileStream = File.OpenRead(file);
        var compression = new CompressionCodecFactory();
        using (var reader = new ArrowFileReader(fileStream, compression))
        {
            var recordBatch = await reader.ReadNextRecordBatchAsync();
            Assert.That(recordBatch.Length == 46);

            var geometryArray = recordBatch.Column(7);
            var polygons = geometryArray.ToWkb();
            Assert.That(polygons.Count() == 46);
            var firstPolygon = polygons.First();
            var wkbreader = new WKBReader();
            var p = wkbreader.Read(firstPolygon);

            Assert.That(p.Coordinates.Length == 1211+1);

        }
    }
}