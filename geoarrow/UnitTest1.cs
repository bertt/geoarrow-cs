using Apache.Arrow;
using Apache.Arrow.Ipc;
using NUnit.Framework;

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
            var geometryArray = (ListArray)recordBatch.Column(5);

            var points = geometryArray.ToNts();

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
            var geometryArray = (ListArray)recordBatch.Column(5);

            var points = geometryArray.ToNts();

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

            var geometryArray = (ListArray)recordBatch.Column(13);
            var lines = geometryArray.ToNts();
            Assert.That(lines.Count() == 256);
            var verticesFirstLine = lines.First().Coordinates.Length;
            Assert.That(verticesFirstLine == 405);
        }
    }

    [Test]
    public async Task Test1()
    {
        var file = "testfixtures/gemeenten2016.arrow";
        var fileStream = File.OpenRead(file);
        var compression = new CompressionCodecFactory();
        using (var reader = new ArrowFileReader(fileStream,compression))
        {
            var recordBatch = await reader.ReadNextRecordBatchAsync();
            Assert.That(recordBatch.Length == 391) ;
            var listArray = recordBatch.Column(35);
            var arrayData = listArray.Data;
            Assert.That(arrayData.Length == 391);
            var children = arrayData.Children;

        }

        Assert.Pass();
    }
}