using Apache.Arrow;
using Apache.Arrow.Ipc;
using System.Runtime.InteropServices;

namespace geoarrow;

public class Tests
{
    [Test]
    public async Task Test2()
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

            // read geometry of first record
            var geometryArray = (ListArray)recordBatch.Column(5);
            var geometryStructArray = (StructArray)geometryArray.GetSlicedValues(0);
            // read x values of struct array
            var xArray = (DoubleArray)geometryStructArray.Fields[0];
            var firstX = xArray.GetValue(0);
            Assert.That(firstX == 277022.69361817511);

            // read y values of struct array
            var yArray = (DoubleArray)geometryStructArray.Fields[1];
            var firstY = yArray.GetValue(0);
            Assert.That(firstY == 4820886.6096734889);
        }

        Assert.Pass();
    }

    private static void NewMethod(IArrowArray int64Array)
    {
        var arrayData = int64Array.Data;

        var memory = arrayData.Buffers[1].Memory;
        var longs = MemoryMarshal.Cast<byte, long>(memory.Span);
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