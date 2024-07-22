using Apache.Arrow.Ipc;

namespace geoarrow;

public class Tests
{
    [Test]
    public async Task Test1()
    {
        var file = "testfixtures/gemeenten2016.arrow";
        var fileStream = File.OpenRead(file);
        var s = new CompressionCodecFactory();
        using (var reader = new ArrowFileReader(fileStream,s))
        {
            var recordBatch = await reader.ReadNextRecordBatchAsync();
            Assert.That(recordBatch.Length == 391) ;
            var geoms = recordBatch.Column(35);
        }

        Assert.Pass();
    }
}