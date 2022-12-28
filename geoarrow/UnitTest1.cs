using Apache.Arrow.Ipc;
using System.Diagnostics;

namespace geoarrow;

public class Tests
{

    [Test]
    public async Task Test1()
    {
        var file = "testfixtures/gemeenten2016.arrow";
        var fileStream = File.OpenRead(file);
        using (var reader = new ArrowFileReader(fileStream))
        {
            // next line gives error: Arrow primitive 'FixedSizeList' is unsupported...
            var recordBatch = await reader.ReadNextRecordBatchAsync();
            Debug.WriteLine("Read record batch with {0} column(s)", recordBatch.ColumnCount);
        }

        Assert.Pass();
    }
}