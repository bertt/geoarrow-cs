# geoarrow

Experimental GeoArrow reader/writer library

Specs: https://github.com/geoarrow/geoarrow

Testdata Arrow file created by:

```
$ ogr2ogr -f arrow gemeenten2016.arrow gemeenten2016.geojson 
```

Sample reading polygon GeoArrow data:

```
var file = "testfixtures/gemeenten2016.arrow";
var fileStream = File.OpenRead(file);
var s = new CompressionCodecFactory();
using (var reader = new ArrowFileReader(fileStream,s))
{
    var recordBatch = await reader.ReadNextRecordBatchAsync();
    Assert.That(recordBatch.Length == 391) ;
    var geoms = recordBatch.Column(35);
}
```