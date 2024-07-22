namespace geoarrow;
using System;
using Apache.Arrow.Compression;
using Apache.Arrow.Ipc;

/// <summary>
/// Creates compression codec implementations for decompressing Arrow IPC data
/// </summary>
public sealed class CompressionCodecFactory : ICompressionCodecFactory
{
    public ICompressionCodec CreateCodec(CompressionCodecType compressionCodecType)
    {
        return CreateCodec(compressionCodecType, null);
    }

    public ICompressionCodec CreateCodec(CompressionCodecType compressionCodecType, int? compressionLevel)
    {
        return compressionCodecType switch
        {
            CompressionCodecType.Lz4Frame => new Lz4CompressionCodec(compressionLevel),
            CompressionCodecType.Zstd => new ZstdCompressionCodec(compressionLevel),
            _ => throw new NotImplementedException($"Compression type {compressionCodecType} is not supported")
        };
    }
}
