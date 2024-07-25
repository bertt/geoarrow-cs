using Apache.Arrow.Ipc;
using K4os.Compression.LZ4;
using K4os.Compression.LZ4.Streams;

namespace Apache.Arrow.Compression
{
    internal sealed class Lz4CompressionCodec : ICompressionCodec
    {
        private readonly LZ4EncoderSettings? _settings;

        public Lz4CompressionCodec(int? compressionLevel = null)
        {
            if (compressionLevel.HasValue)
            {
                if (Enum.IsDefined(typeof(LZ4Level), compressionLevel))
                {
                    _settings = new LZ4EncoderSettings
                    {
                        CompressionLevel = (LZ4Level)compressionLevel,
                    };
                }
                else
                {
                    throw new ArgumentException(
                        $"Invalid LZ4 compression level ({compressionLevel})", nameof(compressionLevel));
                }
            }
        }

        public int Decompress(ReadOnlyMemory<byte> source, Memory<byte> destination)
        {
            using var decoder = LZ4Frame.Decode(source);
            return decoder.ReadManyBytes(destination.Span);
        }

        public void Compress(ReadOnlyMemory<byte> source, Stream destination)
        {
            using var encoder = LZ4Frame.Encode(destination, _settings, leaveOpen: true);
            encoder.WriteManyBytes(source.Span);
        }

        public void Dispose()
        {
        }
    }
}