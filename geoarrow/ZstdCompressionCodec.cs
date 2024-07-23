using Apache.Arrow.Ipc;
using ZstdSharp;

namespace Apache.Arrow.Compression
{
    internal sealed class ZstdCompressionCodec : ICompressionCodec
    {
        private readonly Decompressor _decompressor;
        private readonly Compressor _compressor;

        public ZstdCompressionCodec(int? compressionLevel = null)
        {
            if (compressionLevel.HasValue &&
                (compressionLevel.Value < Compressor.MinCompressionLevel ||
                 compressionLevel.Value > Compressor.MaxCompressionLevel))
            {
                throw new ArgumentException(
                    $"Zstd compression level must be between {Compressor.MinCompressionLevel} and {Compressor.MaxCompressionLevel}",
                    nameof(compressionLevel));
            }

            _decompressor = new Decompressor();
            _compressor = new Compressor(compressionLevel ?? Compressor.DefaultCompressionLevel);
        }

        public int Decompress(ReadOnlyMemory<byte> source, Memory<byte> destination)
        {
            return _decompressor.Unwrap(source.Span, destination.Span);
        }

        public void Compress(ReadOnlyMemory<byte> source, Stream destination)
        {
            using var compressor = new CompressionStream(
                destination, _compressor, preserveCompressor: true, leaveOpen: true);
            compressor.Write(source.Span);
        }

        public void Dispose()
        {
            _decompressor.Dispose();
            _compressor.Dispose();
        }
    }
}