package sarama

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkCompression(b *testing.B) {
	benchmarkCompressionMatrix(b, benchmarkCompress)
}

func BenchmarkDecompression(b *testing.B) {
	benchmarkCompressionMatrix(b, benchmarkDecompress)
}

func benchmarkCompressionMatrix(b *testing.B, benchmarkFn func(*testing.B, CompressionCodec, int, []byte)) {
	zeroesPayload := make([]byte, 1024*1024)
	patternPayload := make([]byte, 1024*1024)
	for i := 0; i < len(patternPayload); i++ {
		patternPayload[i] = byte((i / 256) + i*257)
	}
	codecs := parseTestCodecsEnvVar(b, "TEST_CODECS", []CompressionCodec{
		CompressionGZIP,
		CompressionSnappy,
		CompressionLZ4,
		CompressionZSTD,
	})
	levels := parseTestIntsEnvVar(b, "TEST_LEVELS", []int{5})
	sizes := parseTestIntsEnvVar(b, "TEST_SIZES", []int{128, 1024, 4096, 8192, 65536})
	for _, codec := range codecs {
		for _, level := range levels {
			for _, size := range sizes {
				name := fmt.Sprintf("%s level%d size%d", codec.String(), level, size)
				b.Run(name+" zeroes", func(b *testing.B) {
					b.ReportAllocs()
					benchmarkFn(b, codec, level, zeroesPayload[:size])
				})
				b.Run(name+" pattern", func(b *testing.B) {
					b.ReportAllocs()
					benchmarkFn(b, codec, level, patternPayload[:size])
				})
			}
		}
	}
}

func benchmarkCompress(b *testing.B, codec CompressionCodec, level int, payload []byte) {
	b.ResetTimer()

	for i := 1; i <= b.N; i++ {
		_, err := compress(codec, level, payload)
		if err != nil {
			b.Fatalf("compress error: %v", err)
		}
	}
}

func benchmarkDecompress(b *testing.B, codec CompressionCodec, level int, payload []byte) {
	msg, err := compress(codec, level, payload)
	if err != nil {
		b.Fatalf("compress error: %v", err)
	}
	b.ResetTimer()

	for i := 1; i <= b.N; i++ {
		_, err = decompress(codec, msg)
		if err != nil {
			b.Fatalf("decompress error: %v", err)
		}
	}
}

type justNeedLogf interface {
	Logf(string, ...interface{})
}

func parseTestIntsEnvVar(tOrB justNeedLogf, envName string, def []int) []int {
	env, ok := os.LookupEnv(envName)
	if !ok {
		return def
	}
	res := []int{}
	for _, s := range strings.Split(env, ",") {
		if v, err := strconv.Atoi(s); err == nil {
			res = append(res, v)
		}
	}
	if len(res) == 0 {
		tOrB.Logf("failed to find valid ints in %s, '%s'; using default instead", envName, env)
		return def
	}
	return res
}

func parseTestCodecsEnvVar(tOrB justNeedLogf, envName string, def []CompressionCodec) []CompressionCodec {
	env, ok := os.LookupEnv(envName)
	if !ok {
		return def
	}
	res := []CompressionCodec{}
	for _, codecName := range strings.Split(env, ",") {
		c := CompressionNone
		if err := (&c).UnmarshalText([]byte(codecName)); err == nil {
			res = append(res, c)
		}
	}
	if len(res) == 0 {
		tOrB.Logf("failed to find valid codecs in %s, '%s'; using default instead", envName, env)
		return def
	}
	return res
}
