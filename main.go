package main

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"compress/lzw"
	"compress/zlib"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/davecgh/go-spew/spew"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
	"github.com/rasky/go-lzo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func main() {
	var debugValue bool
	var stripLimit int

	var rootCommand = &cobra.Command{
		Use:   "compression-detector",
		Short: "Compression detector",
		Long: `
This tool attempts to determine the type of compression used in a file.
`,
		Args: cobra.MinimumNArgs(1),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if debugValue {
				logrus.SetLevel(logrus.DebugLevel)
			}
		},
		Run: func(cmd *cobra.Command, args []string) {
			for _, filename := range args {
				var reader io.Reader
				if filename == "-" {
					reader = os.Stdin
				} else {
					fileHandle, err := os.Open(filename)
					if err != nil {
						panic(err)
					}
					defer fileHandle.Close()
					reader = fileHandle
				}
				contents, err := ioutil.ReadAll(reader)
				if err != nil {
					panic(err)
				}
				results := detectCompression(contents, stripLimit)
				spew.Dump(results)
			}
		},
	}
	rootCommand.PersistentFlags().BoolVar(&debugValue, "debug", false, `Enable debug output`)
	rootCommand.PersistentFlags().IntVar(&stripLimit, "strip-limit", 100, `Only strip off (at most) this many bytes from the front (use -1 for no limit)`)

	err := rootCommand.Execute()
	if err != nil {
		panic(err)
	}
}

type DecompressionFunction struct {
	Name       string
	Decompress func([]byte) ([]byte, error)
}

type DecompressionResult struct {
	Name             string
	StartByte        int
	CompressedSize   int
	DecompressedSize int
}

var decompressionFunctions = []DecompressionFunction{
	DecompressionFunction{
		Name: "bzip2",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader := bzip2.NewReader(buffer)
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "gzip",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader, err := gzip.NewReader(buffer)
			if err != nil {
				return nil, err
			}
			defer reader.Close()
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "lzo",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			result, err := lzo.Decompress1X(buffer, 0 /*inLen*/, 0 /*outLen*/)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "lz4",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader := lz4.NewReader(buffer)
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "lzw-lsb-2",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader := lzw.NewReader(buffer, lzw.LSB, 2)
			defer reader.Close()
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "lzw-lsb-3",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader := lzw.NewReader(buffer, lzw.LSB, 3)
			defer reader.Close()
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "lzw-lsb-4",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader := lzw.NewReader(buffer, lzw.LSB, 4)
			defer reader.Close()
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "lzw-lsb-5",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader := lzw.NewReader(buffer, lzw.LSB, 5)
			defer reader.Close()
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "lzw-lsb-6",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader := lzw.NewReader(buffer, lzw.LSB, 6)
			defer reader.Close()
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "lzw-lsb-7",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader := lzw.NewReader(buffer, lzw.LSB, 7)
			defer reader.Close()
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "lzw-lsb-8",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader := lzw.NewReader(buffer, lzw.LSB, 8)
			defer reader.Close()
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "lzw-msb-2",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader := lzw.NewReader(buffer, lzw.MSB, 2)
			defer reader.Close()
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "lzw-msb-3",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader := lzw.NewReader(buffer, lzw.MSB, 3)
			defer reader.Close()
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "lzw-msb-4",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader := lzw.NewReader(buffer, lzw.MSB, 4)
			defer reader.Close()
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "lzw-msb-5",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader := lzw.NewReader(buffer, lzw.MSB, 5)
			defer reader.Close()
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "lzw-msb-6",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader := lzw.NewReader(buffer, lzw.MSB, 6)
			defer reader.Close()
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "lzw-msb-7",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader := lzw.NewReader(buffer, lzw.MSB, 7)
			defer reader.Close()
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "lzw-msb-8",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader := lzw.NewReader(buffer, lzw.MSB, 8)
			defer reader.Close()
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "snappy-block",
		Decompress: func(data []byte) ([]byte, error) {
			return snappy.Decode(nil, data)
		},
	},
	DecompressionFunction{
		Name: "snappy-stream",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader := snappy.NewReader(buffer)
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
	DecompressionFunction{
		Name: "zlib",
		Decompress: func(data []byte) ([]byte, error) {
			buffer := bytes.NewBuffer(data)
			reader, err := zlib.NewReader(buffer)
			if err != nil {
				return nil, err
			}
			defer reader.Close()
			result, err := ioutil.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			if buffer.Len() > 0 {
				return nil, fmt.Errorf("Buffer still has %d bytes (only read %d)", buffer.Len(), len(data)-buffer.Len())
			}
			return result, nil
		},
	},
}

func detectCompression(contents []byte, stripLimit int) []DecompressionResult {
	results := []DecompressionResult{}

	if stripLimit < 0 {
		stripLimit = len(contents)
	}
	for startByte := 0; startByte < stripLimit; startByte++ {
		theseContents := contents[startByte:]
		logrus.Debugf("Start byte %d (%d)", startByte, len(theseContents))

		for _, decompressionInfo := range decompressionFunctions {
			decompressedBytes, err := decompressionInfo.Decompress(theseContents)
			if err != nil {
				logrus.Debugf("Could not decompress from byte %d with %s: %v", startByte, decompressionInfo.Name, err)
				continue
			}

			if len(decompressedBytes) > 0 {
				logrus.Infof("Successfully decompressed from byte %d with %s: %d -> %d", startByte, decompressionInfo.Name, len(theseContents), len(decompressedBytes))
				result := DecompressionResult{
					Name:             decompressionInfo.Name,
					StartByte:        startByte,
					CompressedSize:   len(theseContents),
					DecompressedSize: len(decompressedBytes),
				}
				results = append(results, result)
			}
		}

		if len(results) > 0 {
			break
		}
	}

	return results
}
