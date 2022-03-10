package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	lwsf "chainmaker.org/chainmaker/lws/file"
	"github.com/dustin/go-humanize"
)

var (
	sizeList  string
	writeTime *uint
)

var (
	value1024 = "5f7575632450456b625c49725d336a7e2158245a7c652b6e2252495e453254353f5b453c5e50" +
		"5d6d734c732c2a40763d4c294168347c4638535a2f7730444c215a5333534d745c353733554f5c3d6d545" +
		"e474d286e4e2849554928433379315d366726477d72454628362a6a3b442c716c6f2a544e63565c4f2827" +
		"443f662527696561405d296f56703727474a2b657d322e316b2d57443971655724555d3c4129783c6a292" +
		"2405555665e5c644f2769217176634c613c4358265634502a546350754c25384e2c786c652f5775623332" +
		"52292a48202e685e5b382d353f7a7c6d22617c692e774d69366a646b696a51294c3162334b65425067327" +
		"d74307c246221523a6a697356393c66345e687e7b763362357851764f552159695f2f7876664e54657c54" +
		"24563844492c664a4021436a6d70222b795670534370502032623b434f3a286f2f35453f2d517a50666d6" +
		"c4c29224e4673655a2c4f2f57637d43756a2e756d7d236e5c4674326d2c2c2b3e51734362246a7d697e2d" +
		"46733d5d337a376746443e6122217225727024205c2f7825687d5a52332328606963293857393b2841396" +
		"b225f73652f533f302e7359522d2a634b2e6f2b236e7a66432c6d6d7851565e385a494146433f3332573d" +
		"5225542c5c29525861703c2956215e4e24514e6b32233e2a3b3b5d406e2c6b5525426135683d563d6a5e7335786e757e47"

	valueNKB = ""
	valueMB  = ""
	valueNMB = ""

	testDataPath = "./data"
)

type walFunc func(args ...interface{}) (lwsf.WalFile, error)

func init() {
	flagInit()

	for i := 0; i < 512; i++ {
		valueNKB = valueNKB + value1024
	}

	for i := 0; i < 1024; i++ {
		valueMB = valueMB + value1024
	}

	for i := 0; i < 1; i++ {
		valueNMB = valueNMB + valueMB
	}
}

func flagInit() {
	flag.StringVar(&sizeList, "sizeList", "1",
		"input test data size, you can input multi size (MB) with ',' split, sp: 1,10,20")

	writeTime = flag.Uint("writeTime", 1, "input data times for test")
}

func main() {
	flag.Parse()
	mapSize := 1 << 30
	if err := mkdatadir(testDataPath); err != nil {
		panic(err)
	}

	if len(sizeList) == 0 {
		panic("empty sizeList")
	}

	sizeStrs := strings.Split(sizeList, ",")
	sizeUList := make([]int, 0, len(sizeStrs))
	for _, v := range sizeStrs {
		if vu, err := strconv.Atoi(v); err != nil {
			panic(fmt.Sprintf("invalidate size string: %s", v))
		} else {
			sizeUList = append(sizeUList, vu)
		}
	}

	for _, sz := range sizeUList {
		runFunc(sz, mapSize, *writeTime)
	}
}

func mmapf(args ...interface{}) (lwsf.WalFile, error) {
	if len(args) != 2 {
		panic("invalidate args")
	}
	return lwsf.NewMmapFile(args[0].(string), args[1].(int))
}

func filef(args ...interface{}) (lwsf.WalFile, error) {
	if len(args) != 1 {
		panic("invalidate args")
	}
	return lwsf.NewFile(args[0].(string))
}

func mmapfnf(dataSize int) string {
	return path.Join(testDataPath, strings.Replace(
		fmt.Sprintf("/test_mmap_%s.wal", humanize.Bytes(uint64(dataSize))),
		" ", "", -1))
}

func filefnf(dataSize int) string {
	return path.Join(testDataPath, strings.Replace(
		fmt.Sprintf("/test_file_%s.wal", humanize.Bytes(uint64(dataSize))),
		" ", "", -1))
}

func mmaptestf(dataT string, writeTimes uint, testFunc walFunc, args ...interface{}) time.Duration {
	fileSize := 1 << 30
	f, err := testFunc(args...)
	err = f.Truncate(int64(fileSize))
	if err != nil {
		panic(err)
	}
	data := []byte(dataT)
	start := time.Now()
	for i := 0; i < int(writeTimes); i++ {
		f.Write(data)
		f.Sync()
	}
	f.Close()
	return time.Since(start)
}

func runFunc(dataSizeMB, mapSize int, writeTimes uint) {
	dataNMB := ""
	for i := 0; i < dataSizeMB; i++ {
		dataNMB = dataNMB + valueMB
	}
	mmapTime := mmaptestf(dataNMB, writeTimes, mmapf, mmapfnf(len(dataNMB)), mapSize)
	fileTime := mmaptestf(dataNMB, writeTimes, filef, filefnf(len(dataNMB)))
	choose := "mmap"
	if mmapTime >= fileTime {
		choose = "file"
	}
	fmt.Println(fmt.Sprintf("dataSize: %s, writeTime: %d, time used(mmap: %d, file: %d)ms, choose: %s",
		humanize.Bytes(uint64(len(dataNMB))), writeTimes, mmapTime.Milliseconds(), fileTime.Milliseconds(), choose))
}

func mkdatadir(path string) error {
	_, err := os.Stat(path)
	if err == nil {
		return nil
	}
	if os.IsNotExist(err) {
		if err1 := os.MkdirAll(path, os.ModePerm); err1 != nil {
			return nil
		}
	} else {
		return err
	}
	return nil
}
