package mapreduce

import (
	"hash/fnv"
	"io/ioutil"
	"os"
	"encoding/json"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	//
	// doMap manages one map task: it should read one of the input files
	// (inFile), call the user-defined map function (mapF) for that file's
	// contents, and partition mapF's output into nReduce intermediate files.
	//
	// There is one intermediate file per reduce task. The file name
	// includes both the map task number and the reduce task number. Use
	// the filename generated by reduceName(jobName, mapTask, r)
	// as the intermediate file for reduce task r. Call ihash() (see
	// below) on each key, mod nReduce, to pick r for a key/value pair.
	//
	// mapF() is the map function provided by the application. The first
	// argument should be the input file name, though the map function
	// typically ignores it. The second argument should be the entire
	// input file contents. mapF() returns a slice containing the
	// key/value pairs for reduce; see common.go for the definition of
	// KeyValue.
	//
	// Look at Go's ioutil and os packages for functions to read
	// and write files.
	//
	// Coming up with a scheme for how to format the key/value pairs on
	// disk can be tricky, especially when taking into account that both
	// keys and values could contain newlines, quotes, and any other
	// character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	//
	// Your code here (Part I).
	//

	// 读文件
	mapFile, fileError := ioutil.ReadFile(inFile)
	if fileError != nil {
		panic(fileError)
	}

	// 调用自定义map函数
	kvs := mapF(inFile, string(mapFile))

	// 初始化r / value list pair
	resultMap := map[int][]KeyValue{}

	// 获取对应的r
	for _, kv := range kvs {
		// hash结果执行mod nReduce
		r := ihash(kv.Key) % nReduce
		if _, ok := resultMap[r];!ok {
			resultMap[r] = []KeyValue{}
		}

		resultMap[r] = append(resultMap[r], kv)
	}

	// 输出中间文件
	for r, kvList := range resultMap{
		// 获取中间文件名
		intermediateFileName := reduceName(jobName, mapTask, r)
		// 编码json
		f, err := os.Create(intermediateFileName)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		encoder := json.NewEncoder(f)
		err = encoder.Encode(kvList)
		if err != nil {
			panic(err)
		}
	}
}

func ihash(s string) int {
	// fnv hash算法
	// FNV能快速hash大量数据并保持较小的冲突率，它的高度分散使它适用于hash一些非常相近的字符串，比如URL，hostname，文件名，text，IP地址等。
	h := fnv.New32a()
	h.Write([]byte(s))
	// 0x7fffffff 为32位最大正整数，hash值与其做与运算代表将hash值取正
	return int(h.Sum32() & 0x7fffffff)
}
