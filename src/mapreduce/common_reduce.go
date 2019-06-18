package mapreduce

import (
	"os"
	"encoding/json"
	"fmt"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	// Reduce处理的数据
	reduceMap := map[string][]string{}

	// 读取临时文件
	for i:=0; i<=nMap; i++ {
		// 获取临时文件
		intermediateFileName := reduceName(jobName, i, reduceTask)
		fmt.Println(intermediateFileName)
		f, err := os.Open(intermediateFileName)
		if err != nil {
			//panic(err)
			continue
		}
		defer f.Close()

		insList := []KeyValue{}
		decoder := json.NewDecoder(f)
		decoder.Decode(&insList)

		fmt.Println(intermediateFileName)

		for _, ins := range insList {
			if _, ok := reduceMap[ins.Key]; !ok {
				reduceMap[ins.Key] = []string{}
			}

			reduceMap[ins.Key] = append(reduceMap[ins.Key], ins.Value)
		}
	}

	// 输出最终文件
	f, err := os.OpenFile(outFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// 集中处理数据
	for key, values := range reduceMap{
		outputValue := reduceF(key, values)
		insOut, err := json.Marshal(KeyValue{
			Key: key,
			Value: outputValue,
		})

		if err != nil {
			panic(err)
		}

		f.Write(insOut)
	}


}
