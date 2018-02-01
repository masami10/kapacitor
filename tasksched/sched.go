package tasksched

import (
	"github.com/coreos/etcd/clientv3"
	"context"
	"log"
	"fmt"
	"runtime"
)

func cfs(cli *clientv3.Client, hostname string) (int, int) {
	// 计算延迟事务时间
	// 1. 查询总任务数
	resp, err := cli.Get(context.TODO(), prefixTasksIdKey, clientv3.WithPrefix())
	if err != nil {
		log.Fatal("E! failed to get total task num", err)
	}
	taskNum := len(resp.Kvs)

	// 2. 查询节点数
	resp, err = cli.Get(context.TODO(), prefixHostnameKey, clientv3.WithPrefix())
	nodeNum := len(resp.Kvs)

	// 3. 查询节点任务数
	tasksInNodeKey := prefixTasksInNodeKey + hostname
	resp, err = cli.Get(context.TODO(), tasksInNodeKey, clientv3.WithPrefix())
	nodeTaskNum := len(resp.Kvs)

	// 4. 计算每个节点应该分配的任务数
	avgTaskNum := (taskNum)/nodeNum + 1

	fmt.Println(runtime.Caller(1))
	fmt.Println(nodeTaskNum, avgTaskNum)

	return avgTaskNum, nodeTaskNum
}
