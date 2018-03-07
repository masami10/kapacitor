package tasksched

import (
	"github.com/coreos/etcd/clientv3"
	"context"
	"log"
)

func cfs(cli *clientv3.Client, serverId string) (int, int) {
	// 1. 查询节点数
	resp, err := cli.Get(context.TODO(), prefixHostnameKey, clientv3.WithPrefix(), clientv3.WithCountOnly())
	nodeNum := resp.Count
	if nodeNum == 0 {
		return -1, 0
	}

	// 2. 查询总任务数
	resp, err = cli.Get(context.TODO(), prefixTasksIdKey, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		log.Fatal("E! failed to get total task num", err)
	}
	taskNum := resp.Count



	// 3. 查询节点任务数
	tasksInNodeKey := prefixTasksInNodeKey + serverId
	resp, err = cli.Get(context.TODO(), tasksInNodeKey, clientv3.WithPrefix(), clientv3.WithCountOnly())
	nodeTaskNum := resp.Count

	// 4. 计算每个节点应该分配的任务数
	avgTaskNum := (taskNum)/nodeNum + 1

	//fmt.Println(runtime.Caller(1))
	//fmt.Println(nodeTaskNum, avgTaskNum)

	return int(avgTaskNum), int(nodeTaskNum)
}
