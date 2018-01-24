package tasksched

import (
	"strconv"
	"github.com/coreos/etcd/clientv3"
	"context"
	"time"
	"log"
)

func cfs(cli *clientv3.Client, hostname string) time.Duration {
	// 计算延迟事务时间
	// 1. 查询总任务数
	taskNum := int64(0)
	resp, err := cli.Get(context.TODO(), totalTaskNumKey)
	if err != nil {
		log.Fatal("E! failed to get total task num", err)
	}
	for _, ev := range resp.Kvs {
		if string(ev.Key) == totalTaskNumKey {
			taskNum, _ = strconv.ParseInt(string(ev.Value), 10, 64)
		}
	}
	// 2. 查询节点数
	resp, err = cli.Get(context.TODO(), prefixHostnameKey, clientv3.WithPrefix())
	nodeNum := int64(len(resp.Kvs))

	// 3. 查询节点任务数
	tasksInNodeKey := prefixTasksInNodeKey + hostname
	resp, err = cli.Get(context.TODO(), tasksInNodeKey, clientv3.WithPrefix())
	nodeTaskNum := int64(len(resp.Kvs))

	// 4. 计算每个节点应该分配的任务数
	avgTaskNum := (taskNum+1)/nodeNum + 1

	// 5. 计算延迟抓取时间
	diff := nodeTaskNum - avgTaskNum

	delay := int64(0)
	if diff >= 0 {
		delay = diff + 1
	}

	return time.Duration(delay) * time.Second
}
