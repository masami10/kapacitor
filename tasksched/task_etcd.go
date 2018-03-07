package tasksched

import (
	"fmt"
	"strconv"

	"bytes"
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/masami10/kapacitor/client/v1"
	"github.com/masami10/kapacitor/server"
	"github.com/masami10/kapacitor/services/httpd"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
	"github.com/masami10/kapacitor/services/diagnostic"
	"github.com/masami10/kapacitor/keyvalue"
)

const (
	prefixHostnameKey       = "kapacitors/hostname/"
	prefixHostnameRevKey    = "kapacitors/rev/"
	prefixTasksIdKey        = "tasks/id/"
	prefixTasksHostnameKey  = "tasks/task_id/"
	prefixTasksConfigKey    = "tasks/config/"
	prefixTasksInfo         = "tasks/info/"
	prefixTasksIsCreatedKey = "tasks/is_created/"
	prefixTasksInNodeKey    = "tasks/in_node/"
	prefixNewTasksKey       = "tasks/new/in_node/"
	prefixOpPatchKey        = "tasks/op/patch/"
	opDeleteKey             = "tasks/op/delete"
	prefixOpResponseKey     = "tasks/op_response/rev/"
)

const CFS_ALGO = "CFS"

const respTemplate = `{"status_code": "%s", "data": %s}`

type Diagnostic interface {
	Debug(msg string, ctx ...keyvalue.T)
	Info(msg string, ctx ...keyvalue.T)
	Error(msg string, err error, ctx ...keyvalue.T)
}

type TaskEtcd struct {
	Cli      *clientv3.Client
	Kvc      clientv3.KV
	Kcli     *client.Client
	Logger   Diagnostic
	ServerId string
}

func connect(url string, skipSSL bool) (*client.Client, error) {
	return client.New(client.Config{
		URL:                url,
		InsecureSkipVerify: skipSSL,
	})
}

func NewTaskEtcd(c *server.Config, serverId string, diagService *diagnostic.Service) (*TaskEtcd, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(c.EtcdServers, ","),
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	kvc := clientv3.NewKV(cli)

	kcli, err := connect("http://localhost:9092", false)

	if err != nil {
		return nil, err
	}

	//l := logService.NewLogger("[taskEtcd] ", log.LstdFlags)
	l := diagService.NewServerHandler()

	return &TaskEtcd{
		Cli:      cli,
		Kcli:     kcli,
		Kvc:      kvc,
		Logger:   l,
		ServerId: serverId,
	}, nil
}

func (te *TaskEtcd) RegistryToEtcd(c *server.Config, kch chan int) {
	cli := te.Cli
	kvc := te.Kvc
	hostName := c.Hostname
	hostnameKey := prefixHostnameKey + hostName + "/" + te.ServerId

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	// grant
	resp, err := cli.Grant(ctx, c.EtcdHearbeatInterval)
	if err != nil {
		te.Logger.Error("E! failed to grant", err)
	}
	cancel()

	// put key
	_, err = cli.Put(context.TODO(), hostnameKey, hostName, clientv3.WithLease(resp.ID))
	if err != nil {
		te.Logger.Error("E! failed to put hostname key", err)
	}

	// 加hostname是为了防止分布式系统中uuid重复
	hostnameRevKey := prefixHostnameRevKey + hostName + "/" + te.ServerId

	_, err = kvc.Txn(context.TODO()).
		If(clientv3.Compare(clientv3.Value(hostnameKey), "=", hostName)).
		Then(clientv3.OpPut(hostnameRevKey, "0")).
		Commit()
	if err != nil {
		te.Logger.Error("E! txn failed ", err)
	}

	ch, err := cli.KeepAlive(context.TODO(), resp.ID)
	if err != nil {
		te.Logger.Error("E! failed to keepalive ", err)
	}
	go func() {

		for kresp := range ch {
			kresp.GetRevision()
		}
		te.Logger.Error("E! etcd down", nil)
	}()

	<-kch
}

func (te *TaskEtcd) WatchTaskid(c *server.Config) {
	// put
	cli := te.Cli

	rch := cli.Watch(context.Background(), prefixTasksIdKey, clientv3.WithPrefix(), clientv3.WithFilterDelete())

	go te.CreateUnwatchedTask(c)

	nodeTaskNum := 0
	avgNum := 0

	var wg sync.WaitGroup
	for wresp := range rch {
		revision := strconv.FormatInt(wresp.Header.GetRevision(), 10)
		for _, ev := range wresp.Events {
			// 计算延迟事务时间
			//delayTime := 0
			if c.TaskSchedAlgo == CFS_ALGO {
				if avgNum <= nodeTaskNum {
					wg.Wait()
					avgNum, nodeTaskNum = cfs(te.Cli, te.ServerId)
				}
			} else {
				te.Logger.Error("E! The scheduling algorithm" + c.TaskSchedAlgo + " is not supported", nil)
			}

			if avgNum < nodeTaskNum {
				continue
			}

			fmt.Println("avgNum=", avgNum, "nodeTaskNum=", nodeTaskNum, "***********************************************************")

			nodeTaskNum += 1

			wg.Add(1)
			taskId := string(ev.Kv.Value)

			go func(taskId, revision string) {
				defer wg.Done()
				te.CreateNewTask(taskId, revision)
			}(taskId, revision)

		}
	}
}

type TaskConfig struct {
	// Unique identifier for the task
	ID string `json:"id"`
	// The task type (stream|batch).
	Type string `json:"type"`
	// The DBs and RPs the task is allowed to access.
	DBRPs interface{} `json:"dbrps"`
	//DBRPs string `json:"dbrps"`
	// The TICKscript for the task.
	Script string `json:"script"`
	// ID of task template
	TemplateID string `json:"template-id"`
	// Set of vars for a templated task
	Vars interface{} `json:"vars"`
	//Vars string `json:"vars"`
	// Status of the task
	Status string `json:"status"`
}

func CheckTaskCreated(taskId string, cli *clientv3.Client, l Diagnostic) string {
	taskIdIsCreated := prefixTasksIsCreatedKey + taskId

	isTaskCreated := "false"
	gresp, err := cli.Get(context.TODO(), taskIdIsCreated)
	if err != nil {
		l.Error("E! failed to get tasks info: ", err)
	}
	if len(gresp.Kvs) == 0 {
		return ""
	}
	for _, ev := range gresp.Kvs {
		if string(ev.Key) == taskIdIsCreated {
			isTaskCreated = string(ev.Value)
		}
	}

	return isTaskCreated
}

func (te *TaskEtcd) WatchPatchKey(c *server.Config) {
	cli := te.Cli
	kvc := te.Kvc
	rch := cli.Watch(context.TODO(), prefixOpPatchKey, clientv3.WithPrefix(), clientv3.WithFilterDelete())
	for wresp := range rch {
		revision := strconv.FormatInt(wresp.Header.GetRevision(), 10)
		for _, ev := range wresp.Events {
			go func(ev *clientv3.Event, revision string) {
				key := string(ev.Kv.Key)
				data := string(ev.Kv.Value)
				taskId := strings.Split(key, "/")[3]

				// 等待被任务修改或执行修改任务
				isTaskCreated := "false"
				t1 := time.Now()
				for {
					// 检查任务是否被修改，如果被修改，key会被删除
					gresp, err := cli.Get(context.TODO(), key)
					if err != nil {
						te.Logger.Error("E! failed to get patch key info: ", err)
					}
					if len(gresp.Kvs) == 0 {
						return
					}

					// 获取task所在的节点
					taskIdHostnameKey := prefixTasksHostnameKey + taskId
					gresp, err = cli.Get(context.TODO(), taskIdHostnameKey)
					if err != nil {
						te.Logger.Error("E! failed to get tasks info: ", err)
					}
					if len(gresp.Kvs) == 0 {
						return
					}
					var serverId string
					for _, ev := range gresp.Kvs {
						if string(ev.Key) == taskIdHostnameKey {
							serverId = string(ev.Value)
						}
					}

					if serverId != te.ServerId {
						return
					}
					// 检查任务是否被创建
					isTaskCreated = CheckTaskCreated(taskId, cli, te.Logger)
					if isTaskCreated == "" {
						//　任务未被创建
						return
					}
					if isTaskCreated == "true" {
						break
					}
					fmt.Println(runtime.Caller(0))
					fmt.Println(taskId, serverId)

					// 如果半个小时都没创建成功，任务可能出错，直接退出
					if time.Since(t1).Seconds() > 30*60 {
						return
					}
					time.Sleep(5 * time.Second)
				}

				grResp, err := cli.Grant(context.TODO(), 60)
				if err != nil {
					te.Logger.Error("E! failed to grant", err)
				}

				var t client.Task
				t, statusCode, err := updateTask(taskId, data, te.Kcli)
				if err != nil {
					// 修改任务失败
					respData := fmt.Sprintf(respTemplate, statusCode, `"`+err.Error()+`"`)

					_, err = kvc.Txn(context.TODO()).
						If(clientv3.Compare(clientv3.ModRevision(prefixOpPatchKey+taskId), "=", wresp.Header.GetRevision())).
						Then(clientv3.OpPut(prefixOpResponseKey+revision, respData, clientv3.WithLease(grResp.ID)),
							clientv3.OpDelete(prefixOpPatchKey+taskId)).
						Commit()
					if err != nil {
						te.Logger.Error("E! txn update task info error: ", err)
					}
					return
				}

				// 给kapacitor客户端响应内容
				taskInfo := httpd.MarshalJSON(t, false)
				respData := fmt.Sprintf(respTemplate, statusCode, taskInfo)

				var rawTask TaskConfig
				rawTask.ID = t.ID
				rawTask.Type = t.Type.String()
				rawTask.Script = t.TICKscript
				rawTask.DBRPs = t.DBRPs
				rawTask.Status = t.Status.String()
				rawTask.TemplateID = t.TemplateID
				rawTask.Vars = t.Vars

				taskConfigData := httpd.MarshalJSON(rawTask, false)

				_, err = kvc.Txn(context.TODO()).
					If(clientv3.Compare(clientv3.ModRevision(prefixOpPatchKey+taskId), "=", wresp.Header.GetRevision())).
					Then(clientv3.OpPut(prefixOpResponseKey+revision, respData, clientv3.WithLease(grResp.ID)),
						clientv3.OpPut(prefixTasksInfo+taskId, string(taskInfo)),
						clientv3.OpPut(prefixTasksConfigKey+taskId, string(taskConfigData)),
						clientv3.OpDelete(prefixOpPatchKey+taskId)).
					Commit()
				if err != nil {
					te.Logger.Error("E! txn update task info error: ", err)
				}
			}(ev, revision)

		}
	}
}

func (te *TaskEtcd) WatchDeleteKey(c *server.Config) {
	cli := te.Cli
	kvc := te.Kvc
	rch := cli.Watch(context.TODO(), opDeleteKey, clientv3.WithPrefix(), clientv3.WithFilterDelete())

	for wresp := range rch {
		revision := strconv.FormatInt(wresp.Header.GetRevision(), 10)
		for _, ev := range wresp.Events {
			taskId := string(ev.Kv.Value)

			go func(taskId, revision string) {
				// 获取task所在的节点
				taskIdKey := prefixTasksIdKey + taskId
				taskIdHostnameKey := prefixTasksHostnameKey + taskId

				// 等待被任务删除或执行删除任务
				var serverId string
				isTaskCreated := "false"

				fmt.Println(runtime.Caller(0))
				fmt.Println(taskId)

				t1 := time.Now()
				for {
					gresp, err := cli.Get(context.TODO(), taskIdHostnameKey)
					if err != nil {
						te.Logger.Error("E! get task info error: ", err)
					}
					if len(gresp.Kvs) == 0 {
						return
					}

					for _, ev := range gresp.Kvs {
						if string(ev.Key) == taskIdHostnameKey {
							serverId = string(ev.Value)
						}
					}

					if serverId != te.ServerId {
						return
					}

					// 检查任务是否被创建
					isTaskCreated = CheckTaskCreated(taskId, cli, te.Logger)
					if isTaskCreated == "" {
						//　任务未被创建
						return
					}

					if isTaskCreated == "true" {
						break
					}
					fmt.Println(runtime.Caller(0))
					fmt.Println(taskId, serverId)

					// 如果半个小时都没创建成功，任务可能出错，直接退出
					if time.Since(t1).Seconds() > 30*60 {
						return
					}
					time.Sleep(5 * time.Second)
				}

				// 删除kapacitor上任务, 任务总数减一
				statusCode, derr := deleteTask(taskId, te.Kcli)

				grResp, gerr := cli.Grant(context.TODO(), 60)
				if gerr != nil {
					te.Logger.Error("E! failed to grant", gerr)
				}

				if derr != nil {

					respData := fmt.Sprintf(respTemplate, statusCode, `"`+derr.Error()+`"`)
					_, perr := cli.Put(context.TODO(), prefixOpResponseKey+revision, respData, clientv3.WithLease(grResp.ID))
					if perr != nil {
						te.Logger.Error("E! faliled to put response info: ", perr)
					}
					return
				}

				grResp, gerr = cli.Grant(context.TODO(), 60)
				if gerr != nil {
					te.Logger.Error("E! failed to grant", gerr)
				}

				respData := fmt.Sprintf(respTemplate, statusCode, `""`)

				tresp, terr := kvc.Txn(context.TODO()).
					If(clientv3.Compare(clientv3.Value(taskIdHostnameKey), "=", te.ServerId)).
					Then(clientv3.OpDelete(taskIdKey),
						clientv3.OpDelete(taskIdHostnameKey),
						clientv3.OpDelete(prefixTasksInNodeKey+te.ServerId+"/"+taskId),
						clientv3.OpDelete(prefixTasksInfo+taskId),
						clientv3.OpDelete(prefixTasksConfigKey+taskId),
						clientv3.OpDelete(prefixTasksIsCreatedKey+taskId),
						clientv3.OpPut(prefixOpResponseKey+revision, respData, clientv3.WithLease(grResp.ID))).
					Commit()

				if terr != nil {
					te.Logger.Error("E! failed to delete etcd task info: ", terr)
				}

				fmt.Println(runtime.Caller(0))
				fmt.Println(tresp.Succeeded)

			}(taskId, revision)

		}
	}
}

func (te *TaskEtcd) CreateUnwatchedTask(c *server.Config) {
	if c.TaskSchedAlgo != CFS_ALGO {
		te.Logger.Error("E! The scheduling algorithm" + c.TaskSchedAlgo + " is not supported", nil)
	}

	cli := te.Cli

	avgTaskNum, iTaskNum := cfs(cli, te.ServerId)
	if iTaskNum >= avgTaskNum {
		return
	}

	//
	var allTaskIds []string

	resp, err := cli.Get(context.TODO(), prefixTasksIdKey, clientv3.WithPrefix())
	if err != nil {
		te.Logger.Error("E! Failed to get task id", err)
	}
	for _, ev := range resp.Kvs {
		allTaskIds = append(allTaskIds, string(ev.Value))
	}
	sort.SliceStable(allTaskIds, func(i, j int) bool {
		return strings.Compare(allTaskIds[i], allTaskIds[j]) < 0
	})

	//
	var createdTasksIds []string
	resp, err = cli.Get(context.TODO(), prefixTasksInNodeKey, clientv3.WithPrefix())
	if err != nil {
		te.Logger.Error("E! ", err)
	}
	for _, ev := range resp.Kvs {
		createdTasksIds = append(createdTasksIds, string(ev.Value))
	}

	sort.SliceStable(createdTasksIds, func(i, j int) bool {
		return strings.Compare(createdTasksIds[i], createdTasksIds[j]) < 0
	})

	//
	diff := func(a, b []string) []string {
		mb := map[string]bool{}
		for _, x := range b {
			mb[x] = true
		}
		ab := []string{}
		for _, x := range a {
			if _, ok := mb[x]; !ok {
				ab = append(ab, x)
			}
		}
		return ab
	}

	uncreatedTaskIds := diff(allTaskIds, createdTasksIds)

	fmt.Println(runtime.Caller(0))
	fmt.Println(uncreatedTaskIds)

	var wg sync.WaitGroup
	nodeTaskNum := 0
	avgNum := 0
	for _, id := range uncreatedTaskIds {
		if avgNum <= nodeTaskNum {
			wg.Wait()
			avgNum, nodeTaskNum = cfs(te.Cli, te.ServerId)
			if avgNum < nodeTaskNum {
				break
			}
		}
		nodeTaskNum += 1

		wg.Add(1)
		go func(taskId string) {
			defer wg.Done()
			te.CreateNewTask(taskId, "0")
		}(id)
	}
	wg.Wait()
}

func (te *TaskEtcd) CreateNewTask(taskId, revision string) {
	cli := te.Cli
	kvc := te.Kvc

	//事务创建任务

	// 重新查询revision
	if revision == "0" {
		resp, err := cli.Get(context.TODO(), prefixTasksIdKey+taskId)
		if err != nil {
			te.Logger.Error("E! failed to get task info: ", err)
		}
		if len(resp.Kvs) == 0 {
			return
		}
		revision = strconv.FormatInt(resp.Kvs[0].ModRevision, 10)
	}

	taskIdHostnameKey := prefixTasksHostnameKey + taskId
	isCreatedKey := prefixTasksIsCreatedKey + taskId
	newTaskIdKey := prefixNewTasksKey + te.ServerId + "/" + taskId

	kresp, err := kvc.Txn(context.TODO()).
		If(clientv3.Compare(clientv3.CreateRevision(taskIdHostnameKey), "=", 0), clientv3.Compare(clientv3.CreateRevision(isCreatedKey), "=", 0)).
		Then(clientv3.OpPut(taskIdHostnameKey, te.ServerId), clientv3.OpPut(newTaskIdKey, taskId)).
		Commit()
	if err != nil {
		te.Logger.Error("E! failed to set task's hostname: ", err)
	}

	// 检查事务条件是否成立
	if kresp.Succeeded == false {
		return
	}

	// 获取任务信息
	taskConfigKey := prefixTasksConfigKey + taskId

	gresp, err := cli.Get(context.TODO(), taskConfigKey)
	if err != nil {
		te.Logger.Error("E! failed to get task configuration: ", err)
	}
	if len(gresp.Kvs) == 0 {
		_, terr := kvc.Txn(context.TODO()).
			If(clientv3.Compare(clientv3.CreateRevision(taskIdHostnameKey), "!=", 0)).
			Then(clientv3.OpDelete(taskIdHostnameKey),
				clientv3.OpDelete(newTaskIdKey)).
			Commit()
		if terr != nil {
			te.Logger.Error("E! failed to delete taskIdHostnameKey: ", terr)
		}
	}

	for _, evc := range gresp.Kvs {
		if string(evc.Key) == taskConfigKey {
			taskConfig := string(evc.Value)

			var t client.Task
			t, statusCode, cerr := createTask(taskConfig, te.Kcli)
			if cerr != nil {
				// 创建任务失败
				// 删除etcd上任务信息
				te.Logger.Info("E! create tasks error: " + cerr.Error())
				grResp, gerr := cli.Grant(context.TODO(), 60)
				if gerr != nil {
					te.Logger.Error("E! etcd grant time error: ", gerr)
				}
				respData := fmt.Sprintf(respTemplate, statusCode, `"`+cerr.Error()+`"`)

				_, terr := kvc.Txn(context.TODO()).
					If(clientv3.Compare(clientv3.Value(taskIdHostnameKey), "=", te.ServerId)).
					Then(clientv3.OpDelete(taskIdHostnameKey),
						clientv3.OpDelete(newTaskIdKey),
						clientv3.OpDelete(prefixTasksIdKey+taskId),
						clientv3.OpDelete(prefixTasksConfigKey+taskId),
						clientv3.OpPut(prefixOpResponseKey+revision, respData, clientv3.WithLease(grResp.ID)),
					).
					Commit()
				if terr != nil {
					te.Logger.Error("E! etcd error while delete task information: ", terr)
				}
				break
			}

			// 成功创建任务
			// 内容推送给kapacitor客户端
			taskInfo := httpd.MarshalJSON(t, false)
			respData := fmt.Sprintf(respTemplate, statusCode, taskInfo)

			//
			taskIdInNodeKey := prefixTasksInNodeKey + te.ServerId + "/" + taskId
			taskIdHostnameKey := prefixTasksHostnameKey + taskId

			grResp, err := cli.Grant(context.TODO(), 60)
			if err != nil {
				te.Logger.Error("E! etcd grant time error: ", err)
			}

			tresp, terr := kvc.Txn(context.TODO()).
				If(clientv3.Compare(clientv3.Value(taskIdHostnameKey), "=", te.ServerId)).
				Then(clientv3.OpPut(isCreatedKey, "true"),
					clientv3.OpPut(taskIdInNodeKey, taskId),
					clientv3.OpPut(prefixOpResponseKey+revision, respData, clientv3.WithLease(grResp.ID)),
					clientv3.OpPut(prefixTasksInfo+taskId, string(taskInfo)),
					clientv3.OpDelete(newTaskIdKey),
				).
				Commit()

			if terr != nil {
				te.Logger.Error("E! get total task num error: ", terr)
			}
			fmt.Println(runtime.Caller(0))
			fmt.Println(tresp.Succeeded)
			if tresp.Succeeded == true {
				break
			}

		}
	}
}

func (te *TaskEtcd) WatchDeleteTasksInNodeKey(c *server.Config) {
	cli := te.Cli
	rch := cli.Watch(context.TODO(), prefixTasksHostnameKey, clientv3.WithPrefix(), clientv3.WithFilterPut())

	nodeTaskNum := 0
	avgNum := 0

	var wg sync.WaitGroup
	for wresp := range rch {

		for _, ev := range wresp.Events {
			taskId := strings.Split(string(ev.Kv.Key), "/")[2]
			resp, err := cli.Get(context.TODO(), prefixTasksIdKey+taskId)
			if err != nil {
				te.Logger.Error("E! failed to get task id info: ", err)
			}
			if len(resp.Kvs) == 0 {
				continue
			}

			fmt.Println(runtime.Caller(0))
			fmt.Println(taskId)

			if c.TaskSchedAlgo == CFS_ALGO {
				if avgNum <= nodeTaskNum {
					wg.Wait()
					avgNum, nodeTaskNum = cfs(te.Cli, te.ServerId)
				}
			} else {
				te.Logger.Error("E! The scheduling algorithm" + c.TaskSchedAlgo + " is not supported", nil)
			}

			if avgNum < nodeTaskNum {
				continue
			}

			nodeTaskNum += 1

			wg.Add(1)
			go func(taskId, revision string) {
				defer wg.Done()
				te.CreateNewTask(taskId, revision)
			}(taskId, "0")

		}
	}

}

func (te *TaskEtcd) WatchHostname(c *server.Config) {
	// 当节点失效后的处理

	cli := te.Cli
	kvc := te.Kvc
	rch := cli.Watch(context.TODO(), prefixHostnameKey, clientv3.WithPrefix(), clientv3.WithFilterPut())

	for wresp := range rch {
		//revision := strconv.FormatInt(wresp.Header.GetRevision(), 10)
		for _, ev := range wresp.Events {
			downHostname := strings.Split(string(ev.Kv.Key), "/")[2]
			downHostnameId := strings.Split(string(ev.Kv.Key), "/")[3]

			downRevKey := prefixHostnameRevKey + downHostname + "/" + downHostnameId

			// 把is_created设置成false

			var taskIds []string
			inNodeKey := prefixTasksInNodeKey + downHostnameId

			resp, err := cli.Get(context.TODO(), inNodeKey, clientv3.WithPrefix())
			if err != nil {
				te.Logger.Error("E! get tasks error: ", err)
			}
			for _, ev := range resp.Kvs {
				taskIds = append(taskIds, string(ev.Value))
			}

			fmt.Println(runtime.Caller(0))
			fmt.Println(taskIds)

			var ops []clientv3.Op
			for _, id := range taskIds {
				isCreatedKey := prefixTasksIsCreatedKey + id
				taskInNodeKey := prefixTasksInNodeKey + downHostnameId + "/" + id

				ops = append(ops, clientv3.OpPut(isCreatedKey, "false"))
				ops = append(ops, clientv3.OpDelete(taskInNodeKey))
			}

			// 一次都没创建的任务,删除
			prefixNodeNewTasksKey := prefixNewTasksKey + downHostnameId
			resp, err = cli.Get(context.TODO(), prefixNodeNewTasksKey, clientv3.WithPrefix())
			if err != nil {
				te.Logger.Error("E! failed to get tasks: ", err)
			}
			for _, ev := range resp.Kvs {
				taskId := string(ev.Value)
				ops = append(ops, clientv3.OpDelete(prefixNodeNewTasksKey+"/"+taskId))
				ops = append(ops, clientv3.OpDelete(prefixTasksHostnameKey+taskId))
			}

			//ops = append(ops, clientv3.OpPut("foo",c.Hostname + " "+ te.ServerId + " " + downHostname + " " + strings.Join(taskIds, ","))) // 11111111111111111111111111111111

			// 删除rev key
			ops = append(ops, clientv3.OpDelete(downRevKey))

			//
			_, err = kvc.Txn(context.TODO()).
				If(clientv3.Compare(clientv3.CreateRevision(downRevKey), "!=", 0)).
				Then(ops...).
				Commit()

			if err != nil {
				te.Logger.Error("E! failed update down kapacitor's tasks ", err)
			}

		}
	}
}

func (te *TaskEtcd) RecreateTasks(c *server.Config) {
	// 把失效节点的任务重新创建

	if c.TaskSchedAlgo != CFS_ALGO {
		te.Logger.Error("E! The scheduling algorithm" + c.TaskSchedAlgo + " is not supported", nil)
	}

	cli := te.Cli
	kvc := te.Kvc

	avgTaskNum, iTaskNum := cfs(cli, te.ServerId)
	if iTaskNum > avgTaskNum {
		return
	}

	// 获取is_created = false 的所有任务id
	resp, err := cli.Get(context.TODO(), prefixTasksIsCreatedKey, clientv3.WithPrefix())
	if err != nil {
		te.Logger.Info("E! get uncreate tasks error: " + err.Error())
		return
	}

	var taskIds []string
	for _, ev := range resp.Kvs {
		if string(ev.Value) != "false" {
			continue
		}
		key := string(ev.Key)
		id := strings.Split(key, "/")[2]
		taskIds = append(taskIds, id)
	}

	var wg sync.WaitGroup
	modifiedTasksNum := 0
	for _, id := range taskIds {
		if modifiedTasksNum+iTaskNum >= avgTaskNum {
			break
		}
		modifiedTasksNum += 1

		wg.Add(1)
		go func(taskId string) { //
			defer wg.Done()

			taskIdHostnameKey := prefixTasksHostnameKey + taskId

			fmt.Println(taskIdHostnameKey)
			fmt.Println(runtime.Caller(1))

			tresp, err := kvc.Txn(context.TODO()).
				If(clientv3.Compare(clientv3.Value(prefixTasksIsCreatedKey+taskId), "=", "false")).
				Then(clientv3.OpPut(prefixTasksIsCreatedKey+taskId, "processing"),
					clientv3.OpPut(taskIdHostnameKey, te.ServerId),
					clientv3.OpPut(prefixTasksInNodeKey+te.ServerId+"/"+taskId, taskId),
				).
				Commit()
			if err != nil {
				te.Logger.Error("E! txn update task info error", err)
			}

			// 检查id所在的节点是否被修改成功
			if tresp.Succeeded == true {
				// 创建任务

				taskConfigKey := prefixTasksConfigKey + taskId
				gresp, err := cli.Get(context.TODO(), taskConfigKey)
				if err != nil {
					te.Logger.Error("E! faile to get task configuration", err)
				}

				for _, ev := range gresp.Kvs {
					if string(ev.Key) == taskConfigKey {
						taskConfig := string(ev.Value)

						_, _, err := createTask(taskConfig, te.Kcli)
						if err != nil {
							// 创建任务失败
							return
						}

						//
						_, err = kvc.Txn(context.TODO()).
							If(clientv3.Compare(clientv3.Value(prefixTasksIsCreatedKey+taskId), "=", "processing")).
							Then(clientv3.OpPut(prefixTasksIsCreatedKey+taskId, "true")).
							Commit()
						if err != nil {
							te.Logger.Error("E! txn update task info error: ", err)
						}

					}
				}
			}
		}(id) //

	}
	wg.Wait()
}

func createTask(task_config string, kcli *client.Client) (client.Task, string, error) {
	url := "http://localhost:9092/kapacitor/v1/tasks"

	return doTask("POST", url, task_config, kcli, http.StatusOK)
}

func updateTask(id string, data string, kcli *client.Client) (client.Task, string, error) {
	url := "http://localhost:9092/kapacitor/v1/tasks/" + id

	return doTask("PATCH", url, data, kcli, http.StatusOK)
}

func deleteTask(id string, kcli *client.Client) (string, error) {
	url := "http://localhost:9092/kapacitor/v1/tasks/" + id

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return "500", err
	}
	req.Header.Set("Content-Type", "application/json")

	_, err = kcli.Do(req, nil, http.StatusNoContent)

	statusCode := strconv.Itoa(http.StatusNoContent)
	if err != nil {
		fmt.Println(err)
		statusCode = "400"
	}

	return statusCode, err
}

func doTask(method string, url string, postData string, kcli *client.Client, code int) (client.Task, string, error) {
	t := client.Task{}

	var buf bytes.Buffer
	if len(postData) != 0 {
		buf.Write([]byte(postData))
	}
	req, err := http.NewRequest(method, url, &buf)
	if err != nil {
		return t, "500", err
	}
	req.Header.Set("Content-Type", "application/json")

	_, err = kcli.Do(req, &t, code)

	statusCode := strconv.Itoa(code)
	if err != nil {
		fmt.Println(err)
		statusCode = "400"
	}

	return t, statusCode, err
}
