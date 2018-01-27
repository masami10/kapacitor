package tasksched

import (
	"fmt"
	"log"
	"strconv"

	"bytes"
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/masami10/kapacitor/client/v1"
	"github.com/masami10/kapacitor/server"
	"github.com/masami10/kapacitor/services/logging"
	"net/http"
	"strings"
	"time"
	"github.com/masami10/kapacitor/services/httpd"
	"github.com/masami10/kapacitor/uuid"
)

const (
	prefixHostnameKey       = "kapacitors/hostname/"
	prefixHostnameRevKey    = "kapacitors/rev/"
	prefixTasksIdKey        = "tasks/id/"
	prefixTasksConfigKey    = "tasks/config/"
	prefixTasksInfo			= "tasks/info/"
	prefixTasksIsCreatedKey = "tasks/is_created/"
	prefixTasksInNodeKey    = "tasks/in_node/"
	totalTaskNumKey         = "kapacitors/task_num"
	prefixOpPatchKey        = "tasks/op/patch/"
	opDeleteKey             = "tasks/op/delete"
	prefixOpResponseKey     = "tasks/op_response/rev/"
)

const CFS_ALGO = "CFS"

const respTemplate = `{"status_code": "%s", "data": %s}`

type TaskEtcd struct {
	Cli    *clientv3.Client
	Kcli   *client.Client
	Logger *log.Logger
}

func connect(url string, skipSSL bool) (*client.Client, error) {
	return client.New(client.Config{
		URL:                url,
		InsecureSkipVerify: skipSSL,
	})
}

func NewTaskEtcd(c *server.Config, logService logging.Interface) (*TaskEtcd, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(c.EtcdServers, ","),
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	kcli, err := connect("http://localhost:9092", false)

	if err != nil {
		return nil, err
	}

	l := logService.NewLogger("[taskEtcd] ", log.LstdFlags)

	return &TaskEtcd{
		Cli:    cli,
		Kcli:   kcli,
		Logger: l,
	}, nil
}

func (te *TaskEtcd) RegistryToEtcd(c *server.Config, kch chan int) {
	cli := te.Cli
	hostName := c.Hostname
	hostnameId := uuid.New().String()
	hostnameKey := prefixHostnameKey + hostName + "/" + hostnameId

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	// grant
	resp, err := cli.Grant(ctx, c.EtcdHearbeatInterval)
	if err != nil {
		te.Logger.Fatal("E! failed to grant", err)
	}
	cancel()

	// put key
	_, err = cli.Put(context.TODO(), hostnameKey, hostName, clientv3.WithLease(resp.ID))
	if err != nil {
		te.Logger.Fatal("E! failed to put hostname key", err)
	}

	hostnameRevKey := prefixHostnameRevKey + hostName + "/" + hostnameId

	kvc := clientv3.NewKV(cli)
	_, err = kvc.Txn(context.TODO()).
		If(clientv3.Compare(clientv3.Value(hostnameKey), "=", hostName)).
		Then(clientv3.OpPut(hostnameRevKey, "0")).
		Commit()
	if err != nil {
		te.Logger.Fatal("E! txn failed ", err)
	}

	_, err = kvc.Txn(context.TODO()).
		If(clientv3.Compare(clientv3.CreateRevision(totalTaskNumKey), "=", 0)).
		Then(clientv3.OpPut(totalTaskNumKey, "0")).
		Commit()
	if err != nil {
		te.Logger.Fatal("E! txn failed ", err)
	}

	ch, err := cli.KeepAlive(context.TODO(), resp.ID)
	if err != nil {
		te.Logger.Fatal("E! failed to keepalive ", err)
	}
	go func() {

		for kresp := range ch {
			kresp.GetRevision()
		}
		te.Logger.Fatal("E! etcd down")
	}()

	<-kch
}

func (te *TaskEtcd) WatchTaskid(c *server.Config) {
	// put
	cli := te.Cli

	rch := cli.Watch(context.Background(), prefixTasksIdKey, clientv3.WithPrefix())

	for wresp := range rch {
		revision := strconv.FormatInt(wresp.Header.GetRevision(), 10)
		for _, ev := range wresp.Events {
			evType := ev.Type.String()
			if evType == "PUT" {
				// 计算延迟事务时间
				delayTime := time.Duration(0)
				if c.TaskSchedAlgo == CFS_ALGO {
					delayTime = cfs(te.Cli, c.Hostname)
				} else {
					te.Logger.Fatal("E! The scheduling algorithm" + c.TaskSchedAlgo + " is not supported")
				}

				//time.Sleep(time.Duration(delayTime * 100) * time.Microsecond)
				time.Sleep(delayTime)

				fmt.Println(delayTime, "***********************************************************")
				//事务创建任务
				taskId := string(ev.Kv.Value)

				// 事务修改etcd任务所在节点信息
				taskIdKey := prefixTasksIdKey + taskId
				isCreatedKey := prefixTasksIsCreatedKey + taskId
				kvc := clientv3.NewKV(cli)
				kresp, err := kvc.Txn(context.TODO()).
					If(clientv3.Compare(clientv3.Value(taskIdKey), "=", taskId), clientv3.Compare(clientv3.CreateRevision(isCreatedKey), "=", 0)).
					Then(clientv3.OpPut(taskIdKey, c.Hostname)).
					Commit()
				if err != nil {
					te.Logger.Println("E! failed to set task's hostname: ", err)
					continue
				}

				// 检查事务条件是否成立
				if kresp.Succeeded == false {
					continue
				}

				// 获取任务信息
				taskConfigKey := prefixTasksConfigKey + taskId

				gresp, err := cli.Get(context.TODO(), taskConfigKey)
				if err != nil {
					te.Logger.Fatal("E! failed to get task configuration: ", err)
				}

				for _, ev := range gresp.Kvs {
					if string(ev.Key) == taskConfigKey {
						taskConfig := string(ev.Value)

						var t client.Task
						t, statusCode, cerr := createTask(taskConfig, te.Kcli)
						if cerr != nil {
							// 创建任务失败
							// 删除etcd上任务信息
							te.Logger.Println("E! create tasks error: ", cerr)
							grResp, gerr := cli.Grant(context.TODO(), 60)
							if gerr != nil {
								te.Logger.Fatal("E! etcd grant time error: ", cerr)
							}
							respData := fmt.Sprintf(respTemplate, statusCode, `"`+cerr.Error()+`"`)

							_, terr := kvc.Txn(context.TODO()).
								If(clientv3.Compare(clientv3.Value(taskIdKey), "=", c.Hostname)).
								Then(clientv3.OpDelete(taskIdKey),
									clientv3.OpDelete(prefixTasksConfigKey+taskId),
									clientv3.OpPut(prefixOpResponseKey+revision, respData, clientv3.WithLease(grResp.ID)),
										).
								Commit()
							if terr != nil {
								te.Logger.Fatal("E! etcd error while delete task information: ", terr)
							}
							break
						}

						// 成功创建任务
						// 内容推送给kapacitor客户端
						//r, err := json.Marshal(t)
						//if err != nil {
						//	te.Logger.Println("E! json marshal task error: ", err)
						//}
						taskInfo := httpd.MarshalJSON(t, false)
						respData := fmt.Sprintf(respTemplate, statusCode, taskInfo)

						//fmt.Println("555555555555555", string(r))

						grResp, err := cli.Grant(context.TODO(), 60)
						if err != nil {
							te.Logger.Fatal("E! etcd grant time error: ", err)
						}

						//
						taskIdInNodeKey := prefixTasksInNodeKey + c.Hostname + "/" + taskId
						kvc := clientv3.NewKV(cli)
						taskIdKey := prefixTasksIdKey + taskId

						_, err = kvc.Txn(context.TODO()).
							If(clientv3.Compare(clientv3.Value(taskIdKey), "=", c.Hostname)).
							Then(clientv3.OpPut(isCreatedKey, "true"),
								clientv3.OpPut(taskIdInNodeKey, taskId),
								clientv3.OpPut(prefixOpResponseKey+revision, respData, clientv3.WithLease(grResp.ID)),
									clientv3.OpPut(prefixTasksInfo+taskId, string(taskInfo))).
							Commit()
						if err != nil {
							te.Logger.Fatal("E! put task info error: ", err)
						}

						for {
							// 把任务数量加一
							var totalTaskNum string
							var newTotalTaskNum string
							fmt.Println(247)
							resp, err := cli.Get(context.TODO(), totalTaskNumKey)
							if err != nil {
								te.Logger.Fatal("E! get total task num error: ", err)
							}
							for _, ev := range resp.Kvs {
								if string(ev.Key) == totalTaskNumKey {
									totalTaskNum = string(ev.Value)
									num, _ := strconv.ParseInt(string(ev.Value), 10, 64)
									newTotalTaskNum = strconv.FormatInt(num+1, 10)
								}
							}
							tresp, terr := cli.Txn(context.TODO()).
								If(clientv3.Compare(clientv3.Value(totalTaskNumKey), "=", totalTaskNum)).
								Then(clientv3.OpPut(totalTaskNumKey, newTotalTaskNum)).
								Commit()
							if terr != nil {
								te.Logger.Fatal("E! update total task num error: ", terr)
							}

							fmt.Println(tresp.Succeeded)
							if tresp.Succeeded == true {
								break
							}
							time.Sleep(100 * time.Microsecond)
						}

					}
				}

			}
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

func WaitForTaskCreated(taskId string, cli *clientv3.Client, l *log.Logger) string {
	taskIdIsCreated := prefixTasksIsCreatedKey + taskId

	isTaskCreated := "false"
	for {
		gresp, err := cli.Get(context.TODO(), taskIdIsCreated)
		if err != nil {
			l.Fatalln("E! failed to get tasks info: ", err)
		}
		if len(gresp.Kvs) == 0 {
			break
		}
		for _, ev := range gresp.Kvs {
			if string(ev.Key) == taskIdIsCreated {
				isTaskCreated = string(ev.Value)
				fmt.Println("isTaskCreated", isTaskCreated)
			}
		}
		if isTaskCreated != "true" {
			fmt.Println(taskId, "isTaskCreated", isTaskCreated)
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}

	return isTaskCreated
}

func (te *TaskEtcd) WatchPatchKey(c *server.Config) {
	cli := te.Cli
	kvc := clientv3.NewKV(cli)
	rch := cli.Watch(context.TODO(), prefixOpPatchKey, clientv3.WithPrefix())
	for wresp := range rch {
		revision := strconv.FormatInt(wresp.Header.GetRevision(), 10)
		for _, ev := range wresp.Events {
			if ev.Type.String() == "PUT" {
				key := string(ev.Kv.Key)
				data := string(ev.Kv.Value)
				taskId := strings.Split(key, "/")[3]

				// 检查任务是否在节点上创建并等待任务创建
				//isTaskCreated := WaitForTaskCreated(taskId, cli, te.Logger)
				//if isTaskCreated != "true" {
				//	continue
				//}

				// 等待被任务修改或执行修改任务
				isTaskUpdated := "false"
				isTaskCreated := "false"
				for {
					// 检查任务是否被修改，如果被修改，key会被删除
					gresp, err := cli.Get(context.TODO(), key)
					if err != nil {
						te.Logger.Fatalln("E! failed to get patch key info: ", err)
					}
					if len(gresp.Kvs) == 0 {
						isTaskUpdated = "true"
						break
					}

					// 获取task所在的节点
					taskIdKey := prefixTasksIdKey + taskId
					gresp, err = cli.Get(context.TODO(), taskIdKey)
					if err != nil {
						te.Logger.Fatalln("E! failed to get tasks info: ", err)
					}
					if len(gresp.Kvs) == 0 {
						isTaskUpdated = "true"
						break
					}
					var taskHostname string
					for _, ev := range gresp.Kvs {
						if string(ev.Key) == taskIdKey {
							taskHostname = string(ev.Value)
						}
					}
					if taskHostname == c.Hostname {
						// 等待任务被创建
						isTaskCreated = WaitForTaskCreated(taskId, cli, te.Logger)
						break
					}
					fmt.Println(taskId, taskHostname)

					time.Sleep(10 * time.Millisecond)
				}
				if isTaskUpdated == "true" {
					continue
				}

				if isTaskCreated != "true" {
					continue
				}

				//// 获取task所在的节点
				//taskIdKey := prefixTasksIdKey + taskId
				//gresp, err := cli.Get(context.TODO(), taskIdKey)
				//if err != nil {
				//	te.Logger.Fatalln("E! failed to get tasks info: ", err)
				//}
				//
				//fmt.Println(taskIdKey, len(gresp.Kvs))
				//
				//var taskHostname string
				//for _, ev := range gresp.Kvs {
				//	if string(ev.Key) == taskIdKey {
				//		taskHostname = string(ev.Value)
				//	}
				//}
				//if taskHostname != c.Hostname {
				//	continue
				//}


				grResp, err := cli.Grant(context.TODO(), 60)
				if err != nil {
					te.Logger.Fatal("E! failed to grant", err)
				}

				var t client.Task
				t, statusCode, err := updateTask(taskId, data,te.Kcli)
				if err != nil {
					// 修改任务失败
					respData := fmt.Sprintf(respTemplate, statusCode, `"`+err.Error()+`"`)

					_, err = kvc.Txn(context.TODO()).
						If(clientv3.Compare(clientv3.ModRevision(prefixOpPatchKey+taskId), "=", wresp.Header.GetRevision())).
						Then(clientv3.OpPut(prefixOpResponseKey+revision, respData, clientv3.WithLease(grResp.ID)),
						clientv3.OpDelete(prefixOpPatchKey+taskId)).
						Commit()
					if err != nil {
						te.Logger.Fatal("E! txn update task info error: ", err)
					}
					continue
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
					te.Logger.Fatal("E! txn update task info error: ", err)
				}
			}
		}
	}
}

func (te *TaskEtcd) WatchDeleteKey(c *server.Config) {
	cli := te.Cli
	rch := cli.Watch(context.TODO(), opDeleteKey, clientv3.WithPrefix())
	kvc := clientv3.NewKV(cli)
	for wresp := range rch {
		revision := strconv.FormatInt(wresp.Header.GetRevision(), 10)
		for _, ev := range wresp.Events {
			if ev.Type.String() == "PUT" {
				taskId := string(ev.Kv.Value)
				// 获取task所在的节点
				taskIdKey := prefixTasksIdKey + taskId

				//// 检查任务是否在节点上创建并等待任务创建
				//WaitForTaskCreated(taskId, cli, te.Logger)


				// 等待被任务删除或执行删除任务
				var taskHostname string
				isTaskDeleted := "false"
				isTaskCreated := "false"
				for {
					gresp, err := cli.Get(context.TODO(), taskIdKey)
					if err != nil {
						te.Logger.Fatalln("E! get task info error: ", err)
					}
					if len(gresp.Kvs) == 0 {
						isTaskDeleted = "true"
						break
					}

					for _, ev := range gresp.Kvs {
						if string(ev.Key) == taskIdKey {
							taskHostname = string(ev.Value)
						}
					}
					if taskHostname == c.Hostname {
						// 等待任务被创建
						isTaskCreated = WaitForTaskCreated(taskId, cli, te.Logger)
						break
					}
					fmt.Println(taskId, taskHostname)

					time.Sleep(10 * time.Microsecond)
				}
				if isTaskDeleted == "true" {
					continue
				}
				if isTaskCreated != "true" {
					continue
				}

				//gresp, err := cli.Get(context.TODO(), taskIdKey)
				//if err != nil {
				//	te.Logger.Fatalln("E! get task info error: ", err)
				//}
				//if len(gresp.Kvs) == 0 {
				//	continue
				//}
				//
				//var taskHostname string
				//
				//for _, ev := range gresp.Kvs {
				//	if string(ev.Key) == taskIdKey {
				//		taskHostname = string(ev.Value)
				//	}
				//}
				//if taskHostname != c.Hostname {
				//	continue
				//}


				// 删除kapacitor上任务, 任务总数减一
				statusCode, derr := deleteTask(taskId, te.Kcli)

				grResp, gerr := cli.Grant(context.TODO(), 60)
				if gerr != nil {
					te.Logger.Fatal("E! failed to grant", gerr)
				}

				if derr != nil {

					respData := fmt.Sprintf(respTemplate, statusCode, `"`+derr.Error()+`"`)
					_, perr := cli.Put(context.TODO(), prefixOpResponseKey+revision, respData, clientv3.WithLease(grResp.ID))
					if perr != nil {
						te.Logger.Fatal("E! faliled to put response info: ", perr)
					}
					continue
				}


				var totalTaskNum string
				var newTotalTaskNum string

				for {
					gresp, err := cli.Get(context.TODO(), totalTaskNumKey)
					if err != nil {
						te.Logger.Fatal("E! get total task num error: ", err)
					}
					for _, ev := range gresp.Kvs {
						if string(ev.Key) == totalTaskNumKey {
							num, _ := strconv.ParseInt(string(ev.Value), 10, 64)
							totalTaskNum = strconv.FormatInt(num, 10)
							newTotalTaskNum = strconv.FormatInt(num-1, 10)
						}
					}

					respData := fmt.Sprintf(respTemplate, statusCode, `""`)

					tresp, terr := kvc.Txn(context.TODO()).
						If(clientv3.Compare(clientv3.Value(taskIdKey), "=", taskHostname), clientv3.Compare(clientv3.Value(totalTaskNumKey), "=", totalTaskNum)).
						Then(clientv3.OpDelete(taskIdKey),
						clientv3.OpDelete(prefixTasksInNodeKey+taskHostname+"/"+taskId),
						clientv3.OpDelete(prefixTasksInfo+taskId),
						clientv3.OpDelete(prefixTasksConfigKey+taskId),
						clientv3.OpDelete(prefixTasksIsCreatedKey+taskId),
						clientv3.OpPut(totalTaskNumKey, newTotalTaskNum),
						clientv3.OpPut(prefixOpResponseKey+revision, respData, clientv3.WithLease(grResp.ID))).
						Commit()
					if terr != nil {
						te.Logger.Fatal("E! failed to delete etcd task info: ", err)
					}
					fmt.Println(tresp.Succeeded)
					if tresp.Succeeded == true {
						break
					}
				}

			}
		}
	}
}

func (te *TaskEtcd) WatchHostname(c *server.Config) {
	// 当节点失效后的处理

	cli := te.Cli
	kvc := clientv3.NewKV(cli)
	rch := cli.Watch(context.TODO(), prefixHostnameKey, clientv3.WithPrefix())

	for wresp := range rch {
		//revision := strconv.FormatInt(wresp.Header.GetRevision(), 10)
		for _, ev := range wresp.Events {
			fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			if ev.Type.String() == "DELETE" {
				downHostname := strings.Split(string(ev.Kv.Key), "/")[2]
				downHostnameId := strings.Split(string(ev.Kv.Key), "/")[3]

				downRevKey := prefixHostnameRevKey + downHostname + "/" + downHostnameId

				// 把is_created设置成false

				var taskIds []string
				inNodeKey := prefixTasksInNodeKey + downHostname
				resp, err := cli.Get(context.TODO(), inNodeKey, clientv3.WithPrefix())
				if err != nil {
					te.Logger.Fatal("E! get tasks error: ", err)
				}
				for _, ev := range resp.Kvs {
					taskIds = append(taskIds, string(ev.Value))
				}

				fmt.Println("3333333333333333333333333333333333333")
				fmt.Println(taskIds)

				var ops []clientv3.Op
				for _, id := range taskIds {
					isCreatedKey := prefixTasksIsCreatedKey + id
					taskInNodeKey := prefixTasksInNodeKey + downHostname + "/" + id

					ops = append(ops, clientv3.OpPut(isCreatedKey, "false"))
					ops = append(ops, clientv3.OpDelete(taskInNodeKey))
				}
				// 删除rev key
				ops = append(ops, clientv3.OpDelete(downRevKey))

				//
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err = kvc.Txn(ctx).
					If(clientv3.Compare(clientv3.CreateRevision(downRevKey), "!=", 0)).
					Then(ops...).
					Commit()
				cancel()
				if err != nil {
					te.Logger.Fatal("E! failed update down kapacitor's tasks ", err)
				}

			}
		}
	}
}

func (te *TaskEtcd) RecreateTasks(c *server.Config) {
	// 把失效节点的任务重新创建

	if c.TaskSchedAlgo != CFS_ALGO {
		te.Logger.Fatal("E! The scheduling algorithm" + c.TaskSchedAlgo + " is not supported")
	}

	cli := te.Cli
	kvc := clientv3.NewKV(cli)

	resp, err := cli.Get(context.TODO(), prefixHostnameKey, clientv3.WithPrefix())
	if err != nil {
		te.Logger.Println("E! get hostname error: ", err)
		return
	}
	kapacitorNum := len(resp.Kvs)

	if kapacitorNum == 0 {
		return
	}

	// 查询本节点任务数量
	taskInNodeKey := prefixTasksInNodeKey + c.Hostname
	resp, err = cli.Get(context.TODO(), taskInNodeKey, clientv3.WithPrefix())
	if err != nil {
		te.Logger.Println("E! failed to get kapacitor tasks: ", err)
		return
	}
	iTaskNum := len(resp.Kvs)

	// 获取is_created = false 的所有任务id
	resp, err = cli.Get(context.TODO(), prefixTasksIsCreatedKey, clientv3.WithPrefix())
	if err != nil {
		te.Logger.Println("E! get uncreate tasks error: ", err)
		return
	}
	totalTaskNum := len(resp.Kvs)

	var taskIds []string
	for _, ev := range resp.Kvs {
		if string(ev.Value) != "false" {
			continue
		}
		key := string(ev.Key)
		id := strings.Split(key, "/")[2]
		taskIds = append(taskIds, id)

		fmt.Printf("%s = %s\n", ev.Key, ev.Value)
	}
	//uncreatedTaskNum := len(taskIds)

	avgTaskNum := totalTaskNum/kapacitorNum + 1

	//fmt.Println(taskIds)
	//fmt.Println(iTaskNum, kapacitorNum, avgTaskNum)

	// 事务修改avg_task_num个任务status = processing , 和任务所在的节点

	modifiedTasksNum := 0
	for _, id := range taskIds {
		fmt.Println(iTaskNum, kapacitorNum, avgTaskNum, modifiedTasksNum + iTaskNum, "uuuuuuuuuuuuuuuuuuuuuuuuuuuu")

		if modifiedTasksNum + iTaskNum >= avgTaskNum {
			fmt.Println("break 00000000000000000000000000000000000000000000000000000")
			break
		}

		taskIdKey := prefixTasksIdKey + id

		fmt.Println(taskIdKey)
		fmt.Println("-----222222222222222222223333333333333333333333333333333337777777777777777")

		tresp, err := kvc.Txn(context.TODO()).
			If(clientv3.Compare(clientv3.Value(prefixTasksIsCreatedKey+id), "=", "false")).
			Then(clientv3.OpPut(prefixTasksIsCreatedKey+id, "processing"),
				clientv3.OpPut(taskIdKey, c.Hostname),
				clientv3.OpPut(prefixTasksInNodeKey+c.Hostname+"/"+id, id),
					).
			Commit()
		if err != nil {
			te.Logger.Println("E! txn update task info error", err)
			continue
		}

		// 检查id所在的节点是否被修改成功
		if tresp.Succeeded == true {
			fmt.Println("successed=true", "22222222222222222222222222222222222222222222222222222222")
			// 创建任务
			taskConfigKey := prefixTasksConfigKey + id
			gresp, err := cli.Get(context.TODO(), taskConfigKey)
			if err != nil {
				te.Logger.Fatal("E! faile to get task configuration", err)
			}

			for _, ev := range gresp.Kvs {
				if string(ev.Key) == taskConfigKey {
					taskConfig := string(ev.Value)

					_, _, err := createTask(taskConfig, te.Kcli)
					if err != nil {
						// 创建任务失败
						te.Logger.Println("E! recreate task error: ", err)
						_, err := cli.Put(context.TODO(), prefixTasksIsCreatedKey+id, "false")
						if err != nil {
							te.Logger.Fatal("E! failed to reset task info", err)
						}

						break
					}
					fmt.Println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

					//
					_, err = kvc.Txn(context.TODO()).
						If(clientv3.Compare(clientv3.Value(prefixTasksIsCreatedKey+id), "=", "processing")).
						Then(clientv3.OpPut(prefixTasksIsCreatedKey+id, "true")).
						Commit()
					if err != nil {
						te.Logger.Fatal("E! txn update task info error: ", err)
					}
					modifiedTasksNum += 1

				}
			}
		}

	}
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