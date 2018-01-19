package tasksched

import (
	"fmt"
	"log"
	"strconv"

	"bytes"
	"context"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"github.com/masami10/kapacitor/client/v1"
	"github.com/masami10/kapacitor/server"
	"net/http"
	"strings"
	"time"
)

const (
	prefixHostnameKey       = "kapacitors/hostname/"
	prefixHostnameRevKey    = "kapacitors/rev/"
	prefixTasksIdKey        = "tasks/id/"
	prefixTasksConfigKey    = "tasks/config/"
	prefixTasksIsCreatedKey = "tasks/is_created/"
	prefixTasksInNodeKey    = "tasks/in_node/"
	totalTaskNumKey     = "kapacitors/task_num/"
	prefixOpPatchKey    = "tasks/op/patch/"
	opDeleteKey         = "tasks/op/delete"
	prefixOpResponseKey = "tasks/op_response/rev/"
)

const CFS_ALGO = "CFS"

type TaskEtcd struct {
	Cli *clientv3.Client
	Kcli *client.Client
}

func connect(url string, skipSSL bool) (*client.Client, error) {
	return client.New(client.Config{
		URL:                url,
		InsecureSkipVerify: skipSSL,
	})
}

func NewTaskEtcd(c *server.Config) (*TaskEtcd, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: strings.Split(c.EtcdServers, ","),
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	kcli, err := connect( "http://"+c.Hostname+":9092", false)

	if err != nil {
		return nil, err
	}

	return &TaskEtcd{
		Cli: cli,
		Kcli: kcli,
	}, nil
}


func (te *TaskEtcd) RegistryToEtcd(c *server.Config, kch chan int) {
	cli := te.Cli
	hostName := c.Hostname
	hostnameKey := prefixHostnameKey + hostName
	hostnameRevKey := prefixHostnameRevKey + hostName

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	// grant
	resp, err := cli.Grant(ctx, 5)
	if err != nil {
		fmt.Println(err)
		 log.Fatal(err)
	}
	cancel()

	// put key
	ctx2, cancel2 := context.WithTimeout(context.Background(), 1*time.Second)
	_, err = cli.Put(ctx2, hostnameKey, hostName, clientv3.WithLease(resp.ID))
	if err != nil {
		fmt.Println(err)
		 log.Fatal(err)
	}
	cancel2()

	kvc := clientv3.NewKV(cli)
	_, err = kvc.Txn(context.TODO()).
		If(clientv3.Compare(clientv3.Value(hostnameKey), "=", hostName)).
		Then(clientv3.OpPut(hostnameRevKey, "0")).
		Commit()
	if err != nil {
		 log.Fatal(err)
	}

	ch, err := cli.KeepAlive(context.TODO(), resp.ID)
	if err != nil {
		 log.Fatal(err)
		
	}
	go func() {
		for kresp := range ch {
			//kch <- 1
			fmt.Println("TTL: ", kresp.TTL)
		}
	}()

	<-kch
}

func (te *TaskEtcd) WatchTaskid(c *server.Config) {
	// put delete
	cli := te.Cli

	rch := cli.Watch(context.Background(), prefixTasksIdKey, clientv3.WithPrefix())

	for wresp := range rch {
		revision := strconv.FormatInt(wresp.Header.GetRevision(), 10)
		for _, ev := range wresp.Events {
			evType := ev.Type.String()
			if evType == "PUT" {
				// 计算延迟事务时间
				delayTime := time.Duration(0)
				if c.TaskSchedAlgo == CFS_ALGO{
					delayTime = cfs(te.Cli, c.Hostname)
				}else {
					log.Fatal("The scheduling algorithm"+c.TaskSchedAlgo+" is not supported")
				}

				//time.Sleep(time.Duration(delayTime * 100) * time.Microsecond)
				time.Sleep(delayTime)

				fmt.Println(delayTime)
				//事务创建任务
				taskId := string(ev.Kv.Value)

				// 事务修改etcd任务所在节点信息
				taskIdKey := prefixTasksIdKey + taskId
				kvc := clientv3.NewKV(cli)
				kresp, err := kvc.Txn(context.TODO()).
					If(clientv3.Compare(clientv3.Value(taskIdKey), "=", taskId)).
					Then(clientv3.OpPut(taskIdKey, c.Hostname)).
					Commit()
				if err != nil {
					 log.Fatal(err)
				}

				// 检查事务条件是否成立
				if kresp.Succeeded == false {
					continue
				}

				// 获取任务信息
				taskConfigKey := prefixTasksConfigKey + taskId

				fmt.Println("6666666666666666666666666666666666666666666666666666666666")
				fmt.Println(taskConfigKey)

				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				gresp, err := cli.Get(ctx, taskConfigKey)
				if err != nil {
					 log.Fatal(err)
				}
				cancel()

				for _, ev := range gresp.Kvs {
					if string(ev.Key) == taskConfigKey {
						taskConfig := string(ev.Value)

						var t client.Task
						t, err = createTask(taskConfig, te.Kcli)
						if err != nil {
							// 创建任务失败
							_, cerr := cli.Put(context.TODO(), prefixOpResponseKey+revision, `{"error":"`+err.Error()+`"}`)
							if cerr != nil {
								 log.Println(cerr)
							}
							 log.Fatal("E! ", err)
						}

						// 内容推送给kapacitor客户端
						r, err := json.Marshal(t)
						if err != nil {
							 log.Fatal(err)
						}
						fmt.Println("555555555555555", string(r))

						grResp, err := cli.Grant(context.TODO(), 60)
						if err != nil {
							 log.Fatal(err)
						}

						//
						isCreatedKey := prefixTasksIsCreatedKey + taskId
						tasksInNodeKey := prefixTasksInNodeKey + c.Hostname
						//keyStatus := prefixTasksStatusKey + taskId
						taskIdInNodeKey := tasksInNodeKey + "/" + taskId
						kvc := clientv3.NewKV(cli)
						taskIdKey := prefixTasksIdKey + taskId
						_, err = kvc.Txn(context.TODO()).
							If(clientv3.Compare(clientv3.Value(taskIdKey), "=", c.Hostname)).
							Then(clientv3.OpPut(isCreatedKey, "true"),
								//clientv3.OpPut(keyStatus, t.Status.String()),
								clientv3.OpPut(taskIdInNodeKey, taskId),
								clientv3.OpPut(prefixOpResponseKey+revision, string(r), clientv3.WithLease(grResp.ID))).
							Commit()
						if err != nil {
							 log.Fatal(err)
						}

						// 把任务数量加一
						totalTaskNum := "1"

						resp, err := cli.Get(context.TODO(), totalTaskNumKey)
						if err != nil {
							 log.Fatal(err)
						}
						for _, ev := range resp.Kvs {
							if string(ev.Key) == totalTaskNumKey {
								num, _ := strconv.ParseInt(string(ev.Value), 10, 64)
								totalTaskNum = strconv.FormatInt(num+1, 10)
							}
						}
						_, err = cli.Put(context.TODO(), totalTaskNumKey, totalTaskNum)
						if err != nil {
							log.Fatal(err)
						}

					}
				}

			} else if evType == "DELETE" {

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

				// 获取task所在的节点
				taskIdKey := prefixTasksIdKey + taskId
				gresp, err := cli.Get(context.TODO(), taskIdKey)
				if err != nil {
					 log.Fatal(err)
				}

				fmt.Println(taskIdKey, len(gresp.Kvs))

				var taskHostname string
				for _, ev := range gresp.Kvs {
					if string(ev.Key) == taskIdKey {
						taskHostname = string(ev.Value)
					}
				}

				fmt.Println("66666666666666666666666666666666111", taskHostname)
				fmt.Println(data)

				grResp, err := cli.Grant(context.TODO(), 60)
				if err != nil {
					 log.Fatal(err)
				}

				var t client.Task
				t, err = updateTask(taskId, data, taskHostname, te.Kcli)
				if err != nil {
					// 修改任务失败
					_, cerr := cli.Put(context.TODO(), prefixOpResponseKey+revision, `{"error":"`+err.Error()+`"}`, clientv3.WithLease(grResp.ID))
					if cerr != nil {
						 log.Println(cerr)
					}
					 log.Fatal(err)
				}

				// 给kapacitor客户端响应内容
				var buf bytes.Buffer
				err = json.NewEncoder(&buf).Encode(t)
				if err != nil {
					 log.Fatal(err)
				}
				fmt.Println("555555555555555", buf.String())

				var rawTask2 TaskConfig
				rawTask2.ID = t.ID
				rawTask2.Type = t.Type.String()
				rawTask2.Script = t.TICKscript
				rawTask2.DBRPs = t.DBRPs
				rawTask2.Status = t.Status.String()
				rawTask2.TemplateID = t.TemplateID
				rawTask2.Vars = t.Vars

				r, err := json.Marshal(rawTask2)
				if err != nil {
					 log.Fatal(err)
				}

				fmt.Println(string(r), "34444443333333333333333333444444444444444")

				_, err = kvc.Txn(context.TODO()).
					If(clientv3.Compare(clientv3.ModRevision(prefixOpPatchKey+taskId), "=", wresp.Header.GetRevision())).
					Then(clientv3.OpPut(prefixOpResponseKey+revision, buf.String(), clientv3.WithLease(grResp.ID)),
						clientv3.OpPut(prefixTasksConfigKey+taskId, string(r)),
						clientv3.OpDelete(prefixOpPatchKey+taskId)).
					Commit()
				if err != nil {
					 log.Fatal(err)
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
		for _, ev := range wresp.Events {
			if ev.Type.String() == "PUT" {
				taskId := string(ev.Kv.Value)
				// 检查etcd上taskId是否存在
				gresp, err := cli.Get(context.TODO(), prefixTasksIdKey+taskId)
				if err != nil {
					 log.Fatal(err)
				}
				if len(gresp.Kvs) == 0 {
					continue
				}

				// 获取task所在的节点
				taskIdKey := prefixTasksIdKey + taskId
				gresp, err = cli.Get(context.TODO(), taskIdKey)
				if err != nil {
					 log.Fatal(err)
				}
				if len(gresp.Kvs) == 0 {
					continue
				}

				var taskHostname string
				for _, ev := range gresp.Kvs {
					if string(ev.Key) == taskIdKey {
						taskHostname = string(ev.Value)
					}
				}

				fmt.Println("7777777777777777777777777", taskHostname)

				// 删除kapacitor上任务, 任务总数减一
				err = deleteTask(taskId, taskHostname, te.Kcli)

				var totalTaskNum string
				gresp, err = cli.Get(context.TODO(), totalTaskNumKey)
				if err != nil {
					 log.Fatal(err)
				}
				for _, ev := range gresp.Kvs {
					if string(ev.Key) == totalTaskNumKey {
						num, _ := strconv.ParseInt(string(ev.Value), 10, 64)
						totalTaskNum = strconv.FormatInt(num-1, 10)
					}
				}

				tresp, terr := kvc.Txn(context.TODO()).
					If(clientv3.Compare(clientv3.Value(taskIdKey), "=", taskHostname)).
					Then(clientv3.OpDelete(taskIdKey),
						clientv3.OpDelete(prefixTasksInNodeKey+taskHostname+"/"+taskId),
						clientv3.OpDelete(prefixTasksConfigKey+taskId),
						clientv3.OpDelete(prefixTasksIsCreatedKey+taskId),
						clientv3.OpPut(totalTaskNumKey, totalTaskNum)).
					Commit()
				if terr != nil {
					 log.Fatal(terr)
				}
				fmt.Println("1111111111111111111111111", tresp.Succeeded)

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
		revision := strconv.FormatInt(wresp.Header.GetRevision(), 10)
		for _, ev := range wresp.Events {
			fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			if ev.Type.String() == "DELETE" {
				downHostname := strings.Split(string(ev.Kv.Key), "/")[2]

				fmt.Println(downHostname, "99999999999999999999999999999")

				downRevKey := prefixHostnameRevKey + downHostname

				// 把is_created设置成false

				var taskIds []string
				inNodeKey := prefixTasksInNodeKey + downHostname
				resp, err := cli.Get(context.TODO(), inNodeKey, clientv3.WithPrefix())
				if err != nil {
					log.Fatal(err)
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
				// 修改rev值
				ops = append(ops, clientv3.OpPut(downRevKey, revision))

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err = kvc.Txn(ctx).
					If(clientv3.Compare(clientv3.Value(downRevKey), "!=", revision)).
					Then(ops...).
					Commit()
				if err != nil {
					 log.Fatal(err)
				}
				cancel()

			}
		}
	}
}

func (te *TaskEtcd) RecreateTasks(c *server.Config) {
	// 把失效节点的任务重新创建

	if c.TaskSchedAlgo != CFS_ALGO{
		log.Fatal("The scheduling algorithm"+c.TaskSchedAlgo+" is not supported")
	}

	cli := te.Cli
	resp, err := cli.Get(context.TODO(), prefixHostnameKey, clientv3.WithPrefix())
	if err != nil {
		 log.Fatal(err)
	}
	kapacitorNum := len(resp.Kvs)

	fmt.Println("kapacitor_num", kapacitorNum)
	if kapacitorNum == 0 {
		return
	}

	// 获取is_created = false 的所有任务id
	resp, err = cli.Get(context.TODO(), prefixTasksIsCreatedKey, clientv3.WithPrefix())
	if err != nil {
		 log.Fatal(err)
	}
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
	uncreatedTaskNum := len(taskIds)

	avgTaskNum := uncreatedTaskNum/kapacitorNum + 1

	fmt.Println(taskIds)
	fmt.Println(uncreatedTaskNum, kapacitorNum, avgTaskNum)

	// 事务修改avg_task_num个任务status = processing , 和任务所在的节点

	modifiedTasksNum := 0
	kvc := clientv3.NewKV(cli)
	for _, id := range taskIds {
		if modifiedTasksNum == avgTaskNum {
			fmt.Println("break 00000000000000000000000000000000000000000000000000000")
			break
		}

		taskInKpKey := prefixTasksIdKey + id

		fmt.Println(taskInKpKey)
		fmt.Println("-----222222222222222222223333333333333333333333333333333337777777777777777")

		tresp, err := kvc.Txn(context.TODO()).
			If(clientv3.Compare(clientv3.Value(prefixTasksIsCreatedKey+id), "=", "false")).
			Then(clientv3.OpPut(prefixTasksIsCreatedKey+id, "processing"), clientv3.OpPut(taskInKpKey, c.Hostname)).
			Commit()
		if err != nil {
			log.Fatal(err)
		}

		// 检查id所在的节点是否被修改成功
		if tresp.Succeeded == true {
			fmt.Println("successed=true", "22222222222222222222222222222222222222222222222222222222")
			// 创建任务
			taskConfigKey := prefixTasksConfigKey + id
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			gresp, err := cli.Get(ctx, taskConfigKey)
			if err != nil {
				log.Fatal(err)
			}
			cancel()

			for _, ev := range gresp.Kvs {
				if string(ev.Key) == taskConfigKey {
					taskConfig := string(ev.Value)

					_, err = createTask(taskConfig, te.Kcli)
					if err != nil {
						// 创建任务失败
						fmt.Println("create task", id, "666666666666666666666666666666666666666666666666666666666")
						 log.Fatal(err)
					}
					fmt.Println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

					// todo 要改成事务操作
					_, err := cli.Put(context.TODO(), prefixTasksIsCreatedKey+id, "true")
					if err != nil {
						 log.Fatal(err)
					}
					_, err = cli.Put(context.TODO(), prefixTasksIdKey+id, c.Hostname)
					if err != nil {
						 log.Fatal(err)
					}
					_, err = cli.Put(context.TODO(), prefixTasksInNodeKey+c.Hostname+"/"+id, id)
					if err != nil {
						 log.Fatal(err)
					}
				}
			}
			//func () {
			//	// 创建任务
			//}()
		}

	}
}


func createTask(task_config string, kcli *client.Client) (client.Task, error) {
	//client := &http.Client{}
	url := "http://localhost:9092/kapacitor/v1/tasks"
	fmt.Println(task_config)

	var buf bytes.Buffer
	buf.Write([]byte(task_config))
	req, err := http.NewRequest("POST", url, &buf)
	if err != nil {
		//return client.Task{}, err
	}
	fmt.Println("88888888888888888888888888888888888888888888888888888888888888888")
	req.Header.Set("Content-Type", "application/json")

	t := client.Task{}

	_, err = kcli.Do(req, &t, http.StatusOK)

	fmt.Println("777777777777777777777777777777777777777777777777777777777777777777")

	return t, err
}

func updateTask(id string, data string, hostname string, kcli *client.Client) (client.Task, error) {
	url := "http://" + hostname + ":9092/kapacitor/v1/tasks/" + id
	var buf bytes.Buffer
	buf.Write([]byte(data))
	req, err := http.NewRequest("PATCH", url, &buf)
	if err != nil {
		//return client.Task{}, err
	}
	fmt.Println("777777777777777777777777778888888888888888888888888888888")
	req.Header.Set("Content-Type", "application/json")

	t := client.Task{}

	_, err = kcli.Do(req, &t, http.StatusOK)

	fmt.Println("777777777777777777777777777777777777777777777777777777777777777777")

	return t, err
}

func deleteTask(id string, hostname string, kcli *client.Client) error {
	url := "http://" + hostname + ":9092/kapacitor/v1/tasks/" + id

	var buf bytes.Buffer
	req, err := http.NewRequest("DELETE", url, &buf)
	if err != nil {
		//return client.Task{}, err
	}
	fmt.Println("66666666666666666666666666668888888888888888888888888888888")
	req.Header.Set("Content-Type", "application/json")

	t := client.Task{}

	_, err = kcli.Do(req, &t, http.StatusOK)

	return err
}
