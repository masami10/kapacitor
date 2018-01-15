package run

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/BurntSushi/toml"
	"github.com/masami10/kapacitor/server"
	"github.com/masami10/kapacitor/services/logging"
	"github.com/masami10/kapacitor/tick"
	"github.com/coreos/etcd/clientv3"
	"time"
	"context"
	"net/http"
	"github.com/masami10/kapacitor/client/v1"
	"bytes"
	"strings"
)

const logo = `
'##:::'##::::'###::::'########:::::'###:::::'######::'####:'########::'#######::'########::
 ##::'##::::'## ##::: ##.... ##:::'## ##:::'##... ##:. ##::... ##..::'##.... ##: ##.... ##:
 ##:'##::::'##:. ##:: ##:::: ##::'##:. ##:: ##:::..::: ##::::: ##:::: ##:::: ##: ##:::: ##:
 #####::::'##:::. ##: ########::'##:::. ##: ##:::::::: ##::::: ##:::: ##:::: ##: ########::
 ##. ##::: #########: ##.....::: #########: ##:::::::: ##::::: ##:::: ##:::: ##: ##.. ##:::
 ##:. ##:: ##.... ##: ##:::::::: ##.... ##: ##::: ##:: ##::::: ##:::: ##:::: ##: ##::. ##::
 ##::. ##: ##:::: ##: ##:::::::: ##:::: ##:. ######::'####:::: ##::::. #######:: ##:::. ##:
..::::..::..:::::..::..:::::::::..:::::..:::......:::....:::::..::::::.......:::..:::::..::

`

const (
	prefixHostnameKey       = "kapacitors/hostname/"
	prefixHostnameRevKey    = "kapacitors/rev/"
	prefixTasksIdKey        = "tasks/id/"
	prefixTasksConfigKey    = "tasks/config/"
	prefixTasksIsCreatedKey = "tasks/is_created/"
	prefixTasksInNodeKey    = "tasks/in_node/"
	prefixTasksStatusKey    = "tasks/status/"
	totalTaskNumKey  		= "kapacitors/task_num/"
)

var kcli *client.Client

// Command represents the command executed by "kapacitord run".
type Command struct {
	Version string
	Branch  string
	Commit  string

	closing chan struct{}
	Closed  chan struct{}

	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer

	Server     *server.Server
	Logger     *log.Logger
	logService *logging.Service
}

// NewCommand return a new instance of Command.
func NewCommand() *Command {
	return &Command{
		closing: make(chan struct{}),
		Closed:  make(chan struct{}),
		Stdin:   os.Stdin,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
	}
}

// Run parses the config from args and runs the server.
func (cmd *Command) Run(args ...string) error {
	// Parse the command line flags.
	options, err := cmd.ParseFlags(args...)
	if err != nil {
		return err
	}

	// Print sweet Kapacitor logo.
	fmt.Print(logo)

	// Parse config
	config, err := cmd.ParseConfig(FindConfigPath(options.ConfigPath))
	if err != nil {
		return fmt.Errorf("parse config: %s", err)
	}

	// Apply any environment variables on top of the parsed config
	if err := config.ApplyEnvOverrides(); err != nil {
		return fmt.Errorf("apply env config: %v", err)
	}

	// Override config hostname if specified in the command line args.
	if options.Hostname != "" {
		config.Hostname = options.Hostname
	}

	// Override config logging file if specified in the command line args.
	if options.LogFile != "" {
		config.Logging.File = options.LogFile
	}

	// Override config logging level if specified in the command line args.
	if options.LogLevel != "" {
		config.Logging.Level = options.LogLevel
	}

	// Initialize Logging Services
	cmd.logService = logging.NewService(config.Logging, cmd.Stdout, cmd.Stderr)
	err = cmd.logService.Open()
	if err != nil {
		return fmt.Errorf("init logging: %s", err)
	}
	// Initialize packages loggers
	tick.SetLogger(cmd.logService.NewLogger("[tick] ", log.LstdFlags))

	// Initialize cmd logger
	cmd.Logger = cmd.logService.NewLogger("[run] ", log.LstdFlags)

	// Mark start-up in log.,
	cmd.Logger.Printf("I! Kapacitor starting, version %s, branch %s, commit %s", cmd.Version, cmd.Branch, cmd.Commit)
	cmd.Logger.Printf("I! Go version %s", runtime.Version())

	// Write the PID file.
	if err := cmd.writePIDFile(options.PIDFile); err != nil {
		return fmt.Errorf("write pid file: %s", err)
	}

	// registry to etcd
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: strings.Split(config.EtcdServers, ","),
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		cmd.Logger.Fatal(err)
	}
	//
	var keepAliveCh chan int
	go cmd.registryToEtcd(config, cli, keepAliveCh)

	// Create server from config and start it.
	buildInfo := server.BuildInfo{Version: cmd.Version, Commit: cmd.Commit, Branch: cmd.Branch}
	s, err := server.New(config, buildInfo, cmd.logService)
	if err != nil {
		return fmt.Errorf("create server: %s", err)
	}
	s.CPUProfile = options.CPUProfile
	s.MemProfile = options.MemProfile
	if err := s.Open(); err != nil {
		return fmt.Errorf("open server: %s", err)
	}
	cmd.Server = s

	// Begin monitoring the server's error channel.
	go cmd.monitorServerErrors()

	// tasks
	kcli, err = connect( "http://localhost:9092", false)
	if err != nil {
		cmd.Logger.Fatal(err)
	}
	go cmd.watchTaskid(config, cli)

	return nil
}

// Close shuts down the server.
func (cmd *Command) Close() error {
	defer close(cmd.Closed)
	close(cmd.closing)
	if cmd.Server != nil {
		return cmd.Server.Close()
	}
	if cmd.logService != nil {
		return cmd.logService.Close()
	}
	return nil
}

func (cmd *Command) monitorServerErrors() {
	for {
		select {
		case err := <-cmd.Server.Err():
			if err != nil {
				cmd.Logger.Println("E! " + err.Error())
			}
		case <-cmd.closing:
			return
		}
	}
}

func (cmd *Command) registryToEtcd(c *server.Config, cli *clientv3.Client, kch chan int) {
	hostName := c.Hostname
	hostnameKey := prefixHostnameKey + hostName
	hostnameRevKey := prefixHostnameRevKey + hostName

	ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Second)
	// grant
	resp, err := cli.Grant(ctx, 5)
	if err != nil {
		fmt.Println(err)
		cmd.Logger.Fatal(err)
	}
	cancel()

	// put key
	ctx2, cancel2 := context.WithTimeout(context.Background(), 1 * time.Second)
	_, err = cli.Put(ctx2, hostnameKey, hostName, clientv3.WithLease(resp.ID))
	if err != nil {
		fmt.Println(err)
		cmd.Logger.Fatal(err)
	}
	cancel2()

	kvc := clientv3.NewKV(cli)
	_, err = kvc.Txn(context.TODO()).
		If(clientv3.Compare(clientv3.Value(hostnameKey), "=", hostName)).
			Then(clientv3.OpPut(hostnameRevKey, "0")).
				Commit()
	if err != nil {
		cmd.Logger.Fatal(err)
	}

	ch, err := cli.KeepAlive(context.TODO(), resp.ID)
	if err != nil {
		cmd.Logger.Fatal(err)
	}
	go func() {
		for kresp := range ch {
			//kch <- 1
			fmt.Println("TTL: ", kresp.TTL)
		}
	}()

	<-kch
}


func (cmd *Command) watchTaskid(c *server.Config, cli *clientv3.Client){
	// put delete

	rch := cli.Watch(context.Background(), prefixTasksIdKey, clientv3.WithPrefix())

	for wresp := range rch {
		for _, ev := range wresp.Events {
			evType := ev.Type.String()
			if evType == "PUT"{
				// 计算延迟事务时间
				// 1. 查询总任务数
				taskNum := int64(0)
				resp, err := cli.Get(context.TODO(), totalTaskNumKey)
				if err != nil {
					cmd.Logger.Fatal(err)
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
				tasksInNodeKey := prefixTasksInNodeKey + c.Hostname
				resp, err = cli.Get(context.TODO(), tasksInNodeKey, clientv3.WithPrefix())
				nodeTaskNum := int64(len(resp.Kvs))

				// 4. 计算每个节点应该分配的任务数
				avgTaskNum := (taskNum + 1) / nodeNum + 1

				// 5. 计算延迟抓取时间
				diff := nodeTaskNum - avgTaskNum

				delayTime := int64(0)
				if diff > 0 {
					delayTime = diff
				}

				time.Sleep(time.Duration(delayTime * 100) * time.Microsecond)
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
					cmd.Logger.Fatal(err)
				}

				// 检查事务条件是否成立
				if kresp.Succeeded == false {
					continue
				}

				// 获取任务信息
				taskConfigKey := prefixTasksConfigKey + taskId

				fmt.Println("6666666666666666666666666666666666666666666666666666666666")
				fmt.Println(taskConfigKey)

				ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Second)
				gresp, err := cli.Get(ctx, taskConfigKey)
				if err != nil {
					log.Fatal(err)
				}
				cancel()

				for _, ev := range gresp.Kvs {
					if string(ev.Key) == taskConfigKey {
						taskConfig := string(ev.Value)

						var t client.Task
						t, err = createTask(taskConfig)
						if err != nil {
							// 创建任务失败
							cmd.Logger.Fatal(err)
						}

						//isCreatedKey := prefixTasksIsCreatedKey + taskId
						//_, err = cli.Put(context.TODO(), isCreatedKey, "true")
						//if err != nil {
						//	cmd.Logger.Fatal(err)
						//}
						//keyStatus := prefixTasksStatusKey + taskId
						//_, err = cli.Put(context.TODO(), keyStatus, t.Status.String())
						//if err != nil {
						//	cmd.Logger.Fatal(err)
						//}
						//
						//_, err = cli.Put(context.TODO(), tasksInNodeKey + taskId, taskId)
						//if err != nil {
						//	cmd.Logger.Fatal(err)
						//}

						//
						isCreatedKey := prefixTasksIsCreatedKey + taskId
						keyStatus := prefixTasksStatusKey + taskId
						taskIdInNodeKey := tasksInNodeKey + "/" + taskId
						kvc := clientv3.NewKV(cli)
						taskIdKey := prefixTasksIdKey + taskId
						_, err = kvc.Txn(context.TODO()).
							If(clientv3.Compare(clientv3.Value(taskIdKey), "=", c.Hostname)).
								Then(clientv3.OpPut(isCreatedKey, "true"),
									clientv3.OpPut(keyStatus, t.Status.String()),
									clientv3.OpPut(taskIdInNodeKey, taskId)).
								Commit()
						if err != nil {
							cmd.Logger.Fatal(err)
						}

						// 把任务数量加一
						totalTaskNum := "1"

						resp, err := cli.Get(context.TODO(), totalTaskNumKey)
						if err != nil {
							cmd.Logger.Fatal(err)
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

			}else if evType == "DELETE" {

			}
		}
	}
}

func connect(url string, skipSSL bool) (*client.Client, error) {
	return client.New(client.Config{
		URL:                url,
		InsecureSkipVerify: skipSSL,
	})
}

func createTask(task_config string) (client.Task, error) {
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


// ParseFlags parses the command line flags from args and returns an options set.
func (cmd *Command) ParseFlags(args ...string) (Options, error) {
	var options Options
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.StringVar(&options.ConfigPath, "config", "", "")
	fs.StringVar(&options.PIDFile, "pidfile", "", "")
	fs.StringVar(&options.Hostname, "hostname", "", "")
	fs.StringVar(&options.CPUProfile, "cpuprofile", "", "")
	fs.StringVar(&options.MemProfile, "memprofile", "", "")
	fs.StringVar(&options.LogFile, "log-file", "", "")
	fs.StringVar(&options.LogLevel, "log-level", "", "")
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, usage) }
	if err := fs.Parse(args); err != nil {
		return Options{}, err
	}
	return options, nil
}

// writePIDFile writes the process ID to path.
func (cmd *Command) writePIDFile(path string) error {
	// Ignore if path is not set.
	if path == "" {
		return nil
	}

	// Ensure the required directory structure exists.
	err := os.MkdirAll(filepath.Dir(path), 0777)
	if err != nil {
		return fmt.Errorf("mkdir: %s", err)
	}

	// Retrieve the PID and write it.
	pid := strconv.Itoa(os.Getpid())
	if err := ioutil.WriteFile(path, []byte(pid), 0666); err != nil {
		return fmt.Errorf("write file: %s", err)
	}

	return nil
}

// ParseConfig parses the config at path.
// Returns a demo configuration if path is blank.
func (cmd *Command) ParseConfig(path string) (*server.Config, error) {
	// Use demo configuration if no config path is specified.
	if path == "" {
		log.Println("No configuration provided, using default settings")
		return server.NewDemoConfig()
	}

	log.Println("Using configuration at:", path)

	config := server.NewConfig()
	if _, err := toml.DecodeFile(path, &config); err != nil {
		return nil, err
	}

	return config, nil
}

var usage = `usage: run [flags]

run starts the Kapacitor server.

        -config <path>
                          Set the path to the configuration file.

        -hostname <name>
                          Override the hostname, the 'hostname' configuration
                          option will be overridden.

        -pidfile <path>
                          Write process ID to a file.

        -log-file <path>
                          Write logs to a file.

        -log-level <level>
                          Sets the log level. One of debug,info,warn,error.
`

// Options represents the command line options that can be parsed.
type Options struct {
	ConfigPath string
	PIDFile    string
	Hostname   string
	CPUProfile string
	MemProfile string
	LogFile    string
	LogLevel   string
}
