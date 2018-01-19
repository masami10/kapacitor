package jiguang

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"time"
	"net/http"
	"sync/atomic"

	"github.com/masami10/kapacitor/alert"

	"encoding/base64"

	"github.com/masami10/kapacitor/services/iotseed"
)

type Service struct {
	configValue atomic.Value
	clientValue atomic.Value
	logger      *log.Logger
	basicToken  atomic.Value

	UserinfoService interface {
		GetActivesTerminalDevices(users string) ([]iotseed.TerminalDevice, error)
	}
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		logger: l,
	}
	s.configValue.Store(c)
	s.clientValue.Store(&http.Client{
		Transport: &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
		},
		Timeout: time.Duration(5 * time.Second),
	})
	s.basicToken.Store("Basic " + base64.StdEncoding.EncodeToString([]byte(c.APIKey+":"+c.APISecret)))
	return s
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) config() Config {
	return s.configValue.Load().(Config)
}

func (s *Service) Update(newConfig []interface{}) error {
	if l := len(newConfig); l != 1 {
		return fmt.Errorf("expected only one new config object, got %d", l)
	}
	if c, ok := newConfig[0].(Config); !ok {
		return fmt.Errorf("expected config object to be of type %T, got %T", c, newConfig[0])
	} else {
		s.configValue.Store(c)
		s.clientValue.Store(&http.Client{
			Transport: &http.Transport{
				Proxy:           http.ProxyFromEnvironment,
			},
			Timeout: time.Duration(5 * time.Second),
		})
		s.basicToken.Store("Basic " + base64.StdEncoding.EncodeToString([]byte(c.APIKey+":"+c.APISecret)))
	}
	return nil
}

func (s *Service) Alert(message, title, users string, details string, level alert.Level) error {
	url, post, err := s.preparePost(message, title, users, details, level)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url,  bytes.NewReader(post))
	if err != nil {
		return err
	}

	resp, err := s.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		pushoverResponse := struct {
			Error string `json:"error"`
		}{}
		err = json.Unmarshal(body, pushoverResponse)
		if err != nil {
			return err
		}
		type response struct {
			Error string `json:"error"`
		}
		r := &response{Error: fmt.Sprintf("failed to understand Jiguang response. code: %d content: %s", resp.StatusCode, pushoverResponse.Error)}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Error)
	}

	return nil
}

func (s *Service) Do(req *http.Request) (*http.Response, error) {
	client := s.clientValue.Load().(*http.Client)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", s.basicToken.Load().(string)) //必须重新load 可能之前已经refresh
	return client.Do(req)
}


type testOptions struct {
	Message  string      `json:"message"`
	Users    string      `json:"uses"`
	Title    string      `json:"title"`
	Details    string      `json:"details"`
	Level    alert.Level `json:"level"`
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{
		Users: 	 "demo@empower.cn",
		Message: "test pushover message",
		Details: "test pushover details",
		Level:   alert.Critical,
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %t", options)
	}

	return s.Alert(
		o.Message,
		o.Title,
		o.Users,
		o.Details,
		o.Level,
	)
}

// priority returns the jiguang priority as defined by the Jiguang API
// documentation https://jiguang.net/api
func priority(level alert.Level) int {
	switch level {
	case alert.OK:
		// send as -2 to generate no notification/alert
		return -2
	case alert.Info:
		// -1 to always send as a quiet notification
		return -1
	case alert.Warning:
		// 0 to display as high-priority and bypass the user's quiet hours,
		return 0
	case alert.Critical:
		// 1 to also require confirmation from the user
		return 1
	}

	return 0
}

type postAudience struct {
	Alias []string `json:"alias,omitempty"` //通过用户别名来发送消息
	RegistrationId []string `json:"registration_id,omitempty"` //通过用户别名来发送消息

}

type PostCommonNotification struct {
	Alert string `json:"alert"`
}

type postIOSNotification struct {
	PostCommonNotification
	Sound            string `json:"sound"` //默认为'default'
	ContentAvailable bool   `json:"content-available"`
}

type postAndroidNotification struct {
	PostCommonNotification
	Title    string `json:"title"`
	Priority int    `json:"priority"`
}

type postNotification struct {
	IOSMessage     postIOSNotification     `json:"ios-message,omitempty"`
	AndoridMessage postAndroidNotification `json:"and-message,omitempty"`
}

type postOptions struct {
	Sendno         int  `json:"sendno,omitempty"`
	TimeToLive     int  `json:"time_to_live,omitempty"`
	ApnsProduction bool `json:"apns_production,omitempty"` //ios 是否推送生产环境
}

type postMessage struct {
	MessageContent         string  `json:"msg_content,omitempty"`
}

type postData struct {
	Platform string       `json:"platform"`
	Audience postAudience `json:"audience,omitempty"`
	//message  postMessage
	Notification interface{} `json:"notification"`
	Message      postMessage `json:"message"`
	Options      postOptions `json:"options,omitempty"`
	CID          string `json:"cid,omitempty"`
}

// 通过两重循环过滤重复元素
func RemoveRepByLoop(slc []string) []string {
	result := []string{}  // 存放结果
	for i := range slc{
		flag := true
		for j := range result{
			if slc[i] == result[j] || slc[i] == ""{
				flag = false  // 存在重复元素，标识为false
				break
			}
		}
		if flag {  // 标识为false，不添加进结果
			result = append(result, slc[i])
		}
	}
	return result
}

func (s *Service) preparePost(message, title, users string, details string, level alert.Level) (string, []byte, error) {
	c := s.config()

	if !c.Enabled {
		return "", nil, errors.New("service is not enabled")
	}
	devices, err := s.UserinfoService.GetActivesTerminalDevices(users)

	if err != nil {
		return "", nil, err
	}

	if devices == nil {
		return "", nil, nil
	}

	pData := postData{
		Platform:     "all",
		Notification: PostCommonNotification{Alert: message}, //现阶段默认设定为message
		Message:	  postMessage{MessageContent: details},
	}

	alias := make([]string, len(devices))

	for i, device := range devices {
		if device.Alias == ""{
			continue
		}
		alias[i] = device.RegisterId
	}

	if len(alias) == 0 {
		return "", nil, fmt.Errorf("没有找到可以发送的对象")
	}

	pData.Audience.RegistrationId = RemoveRepByLoop(alias) //去重

	ret, err := json.Marshal(pData)
	if err != nil {
		return "", nil, err
	}

	return c.URL, ret, nil
}

type HandlerConfig struct {

	// Your message's title, otherwise your apps name is used
	Title string `mapstructure:"title"`

	AtPeopleOnIotseed string `mapstructure:"at-people-on-iotseed"`
}

type handler struct {
	s      *Service
	c      HandlerConfig
	logger *log.Logger
}

func (s *Service) Handler(c HandlerConfig, l *log.Logger) alert.Handler {
	return &handler{
		s:      s,
		c:      c,
		logger: l,
	}
}

func (h *handler) Handle(event alert.Event) {
	if err := h.s.Alert(
		event.State.Message,
		h.c.Title,
		h.c.AtPeopleOnIotseed,
		event.State.Details,
		event.State.Level,
	); err != nil {
		h.logger.Println("E! failed to send event to Jiguang", err)
	}
}
