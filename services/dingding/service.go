package dingding

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync/atomic"

	"github.com/masami10/kapacitor/alert"
	"github.com/pkg/errors"
	"strings"
)

const DefaultUrl = "https://oapi.dingtalk.com/robot/send?access_token="

type Service struct {
	configValue atomic.Value
	clientValue atomic.Value
	logger      *log.Logger
	client      *http.Client
}

func NewService(c Config, l *log.Logger) (*Service, error) {
	tlsConfig, err := getTLSConfig(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
	if err != nil {
		return nil, err
	}
	if tlsConfig.InsecureSkipVerify {
		l.Println("W! dingding service is configured to skip ssl verification")
	}
	s := &Service{
		logger: l,
	}
	s.configValue.Store(c)
	s.clientValue.Store(&http.Client{
		Transport: &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: tlsConfig,
		},
	})
	return s, nil
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
		tlsConfig, err := getTLSConfig(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
		if err != nil {
			return err
		}
		if tlsConfig.InsecureSkipVerify {
			s.logger.Println("W! dingding service is configured to skip ssl verification")
		}
		s.configValue.Store(c)
		s.clientValue.Store(&http.Client{
			Transport: &http.Transport{
				Proxy:           http.ProxyFromEnvironment,
				TLSClientConfig: tlsConfig,
			},
		})
	}
	return nil
}

func (s *Service) Global() bool {
	c := s.config()
	return c.Global
}

func (s *Service) StateChangesOnly() bool {
	c := s.config()
	return c.StateChangesOnly
}

// dingding message type
type messageType struct{
	Content	string	`json:"content"`
}

// dingding at type
type atType struct{
	IsAtAll		bool 		`json:"isAtAll,omitempty"`
	AtMobiles	[]string	`json:"atMobiles,omitempty"`
}

type testOptions struct {
	Channel   string      `json:"channel"`
	Message   string      `json:"message"`
	Level     alert.Level `json:"level"`
	Username  string      `json:"username"`
	IconEmoji string      `json:"icon-emoji"`
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{
		Message: "test dingding message",
		Level:   alert.Critical,
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(o.Channel, o.Channel, o.Message, o.Username, o.IconEmoji, o.Level)
}

func (s *Service) Alert(atPeopleOnMobile string, accessToken string, message, username, iconEmoji string, level alert.Level) error {
	url, post, err := s.preparePost(atPeopleOnMobile, accessToken, message, username, iconEmoji, level)
	if err != nil {
		return err
	}
	client := s.clientValue.Load().(*http.Client)
	resp, err := client.Post(url, "application/json", post)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		type response struct {
			Error string `json:"error"`
		}
		r := &response{Error: fmt.Sprintf("failed to understand dingding response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Error)
	}
	return nil
}

func (s *Service) preparePost(atPeopleOnMobile string, accessToken string, message, username, iconEmoji string, level alert.Level) (string, io.Reader, error) {
	c := s.config()

	if !c.Enabled {
		return "", nil, errors.New("service is not enabled")
	}

	if accessToken == "" {
		if c.AccessToken == "" {
			return "", nil, errors.New("must specify access-token")
		}
		accessToken = c.AccessToken
	}

	a := messageType{
		Content:  message,
	}
	postData := make(map[string]interface{})
	postData["msgtype"] = "text"
	postData["text"] = a

	if atPeopleOnMobile == "" {
		atPeopleOnMobile = c.AtPeopleOnMobile
	}
	if atPeopleOnMobile != "" {
		err := validationDingdingAtPeopleOnMobile(atPeopleOnMobile)
		if err != nil {
			return "", nil, err
		}

		var b atType
		if atPeopleOnMobile == "all" {
			b = atType{
				IsAtAll:true,
			}
		} else {
			s :=strings.Split(atPeopleOnMobile, ",")
			b = atType{
				IsAtAll:false,
				AtMobiles:s,
			}
		}
		postData["at"] = b
	}


	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(postData)
	if err != nil {
		return "", nil, err
	}

	return DefaultUrl + accessToken, &post, nil
}

type HandlerConfig struct {
	// AtPeopleOnMobile
	// If empty uses the at-people-on-mobile from the configuration
	AtPeopleOnMobile string `mapstructure:"at-people-on-mobile"`

	// AccessToken
	AccessToken string `mapstructure:"access-token"`
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
		h.c.AtPeopleOnMobile,
		h.c.AccessToken,
		event.State.Message,
		"",
		"",
		event.State.Level,
	); err != nil {
		h.logger.Println("E! failed to send event to dingding", err)
	}
}

// getTLSConfig creates a tls.Config object from the given certs, key, and CA files.
// you must give the full path to the files.
func getTLSConfig(
	SSLCA, SSLCert, SSLKey string,
	InsecureSkipVerify bool,
) (*tls.Config, error) {
	t := &tls.Config{
		InsecureSkipVerify: InsecureSkipVerify,
	}
	if SSLCert != "" && SSLKey != "" {
		cert, err := tls.LoadX509KeyPair(SSLCert, SSLKey)
		if err != nil {
			return nil, fmt.Errorf(
				"Could not load TLS client key/certificate: %s",
				err)
		}
		t.Certificates = []tls.Certificate{cert}
	} else if SSLCert != "" {
		return nil, errors.New("Must provide both key and cert files: only cert file provided.")
	} else if SSLKey != "" {
		return nil, errors.New("Must provide both key and cert files: only key file provided.")
	}

	if SSLCA != "" {
		caCert, err := ioutil.ReadFile(SSLCA)
		if err != nil {
			return nil, fmt.Errorf("Could not load TLS CA: %s",
				err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		t.RootCAs = caCertPool
	}
	return t, nil
}