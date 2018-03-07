package iotseed

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"sync/atomic"
	"time"

	"github.com/masami10/kapacitor/keyvalue"

	"net/url"

	"bytes"

	"strconv"

	"strings"

	"github.com/pkg/errors"
)

type tokenInfo struct {
	Token              string `json:"token"`
	TokenExpiry        int64  `json:"token_expiry"`
	RefreshToken       string `json:"refreshToken"`
	RefreshTokenExpiry int64  `json:"refreshToken_expiry"`
}

type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic

	Error(msg string, err error)
}

type Service struct {
	configValue atomic.Value
	clientValue atomic.Value
	diag        Diagnostic
	client      *http.Client
	tokenInfo   atomic.Value
}

func NewService(c Config, d Diagnostic) (*Service, error) {
	tlsConfig, err := getTLSConfig(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
	if err != nil {
		return nil, err
	}

	s := &Service{
		diag: d,
	}
	s.configValue.Store(c)
	s.clientValue.Store(&http.Client{
		Transport: &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: tlsConfig,
		},
		Timeout: time.Duration(5 * time.Second),
	})
	return s, nil
}

func (s *Service) Open() error {
	err := s.login()
	return err
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
		s.configValue.Store(c)
		s.clientValue.Store(&http.Client{
			Transport: &http.Transport{
				Proxy:           http.ProxyFromEnvironment,
				TLSClientConfig: tlsConfig,
			},
		})
		s.login() //重新登录刷新token
	}
	return nil
}

type postAuthData struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type postRefreshTokenData struct {
	RefreshToken string `json:"refreshToken"`
}

type authRespData struct {
	Token              string `json:"token"`
	TokenExpiry        string `json:"token_expiry"`
	RefreshToken       string `json:"refreshToken"`
	RefreshTokenExpiry string `json:"refreshToken_expiry"`
}

func (s *Service) login() error {
	c := s.config()
	if !c.Enabled {
		return nil
	}
	authData := postAuthData{
		Username: s.config().Username,
		Password: s.config().Password,
	}

	u, err := url.Parse(c.APIServer)
	if err != nil {
		return err
	}

	b, err := json.Marshal(authData)
	if err != nil {
		fmt.Println("json err:", err)
	}

	postBody := bytes.NewBuffer([]byte(b))

	u.Path = path.Join(u.Path, "/api/v2/auth/login")
	URL := strings.Join([]string{c.APIServer, u.Path}, "")
	req, err := http.NewRequest("POST", URL, postBody)
	if err != nil {
		return err
	}

	resp, err := s.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("认证失败%d", resp.StatusCode)
	}

	res, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	respData := authRespData{}
	err = json.Unmarshal(res, &respData)
	if err != nil {
		return err
	}
	now := time.Now().Unix()

	info := tokenInfo{
		Token:        respData.Token,
		RefreshToken: respData.RefreshToken,
	}

	tokenExpiryTime, err := strconv.ParseInt(respData.TokenExpiry, 10, 64)
	if err != nil {
		return err
	}
	refreshTokenExpiryTime, err := strconv.ParseInt(respData.RefreshTokenExpiry, 10, 64)
	if err != nil {
		return err
	}
	info.TokenExpiry = now + tokenExpiryTime
	info.RefreshTokenExpiry = now + refreshTokenExpiryTime

	s.tokenInfo.Store(info)

	return nil

}

func (s *Service) tokenRefresh() error {
	c := s.config()

	if !c.Enabled{
		return nil
	}

	u, err := url.Parse(c.APIServer)
	if err != nil {
		return err
	}

	token_info := s.tokenInfo.Load().(tokenInfo)
	if time.Now().Unix() > token_info.RefreshTokenExpiry {
		return s.login() //需要重新登录
	}
	// refresh token
	authData := postRefreshTokenData{
		RefreshToken: s.tokenInfo.Load().(tokenInfo).RefreshToken,
	}

	b, err := json.Marshal(authData)
	if err != nil {
		fmt.Println("json err:", err)
	}

	postBody := bytes.NewBuffer([]byte(b))

	u.Path = path.Join(u.Path, "/api/auth/token")
	URL := strings.Join([]string{c.APIServer, u.Path}, "")
	req, err := http.NewRequest("POST", URL, postBody)
	if err != nil {
		return err
	}

	client := s.clientValue.Load().(*http.Client)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req) //refresh token 不得调用service封装的Do方法
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	res, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	respData := authRespData{}
	err = json.Unmarshal(res, &respData)
	if err != nil {
		return err
	}
	now := time.Now().Unix()

	info := tokenInfo{
		Token:        respData.Token,
		RefreshToken: respData.RefreshToken,
	}

	tokenExpiryTime, err := strconv.ParseInt(respData.TokenExpiry, 10, 64)
	if err != nil {
		return err
	}
	refreshTokenExpiryTime, err := strconv.ParseInt(respData.RefreshTokenExpiry, 10, 64)
	if err != nil {
		return err
	}
	info.TokenExpiry = now + tokenExpiryTime
	info.RefreshTokenExpiry = now + refreshTokenExpiryTime

	s.tokenInfo.Store(info)

	return nil

}

func (s *Service) Do(req *http.Request) (*http.Response, error) {
	client := s.clientValue.Load().(*http.Client)
	req.Header.Set("Content-Type", "application/json")
	token_info := s.tokenInfo.Load()
	if token_info != nil {
		if time.Now().Unix() > s.tokenInfo.Load().(tokenInfo).TokenExpiry {
			s.tokenRefresh()
		}
		req.Header.Set("X-Authorization", "Bearer " + s.tokenInfo.Load().(tokenInfo).Token) //必须重新load 可能之前已经refresh
	}

	return client.Do(req)
}

func (s *Service) GetActivesTerminalDevices(users string) ([]TerminalDevice, error) {
	c :=s.configValue.Load().(Config)
	if !c.Enabled || users == "" {
		return nil, nil
	}
	req, err := s.prepareGetDevices(users)
	if err != nil {
		return nil, err
	}

	resp, err := s.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		type response struct {
			Error string `json:"error"`
		}
		r := &response{Error: fmt.Sprintf("failed to understand iotseed response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return nil, errors.New(r.Error)
	} else {
		var result []TerminalDevice
		err := json.Unmarshal(body, &result)
		if err != nil {
			return nil, err
		}
		return result, nil
	}

}

func (s *Service) prepareGetDevices(users string) (*http.Request, error) {

	c := s.config()
	u, err := url.Parse(c.APIServer)
	if err != nil {
		return nil, err
	}

	u.Path = path.Join(u.Path, c.TerminalControllerUri)
	URL := strings.Join([]string{c.APIServer,"api",c.APIVersion, u.Path}, "/")
	req, err := http.NewRequest("GET", URL, nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	q.Add("username", users)
	q.Add("status", "online")
	req.URL.RawQuery = q.Encode()

	return req, nil

}

// iotseed devices
type TerminalDevice struct {
	Alias      string `json:"alias"`
	ID         string `json:"id"`
	RegisterId string `json:"register_id"`
	Status     string `json:"status"`
	Platform   string `json:"platform"`
}

type testOptions struct {
	Uses   string `json:"username"`
	Status string `json:"status"`
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{
		Uses:   "demo",
		Status: "online",
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	_, err := s.GetActivesTerminalDevices(o.Uses)
	return err
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
