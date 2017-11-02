package dingding

import (
	"github.com/pkg/errors"
	"regexp"
)


type Config struct {
	// Whether Dingding integration is enabled.
	Enabled bool `toml:"enabled" override:"enabled"`
	// access_token
	AccessToken	string	`toml:"access-token"  override:"access-token"`
	// The default at people on mobile, can be overridden per alert.
	// 填写all代表at所有人,填写手机号代表at手机号联系人
	// 当需要at多个手机联系人时,填写的手机号之间以","作为分隔符
	// 例子如下：
	// at-people-on-mobile = all (at所有人)
	// at-people-on-mobile = 156xxxx8827,189xxxx8325
	AtPeopleOnMobile string `toml:"at-people-on-mobile" override:"at-people-on-mobile"`
	// Whether all alerts should automatically post to dingding
	Global bool `toml:"global" override:"global"`
	// Whether all alerts should automatically use stateChangesOnly mode.
	// Only applies if global is also set.
	StateChangesOnly bool `toml:"state-changes-only" override:"state-changes-only"`

	// Path to CA file
	SSLCA string `toml:"ssl-ca" override:"ssl-ca"`
	// Path to host cert file
	SSLCert string `toml:"ssl-cert" override:"ssl-cert"`
	// Path to cert key file
	SSLKey string `toml:"ssl-key" override:"ssl-key"`
	// Use SSL but skip chain & host verification
	InsecureSkipVerify bool `toml:"insecure-skip-verify" override:"insecure-skip-verify"`
}

func NewConfig() Config {
	return Config{
		Enabled: false,
		Global: false,
		StateChangesOnly:true,
		InsecureSkipVerify:true,
	}
}

func (c Config) Validate() error {
	if c.Enabled{
		if validationDingdingAtPeopleOnMobile(c.AtPeopleOnMobile) != nil{
			return errors.New("at-people-on-mobile configure error")
		}
	}

	return nil
}

// 用户验证用户配置的at-people-on-mobile是否合法的正则表达式
const pattern =
	"^(all|((13[0-9]|14[5|7]|15[0|1|2|3|5|6|7|8|9]|18[0|1|2|3|5|6|7|8|9])\\d{8}((,(13[0-9]|14[5|7]|15[0|1|2|3|5|6|7|8|9]|18[0|1|2|3|5|6|7|8|9])\\d{8})*)))$"

func validationDingdingAtPeopleOnMobile(str string) error {
	matched, err := regexp.MatchString(pattern, str)

	if matched == false {
		return err
	}

	return nil
}