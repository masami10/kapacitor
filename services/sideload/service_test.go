package sideload_test

import (
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/kapacitor/services/sideload"
)

func NewService() *sideload.Service {
	s := sideload.NewService(nil)
	return s
}

func TestService_Source_Lookup(t *testing.T) {
	s := NewService()

	src, err := s.Source("testdata/src0")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	testCases := []struct {
		order []string
		key   string
		want  interface{}
	}{
		{
			order: []string{
				"host/hostA.yml",
				"default.yml",
			},
			key:  "key0",
			want: 5.0,
		},
		{
			order: []string{
				"host/hostA.yml",
				"default.yml",
			},
			key:  "key1",
			want: "one",
		},
		{
			order: []string{
				"host/hostA.yml",
				"hostgroup/foo.yml",
				"default.yml",
			},
			key:  "key0",
			want: 5.0,
		},
		{
			order: []string{
				"host/hostA.yml",
				"hostgroup/foo.yml",
				"default.yml",
			},
			key:  "key1",
			want: "foo",
		},
	}
	for i, tc := range testCases {
		tc := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			got := src.Lookup(tc.order, tc.key)
			if !cmp.Equal(got, tc.want) {
				t.Errorf("unexpected values: -want/+got:\n%s", cmp.Diff(tc.want, got))
			}
		})
	}
}
