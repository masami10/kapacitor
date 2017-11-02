package kapacitor

import (
	"github.com/masami10/kapacitor/pipeline"
	"github.com/masami10/kapacitor/tick"
)

type Template struct {
	id string
	tp *pipeline.TemplatePipeline
}

func (t *Template) Vars() map[string]tick.Var {
	return t.tp.Vars()
}

func (t *Template) Dot() string {
	return string(t.tp.Dot(t.id))
}
