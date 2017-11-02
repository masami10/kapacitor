package kapacitor

import (
	"log"

	"github.com/masami10/kapacitor/expvar"
	"github.com/masami10/kapacitor/models"
	"github.com/masami10/kapacitor/pipeline"
)

const (
	statsFieldsDefaulted = "fields_defaulted"
	statsTagsDefaulted   = "tags_defaulted"
)

type DefaultNode struct {
	node
	d *pipeline.DefaultNode

	fieldsDefaulted *expvar.Int
	tagsDefaulted   *expvar.Int
}

// Create a new  DefaultNode which applies a transformation func to each point in a stream and returns a single point.
func newDefaultNode(et *ExecutingTask, n *pipeline.DefaultNode, l *log.Logger) (*DefaultNode, error) {
	dn := &DefaultNode{
		node: node{Node: n, et: et, logger: l},
		d:    n,
	}
	dn.node.runF = dn.runDefault
	return dn, nil
}

func (e *DefaultNode) runDefault(snapshot []byte) error {
	e.fieldsDefaulted = &expvar.Int{}
	e.tagsDefaulted = &expvar.Int{}

	e.statMap.Set(statsFieldsDefaulted, e.fieldsDefaulted)
	e.statMap.Set(statsTagsDefaulted, e.tagsDefaulted)
	switch e.Provides() {
	case pipeline.StreamEdge:
		for p, ok := e.ins[0].NextPoint(); ok; p, ok = e.ins[0].NextPoint() {
			e.timer.Start()
			p.Fields, p.Tags = e.setDefaults(p.Fields, p.Tags)
			p.UpdateGroup()
			e.timer.Stop()
			for _, child := range e.outs {
				err := child.CollectPoint(p)
				if err != nil {
					return err
				}
			}
		}
	case pipeline.BatchEdge:
		for b, ok := e.ins[0].NextBatch(); ok; b, ok = e.ins[0].NextBatch() {
			e.timer.Start()
			b.Points = b.ShallowCopyPoints()
			_, b.Tags = e.setDefaults(nil, b.Tags)
			b.UpdateGroup()
			for i := range b.Points {
				b.Points[i].Fields, b.Points[i].Tags = e.setDefaults(b.Points[i].Fields, b.Points[i].Tags)
			}
			e.timer.Stop()
			for _, child := range e.outs {
				err := child.CollectBatch(b)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (d *DefaultNode) setDefaults(fields models.Fields, tags models.Tags) (models.Fields, models.Tags) {
	newFields := fields
	fieldsCopied := false
	for field, value := range d.d.Fields {
		if v := fields[field]; v == nil {
			if !fieldsCopied {
				newFields = newFields.Copy()
				fieldsCopied = true
			}
			d.fieldsDefaulted.Add(1)
			newFields[field] = value
		}
	}
	newTags := tags
	tagsCopied := false
	for tag, value := range d.d.Tags {
		if v := tags[tag]; v == "" {
			if !tagsCopied {
				newTags = newTags.Copy()
				tagsCopied = true
			}
			d.tagsDefaulted.Add(1)
			newTags[tag] = value
		}
	}
	return newFields, newTags
}
