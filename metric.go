// Copyright (c) Tetrate, Inc 2021.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package implementation

import (
	"context"

	"github.com/tetratelabs/telemetry"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

// NewMetricSink returns a new Telemetry facade compatible MetricSink.
func NewMetricSink(logger telemetry.Logger) telemetry.MetricSink {
	return metricSink{logger: logger}
}

type metricSink struct {
	logger telemetry.Logger
}

func convertOptions(opts []telemetry.MetricOption) *metricOptions {
	from := &telemetry.MetricOptions{Unit: "1"}
	to := &metricOptions{}

	for _, opt := range opts {
		opt(from)
	}

	switch from.Unit {
	case telemetry.None:
		to.unit = string(telemetry.None)
	case telemetry.Bytes:
		to.unit = string(telemetry.Bytes)
	case telemetry.Seconds:
		to.unit = string(telemetry.Seconds)
	case telemetry.Milliseconds:
		to.unit = string(telemetry.Milliseconds)
	default:
		to.unit = string(telemetry.None)
	}
	to.tagKeys = make([]tag.Key, 0, len(from.Labels))
	for _, l := range from.Labels {
		to.tagKeys = append(to.tagKeys, l.(*labelImpl).label)
	}
	return to
}

// NewSum creates a new Metric with an aggregation type of Sum (the values will
// be cumulative). That means that data collected by the new Metric will be
// summed before export.
func (m metricSink) NewSum(name, description string, opts ...telemetry.MetricOption) telemetry.Metric {
	mo := convertOptions(opts)
	return m.newFloat64Metric(name, description, view.Sum(), mo)
}

// NewGauge creates a new Metric with an aggregation type of LastValue. That
// means that data collected by the new Metric will export only the last
// recorded value.
func (m metricSink) NewGauge(name, description string, opts ...telemetry.MetricOption) telemetry.Metric {
	mo := convertOptions(opts)
	return m.newFloat64Metric(name, description, view.LastValue(), mo)
}

// NewDistribution creates a new Metric with an aggregation type of Distribution.
// This means that the data collected by the Metric will be collected and
// exported as a histogram, with the specified bounds.
func (m metricSink) NewDistribution(name, description string, bounds []float64, opts ...telemetry.MetricOption) telemetry.Metric {
	mo := convertOptions(opts)
	return m.newFloat64Metric(name, description, view.Distribution(bounds...), mo)
}

// NewLabel creates a new Label to be used as a metrics dimension.
func (m metricSink) NewLabel(name string) telemetry.Label {
	label, _ := tag.NewKey(name)
	return &labelImpl{
		label: label,
	}
}

// ContextWithLabels takes the existing LabelValues collection found in context
// and runs the Label operations as provided by the provided values on top of
// the collection which is then added to the returned context. The function can
// return an error in case the provided values contain invalid label names.
func (m metricSink) ContextWithLabels(ctx context.Context, values ...telemetry.LabelValue) (context.Context, error) {
	if len(values) == 0 {
		return ctx, nil
	}
	mutators := make([]tag.Mutator, len(values))
	for idx, value := range values {
		mutators[idx] = value.(tag.Mutator)
	}
	return tag.New(ctx, mutators...)
}

type labelImpl struct {
	label tag.Key
}

func (l labelImpl) Insert(val string) telemetry.LabelValue {
	return tag.Insert(l.label, val)
}

func (l labelImpl) Update(val string) telemetry.LabelValue {
	return tag.Update(l.label, val)
}

func (l labelImpl) Upsert(val string) telemetry.LabelValue {
	return tag.Upsert(l.label, val)
}

func (l labelImpl) Delete() telemetry.LabelValue {
	return tag.Delete(l.label)
}

type float64Metric struct {
	*stats.Float64Measure

	tags []tag.Mutator
	view *view.View
}

type metricOptions struct {
	unit    string
	tagKeys []tag.Key
}

func (m metricSink) newFloat64Metric(name, description string, aggr *view.Aggregation, o *metricOptions) *float64Metric {
	measure := stats.Float64(name, description, o.unit)
	tagKeys := make([]tag.Key, 0, len(o.tagKeys))

	for _, k := range o.tagKeys {
		tagKeys = append(tagKeys, k)
	}

	newView := &view.View{Measure: measure, TagKeys: tagKeys, Aggregation: aggr}
	if err := view.Register(newView); err != nil {
		m.logger.Error("unable to register metric view", err)
	}

	return &float64Metric{measure, make([]tag.Mutator, 0), newView}
}

func (f *float64Metric) Increment() {
	f.Record(1)
}

func (f *float64Metric) Decrement() {
	f.Record(-1)
}

func (f *float64Metric) Name() string {
	return f.Float64Measure.Name()
}

func (f *float64Metric) Record(value float64) {
	_ = stats.RecordWithTags(context.Background(), f.tags, f.M(value))
}

func (f *float64Metric) RecordContext(ctx context.Context, value float64) {
	_ = stats.RecordWithTags(ctx, f.tags, f.M(value))
}

func (f *float64Metric) With(labelValues ...telemetry.LabelValue) telemetry.Metric {
	if len(labelValues) == 0 {
		return f
	}
	tags := make([]tag.Mutator, len(f.tags), len(f.tags)+len(labelValues))
	copy(tags, f.tags)
	for _, tagValue := range labelValues {
		tags = append(tags, tagValue.(tag.Mutator))
	}
	return &float64Metric{f.Float64Measure, tags, f.view}
}

// ToLogger takes a Logger and returns a Logger interface which will
// emit an Increment() on Logger.Info and Logger.Error calls for this
// metric.
func (f *float64Metric) ToLogger(logger telemetry.Logger) telemetry.Logger {
	return logger.Metric(f)
}
