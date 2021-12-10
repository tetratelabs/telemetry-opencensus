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

// Package opencensus provides a tetratelabs/telemetry compatible metrics
// implementation based on OpenCensus.
package opencensus

import (
	"context"
	"fmt"

	"github.com/tetratelabs/telemetry"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

// New returns a new Telemetry facade compatible MetricSink.
func New(logger telemetry.Logger) telemetry.MetricSink {
	return metricSink{logger: logger}
}

type metricSink struct {
	logger telemetry.Logger
}

// NewSum creates a new Metric with an aggregation type of Sum (the values will
// be cumulative). That means that data collected by the new Metric will be
// summed before export.
func (m metricSink) NewSum(name, description string, opts ...telemetry.MetricOption) telemetry.Metric {
	return m.newFloat64Metric(name, description, view.Sum(), opts)
}

// NewGauge creates a new Metric with an aggregation type of LastValue. That
// means that data collected by the new Metric will export only the last
// recorded value.
func (m metricSink) NewGauge(name, description string, opts ...telemetry.MetricOption) telemetry.Metric {
	return m.newFloat64Metric(name, description, view.LastValue(), opts)
}

// NewDistribution creates a new Metric with an aggregation type of Distribution.
// This means that the data collected by the Metric will be collected and
// exported as a histogram, with the specified bounds.
func (m metricSink) NewDistribution(name, description string, bounds []float64, opts ...telemetry.MetricOption) telemetry.Metric {
	return m.newFloat64Metric(name, description, view.Distribution(bounds...), opts)
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

// Insert will insert the provided value for the Label if not set.
func (l labelImpl) Insert(val string) telemetry.LabelValue {
	return tag.Insert(l.label, val)
}

// Update will update the Label with provided value if already set.
func (l labelImpl) Update(val string) telemetry.LabelValue {
	return tag.Update(l.label, val)
}

// Upsert will insert or replace the provided value for the Label.
func (l labelImpl) Upsert(val string) telemetry.LabelValue {
	return tag.Upsert(l.label, val)
}

// Delete will remove the Label's value.
func (l labelImpl) Delete() telemetry.LabelValue {
	return tag.Delete(l.label)
}

type float64Metric struct {
	*stats.Float64Measure

	tags []tag.Mutator
	view *view.View

	logger telemetry.Logger
}

type metricOptions struct {
	unit    string
	tagKeys []tag.Key
}

func (m metricSink) newFloat64Metric(name, desc string, a *view.Aggregation, opts []telemetry.MetricOption) *float64Metric {
	from := &telemetry.MetricOptions{Unit: "1"}
	for _, opt := range opts {
		opt(from)
	}
	o := &metricOptions{
		unit:    string(from.Unit),
		tagKeys: make([]tag.Key, 0, len(from.Labels)),
	}
	for _, l := range from.Labels {
		o.tagKeys = append(o.tagKeys, l.(*labelImpl).label)
	}
	measure := stats.Float64(name, desc, o.unit)
	newView := &view.View{Measure: measure, TagKeys: o.tagKeys, Aggregation: a}
	err := view.Register(newView)
	if err != nil && m.logger != nil {
		m.logger.Error("unable to register metric view", err)
	}

	return &float64Metric{
		measure,
		make([]tag.Mutator, 0),
		newView,
		m.logger,
	}
}

// Increment records a value of 1 for the current Metric.
// For Sums, this is equivalent to adding 1 to the current value.
// For Gauges, this is equivalent to setting the value to 1.
// For Distributions, this is equivalent to making an observation of value 1.
func (f *float64Metric) Increment() {
	f.Record(1)
}

// Decrement records a value of -1 for the current Metric.
// For Sums, this is equivalent to subtracting -1 to the current value.
// For Gauges, this is equivalent to setting the value to -1.
// For Distributions, this is equivalent to making an observation of value -1.
func (f *float64Metric) Decrement() {
	f.Record(-1)
}

// Name returns the name value of a Metric.
func (f *float64Metric) Name() string {
	return f.Float64Measure.Name()
}

// Record makes an observation of the provided value for the given Metric.
// LabelValues added through With will be processed in sequence.
func (f *float64Metric) Record(value float64) {
	err := stats.RecordWithTags(context.Background(), f.tags, f.M(value))
	if err != nil && f.logger != nil {
		f.logger.Error("unable to record with tags", err,
			"tags", fmt.Sprintf("%+v", f.tags))
	}
}

// RecordContext makes an observation of the provided value for the given
// Metric.
// If LabelValues for registered Labels are found in context, they will be
// processed in sequence, after which the LabelValues added through With
// are handled.
func (f *float64Metric) RecordContext(ctx context.Context, value float64) {
	err := stats.RecordWithTags(ctx, f.tags, f.M(value))
	if err != nil && f.logger != nil {
		f.logger.Error("unable to record with tags", err,
			"tags", fmt.Sprintf("%+v", f.tags))
	}
}

// With returns the Metric with the provided LabelValues encapsulated. This
// allows creating a set of pre-dimensioned data for recording purposes.
// It also allows a way to clear out LabelValues found in an attached
// Context if needing to sanitize.
func (f *float64Metric) With(labelValues ...telemetry.LabelValue) telemetry.Metric {
	if len(labelValues) == 0 {
		return f
	}
	tags := make([]tag.Mutator, len(f.tags), len(f.tags)+len(labelValues))
	copy(tags, f.tags)
	for _, tagValue := range labelValues {
		tags = append(tags, tagValue.(tag.Mutator))
	}
	return &float64Metric{f.Float64Measure, tags, f.view, f.logger}
}
