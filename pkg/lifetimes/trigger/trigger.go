// Copyright (c) 2022 Fujitsu Limited

package trigger

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
)

type Trigger struct {
	startTime metav1.Time
	endTime   metav1.Time
}

func NewTrigger(trigger *lifetimesapi.Trigger) (*Trigger, error) {
	var startTime time.Time
	if trigger.StartTime == "" {
		startTime = time.Now()
	} else {
		var err error
		startTime, err = time.Parse(time.RFC3339, trigger.StartTime)
		if err != nil {
			return nil, err
		}
	}

	endTime, err := time.Parse(time.RFC3339, trigger.EndTime)
	if err != nil {
		return nil, err
	}

	return &Trigger{
		startTime: metav1.NewTime(startTime),
		endTime:   metav1.NewTime(endTime),
	}, nil
}

func (t *Trigger) GetExpiryEpoch() (time.Duration, error) {
	if time.Now().UTC().After(t.endTime.UTC()) {
		return time.Duration(0), fmt.Errorf("'EndTime' is before current time")
	}

	return t.endTime.UTC().Sub(time.Now().UTC()), nil
}

func (t *Trigger) GetEndTime() time.Time {
	return t.endTime.Time
}

func (t *Trigger) GetEndTimeString() string {
	return t.endTime.Format(time.RFC3339)
}

func (t *Trigger) UpdateEndTime(endTimeString string) error {
	endTime, err := time.Parse(time.RFC3339, endTimeString)
	if err != nil {
		return err
	}

	t.endTime = metav1.NewTime(endTime)

	return nil
}

func GetExpiryEpochFromEndTime(endTimeString string) (time.Duration, error) {
	endTime, err := time.Parse(time.RFC3339, endTimeString)
	if err != nil {
		return time.Duration(0), err
	}

	return endTime.UTC().Sub(time.Now().UTC()), nil
}
