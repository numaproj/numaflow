package rater

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

// Test the constructor function
func TestNewTimestampedProcessingTime(t *testing.T) {
	currentTime := time.Now().Unix()
	tpt := NewTimestampedProcessingTime(currentTime)

	if tpt.timestamp != currentTime {
		t.Errorf("Expected timestamp %v, got %v", currentTime, tpt.timestamp)
	}

	if len(tpt.podProcessingTime) != 0 {
		t.Errorf("Expected empty podProcessingTime map, got %v", tpt.podProcessingTime)
	}

	if tpt.lock == nil {
		t.Errorf("Expected non-nil lock")
	}
}

// Test the Update method
func TestUpdate(t *testing.T) {
	tpt := NewTimestampedProcessingTime(time.Now().Unix())
	podName := "Pod1"
	// Creating a new PodProcessingTime
	newPod := &PodProcessingTime{
		name:                podName,
		processingTimeSum:   2400000,
		processingTimeCount: 2,
	}

	tpt.Update(newPod)

	if len(tpt.podProcessingTime) != 1 {
		t.Fatalf("Expected exactly 1 entry in podProcessingTime, got %v", len(tpt.podProcessingTime))
	}

	expectedProcessingTime := 1.2 // ( (2400000/ 2 ) / 1000000
	if tpt.podProcessingTime[podName] != expectedProcessingTime {
		t.Errorf("Expected processing time %v, got %v", expectedProcessingTime, tpt.podProcessingTime[podName])
	}

	// Test updating with nil should not change anything
	beforeUpdate := tpt.PodProcessingTimeSnapshot()
	tpt.Update(nil)
	afterUpdate := tpt.PodProcessingTimeSnapshot()

	if len(beforeUpdate) != len(afterUpdate) {
		t.Fatalf("Update with nil changed the contents of the map")
	}
}

// Test the PodProcessingTimeSnapshot method
func TestPodProcessingTimeSnapshot(t *testing.T) {
	tpt := NewTimestampedProcessingTime(time.Now().Unix())
	tpt.podProcessingTime["Pod1"] = 1.5
	tpt.podProcessingTime["Pod2"] = 2.5

	snapshot := tpt.PodProcessingTimeSnapshot()

	if !reflect.DeepEqual(snapshot, tpt.podProcessingTime) {
		t.Errorf("Snapshot does not match the original map")
	}

	// Test that modifying the snapshot does not affect the original
	snapshot["Pod1"] = 10.0
	if tpt.podProcessingTime["Pod1"] == 10.0 {
		t.Errorf("Modifying snapshot affected the original map")
	}
}

// Test the String method
func TestString(t *testing.T) {
	tpt := NewTimestampedProcessingTime(time.Now().Unix())
	tpt.podProcessingTime["Pod1"] = 1.5

	expected := fmt.Sprintf("{timestamp: %d, podProcessingTime: map[Pod1:%v]}", tpt.timestamp, 1.5)
	result := tpt.String()
	if result != expected {
		t.Errorf("String() output is incorrect. Expected %v, got %v", expected, result)
	}
}
