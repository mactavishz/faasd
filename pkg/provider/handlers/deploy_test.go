package handlers

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/containerd/containerd/oci"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/openfaas/faas-provider/types"
)

func Test_BuildLabels_WithAnnotations(t *testing.T) {
	// Test each combination of nil/non-nil annotation + label
	tables := []struct {
		name       string
		label      map[string]string
		annotation map[string]string
		result     map[string]string
	}{
		{"Empty label and annotations returns empty table map", nil, nil, map[string]string{}},
		{
			"Label with empty annotation returns valid map",
			map[string]string{"L1": "V1"},
			nil,
			map[string]string{"L1": "V1"}},
		{
			"Annotation with empty label returns valid map",
			nil,
			map[string]string{"A1": "V2"},
			map[string]string{fmt.Sprintf("%sA1", annotationLabelPrefix): "V2"}},
		{
			"Label and annotation provided returns valid combined map",
			map[string]string{"L1": "V1"},
			map[string]string{"A1": "V2"},
			map[string]string{
				"L1": "V1",
				fmt.Sprintf("%sA1", annotationLabelPrefix): "V2",
			},
		},
	}

	for _, tc := range tables {

		t.Run(tc.name, func(t *testing.T) {
			request := &types.FunctionDeployment{
				Labels:      &tc.label,
				Annotations: &tc.annotation,
			}

			val, err := buildLabels(request)

			if err != nil {
				t.Fatalf("want: no error got: %v", err)
			}

			if !reflect.DeepEqual(val, tc.result) {
				t.Errorf("Want: %s, got: %s", val, tc.result)
			}
		})
	}
}

func Test_BuildLabels_WithAnnotationCollision(t *testing.T) {
	request := &types.FunctionDeployment{
		Labels: &map[string]string{
			"function_name": "echo",
			fmt.Sprintf("%scurrent-time", annotationLabelPrefix): "Wed 25 Jul 06:41:43 BST 2018",
		},
		Annotations: &map[string]string{"current-time": "Wed 25 Jul 06:41:43 BST 2018"},
	}

	val, err := buildLabels(request)

	if err == nil {
		t.Errorf("Expected an error, got %d values", len(val))
	}

}

func Test_parseCPUNano(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    int64
		wantErr bool
	}{
		{name: "millicores", in: "50m", want: 50_000_000},
		{name: "decimal", in: "0.0625", want: 62_500_000},
		{name: "cores", in: "2", want: 2_000_000_000},
		{name: "trimmed", in: " 500m ", want: 500_000_000},
		{name: "invalid", in: "abc", wantErr: true},
		{name: "negative", in: "-1", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCPUNano(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("want no error, got: %v", err)
			}

			if got != tc.want {
				t.Fatalf("want %d, got %d", tc.want, got)
			}
		})
	}
}

func Test_buildCPULimit(t *testing.T) {
	tests := []struct {
		name       string
		in         string
		wantQuota  int64
		wantPeriod uint64
		wantNil    bool
		wantErr    bool
	}{
		{name: "50m", in: "50m", wantQuota: 5000, wantPeriod: defaultCPUCFSPeriodMicrosecond},
		{name: "one-core", in: "1", wantQuota: 100000, wantPeriod: defaultCPUCFSPeriodMicrosecond},
		{name: "decimal", in: "0.5", wantQuota: 50000, wantPeriod: defaultCPUCFSPeriodMicrosecond},
		{name: "zero", in: "0", wantNil: true},
		{name: "invalid", in: "nope", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildCPULimit(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("want no error, got: %v", err)
			}

			if tc.wantNil {
				if got != nil {
					t.Fatalf("want nil cpu limit, got: %#v", got)
				}
				return
			}

			if got == nil || got.Quota == nil || got.Period == nil {
				t.Fatalf("expected quota/period to be set, got: %#v", got)
			}

			if *got.Quota != tc.wantQuota {
				t.Fatalf("want quota %d, got %d", tc.wantQuota, *got.Quota)
			}

			if *got.Period != tc.wantPeriod {
				t.Fatalf("want period %d, got %d", tc.wantPeriod, *got.Period)
			}
		})
	}
}

func Test_withCPU(t *testing.T) {
	period := uint64(100000)
	quota := int64(5000)

	spec := &oci.Spec{}
	err := withCPU(&specs.LinuxCPU{Quota: &quota, Period: &period})(context.Background(), nil, nil, spec)
	if err != nil {
		t.Fatalf("want no error, got: %v", err)
	}

	if spec.Linux == nil || spec.Linux.Resources == nil || spec.Linux.Resources.CPU == nil {
		t.Fatalf("expected linux cpu resources to be initialized")
	}

	if spec.Linux.Resources.CPU.Quota == nil || *spec.Linux.Resources.CPU.Quota != quota {
		t.Fatalf("want quota %d, got %#v", quota, spec.Linux.Resources.CPU.Quota)
	}

	if spec.Linux.Resources.CPU.Period == nil || *spec.Linux.Resources.CPU.Period != period {
		t.Fatalf("want period %d, got %#v", period, spec.Linux.Resources.CPU.Period)
	}
}
