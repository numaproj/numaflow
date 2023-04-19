package wmb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWMBChecker_ValidateHeadWMB(t *testing.T) {
	var (
		c     = NewWMBChecker(2)
		tests = []struct {
			name        string
			wmbList     []WMB
			wantCounter []int
			want        bool
		}{
			{
				name: "good",
				wmbList: []WMB{
					{
						Idle:      true,
						Offset:    0,
						Watermark: 1000,
					},
					{
						Idle:      true,
						Offset:    0,
						Watermark: 1000,
					},
				},
				wantCounter: []int{
					1, 0,
				},
				want: true,
			},
			{
				name: "diff_head_wmb",
				wmbList: []WMB{
					{
						Idle:      true,
						Offset:    0,
						Watermark: 1000,
					},
					{
						Idle:      true,
						Offset:    2, // diff head wmb, will return false
						Watermark: 3000,
					},
				},
				wantCounter: []int{
					1, 0,
				},
				want: false,
			},
			{
				name: "active_head_wmb_2",
				wmbList: []WMB{
					{
						Idle:      true,
						Offset:    0,
						Watermark: 1000,
					},
					{
						Idle:      false, // not idle, will return false
						Offset:    1,
						Watermark: 2000,
					},
				},
				wantCounter: []int{
					1, 0,
				},
				want: false,
			},
			{
				name: "active_head_wmb_1",
				wmbList: []WMB{
					{
						Idle:      false, // not idle, will return false
						Offset:    2,
						Watermark: 2000,
					},
				},
				wantCounter: []int{
					0,
				},
				want: false,
			},
			{
				name: "good_check_again",
				wmbList: []WMB{
					{
						Idle:      true,
						Offset:    3,
						Watermark: 4000,
					},
					{
						Idle:      true,
						Offset:    3,
						Watermark: 4000,
					},
				},
				wantCounter: []int{
					1, 0,
				},
				want: true,
			},
		}
	)
	for _, test := range tests {
		var result bool
		for i, w := range test.wmbList {
			result = c.ValidateHeadWMB(w)
			assert.Equal(t, test.wantCounter[i], c.GetCounter(), fmt.Sprintf("test [%s] failed: want %d, got %d", test.name, test.wantCounter[i], c.GetCounter()))
		}
		assert.Equal(t, test.want, result, fmt.Sprintf("test [%s] failed: want %t, got %t", test.name, test.want, result))
	}

}
