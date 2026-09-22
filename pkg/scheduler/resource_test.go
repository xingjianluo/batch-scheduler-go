package scheduler

import "testing"

func TestResourceArithmetic(t *testing.T) {
	r := Resource{CPU: 4, Memory: 1024, GPU: 1}
	x := Resource{CPU: 2, Memory: 512, GPU: 1}
	if got := r.Add(x); got != (Resource{6, 1536, 2}) {
		t.Fatalf("Add = %+v", got)
	}
	if got := r.Sub(x); got != (Resource{2, 512, 0}) {
		t.Fatalf("Sub = %+v", got)
	}
	if !r.Fits(x) || !r.Fits(r) {
		t.Fatal("Fits basic failed")
	}
	if r.Fits(Resource{CPU: 5, Memory: 1, GPU: 0}) {
		t.Fatal("Fits should reject CPU overflow")
	}
	if r.Fits(Resource{CPU: 1, Memory: 1, GPU: 2}) {
		t.Fatal("Fits should reject GPU overflow")
	}
}
