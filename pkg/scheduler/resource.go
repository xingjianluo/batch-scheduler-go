package scheduler

// Add 返回 r+x 的资源和。
func (r Resource) Add(x Resource) Resource {
	return Resource{CPU: r.CPU + x.CPU, Memory: r.Memory + x.Memory, GPU: r.GPU + x.GPU}
}

// Sub 返回 r-x（允许为负，调用方需保证语义）。
func (r Resource) Sub(x Resource) Resource {
	return Resource{CPU: r.CPU - x.CPU, Memory: r.Memory - x.Memory, GPU: r.GPU - x.GPU}
}

// Fits 判断 x 是否不超过 r（按维度比较）。任一维度超出即不满足。
func (r Resource) Fits(x Resource) bool {
	return x.CPU <= r.CPU && x.Memory <= r.Memory && x.GPU <= r.GPU
}

// FitsIn 判断需求 req 是否可放入 (used, limit) 区间内，
// 即 used+req 不超过 limit。
func fitsIn(used, limit, req Resource) bool {
	return limit.Fits(used.Add(req))
}
