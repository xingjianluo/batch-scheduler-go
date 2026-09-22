package scheduler

// pool 是进程内资源池，跟踪集群总量、全局已用量与各租户已用量。
// 注意：pool 自身不加锁，由 Scheduler 的互斥锁统一保护。
type pool struct {
	capacity   Resource
	used       Resource
	tenantUsed map[string]Resource
}

func newPool(capacity Resource) *pool {
	return &pool{
		capacity:   capacity,
		tenantUsed: make(map[string]Resource),
	}
}

// canAllocateFor 判断指定租户的 req 是否同时满足集群容量与租户配额。
func (p *pool) canAllocateFor(tenantID string, quota, req Resource) bool {
	return fitsIn(p.used, p.capacity, req) &&
		fitsIn(p.tenantUsage(tenantID), quota, req)
}

// allocate 在判定可行后实际记账分配。返回是否成功（防御性二次检查）。
func (p *pool) allocate(tenantID string, req Resource) bool {
	if !fitsIn(p.used, p.capacity, req) {
		return false
	}
	p.used = p.used.Add(req)
	p.tenantUsed[tenantID] = p.tenantUsed[tenantID].Add(req)
	return true
}

// release 归还资源。
func (p *pool) release(tenantID string, req Resource) {
	p.used = p.used.Sub(req)
	p.tenantUsed[tenantID] = p.tenantUsed[tenantID].Sub(req)
}

// usage 返回集群当前已用量。
func (p *pool) usage() Resource {
	return p.used
}

// tenantUsage 返回指定租户当前已用量。
func (p *pool) tenantUsage(tenantID string) Resource {
	return p.tenantUsed[tenantID]
}
