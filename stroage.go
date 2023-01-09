package go_raft

const (
	keyCurrentTerm       = "CurrentTerm"
	keyLastVoteTerm      = "LastVoteTerm"
	keyLastVoteCandidate = "LastVoteCandidate"
)

// KVStorage 提供稳定存储的抽象
type KVStorage interface {
	// Get 用于存储日志
	Get(key string) (val string, err error)
	// Set 用于存储日志
	Set(key string, val string) error

	// SetUint64 用于存储任期
	SetUint64(key string, val uint64) error
	// GetUint64 用于返回任期
	GetUint64(key string) (uint64, error)
}
