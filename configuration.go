package go_raft

type ServerSuffrage int

const (
	Voter ServerSuffrage = iota
	NonVoter
)

type configuration struct {
	servers []ServerInfo
}
type configurations struct {
	commit      configuration
	latest      configuration
	commitIndex uint64
	latestIndex uint64
}

// ConfigurationStore provides an interface that can optionally be implemented by FSMs
// to store configuration updates made in the replicated log. In general this is only
// necessary for FSMs that mutate durable state directly instead of applying changes
// in memory and snapshotting periodically. By storing configuration changes, the
// persistent FSM state can behave as a complete snapshot, and be able to recover
// without an external snapshot just for persisting the raft configuration.
type ConfigurationStore interface {
	// ConfigurationStore is a superset of the FSM functionality
	LogFSM

	// StoreConfiguration is invoked once a log entry containing a configuration
	// change is committed. It takes the index at which the configuration was
	// written and the configuration value.
	StoreConfiguration(index uint64, configuration configuration)
}
type configurationChangeCommend uint64
type configurationChangeRequest struct {
	command   configurationChangeCommend
	peer      ServerInfo
	pervIndex uint64
}

const (
	AddVoter configurationChangeCommend = iota + 1
	AddNonVoter
	DemoteVoter
	removeServer
)

func (c *configuration) Clone() (copy configuration) {
	copy.servers = append(copy.servers, c.servers...)
	return
}
func (c *configurations) Clone() configurations {
	res := configurations{
		commit:      c.commit.Clone(),
		latest:      c.latest.Clone(),
		commitIndex: c.commitIndex,
		latestIndex: c.latestIndex,
	}
	return res
}
