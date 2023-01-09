package go_raft

type ServerSuffrage int

const (
	Voter ServerSuffrage = iota
	NonVoter
)

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
	commend   configurationChangeCommend
	peer      ServerInfo
	pervIndex uint64
}

const (
	AddVoter configurationChangeCommend = iota + 1
	AddNonVoter
	DemoteVoter
	removeServer
)
