package go_raft

import "testing"

func TestState(t *testing.T) {
	state := newState()
	t.Log(state.GetState())
	state.setState(Candidate)
	t.Log(state.GetState())
	state.setState(5)
	t.Log(state)

}
