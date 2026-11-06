// example_test.go contains runnable examples shown in the package documentation.
package fluxgo_test

import (
	"errors"
	"fmt"

	"github.com/deaprima/fluxgo"
)

func ExampleNewCircuitBreaker() {
	cb := fluxgo.NewCircuitBreaker(fluxgo.Settings{
		Name:   "payment-service",
		Config: fluxgo.DefaultConfig(),
		OnStateChange: func(name string, from, to fluxgo.State) {
			fmt.Printf("%s: %s -> %s\n", name, from, to)
		},
	})

	result, err := cb.Execute(func() (interface{}, error) {
		return "charged", nil
	})
	if errors.Is(err, fluxgo.ErrOpenState) {
		fmt.Println("circuit is open, apply a fallback")
		return
	}
	fmt.Println(result, cb.State())
	// Output: charged closed
}

func ExampleCircuitBreaker_Snapshot() {
	cb := fluxgo.NewCircuitBreaker(fluxgo.Settings{
		Name:   "payment-service",
		Config: fluxgo.DefaultConfig(),
	})

	snap := cb.Snapshot()
	fmt.Println(snap.State, snap.ThetaT, snap.IsWarmingUp)
	// Output: closed 0.6 true
}
