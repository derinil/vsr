package main

import (
	"slices"
	"testing"
)

func TestBrokerPublishKeepsNewest(t *testing.T) {
	tests := []struct {
		name        string
		unsubscribe bool
		msgs        []string
		want        []string
	}{
		{name: "no messages", msgs: nil, want: nil},
		{name: "single message", msgs: []string{"a"}, want: []string{"a"}},
		{name: "slow reader gets newest only", msgs: []string{"a", "b", "c"}, want: []string{"c"}},
		{name: "unsubscribed gets nothing", unsubscribe: true, msgs: []string{"a"}, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b broker
			ch := b.subscribe()
			if tt.unsubscribe {
				b.unsubscribe(ch)
			}
			for _, msg := range tt.msgs {
				b.publish([]byte(msg))
			}
			var got []string
			for len(ch) > 0 {
				got = append(got, string(<-ch))
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("received %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBrokerSubscriberCount(t *testing.T) {
	var b broker
	a := b.subscribe()
	c := b.subscribe()
	if n := b.count(); n != 2 {
		t.Fatalf("count() = %d after two subscribes, want 2", n)
	}
	b.unsubscribe(a)
	b.unsubscribe(c)
	if n := b.count(); n != 0 {
		t.Errorf("count() = %d after unsubscribing all, want 0", n)
	}
}
