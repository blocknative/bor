package eth

import (
	"context"
	"sync/atomic"

	"capnproto.org/go/capnp/v3"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/wetware/ww/pkg/client"
	"golang.org/x/sync/errgroup"
)

var _ node.Lifecycle = (*wwService)(nil)

type wwService struct {
	log log.Logger
	d   *client.Dialer
	b   ethapi.Backend
	n   *client.Node
}

// Start is called after all services have been constructed and the networking
// layer was also initialized to spawn any goroutines required by the service.
func (ww *wwService) Start() (err error) {
	if ww.n, err = ww.d.Dial(context.TODO()); err == nil {
		go ww.serve(context.TODO())
	}

	return
}

// Stop terminates all goroutines belonging to the service, blocking until they
// are all terminated.
func (ww *wwService) Stop() error {
	return ww.n.Close()
}

func (ww *wwService) serve(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var g errgroup.Group

	for _, subscriber := range []func() error{
		ww.subscribeNewTxsEvent(ctx),
		ww.subscribeDropTxsEvent(ctx),
	} {
		g.Go(subscriber)
	}

	if err := g.Wait(); err != nil {
		ww.log.Crit("ww: %s", err)
	}
}

func (ww *wwService) subscribeDropTxsEvent(ctx context.Context) func() error {
	return func() error {
		ch := make(chan core.DropTxsEvent, 16)
		sub := ww.b.SubscribeDropTxsEvent(ch)
		defer sub.Unsubscribe()

		t := ww.n.Join(context.TODO(), "bn.monitor.polygon.tx_dropped")
		defer t.Release()

		var e tdn_eth.EvtTxsDropped
		for {
			select {
			case ev := <-ch:
				if err := e.BindEvent(ev); err != nil {
					ww.log.Warn("read EvtTxsDropped: %s", err)
					continue
				}

				if err := (publisher{t}).Publish(ctx, e.Message()); err != nil {
					ww.log.Warn("publish EvtTxsDropped: %s", err)
					continue
				}

			case err := <-sub.Err():
				return err

			case <-ww.n.Done():
				return nil
			}
		}
	}
}

func (ww *wwService) subscribeNewTxsEvent(ctx context.Context) func() error {
	return func() error {
		ch := make(chan core.NewTxsEvent, 16)
		sub := ww.b.SubscribeNewTxsEvent(ch)
		defer sub.Unsubscribe()

		t := ww.n.Join(context.TODO(), "bn.monitor.polygon.tx_new")
		defer t.Release()

		var e tdn_eth.EvtNewTxs
		for {
			select {
			case ev := <-ch:
				if err := e.BindEvent(ev); err != nil {
					ww.log.Warn("read EvtNewTxs: %s", err)
					continue
				}

				if err := (publisher{t}).Publish(ctx, e.Message()); err != nil {
					ww.log.Warn("publish EvtNewTxs: %s", err)
					continue
				}

			case err := <-sub.Err():
				return err

			case <-ww.n.Done():
				return nil
			}
		}
	}
}

type publisher struct{ t *client.Topic }

func (p publisher) Publish(ctx context.Context, m *capnp.Message) error {
	b, err := m.MarshalPacked()
	if err != nil {
		return err
	}

	return p.t.Publish(ctx, b)
}

type atomicNode atomic.Value

func (an *atomicNode) Close() error {
	return an.Load().Close()
}

func (an *atomicNode) Load() *client.Node {
	if v := (*atomic.Value)(an).Load(); v != nil {
		return v.(*client.Node)
	}

	return nil
}

func (an *atomicNode) Store(n *client.Node) {
	(*atomic.Value)(an).Store(n)
}
