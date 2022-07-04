package eth

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"sync"
	"time"

	"capnproto.org/go/capnp/v3"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/ethconfig"
	"github.com/ethereum/go-ethereum/eth/filters"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/node"
	"github.com/libp2p/go-libp2p"
	libp2pquic "github.com/libp2p/go-libp2p-quic-transport"
	"github.com/thejerf/suture/v4"

	ethlog "github.com/ethereum/go-ethereum/log"
	log "github.com/lthibault/log"
	"github.com/sirupsen/logrus"

	"github.com/wetware/casm/pkg/boot/socket"
	bootutil "github.com/wetware/casm/pkg/boot/util"
	"github.com/wetware/ww/pkg/client"
	"github.com/wetware/ww/pkg/vat"

	"github.com/blocknative/bn/pkg/ethereum"
	serviceutil "github.com/blocknative/bn/pkg/util/service"
)

// wetware side-bus, which streams capnp-encoded events over pubsub.
type wetware struct {
	Backend   ethapi.Backend
	ChainID   *big.Int
	NetworkID uint64
	Enode     string
	Boot      string

	// stateful fields

	ctx    context.Context
	cancel context.CancelFunc

	d client.Dialer

	err <-chan error

	once sync.Once
	log  log.Logger
}

func newWetware(eth *Ethereum, config *ethconfig.Config) *wetware {
	if config.WwDiscover == "" {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	println("networkid", eth.networkID)
	return &wetware{
		Backend:   eth.APIBackend,
		ChainID:   eth.blockchain.Config().ChainID,
		NetworkID: eth.networkID,
		Enode:     eth.p2pServer.NodeInfo().Enode,
		Boot:      config.WwDiscover,

		ctx:    ctx,
		cancel: cancel,

		d: client.Dialer{
			Vat: vat.Network{
				NS: config.WwNamespace,
			},
		},
	}
}

func (ww *wetware) String() string { return "wetware" }

func (ww *wetware) Bind(n interface{ RegisterLifecycle(node.Lifecycle) }) {
	if ww != nil {
		for _, h := range []node.Lifecycle{
			ww.host(),
			ww.boot(),
			ww,
		} {
			n.RegisterLifecycle(h)
		}
	}
}

func (ww *wetware) Log() log.Logger {
	ww.once.Do(func() {
		ww.log = logger{Logger: ethlog.Root()}.
			With(ww.d.Vat).
			WithField("service", ww.String()).
			WithField("boot", ww.Boot)
	})

	return ww.log
}

func (ww *wetware) Start() error {
	s := suture.New("ww-bus", suture.Spec{
		EventHook: serviceutil.NewEventHook(ww.Log(), nil),
	})

	s.Add(ww)

	ww.err = s.ServeBackground(ww.ctx)

	return nil
}

func (ww *wetware) Stop() (err error) {
	if ww != nil {
		ww.cancel()
		if err = <-ww.err; err == context.Canceled {
			err = nil
		}
	}

	return
}

func (ww *wetware) Serve(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ww.Log().Info("started service")

	n, err := ww.d.Dial(ctx)
	if err != nil {
		return fmt.Errorf("dial cluster: %w", err)
	}
	defer n.Close()

	ww.Log().Info("joined cluster")

	return ww.serve(ctx, n)
}

func (ww *wetware) serve(ctx context.Context, n *client.Node) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	chErr := make(chan error, 1)

	go ww.publishPendingTx(ctx, n, chErr)
	go ww.publishConfirmedTx(ctx, n, chErr)
	go ww.publishDroppedTx(ctx, n, chErr)
	go ww.publishReplacementTx(ctx, n, chErr)

	go ww.publishConfirmedBlock(ctx, n, chErr)

	select {
	case err := <-chErr:
		log.Error(err)
		return err

	case <-ctx.Done():
		return ctx.Err()

	case <-n.Done():
		return fmt.Errorf("%s from cluster", client.ErrDisconnected)
	}
}

func (ww *wetware) publishPendingTx(ctx context.Context, n *client.Node, chErr chan error) {
	events := filters.NewEventSystem(ww.Backend, true)

	topic := network(ww.NetworkID) + ".transaction.pending"

	p := publisher{Topic: n.Join(ctx, topic)}
	defer p.Release()

	log := ww.Log().With(p)
	log.Info("joined topic")

	hs := make(chan []common.Hash, 16)
	defer close(hs)

	sub := events.SubscribePendingTxs(hs)
	defer sub.Unsubscribe()

	for {
		select {
		case batch := <-hs:
			for _, h := range batch {
				ethTx := ww.Backend.GetPoolTransaction(h)
				if ethTx == nil {
					log.WithField("tx", h.String()).Warn("transaction not in mempool")
					continue
				}

				var tx ethereum.Transaction

				if err := tx.FromEthTx(ethTx); err != nil {
					log.Warn(err.Error())
					returnError(err, chErr)
					return
				}

				evt, err := ww.newEvtTxDetected(tx)
				if err != nil {
					returnError(err, chErr)
					continue
				}

				evt.SetStatus(ethereum.STATUS_PENDING, "")

				if err := p.Publish(ctx, evt.Message()); err != nil {
					returnError(err, chErr)
					return
				}
			}

			log.WithField("len", len(batch)).Info("published pending transactions")

		case err := <-sub.Err():
			returnError(err, chErr)
			return

		case <-ctx.Done():
			returnError(ctx.Err(), chErr)
			return
		}
	}
}

func (ww *wetware) publishConfirmedTx(ctx context.Context, n *client.Node, chErr chan error) {
	// keep outside of the worker function so that the supervisor can cath any panics
	events := filters.NewEventSystem(ww.Backend, true)

	topic := network(ww.NetworkID) + ".transaction.confirmed"

	p := publisher{Topic: n.Join(ctx, topic)}
	defer p.Release()

	log := ww.Log().With(p)
	log.Info("joined topic")

	hs := make(chan *types.Header, 16)
	defer close(hs)

	sub := events.SubscribeNewHeads(hs)
	defer sub.Unsubscribe()

	for {
		select {
		case header := <-hs:
			block, err := ww.Backend.BlockByHash(ctx, header.Hash())
			if err != nil {
				log.WithField("block", header.Hash().String()).Warn(err)
				continue
			}

			for _, ethTx := range block.Transactions() {
				var tx ethereum.Transaction

				if err := tx.FromEthTx(ethTx); err != nil {
					returnError(err, chErr)
					return
				}

				evt, err := ww.newEvtTxDetected(tx)
				if err != nil {
					returnError(err, chErr)
					return
				}

				evt.SetStatus(ethereum.STATUS_CONFIRMED, "")

				if err := p.Publish(ctx, evt.Message()); err != nil {
					returnError(err, chErr)
					return
				}
			}

			log.WithField("len", len(block.Transactions())).Info("published confirmed transactions")

		case err := <-sub.Err():
			returnError(err, chErr)
			return

		case <-ctx.Done():
			returnError(ctx.Err(), chErr)
			return
		}
	}
}

func (ww *wetware) publishDroppedTx(ctx context.Context, n *client.Node, chErr chan error) {
	topic := network(ww.NetworkID) + ".transaction.dropped"

	p := publisher{Topic: n.Join(ctx, topic)}
	defer p.Release()

	log := ww.Log().With(p)
	log.Info("joined topic")

	ds := make(chan core.DropTxsEvent, 16)
	defer close(ds)

	sub := ww.Backend.SubscribeDropTxsEvent(ds)
	defer sub.Unsubscribe()

	for {
		select {
		case batch := <-ds:
			if batch.Replacement != nil {
				continue
			}

			for _, ethTx := range batch.Txs {
				var tx ethereum.Transaction

				if err := tx.FromEthTx(ethTx); err != nil {
					returnError(err, chErr)
					return
				}

				evt, err := ww.newEvtTxDetected(tx)
				if err != nil {
					returnError(err, chErr)
					return
				}

				evt.SetStatus(ethereum.STATUS_DROPPED, batch.Reason)

				if err := p.Publish(ctx, evt.Message()); err != nil {
					returnError(err, chErr)
					return
				}
			}

			log.WithField("len", len(batch.Txs)).Info("published dropped transactions")

		case err := <-sub.Err():
			returnError(err, chErr)
			return

		case <-ctx.Done():
			returnError(ctx.Err(), chErr)
			return
		}
	}
}

func (ww *wetware) publishReplacementTx(ctx context.Context, n *client.Node, chErr chan error) {
	topic := network(ww.NetworkID) + ".transaction.replaced"

	p := publisher{Topic: n.Join(ctx, topic)}
	defer p.Release()

	log := ww.Log().With(p)
	log.Info("joined topic")

	ds := make(chan core.DropTxsEvent, 16)
	defer close(ds)

	sub := ww.Backend.SubscribeDropTxsEvent(ds)
	defer sub.Unsubscribe()

	for {
		select {
		case batch := <-ds:
			if batch.Replacement == nil {
				continue
			}

			var tx ethereum.Transaction

			if err := tx.FromEthTx(batch.Replacement); err != nil {
				returnError(err, chErr)
				return
			}

			evt, err := ww.newEvtTxDetected(tx)
			if err != nil {
				returnError(err, chErr)
				return
			}

			evt.SetStatus(ethereum.STATUS_REPLACED, "")

			if err := p.Publish(ctx, evt.Message()); err != nil {
				returnError(err, chErr)
				return
			}

			log.WithField("len", 1).Info("published replacement transactions")

		case err := <-sub.Err():
			returnError(err, chErr)
			return

		case <-ctx.Done():
			returnError(ctx.Err(), chErr)
			return
		}
	}
}

func network(nid uint64) string {
	var network string
	switch nid {
	case 1:
		network = "main"
	case 3:
		network = "ropsten"
	case 4:
		network = "rinkeby"
	case 5:
		network = "goerli"
	case 137:
		network = "polygon"
	case 1337802:
		network = "kiln"
	default:
		panic(fmt.Sprintf("unrecognized network ID %d", nid))
	}

	return fmt.Sprintf("bn.monitor.ethereum.%s", network)
}

func (ww *wetware) newEvtTxDetected(tx ethereum.Transaction) (ev ethereum.EvtTxDetected, err error) {
	if err = ev.Alloc(capnp.SingleSegment(nil)); err != nil {
		return ev, err
	}

	ev.SetTimestamp(time.Now())

	if err = ev.SetTransaction(tx); err != nil {
		return ev, err
	}

	// TODO: set region

	if err = ev.SetPeer(ww.Enode, ww.d.Vat.Host.ID().String()); err != nil {
		return ev, err
	}
	return
}

func returnError(err error, chErr chan error) {
	select {
	case chErr <- err:
	default:
	}
}

func (ww *wetware) publishConfirmedBlock(ctx context.Context, n *client.Node, chErr chan error) {
	// keep outside of the worker function so that the supervisor can cath any panics
	events := filters.NewEventSystem(ww.Backend, true)

	topic := network(ww.NetworkID) + ".block.confirmed"

	p := publisher{Topic: n.Join(ctx, topic)}
	defer p.Release()

	log := ww.Log().With(p)
	log.Info("joined topic")

	hs := make(chan *types.Header, 16)
	defer close(hs)

	sub := events.SubscribeNewHeads(hs)
	defer sub.Unsubscribe()

	for {
		select {
		case header := <-hs:

			var hd ethereum.Header

			if err := hd.FromEthHeader(header); err != nil {
				returnError(err, chErr)
				return
			}

			if err := p.Publish(ctx, hd.Message()); err != nil {
				returnError(err, chErr)
				return
			}

			log.WithField("len", 1).Info("published confirmed blocks")

		case err := <-sub.Err():
			returnError(err, chErr)
			return

		case <-ctx.Done():
			returnError(ctx.Err(), chErr)
			return
		}
	}
}

type publisher struct{ client.Topic }

func (e publisher) Publish(ctx context.Context, m *capnp.Message) error {
	b, err := m.MarshalPacked()
	if err != nil {
		return err
	}

	return e.Topic.Publish(ctx, b)
}

// ww lifecycle

func (ww *wetware) host() *hook {
	return &hook{
		start: func() (err error) {
			ww.d.Vat.Host, err = libp2p.New(
				libp2p.NoTransports,
				libp2p.NoListenAddrs,
				libp2p.Transport(libp2pquic.NewTransport))
			return
		},
		stop: func() error { return ww.d.Vat.Host.Close() },
	}
}

func (ww *wetware) boot() *hook {
	return &hook{
		start: func() (err error) {
			ww.d.Boot, err = bootutil.DialString(ww.d.Vat.Host, ww.Boot,
				socket.WithLogger(ww.Log()))
			return
		},
		stop: func() (err error) {
			if c, ok := ww.d.Boot.(io.Closer); ok {
				err = c.Close()
			}
			return
		},
	}
}

type hook struct {
	start, stop func() error
}

func (h *hook) Start() (err error) { return h.call(h.start) }
func (h *hook) Stop() (err error)  { return h.call(h.stop) }

func (h *hook) call(f func() error) (err error) {
	if h != nil && f != nil {
		err = f()
	}

	return
}

// // Flow limiter

// type limiter semaphore.Weighted

// func (l *limiter) StartMessage(ctx context.Context, _ uint64) (gotResponse func(), err error) {
// 	if err = (*semaphore.Weighted)(l).Acquire(ctx, 1); err == nil {
// 		gotResponse = func() { (*semaphore.Weighted)(l).Release(1) }
// 	}

// 	return
// }

// Logging adapter
var _ log.Logger = (*logger)(nil)

type logger struct{ ethlog.Logger }

func (l logger) Fatal(vs ...interface{}) {
	l.Logger.Crit(fmt.Sprintf("%s", vs[0]), vs[1:]...)
}

func (l logger) Fatalf(format string, vs ...interface{}) {
	l.Logger.Crit(fmt.Sprintf(format, vs...))
}

func (l logger) Fatalln(vs ...interface{}) {
	l.Logger.Crit(fmt.Sprintf("%s", vs[0]), vs[1:]...)
}

func (l logger) Trace(vs ...interface{}) {
	l.Logger.Trace(fmt.Sprintf("%s", vs[0]), vs[1:]...)
}

func (l logger) Tracef(format string, vs ...interface{}) {
	l.Logger.Trace(fmt.Sprintf(format, vs...))
}

func (l logger) Traceln(vs ...interface{}) {
	l.Logger.Trace(fmt.Sprintf("%s", vs[0]), vs[1:]...)
}

func (l logger) Debug(vs ...interface{}) {
	l.Logger.Debug(fmt.Sprintf("%s", vs[0]), vs[1:]...)
}

func (l logger) Debugf(format string, vs ...interface{}) {
	l.Logger.Debug(fmt.Sprintf(format, vs...))
}

func (l logger) Debugln(vs ...interface{}) {
	l.Logger.Debug(fmt.Sprintf("%s", vs[0]), vs[1:]...)
}

func (l logger) Info(vs ...interface{}) {
	l.Logger.Info(fmt.Sprintf("%s", vs[0]), vs[1:]...)
}

func (l logger) Infof(format string, vs ...interface{}) {
	l.Logger.Info(fmt.Sprintf(format, vs...))
}

func (l logger) Infoln(vs ...interface{}) {
	l.Logger.Info(fmt.Sprintf("%s", vs[0]), vs[1:]...)
}

func (l logger) Warn(vs ...interface{}) {
	l.Logger.Warn(fmt.Sprintf("%s", vs[0]), vs[1:]...)
}

func (l logger) Warnf(format string, vs ...interface{}) {
	l.Logger.Warn(fmt.Sprintf(format, vs...))
}

func (l logger) Warnln(vs ...interface{}) {
	l.Logger.Warn(fmt.Sprintf("%s", vs[0]), vs[1:]...)
}

func (l logger) Error(vs ...interface{}) {
	l.Logger.Error(fmt.Sprintf("%s", vs[0]), vs[1:]...)
}

func (l logger) Errorf(format string, vs ...interface{}) {
	l.Logger.Error(fmt.Sprintf(format, vs...))
}

func (l logger) Errorln(vs ...interface{}) {
	l.Logger.Error(fmt.Sprintf("%s", vs[0]), vs[1:]...)
}

func (l logger) With(vs log.Loggable) log.Logger {
	m := vs.Loggable()
	ctx := make([]interface{}, 0, len(m))
	for k, v := range m {
		ctx = append(ctx, k)
		ctx = append(ctx, v)
	}

	return logger{
		Logger: l.Logger.New(ctx...),
	}
}

func (l logger) WithError(err error) log.Logger {
	return l.WithField("error", err)
}

func (l logger) WithField(name string, value interface{}) log.Logger {
	return logger{l.Logger.New(name, value)}
}

func (l logger) WithFields(fs logrus.Fields) log.Logger {
	return l.With(log.F(fs))
}
