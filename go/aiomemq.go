package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"unicode/utf8"
)

const defaultPort = 7000
const defaultCacheSize = 100

// Outbound is an unbounded, never-dropping message queue for a single client,
// drained by a dedicated writer goroutine. This mirrors the Rust reference's
// mpsc::unbounded_channel: producers (handleSend on other connections) never
// block and never drop, and there is no risk of the cross-delivery deadlock
// that a bounded blocking channel would create.
type Outbound struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queue  [][]byte
	closed bool
	done   chan struct{}
}

func newOutbound() *Outbound {
	o := &Outbound{done: make(chan struct{})}
	o.cond = sync.NewCond(&o.mu)
	return o
}

// push appends a message to the queue. It never blocks and never drops.
// Safe to call with the SharedState lock held: it only takes the Outbound
// lock, so the lock order is always state -> outbound (no cycle).
func (o *Outbound) push(msg []byte) {
	o.mu.Lock()
	if !o.closed {
		o.queue = append(o.queue, msg)
		o.cond.Signal()
	}
	o.mu.Unlock()
}

// close signals the writer goroutine to flush what remains and exit.
func (o *Outbound) close() {
	o.mu.Lock()
	o.closed = true
	o.cond.Broadcast()
	o.mu.Unlock()
}

// run owns the bufio.Writer: it is the only goroutine that writes to the
// socket, so command responses and delivered messages never race. It drains
// the queue in batches and exits once closed and empty, or on write error.
func (o *Outbound) run(w *bufio.Writer) {
	defer close(o.done)
	for {
		o.mu.Lock()
		for len(o.queue) == 0 && !o.closed {
			o.cond.Wait()
		}
		if len(o.queue) == 0 && o.closed {
			o.mu.Unlock()
			return
		}
		batch := o.queue
		o.queue = nil
		o.mu.Unlock()

		for _, msg := range batch {
			if _, err := w.Write(msg); err != nil {
				return
			}
			if _, err := w.Write([]byte("\r\n")); err != nil {
				return
			}
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}

type Topic struct {
	subscribers map[int64]*Outbound
	cache       []map[string]interface{}
	nextIndex   int64
}

func newTopic() *Topic {
	return &Topic{
		subscribers: make(map[int64]*Outbound),
	}
}

type SharedState struct {
	mu        sync.Mutex
	topics    map[string]*Topic
	cacheSize int
	nextID    int64
}

func newSharedState(cacheSize int) *SharedState {
	return &SharedState{
		topics:    make(map[string]*Topic),
		cacheSize: cacheSize,
	}
}

func (s *SharedState) allocID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextID
	s.nextID++
	return id
}

type Session struct {
	id               int64
	state            *SharedState
	subscribedTopics []string
	out              *Outbound
}

func newSession(state *SharedState) *Session {
	return &Session{
		id:    state.allocID(),
		state: state,
		out:   newOutbound(),
	}
}

func (sess *Session) cleanup() {
	sess.state.mu.Lock()
	defer sess.state.mu.Unlock()
	for _, t := range sess.subscribedTopics {
		if topic, ok := sess.state.topics[t]; ok {
			delete(topic.subscribers, sess.id)
		}
	}
}

func (sess *Session) pushJSON(v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	sess.out.push(data)
}

func (sess *Session) pushSuccess() {
	sess.pushJSON(map[string]interface{}{"success": true})
}

func (sess *Session) pushFailure(reason string) {
	sess.pushJSON(map[string]interface{}{"success": false, "reason": reason})
}

func validateSubscribe(obj map[string]interface{}) bool {
	allowed := map[string]bool{"command": true, "topic": true, "last_seen": true, "cache": true}
	for k := range obj {
		if !allowed[k] {
			return false
		}
	}
	if _, ok := obj["command"].(string); !ok {
		return false
	}
	if _, ok := obj["topic"].(string); !ok {
		return false
	}
	if v, ok := obj["last_seen"]; ok {
		f, ok := v.(float64)
		if !ok {
			return false
		}
		if f != float64(int64(f)) {
			return false
		}
	}
	if v, ok := obj["cache"]; ok {
		if _, ok := v.(bool); !ok {
			return false
		}
	}
	return true
}

func validateUnsubscribe(obj map[string]interface{}) bool {
	if len(obj) != 2 {
		return false
	}
	allowed := map[string]bool{"command": true, "topic": true}
	for k := range obj {
		if !allowed[k] {
			return false
		}
	}
	if _, ok := obj["command"].(string); !ok {
		return false
	}
	if _, ok := obj["topic"].(string); !ok {
		return false
	}
	return true
}

func validateSend(obj map[string]interface{}) bool {
	allowed := map[string]bool{"command": true, "topic": true, "msg": true, "delivery": true, "cache": true}
	for k := range obj {
		if !allowed[k] {
			return false
		}
	}
	if _, ok := obj["command"].(string); !ok {
		return false
	}
	if _, ok := obj["topic"].(string); !ok {
		return false
	}
	if _, ok := obj["msg"].(string); !ok {
		return false
	}
	delivery, ok := obj["delivery"].(string)
	if !ok || (delivery != "all" && delivery != "one") {
		return false
	}
	if v, ok := obj["cache"]; ok {
		if _, ok := v.(bool); !ok {
			return false
		}
	}
	return true
}

func verifyCommand(obj map[string]interface{}) bool {
	cmd, ok := obj["command"].(string)
	if !ok {
		return false
	}
	switch cmd {
	case "subscribe":
		return validateSubscribe(obj)
	case "unsubscribe":
		return validateUnsubscribe(obj)
	case "send":
		return validateSend(obj)
	default:
		return false
	}
}

func (sess *Session) handleSubscribe(obj map[string]interface{}) {
	topicName := obj["topic"].(string)
	lastSeen := int64(-1)
	if v, ok := obj["last_seen"]; ok {
		lastSeen = int64(v.(float64))
	}
	wantCache := true
	if v, ok := obj["cache"]; ok {
		wantCache = v.(bool)
	}

	sess.state.mu.Lock()
	topic, exists := sess.state.topics[topicName]
	if !exists {
		topic = newTopic()
		sess.state.topics[topicName] = topic
	}
	topic.subscribers[sess.id] = sess.out
	sess.subscribedTopics = append(sess.subscribedTopics, topicName)

	// Enqueue the success response and any cached replay while holding the
	// state lock. This guarantees that a concurrent send (which also takes
	// the state lock) is ordered either fully before or fully after this
	// block, so a newly delivered message can never jump ahead of the
	// cached replay for this subscriber.
	sess.pushSuccess()
	if wantCache {
		var newCache []map[string]interface{}
		for _, m := range topic.cache {
			idx := int64(m["index"].(float64))
			if idx > lastSeen {
				sess.pushJSON(m)
			}
			delivery, _ := m["delivery"].(string)
			if idx <= lastSeen || delivery == "all" {
				newCache = append(newCache, m)
			}
		}
		topic.cache = newCache
	}
	sess.state.mu.Unlock()
}

func (sess *Session) handleUnsubscribe(obj map[string]interface{}) {
	topicName := obj["topic"].(string)

	sess.state.mu.Lock()
	if topic, ok := sess.state.topics[topicName]; ok {
		delete(topic.subscribers, sess.id)
	}
	sess.state.mu.Unlock()

	var newTopics []string
	for _, t := range sess.subscribedTopics {
		if t != topicName {
			newTopics = append(newTopics, t)
		}
	}
	sess.subscribedTopics = newTopics

	sess.pushSuccess()
}

func (sess *Session) handleSend(obj map[string]interface{}) {
	topicName := obj["topic"].(string)
	delivery := obj["delivery"].(string)
	doCache := true
	if v, ok := obj["cache"]; ok {
		doCache = v.(bool)
	}

	sess.state.mu.Lock()
	topic, exists := sess.state.topics[topicName]
	if !exists {
		topic = newTopic()
		sess.state.topics[topicName] = topic
	}

	obj["index"] = float64(topic.nextIndex)
	topic.nextIndex++

	msgBytes, _ := json.Marshal(obj)

	// Deliver under the state lock. push() never blocks (unbounded queue),
	// so there is no reason to release the lock first, and holding it keeps
	// per-topic delivery order well defined. The same msgBytes is shared
	// across subscribers; it is never mutated, so sharing is safe.
	if delivery == "all" {
		for _, out := range topic.subscribers {
			out.push(msgBytes)
		}
	} else {
		// delivery == "one": pick one random subscriber if any.
		if len(topic.subscribers) > 0 {
			keys := make([]int64, 0, len(topic.subscribers))
			for k := range topic.subscribers {
				keys = append(keys, k)
			}
			chosen := keys[rand.Intn(len(keys))]
			topic.subscribers[chosen].push(msgBytes)
			doCache = false
		}
	}

	if doCache {
		topic.cache = append(topic.cache, obj)
		if len(topic.cache) > sess.state.cacheSize {
			topic.cache = topic.cache[1:]
		}
	}
	sess.state.mu.Unlock()

	sess.pushSuccess()
}

// processLine returns false when the client should be disconnected.
func (sess *Session) processLine(lineBytes []byte) bool {
	// Strip trailing \r\n (ReadBytes includes the delimiter).
	for len(lineBytes) > 0 && (lineBytes[len(lineBytes)-1] == '\r' || lineBytes[len(lineBytes)-1] == '\n') {
		lineBytes = lineBytes[:len(lineBytes)-1]
	}

	if string(lineBytes) == "quit" {
		return false
	}
	if len(lineBytes) == 0 {
		return true
	}

	if !utf8.Valid(lineBytes) {
		sess.pushFailure("Could not decode input as UTF-8")
		return true
	}

	var obj map[string]interface{}
	if err := json.Unmarshal(lineBytes, &obj); err != nil {
		sess.pushFailure("Could not parse json")
		return true
	}

	if !verifyCommand(obj) {
		sess.pushFailure("Malformed json message")
		return true
	}

	switch obj["command"].(string) {
	case "subscribe":
		sess.handleSubscribe(obj)
	case "unsubscribe":
		sess.handleUnsubscribe(obj)
	case "send":
		sess.handleSend(obj)
	}
	return true
}

func handleClient(conn net.Conn, state *SharedState) {
	sess := newSession(state)
	w := bufio.NewWriter(conn)
	go sess.out.run(w)

	defer func() {
		sess.cleanup()  // remove from topics: no further deliveries enqueued
		sess.out.close() // let the writer flush what remains and exit
		<-sess.out.done  // wait for the writer before closing the socket
		conn.Close()
	}()

	reader := bufio.NewReaderSize(conn, 128*1024)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if !sess.processLine(line) {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func main() {
	port := defaultPort
	cacheSize := defaultCacheSize

	args := os.Args[1:]
	if len(args) >= 1 {
		if p, err := strconv.Atoi(args[0]); err == nil {
			port = p
		}
	}
	if len(args) >= 2 {
		if c, err := strconv.Atoi(args[1]); err == nil {
			cacheSize = c
		}
	}

	state := newSharedState(cacheSize)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to listen: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Fprintf(os.Stderr, "Listening on %s\n", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Accept error: %v\n", err)
			continue
		}
		go handleClient(conn, state)
	}
}
