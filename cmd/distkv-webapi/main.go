// distkv-webapi exposes a small HTTP JSON API in front of DistKV for the demo UI.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rohithreddy/distkv/client"
)

func main() {
	cluster := os.Getenv("DISTKV_CLUSTER")
	if cluster == "" {
		cluster = "distkv-node1:7001,distkv-node2:7002,distkv-node3:7003"
	}
	addr := os.Getenv("LISTEN")
	if addr == "" {
		addr = ":8080"
	}

	addrs := splitAddrs(cluster)
	c := client.New(addrs)
	mux := http.NewServeMux()
	api := &api{c: c}

	mux.HandleFunc("GET /api/health", api.health)
	mux.HandleFunc("GET /api/kv/{key}", api.getKV)
	mux.HandleFunc("PUT /api/kv/{key}", api.putKV)
	mux.HandleFunc("DELETE /api/kv/{key}", api.deleteKV)
	mux.HandleFunc("GET /api/status", api.status)
	mux.HandleFunc("GET /api/members", api.listMembers)
	mux.HandleFunc("POST /api/members", api.addMember)
	mux.HandleFunc("DELETE /api/members/{id}", api.removeMember)

	log.Printf("distkv-webapi listening on %s (cluster=%v)", addr, addrs)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

type api struct {
	c *client.Client
}

func (a *api) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *api) getKV(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	val, found, err := a.c.Get(ctx, key)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "found": found, "value": string(val)})
}

func (a *api) putKV(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	value := body
	ct := r.Header.Get("Content-Type")
	if strings.Contains(ct, "application/json") {
		var payload struct {
			Value string `json:"value"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}
		value = []byte(payload.Value)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := a.c.Put(ctx, key, value); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "key": key})
}

func (a *api) deleteKV(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := a.c.Delete(ctx, key); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "key": key})
}

func (a *api) status(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	type nodeStatus struct {
		Addr         string `json:"addr"`
		NodeID       uint64 `json:"node_id,omitempty"`
		IsLeader     bool   `json:"is_leader"`
		LeaderID     uint64 `json:"leader_id,omitempty"`
		Term         uint64 `json:"term,omitempty"`
		CommitIndex  uint64 `json:"commit_index,omitempty"`
		AppliedIndex uint64 `json:"applied_index,omitempty"`
		Error        string `json:"error,omitempty"`
	}
	var nodes []nodeStatus
	for _, addr := range a.c.Addrs() {
		st, err := a.c.Status(ctx, addr)
		if err != nil {
			nodes = append(nodes, nodeStatus{Addr: addr, Error: err.Error()})
			continue
		}
		nodes = append(nodes, nodeStatus{
			Addr:         addr,
			NodeID:       st.NodeId,
			IsLeader:     st.IsLeader,
			LeaderID:     st.LeaderId,
			Term:         st.Term,
			CommitIndex:  st.CommitIndex,
			AppliedIndex: st.AppliedIndex,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": nodes})
}

func (a *api) listMembers(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	resp, err := a.c.ListMembers(ctx)
	if err != nil {
		writeErr(w, err)
		return
	}
	type member struct {
		ID       uint64 `json:"id"`
		RaftAddr string `json:"raft_addr"`
		Voting   bool   `json:"voting"`
	}
	out := make([]member, 0, len(resp.Members))
	for _, m := range resp.Members {
		out = append(out, member{ID: m.Id, RaftAddr: m.RaftAddr, Voting: m.Voting})
	}
	writeJSON(w, http.StatusOK, map[string]any{"members": out, "in_joint": resp.InJoint})
}

func (a *api) addMember(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID       uint64 `json:"id"`
		RaftAddr string `json:"raft_addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.ID == 0 || req.RaftAddr == "" {
		http.Error(w, "id and raft_addr required", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if err := a.c.AddMember(ctx, req.ID, req.RaftAddr); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *api) removeMember(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil || id == 0 {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if err := a.c.RemoveMember(ctx, id); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func splitAddrs(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	msg := err.Error()
	code := http.StatusBadGateway
	if strings.Contains(msg, "not leader") {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]string{"error": msg})
	fmt.Fprintf(os.Stderr, "api error: %v\n", err)
}
