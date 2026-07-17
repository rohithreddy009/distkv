import { FormEvent, useCallback, useEffect, useState } from "react";
import { api, Member, NodeStatus } from "./api";

export default function App() {
  const [key, setKey] = useState("hello");
  const [value, setValue] = useState("world");
  const [kvMsg, setKvMsg] = useState<string | null>(null);
  const [kvErr, setKvErr] = useState(false);
  const [busy, setBusy] = useState(false);

  const [nodes, setNodes] = useState<NodeStatus[]>([]);
  const [members, setMembers] = useState<Member[]>([]);
  const [inJoint, setInJoint] = useState(false);
  const [clusterMsg, setClusterMsg] = useState<string | null>(null);
  const [clusterErr, setClusterErr] = useState(false);

  const [newId, setNewId] = useState("4");
  const [newAddr, setNewAddr] = useState("distkv-node4:8004");

  const refreshCluster = useCallback(async () => {
    try {
      const [st, mem] = await Promise.all([api.status(), api.members()]);
      setNodes(st.nodes);
      setMembers(mem.members);
      setInJoint(mem.in_joint);
      setClusterErr(false);
      setClusterMsg(null);
    } catch (e) {
      setClusterErr(true);
      setClusterMsg(e instanceof Error ? e.message : String(e));
    }
  }, []);

  useEffect(() => {
    void refreshCluster();
    const t = setInterval(() => void refreshCluster(), 4000);
    return () => clearInterval(t);
  }, [refreshCluster]);

  async function runKV(fn: () => Promise<string | void>) {
    setBusy(true);
    setKvErr(false);
    try {
      const msg = await fn();
      if (msg !== undefined && msg !== null && msg !== "") {
        setKvMsg(msg);
      }
    } catch (e) {
      setKvErr(true);
      setKvMsg(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  function onPut(e: FormEvent) {
    e.preventDefault();
    void runKV(async () => {
      await api.put(key, value);
      return `put ${key} → ok`;
    });
  }

  function onGet(e: FormEvent) {
    e.preventDefault();
    void runKV(async () => {
      const r = await api.get(key);
      return r.found ? `get ${key} → ${r.value}` : `get ${key} → not found`;
    });
  }

  function onDelete(e: FormEvent) {
    e.preventDefault();
    void runKV(async () => {
      await api.del(key);
      return `delete ${key} → ok`;
    });
  }

  async function onAddMember(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    try {
      await api.addMember(Number(newId), newAddr);
      setClusterErr(false);
      setClusterMsg(`added member ${newId}`);
      await refreshCluster();
    } catch (err) {
      setClusterErr(true);
      setClusterMsg(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  async function onRemove(id: number) {
    setBusy(true);
    try {
      await api.removeMember(id);
      setClusterErr(false);
      setClusterMsg(`removed member ${id}`);
      await refreshCluster();
    } catch (err) {
      setClusterErr(true);
      setClusterMsg(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="shell">
      <header className="hero">
        <h1 className="brand">
          Dist<em>KV</em>
        </h1>
        <p className="tagline">
          A replicated key-value store you can poke live — write keys, watch
          leaders, grow or shrink the cluster without a restart.
        </p>
      </header>

      <section className="section">
        <h2>Keys</h2>
        <p className="lead">Put, get, or delete a value in the live cluster.</p>
        <form className="row" onSubmit={onPut}>
          <input
            aria-label="Key"
            placeholder="key"
            value={key}
            onChange={(e) => setKey(e.target.value)}
            required
          />
          <input
            aria-label="Value"
            placeholder="value"
            value={value}
            onChange={(e) => setValue(e.target.value)}
          />
          <button className="btn" type="submit" disabled={busy}>
            Put
          </button>
          <button
            className="btn secondary"
            type="button"
            disabled={busy}
            onClick={onGet}
          >
            Get
          </button>
          <button
            className="btn secondary"
            type="button"
            disabled={busy}
            onClick={onDelete}
          >
            Delete
          </button>
        </form>
        {kvMsg ? (
          <div className={`result${kvErr ? " error" : ""}`}>{kvMsg}</div>
        ) : null}
      </section>

      <section className="section">
        <h2>Cluster</h2>
        <p className="lead">Who is leader, and who is allowed to vote.</p>
        {inJoint ? (
          <div className="joint">Membership change in progress (joint config)</div>
        ) : null}
        <div className="nodes">
          {nodes.map((n) => (
            <div className="node" key={n.addr}>
              <span
                className={`pulse${n.error ? " err" : n.is_leader ? " live" : ""}`}
              />
              <div>
                <div>
                  node {n.node_id ?? "?"} · {n.addr}
                </div>
                <div className="meta">
                  {n.error
                    ? n.error
                    : `term ${n.term ?? 0} · commit ${n.commit_index ?? 0} · applied ${n.applied_index ?? 0}`}
                </div>
              </div>
              {n.is_leader ? <span className="badge">leader</span> : <span />}
            </div>
          ))}
        </div>

        <ul className="members" style={{ marginTop: "1.25rem" }}>
          {members.map((m) => (
            <li key={m.id}>
              <span>
                voter {m.id} · {m.raft_addr || "(no addr)"}
              </span>
              <button
                className="btn danger"
                type="button"
                disabled={busy || members.length <= 1}
                onClick={() => void onRemove(m.id)}
              >
                Remove
              </button>
            </li>
          ))}
        </ul>

        <form className="row" style={{ marginTop: "1.25rem" }} onSubmit={onAddMember}>
          <input
            aria-label="New member id"
            placeholder="id"
            value={newId}
            onChange={(e) => setNewId(e.target.value)}
            required
          />
          <input
            aria-label="Raft address"
            placeholder="host:port"
            value={newAddr}
            onChange={(e) => setNewAddr(e.target.value)}
            required
          />
          <button className="btn" type="submit" disabled={busy}>
            Add member
          </button>
          <button
            className="btn secondary"
            type="button"
            disabled={busy}
            onClick={() => void refreshCluster()}
          >
            Refresh
          </button>
        </form>
        {clusterMsg ? (
          <div className={`result${clusterErr ? " error" : ""}`}>{clusterMsg}</div>
        ) : null}
      </section>
    </div>
  );
}
