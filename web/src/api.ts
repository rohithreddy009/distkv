export type NodeStatus = {
  addr: string;
  node_id?: number;
  is_leader: boolean;
  leader_id?: number;
  term?: number;
  commit_index?: number;
  applied_index?: number;
  error?: string;
};

export type Member = {
  id: number;
  raft_addr: string;
  voting: boolean;
};

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: {
      Accept: "application/json",
      ...(init?.body ? { "Content-Type": "application/json" } : {}),
      ...init?.headers,
    },
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error((data as { error?: string }).error || res.statusText);
  }
  return data as T;
}

export const api = {
  health: () => req<{ status: string }>("/api/health"),
  get: (key: string) =>
    req<{ key: string; found: boolean; value: string }>(
      `/api/kv/${encodeURIComponent(key)}`
    ),
  put: (key: string, value: string) =>
    req<{ status: string }>(`/api/kv/${encodeURIComponent(key)}`, {
      method: "PUT",
      body: JSON.stringify({ value }),
    }),
  del: (key: string) =>
    req<{ status: string }>(`/api/kv/${encodeURIComponent(key)}`, {
      method: "DELETE",
    }),
  status: () => req<{ nodes: NodeStatus[] }>("/api/status"),
  members: () =>
    req<{ members: Member[]; in_joint: boolean }>("/api/members"),
  addMember: (id: number, raft_addr: string) =>
    req<{ status: string }>("/api/members", {
      method: "POST",
      body: JSON.stringify({ id, raft_addr }),
    }),
  removeMember: (id: number) =>
    req<{ status: string }>(`/api/members/${id}`, { method: "DELETE" }),
};
